# Databricks notebook source
# notebooks/05_workspace_config.py

# COMMAND ----------
# MAGIC %md # 05 — Backup Workspace Config
# MAGIC
# MAGIC Ce notebook sauvegarde :
# MAGIC
# MAGIC | Section | Contenu | Chemin ADLS |
# MAGIC |---------|---------|-------------|
# MAGIC | 5.1 | ACLs notebooks/dossiers workspace | `workspace_config/workspace_acls.json` |
# MAGIC | 5.2 | ACLs repos Git | `workspace_config/repos_acls.json` |
# MAGIC | 5.3 | Sources notebooks (Python/SQL/Scala) | `notebooks/{chemin_workspace}/` |
# MAGIC | 5.4 | Définitions jobs (JSON complet) | `jobs/jobs_all.json` + `jobs/{id}_{name}.json` |

# COMMAND ----------
import base64
import json
import re
import requests
from datetime import date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def _uc_put(path: str, content: str) -> None:
    tmp = path + ".__tmp__"
    try: dbutils.fs.rm(tmp, recurse=True)
    except: pass
    spark.createDataFrame([(line,) for line in content.split("\n")], "value STRING") \
        .coalesce(1).write.mode("overwrite").text(tmp)
    parts = [f.path for f in dbutils.fs.ls(tmp)
             if not f.name.startswith("_") and not f.name.startswith(".")]
    try: dbutils.fs.rm(path)
    except: pass
    dbutils.fs.mv(parts[0], path)
    dbutils.fs.rm(tmp, recurse=True)

# COMMAND ----------
dbutils.widgets.text("backup_root",        "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",        str(date.today()), "Date backup YYYY-MM-DD")
dbutils.widgets.text("workspace_paths",    "/Shared", "Chemins workspace à exporter (séparés par virgule)")
dbutils.widgets.text("export_notebooks",   "true",  "Exporter les sources notebooks (true/false)")
dbutils.widgets.text("export_jobs",        "true",  "Exporter les définitions jobs (true/false)")

backup_root      = dbutils.widgets.get("backup_root")
backup_date      = dbutils.widgets.get("backup_date")
workspace_paths  = [p.strip() for p in dbutils.widgets.get("workspace_paths").split(",")]
export_notebooks = dbutils.widgets.get("export_notebooks").lower() == "true"
export_jobs      = dbutils.widgets.get("export_jobs").lower()      == "true"

token   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
headers = {"Authorization": f"Bearer {token}"}

output_base = f"{backup_root}/{backup_date}"
print(f"[OK] Workspace paths : {workspace_paths}")
print(f"[OK] export_notebooks={export_notebooks} | export_jobs={export_jobs}")

# COMMAND ----------
# MAGIC %md ## 5.1 — ACLs Workspace (notebooks, dossiers)

# COMMAND ----------
def get_acl(object_type, object_id):
    r = requests.get(
        f"{host}/api/2.0/permissions/{object_type}/{object_id}",
        headers=headers, timeout=15
    )
    if r.ok:
        return r.json().get("access_control_list", [])
    return []

def list_workspace_recursive(path, max_depth=4, depth=0):
    if depth > max_depth:
        return []
    r = requests.get(
        f"{host}/api/2.0/workspace/list",
        headers=headers, params={"path": path}, timeout=15
    )
    if not r.ok:
        return []
    objects = r.json().get("objects", [])
    result = []
    for obj in objects:
        result.append(obj)
        if obj.get("object_type") == "DIRECTORY":
            result.extend(list_workspace_recursive(obj["path"], max_depth, depth + 1))
    return result

type_map = {"NOTEBOOK": "notebooks", "DIRECTORY": "directories", "REPO": "repos", "FILE": "files"}

all_objects = []
for ws_path in workspace_paths:
    objs = list_workspace_recursive(ws_path)
    all_objects.extend(objs)
    print(f"  {len(objs)} objets sous {ws_path}")

acls_backup = []
for obj in all_objects:
    obj_type  = obj.get("object_type")
    obj_id    = obj.get("object_id")
    perm_type = type_map.get(obj_type)
    if not perm_type or not obj_id:
        continue
    acl = get_acl(perm_type, obj_id)
    explicit_acl = [a for a in acl if a.get("all_permissions") and
                    any(not p.get("inherited") for p in a.get("all_permissions", []))]
    if explicit_acl:
        acls_backup.append({
            "path": obj.get("path"), "object_type": obj_type,
            "object_id": obj_id, "acl": explicit_acl,
        })

_uc_put(f"{output_base}/workspace_config/workspace_acls.json",
        json.dumps(acls_backup, indent=2))
print(f"[OK] {len(acls_backup)} ACLs workspace sauvegardées")

# COMMAND ----------
# MAGIC %md ## 5.2 — ACLs Repos Git

# COMMAND ----------
resp  = requests.get(f"{host}/api/2.0/repos", headers=headers, timeout=30)
repos = resp.json().get("repos", []) if resp.ok else []

repos_acls = []
for repo in repos:
    repo_id = repo.get("id")
    acl     = get_acl("repos", repo_id)
    explicit_acl = [a for a in acl if a.get("all_permissions") and
                    any(not p.get("inherited") for p in a.get("all_permissions", []))]
    repos_acls.append({
        "repo_id": repo_id,
        "path":    repo.get("path", ""),
        "url":     repo.get("url", ""),
        "branch":  repo.get("branch", ""),
        "acl":     explicit_acl,
    })

_uc_put(f"{output_base}/workspace_config/repos_acls.json",
        json.dumps(repos_acls, indent=2))
print(f"[OK] {len(repos_acls)} repos sauvegardés")

# COMMAND ----------
# MAGIC %md ## 5.3 — Export sources notebooks

# COMMAND ----------
nb_ok    = 0
nb_error = 0

if export_notebooks:
    notebooks = [o for o in all_objects if o.get("object_type") == "NOTEBOOK"]
    print(f"[INFO] {len(notebooks)} notebooks à exporter")

    # Extension par langage
    lang_ext = {"PYTHON": ".py", "SQL": ".sql", "SCALA": ".scala", "R": ".r"}

    for nb in notebooks:
        nb_path = nb.get("path", "")
        nb_lang = nb.get("language", "PYTHON")
        ext     = lang_ext.get(nb_lang, ".py")

        try:
            r = requests.get(
                f"{host}/api/2.0/workspace/export",
                headers=headers,
                params={"path": nb_path, "format": "SOURCE", "direct_download": False},
                timeout=30
            )
            if not r.ok:
                print(f"  [WARN] Export échoué {nb_path}: {r.status_code}")
                nb_error += 1
                continue

            content_b64 = r.json().get("content", "")
            content     = base64.b64decode(content_b64).decode("utf-8")

            # Chemin ADLS : notebooks/Shared/dossier/notebook.py
            relative    = nb_path.lstrip("/")
            dest_path   = f"{output_base}/notebooks/{relative}{ext}"
            _uc_put(dest_path, content)
            nb_ok += 1

        except Exception as e:
            print(f"  [WARN] {nb_path}: {e}")
            nb_error += 1

    print(f"[OK] Notebooks exportés : {nb_ok} succès, {nb_error} erreurs")
else:
    print("[INFO] Export notebooks désactivé (export_notebooks=false)")

# COMMAND ----------
# MAGIC %md ## 5.4 — Export définitions jobs

# COMMAND ----------
jobs_ok    = 0
jobs_error = 0
all_jobs   = []

if export_jobs:
    # Pagination API 2.1
    params = {"expand_tasks": True, "limit": 100}
    while True:
        r = requests.get(f"{host}/api/2.1/jobs/list",
                         headers=headers, params=params, timeout=30)
        if not r.ok:
            print(f"[WARN] Jobs API échoué : {r.status_code} — {r.text[:200]}")
            break
        data = r.json()
        all_jobs.extend(data.get("jobs", []))
        if not data.get("has_more"):
            break
        params["page_token"] = data.get("next_page_token", "")

    if all_jobs:
        # Fichier consolidé
        _uc_put(f"{output_base}/jobs/jobs_all.json",
                json.dumps(all_jobs, indent=2))

        # Fichier individuel par job
        for job in all_jobs:
            job_id   = job.get("job_id", "unknown")
            job_name = re.sub(r"[^\w\-]", "_", job.get("settings", {}).get("name", str(job_id)))
            try:
                _uc_put(f"{output_base}/jobs/{job_id}_{job_name}.json",
                        json.dumps(job, indent=2))
                jobs_ok += 1
            except Exception as e:
                print(f"  [WARN] Job {job_id}: {e}")
                jobs_error += 1

        print(f"[OK] {jobs_ok} jobs exportés ({jobs_error} erreurs)")
    else:
        print("[WARN] Aucun job trouvé ou API inaccessible")
else:
    print("[INFO] Export jobs désactivé (export_jobs=false)")

# COMMAND ----------
# MAGIC %md ## 5.5 — Résumé

# COMMAND ----------
summary = {
    "workspace_acls_count": len(acls_backup),
    "repos_count":          len(repos_acls),
    "notebooks_ok":         nb_ok,
    "notebooks_error":      nb_error,
    "jobs_ok":              jobs_ok,
    "jobs_error":           jobs_error,
}

print(f"""
╔══════════════════════════════════════════╗
║     WORKSPACE CONFIG BACKUP — RÉSUMÉ    ║
╠══════════════════════════════════════════╣
║  ACLs workspace   : {len(acls_backup):<21} ║
║  Repos            : {len(repos_acls):<21} ║
║  Notebooks exportés: {nb_ok:<20} ║
║  Notebooks erreurs : {nb_error:<20} ║
║  Jobs exportés    : {jobs_ok:<21} ║
║  Jobs erreurs     : {jobs_error:<21} ║
╚══════════════════════════════════════════╝
""")

dbutils.notebook.exit(json.dumps(summary))
