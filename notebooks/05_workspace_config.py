# Databricks notebook source
# notebooks/05_workspace_config.py

# COMMAND ----------
# MAGIC %md # 05 — Backup Workspace Config
# MAGIC
# MAGIC Ce notebook sauvegarde :
# MAGIC
# MAGIC | Section | Contenu | Chemin ADLS |
# MAGIC |---------|---------|-------------|
# MAGIC | 5.1 | ACLs notebooks/dossiers/fichiers workspace | `workspace_config/workspace_acls.json` |
# MAGIC | 5.2 | ACLs repos Git | `workspace_config/repos_acls.json` |
# MAGIC | 5.3 | Notebooks, fichiers et dashboards du workspace (contenu brut) | `workspace/objects/` (parquet) + `workspace/manifest.json` |
# MAGIC | 5.4 | Définitions jobs (JSON complet) + permissions | `jobs/jobs_all.json`, `jobs/{id}_{name}.json`, `jobs/jobs_permissions.json` |
# MAGIC | 5.5 | Pipelines (définition + permissions) | `pipelines/pipelines_all.json` |
# MAGIC
# MAGIC Le contenu est exporté en format AUTO (un notebook garde son format de stockage, source ou .ipynb ; un fichier .sh reste
# MAGIC un fichier) et écrit par lots dans un seul jeu de données : un job Spark par lot, et non plus par
# MAGIC fichier, ce qui rend l'export de tout le workspace (milliers d'objets) praticable.

# COMMAND ----------
import base64
import concurrent.futures
import json
import re
import requests
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import BinaryType, LongType, StringType, StructField, StructType

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

def _short(e) -> str:
    return " ".join(str(e).split("JVM stacktrace:")[0].split())[:300]

# COMMAND ----------
dbutils.widgets.text("backup_root",        "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",        str(date.today()), "Date backup YYYY-MM-DD")
dbutils.widgets.text("workspace_paths",    "/", "Chemins workspace à exporter (séparés par virgule)")
dbutils.widgets.text("exclude_paths",      "/Repos", "Chemins exclus (séparés par virgule) — /Repos : contenu dans Git")
dbutils.widgets.text("export_notebooks",   "true",  "Exporter notebooks / fichiers / dashboards (true/false)")
dbutils.widgets.text("export_jobs",        "true",  "Exporter les définitions et permissions des jobs (true/false)")
dbutils.widgets.text("export_pipelines",   "true",  "Exporter les définitions et permissions des pipelines (true/false)")
dbutils.widgets.text("max_parallel",       "4",     "Appels API simultanés (au-delà, l'API limite le débit : HTTP 429)")
# lib/ est déployé par le bundle à côté de notebooks/ : …/files/notebooks/x → …/files/lib
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_path = _nb_path if _nb_path.startswith("/Workspace") else "/Workspace" + _nb_path
dbutils.widgets.text("lib_path", _nb_path.rsplit("/notebooks/", 1)[0] + "/lib", "Chemin vers lib/")

backup_root      = dbutils.widgets.get("backup_root")
backup_date      = dbutils.widgets.get("backup_date")
workspace_paths  = [p.strip() for p in dbutils.widgets.get("workspace_paths").split(",") if p.strip()]
exclude_paths    = [p.strip().rstrip("/") for p in dbutils.widgets.get("exclude_paths").split(",") if p.strip()]
export_notebooks = dbutils.widgets.get("export_notebooks").lower() == "true"
export_jobs      = dbutils.widgets.get("export_jobs").lower()      == "true"
export_pipelines = dbutils.widgets.get("export_pipelines").lower() == "true"
max_parallel     = max(1, int(dbutils.widgets.get("max_parallel")))

token   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
headers = {"Authorization": f"Bearer {token}"}

import sys
sys.path.insert(0, dbutils.widgets.get("lib_path"))
from workspace_export import call_with_retry, list_all_jobs


def _get(url, **kw):
    """GET rejoué sur limitation de débit (429) : sans réessai, des objets manquaient en silence."""
    return call_with_retry(lambda: requests.get(url, headers=headers, **kw))

output_base = f"{backup_root}/{backup_date}"
print(f"[OK] Workspace paths : {workspace_paths} | exclus : {exclude_paths}")
print(f"[OK] export_notebooks={export_notebooks} | export_jobs={export_jobs} | export_pipelines={export_pipelines}")

# COMMAND ----------
# MAGIC %md ## 5.1 — Inventaire et ACLs Workspace

# COMMAND ----------
def get_acl(object_type, object_id):
    r = _get(f"{host}/api/2.0/permissions/{object_type}/{object_id}", timeout=15)
    if r.ok:
        return r.json().get("access_control_list", [])
    return []

def _excluded(path: str) -> bool:
    return any(path == x or path.startswith(x + "/") for x in exclude_paths)

unreadable_dirs = []

def list_workspace_recursive(root: str) -> list:
    """Parcours complet, sans limite de profondeur (l'ancienne limite à 4 niveaux omettait en
    silence les objets plus profonds). Les dossiers illisibles sont comptés et signalés au lieu
    d'être ignorés sans trace. Les repos Git ne sont pas parcourus : leur contenu est dans Git."""
    result, stack = [], [root]
    while stack:
        path = stack.pop()
        if _excluded(path):
            continue
        r = _get(f"{host}/api/2.0/workspace/list", params={"path": path}, timeout=30)
        if not r.ok:
            unreadable_dirs.append(f"{path} (HTTP {r.status_code})")
            continue
        for obj in r.json().get("objects", []):
            if _excluded(obj.get("path", "")):
                continue
            result.append(obj)
            if obj.get("object_type") == "DIRECTORY":
                stack.append(obj["path"])
    return result

type_map = {"NOTEBOOK": "notebooks", "DIRECTORY": "directories", "REPO": "repos", "FILE": "files"}

all_objects = []
for ws_path in workspace_paths:
    objs = list_workspace_recursive(ws_path)
    all_objects.extend(objs)
    print(f"  {len(objs)} objets sous {ws_path}")
if unreadable_dirs:
    print(f"[WARN] {len(unreadable_dirs)} dossier(s) illisible(s) pour l'identité du job (non sauvegardés) :")
    for d in unreadable_dirs[:50]:
        print(f"  - {d}")

def _explicit_acl_entry(obj):
    perm_type = type_map.get(obj.get("object_type"))
    if not perm_type or not obj.get("object_id"):
        return None
    acl = get_acl(perm_type, obj["object_id"])
    explicit_acl = [a for a in acl if a.get("all_permissions") and
                    any(not p.get("inherited") for p in a.get("all_permissions", []))]
    if explicit_acl:
        return {"path": obj.get("path"), "object_type": obj.get("object_type"),
                "object_id": obj.get("object_id"), "acl": explicit_acl}
    return None

with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
    acls_backup = [a for a in ex.map(_explicit_acl_entry, all_objects) if a]

_uc_put(f"{output_base}/workspace_config/workspace_acls.json", json.dumps(acls_backup, indent=2))
print(f"[OK] {len(acls_backup)} ACLs workspace sauvegardées")

# COMMAND ----------
# MAGIC %md ## 5.2 — ACLs Repos Git

# COMMAND ----------
resp  = _get(f"{host}/api/2.0/repos", timeout=30)
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
# MAGIC %md ## 5.3 — Export des notebooks, fichiers et dashboards

# COMMAND ----------
nb_ok    = 0
nb_error = 0
files_ok = 0
EXPORT_TYPES = {"NOTEBOOK", "FILE", "DASHBOARD"}
BATCH_SIZE   = 500   # objets par écriture : borne la mémoire du driver
objects_path = f"{output_base}/workspace/objects"
OBJECT_SCHEMA = StructType([
    StructField("path", StringType()), StructField("object_type", StringType()),
    StructField("language", StringType()), StructField("file_type", StringType()),
    StructField("size", LongType()), StructField("content", BinaryType()),
])

def export_object(obj: dict):
    """(ligne à écrire, erreur) — export AUTO : contenu natif (source, .ipynb, fichier brut)."""
    path = obj.get("path", "")
    try:
        r = _get(f"{host}/api/2.0/workspace/export", params={"path": path, "format": "AUTO"}, timeout=60)
        if not r.ok:
            return None, f"{path}: HTTP {r.status_code}"
        d = r.json()
        content = base64.b64decode(d.get("content", ""))
        return (path, obj.get("object_type"), obj.get("language"), d.get("file_type") or "",
                len(content), bytearray(content)), None
    except Exception as e:
        return None, f"{path}: {_short(e)}"

manifest_entries = []
if export_notebooks:
    to_export = [o for o in all_objects if o.get("object_type") in EXPORT_TYPES]
    print(f"[INFO] {len(to_export)} objet(s) à exporter")
    try: dbutils.fs.rm(objects_path, recurse=True)   # reprise : réécriture complète du jour
    except Exception: pass
    for start in range(0, len(to_export), BATCH_SIZE):
        batch = to_export[start:start + BATCH_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
            results = list(ex.map(export_object, batch))
        rows = [row for row, err in results if row]
        for _, err in results:
            if err:
                nb_error += 1
                print(f"  [WARN] Export échoué {err}")
        if rows:
            spark.createDataFrame(rows, OBJECT_SCHEMA).coalesce(1).write.mode("append").parquet(objects_path)
        for row in rows:
            manifest_entries.append({"path": row[0], "object_type": row[1], "language": row[2],
                                     "file_type": row[3], "size": row[4]})
            if row[1] == "NOTEBOOK":
                nb_ok += 1
            else:
                files_ok += 1
        print(f"  … {min(start + BATCH_SIZE, len(to_export))}/{len(to_export)}")

    _uc_put(f"{output_base}/workspace/manifest.json", json.dumps({
        "workspace_paths": workspace_paths, "exclude_paths": exclude_paths,
        "unreadable_dirs": unreadable_dirs, "objects": manifest_entries,
    }))
    print(f"[OK] Exportés : {nb_ok} notebook(s), {files_ok} fichier(s)/dashboard(s), {nb_error} erreur(s)")
else:
    print("[INFO] Export notebooks désactivé (export_notebooks=false)")

# COMMAND ----------
# MAGIC %md ## 5.4 — Export définitions et permissions des jobs

# COMMAND ----------
jobs_ok    = 0
jobs_error = 0
all_jobs   = []

if export_jobs:
    def _api_get(path, params):
        r = _get(f"{host}{path}", params=params, timeout=30)
        r.raise_for_status()
        return r.json()

    # Définitions complètes, tâches comprises (lib/workspace_export.list_all_jobs)
    all_jobs = list_all_jobs(_api_get)

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
                print(f"  [WARN] Job {job_id}: {_short(e)}")
                jobs_error += 1

        # Permissions des jobs (restaurées par 09_restore_jobs)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
            job_acls = dict(zip([str(j["job_id"]) for j in all_jobs],
                                ex.map(lambda j: get_acl("jobs", j["job_id"]), all_jobs)))
        _uc_put(f"{output_base}/jobs/jobs_permissions.json", json.dumps(job_acls))

        print(f"[OK] {jobs_ok} jobs exportés ({jobs_error} erreurs), permissions de {len(job_acls)} jobs")
    else:
        print("[WARN] Aucun job trouvé ou API inaccessible")
else:
    print("[INFO] Export jobs désactivé (export_jobs=false)")

# COMMAND ----------
# MAGIC %md ## 5.5 — Export des pipelines (définition + permissions)

# COMMAND ----------
pipelines_ok    = 0
pipelines_error = 0

if export_pipelines:
    statuses, params = [], {"max_results": 100}   # au-delà de 100 : HTTP 400
    while True:
        r = _get(f"{host}/api/2.0/pipelines", params=params, timeout=30)
        if not r.ok:
            print(f"[WARN] Pipelines API échoué : {r.status_code} — {r.text[:200]}")
            break
        d = r.json()
        statuses += d.get("statuses", []) or []
        if not d.get("next_page_token"):
            break
        params["page_token"] = d["next_page_token"]

    def export_pipeline(status: dict):
        pid = status["pipeline_id"]
        r = _get(f"{host}/api/2.0/pipelines/{pid}", timeout=30)
        if not r.ok:
            return None, f"{status.get('name', pid)}: HTTP {r.status_code}"
        p = r.json()
        return {"pipeline_id": pid, "name": p.get("name") or p.get("spec", {}).get("name"),
                "creator_user_name": p.get("creator_user_name"), "spec": p.get("spec", {}),
                "acl": get_acl("pipelines", pid)}, None

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
        results = list(ex.map(export_pipeline, statuses))
    pipelines = [p for p, err in results if p]
    for _, err in results:
        if err:
            pipelines_error += 1
            print(f"  [WARN] Pipeline {err}")
    pipelines_ok = len(pipelines)
    _uc_put(f"{output_base}/pipelines/pipelines_all.json", json.dumps(pipelines, indent=2))
    print(f"[OK] {pipelines_ok} pipeline(s) exporté(s) ({pipelines_error} erreurs)")
else:
    print("[INFO] Export pipelines désactivé (export_pipelines=false)")

# COMMAND ----------
# MAGIC %md ## 5.6 — Résumé

# COMMAND ----------
summary = {
    "workspace_acls_count": len(acls_backup),
    "repos_count":          len(repos_acls),
    "notebooks_ok":         nb_ok,
    "files_ok":             files_ok,
    "notebooks_error":      nb_error,
    "unreadable_dirs":      len(unreadable_dirs),
    "jobs_ok":              jobs_ok,
    "jobs_error":           jobs_error,
    "pipelines_ok":         pipelines_ok,
    "pipelines_error":      pipelines_error,
}

print(f"""
╔══════════════════════════════════════════╗
║     WORKSPACE CONFIG BACKUP — RÉSUMÉ    ║
╠══════════════════════════════════════════╣
║  ACLs workspace    : {len(acls_backup):<20} ║
║  Repos             : {len(repos_acls):<20} ║
║  Notebooks exportés: {nb_ok:<20} ║
║  Fichiers exportés : {files_ok:<20} ║
║  Erreurs d'export  : {nb_error:<20} ║
║  Dossiers illisibles: {len(unreadable_dirs):<19} ║
║  Jobs exportés     : {jobs_ok:<20} ║
║  Pipelines exportés: {pipelines_ok:<20} ║
╚══════════════════════════════════════════╝
""")

n_failed = nb_error + jobs_error + pipelines_error
if n_failed:
    raise RuntimeError(f"{n_failed} objet(s) non sauvegardé(s) malgré les réessais (détail ci-dessus) "
                       f"— backup du workspace incomplet : {json.dumps(summary)}")

dbutils.notebook.exit(json.dumps(summary))
