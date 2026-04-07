# Databricks notebook source
# notebooks/05_workspace_config.py

# COMMAND ----------
# MAGIC %md # 05 — Backup Workspace Config (ACLs + Cluster Policies)
# MAGIC
# MAGIC Ce notebook sauvegarde :
# MAGIC - **ACLs workspace** : permissions sur notebooks, dossiers, repos
# MAGIC - **Cluster policies** : définitions des policies
# MAGIC - **Configurations clusters** : paramètres des clusters existants

# COMMAND ----------
import json
import requests
from datetime import date

# COMMAND ----------
dbutils.widgets.text("backup_root", "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup YYYY-MM-DD")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")

# Auth via token de la session courante
token   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
headers = {"Authorization": f"Bearer {token}"}

output_path = f"{backup_root}/{backup_date}/workspace_config"
print(f"[OK] Backup workspace config → {output_path}")

# COMMAND ----------
# MAGIC %md ## 5.1 — Backup Cluster Policies

# COMMAND ----------
resp = requests.get(f"{host}/api/2.0/policies/clusters/list", headers=headers, timeout=30)
resp.raise_for_status()
policies = resp.json().get("policies", [])

dbutils.fs.put(
    f"{output_path}/cluster_policies.json",
    json.dumps(policies, indent=2),
    overwrite=True
)
print(f"[OK] {len(policies)} cluster policies sauvegardées")
for p in policies:
    print(f"  • {p.get('name')} (id={p.get('policy_id')})")

# COMMAND ----------
# MAGIC %md ## 5.2 — Backup Configurations Clusters

# COMMAND ----------
resp = requests.get(f"{host}/api/2.0/clusters/list", headers=headers, timeout=30)
resp.raise_for_status()
clusters = resp.json().get("clusters", [])

# Garder uniquement les champs utiles pour la restauration
cluster_configs = []
for c in clusters:
    cluster_configs.append({
        "cluster_id":             c.get("cluster_id"),
        "cluster_name":           c.get("cluster_name"),
        "spark_version":          c.get("spark_version"),
        "node_type_id":           c.get("node_type_id"),
        "driver_node_type_id":    c.get("driver_node_type_id"),
        "autoscale":              c.get("autoscale"),
        "num_workers":            c.get("num_workers"),
        "autotermination_minutes":c.get("autotermination_minutes"),
        "spark_conf":             c.get("spark_conf", {}),
        "spark_env_vars":         c.get("spark_env_vars", {}),
        "cluster_source":         c.get("cluster_source"),
        "policy_id":              c.get("policy_id"),
        "data_security_mode":     c.get("data_security_mode"),
        "runtime_engine":         c.get("runtime_engine"),
        "state":                  c.get("state"),
    })

dbutils.fs.put(
    f"{output_path}/clusters.json",
    json.dumps(cluster_configs, indent=2),
    overwrite=True
)
print(f"[OK] {len(cluster_configs)} clusters sauvegardés")
for c in cluster_configs:
    print(f"  • {c.get('cluster_name')} [{c.get('state')}] (id={c.get('cluster_id')})")

# COMMAND ----------
# MAGIC %md ## 5.3 — Backup ACLs Workspace (notebooks, dossiers)

# COMMAND ----------
def get_object_id(path):
    """Résoudre le chemin workspace → object_id numérique."""
    r = requests.get(
        f"{host}/api/2.0/workspace/get-status",
        headers=headers,
        params={"path": path},
        timeout=15
    )
    if r.ok:
        return r.json().get("object_id")
    return None

def get_acl(object_type, object_id):
    """Récupérer les ACLs d'un objet workspace."""
    r = requests.get(
        f"{host}/api/2.0/permissions/{object_type}/{object_id}",
        headers=headers,
        timeout=15
    )
    if r.ok:
        return r.json().get("access_control_list", [])
    return []

def list_workspace_recursive(path, max_depth=3, depth=0):
    """Lister récursivement les objets workspace."""
    if depth > max_depth:
        return []
    r = requests.get(
        f"{host}/api/2.0/workspace/list",
        headers=headers,
        params={"path": path},
        timeout=15
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

# COMMAND ----------
# Lister tous les objets workspace depuis /Shared
print("Listage des objets workspace...")
all_objects = list_workspace_recursive("/Shared")
print(f"  {len(all_objects)} objets trouvés sous /Shared")

# Mapper object_type → type d'API permissions
type_map = {
    "NOTEBOOK":  "notebooks",
    "DIRECTORY": "directories",
    "REPO":      "repos",
    "FILE":      "files",
}

acls_backup = []
skipped = 0

for obj in all_objects:
    obj_type   = obj.get("object_type")
    obj_path   = obj.get("path")
    obj_id     = obj.get("object_id")
    perm_type  = type_map.get(obj_type)

    if not perm_type or not obj_id:
        skipped += 1
        continue

    acl = get_acl(perm_type, obj_id)
    # Garder uniquement les ACLs non-héritées (celles explicitement définies)
    explicit_acl = [a for a in acl if a.get("all_permissions") and
                    any(not p.get("inherited") for p in a.get("all_permissions", []))]

    if explicit_acl:
        acls_backup.append({
            "path":        obj_path,
            "object_type": obj_type,
            "object_id":   obj_id,
            "acl":         explicit_acl,
        })

print(f"[OK] {len(acls_backup)} objets avec ACLs explicites trouvés ({skipped} ignorés)")

dbutils.fs.put(
    f"{output_path}/workspace_acls.json",
    json.dumps(acls_backup, indent=2),
    overwrite=True
)
print(f"[OK] ACLs sauvegardées → {output_path}/workspace_acls.json")

# COMMAND ----------
# MAGIC %md ## 5.4 — Backup ACLs Repos

# COMMAND ----------
resp = requests.get(f"{host}/api/2.0/repos", headers=headers, timeout=30)
repos = resp.json().get("repos", []) if resp.ok else []

repos_acls = []
for repo in repos:
    repo_id  = repo.get("id")
    repo_url = repo.get("url", "")
    repo_path= repo.get("path", "")
    acl = get_acl("repos", repo_id)
    explicit_acl = [a for a in acl if a.get("all_permissions") and
                    any(not p.get("inherited") for p in a.get("all_permissions", []))]
    repos_acls.append({
        "repo_id":   repo_id,
        "path":      repo_path,
        "url":       repo_url,
        "acl":       explicit_acl,
    })

dbutils.fs.put(
    f"{output_path}/repos_acls.json",
    json.dumps(repos_acls, indent=2),
    overwrite=True
)
print(f"[OK] {len(repos_acls)} repos sauvegardés avec leurs ACLs")

# COMMAND ----------
# MAGIC %md ## 5.5 — Résumé

# COMMAND ----------
summary = {
    "cluster_policies_count": len(policies),
    "clusters_count":         len(cluster_configs),
    "workspace_acls_count":   len(acls_backup),
    "repos_count":            len(repos_acls),
}

print(f"""
╔══════════════════════════════════════════╗
║     WORKSPACE CONFIG BACKUP — RÉSUMÉ    ║
╠══════════════════════════════════════════╣
║  Cluster policies : {len(policies):<21} ║
║  Clusters         : {len(cluster_configs):<21} ║
║  ACLs workspace   : {len(acls_backup):<21} ║
║  Repos            : {len(repos_acls):<21} ║
╚══════════════════════════════════════════╝
""")

dbutils.notebook.exit(json.dumps(summary))
