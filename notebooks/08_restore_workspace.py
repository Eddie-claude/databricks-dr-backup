# Databricks notebook source
# notebooks/08_restore_workspace.py

# COMMAND ----------
# MAGIC %md # 08 — Restauration Workspace (Notebooks + ACLs)
# MAGIC
# MAGIC Ce notebook restaure les éléments sauvegardés par `05_workspace_config` :
# MAGIC
# MAGIC | Section | Contenu restauré | Source backup |
# MAGIC |---------|-----------------|---------------|
# MAGIC | 8.1 | Sources notebooks (.py / .sql / .scala) | `{date}/notebooks/` |
# MAGIC | 8.2 | ACLs notebooks et dossiers workspace | `{date}/workspace_config/workspace_acls.json` |
# MAGIC | 8.3 | ACLs repos Git | `{date}/workspace_config/repos_acls.json` |
# MAGIC
# MAGIC **dry_run = true** : affiche les actions sans rien modifier — toujours commencer par là.

# COMMAND ----------
import base64
import json
import requests
from datetime import date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def _uc_head(path: str) -> str:
    """Lit un fichier texte depuis ADLS (UC-aware)."""
    return "\n".join(r.value for r in spark.read.text(path).collect())

def _uc_read_binary(path: str) -> str:
    """Lit un fichier source notebook depuis ADLS et retourne son contenu."""
    try:
        return "\n".join(r.value for r in spark.read.text(path).collect())
    except Exception as e:
        raise RuntimeError(f"Impossible de lire {path}: {e}")

token   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
headers = {"Authorization": f"Bearer {token}"}

# COMMAND ----------
# MAGIC %md ## Paramètres

# COMMAND ----------
dbutils.widgets.text(     "backup_root",      "", "Backup root (abfss://...)")
dbutils.widgets.text(     "backup_date",       str(date.today()), "Date du backup à restaurer (YYYY-MM-DD)")
dbutils.widgets.dropdown( "restore_type",      "notebooks", ["notebooks", "acls", "both"], "Type de restauration")
dbutils.widgets.text(     "notebook_filter",   "", "Filtre chemin notebook (ex: /Shared/mon-dossier ou vide = tous)")
dbutils.widgets.text(     "target_workspace_path", "", "Dossier cible workspace (vide = chemin d'origine)")
dbutils.widgets.dropdown( "dry_run",           "true", ["true", "false"], "Dry-run (true = simulation)")

backup_root           = dbutils.widgets.get("backup_root").strip().rstrip("/")
backup_date           = dbutils.widgets.get("backup_date").strip()
restore_type          = dbutils.widgets.get("restore_type")
notebook_filter       = dbutils.widgets.get("notebook_filter").strip()
target_workspace_path = dbutils.widgets.get("target_workspace_path").strip().rstrip("/")
dry_run               = dbutils.widgets.get("dry_run").lower() == "true"

notebooks_backup_root = f"{backup_root}/{backup_date}/notebooks"
acls_backup_path      = f"{backup_root}/{backup_date}/workspace_config/workspace_acls.json"
repos_acls_path       = f"{backup_root}/{backup_date}/workspace_config/repos_acls.json"

prefix = "[DRY-RUN] " if dry_run else ""
print(f"[OK] backup_root    = {backup_root}")
print(f"[OK] backup_date    = {backup_date}")
print(f"[OK] restore_type   = {restore_type}")
print(f"[OK] notebook_filter= {notebook_filter or '(tous)'}")
target_path_label = target_workspace_path or "(chemin d'origine)"
print(f"[OK] target_path    = {target_path_label}")
print(f"[OK] dry_run        = {dry_run}")

# COMMAND ----------
# MAGIC %md ## Étape 1 — Dates de backup disponibles

# COMMAND ----------
import re
DATE_PAT = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def list_backup_dates(root):
    try:
        return sorted([
            f.name.rstrip("/") for f in dbutils.fs.ls(root)
            if DATE_PAT.match(f.name.rstrip("/"))
        ], reverse=True)
    except Exception as e:
        print(f"  [WARN] list_backup_dates échoué: {e}")
        return []

available_dates = list_backup_dates(backup_root)
print(f"── Dates de backup disponibles ({len(available_dates)}) ──────────────")
for d in available_dates[:10]:
    marker = " ← sélectionné" if d == backup_date else ""
    print(f"  {d}{marker}")
if len(available_dates) > 10:
    print(f"  ... et {len(available_dates) - 10} autres")

# COMMAND ----------
# MAGIC %md ## 8.1 — Restauration des sources notebooks

# COMMAND ----------
nb_ok    = 0
nb_skip  = 0
nb_error = 0

lang_map = {".py": "PYTHON", ".sql": "SQL", ".scala": "SCALA", ".r": "R"}

def list_notebook_files(adls_root):
    """Liste récursivement tous les fichiers notebook dans le backup."""
    files = []
    def recurse(path):
        try:
            items = list(dbutils.fs.ls(path))
        except Exception as e:
            print(f"  [WARN] ls échoué sur {path}: {e}")
            return
        for f in items:
            if f.name.endswith("/"):
                recurse(f.path.rstrip("/"))
            elif any(f.name.endswith(ext) for ext in lang_map):
                files.append(f.path)
    recurse(adls_root)
    return files

def adls_path_to_workspace_path(adls_path, adls_root):
    """Convertit un chemin ADLS en chemin workspace."""
    rel = adls_path.replace(adls_root, "").lstrip("/")
    # Retirer l'extension
    for ext in lang_map:
        if rel.endswith(ext):
            rel = rel[:-len(ext)]
            break
    return "/" + rel

if restore_type in ("notebooks", "both"):
    print(f"\n{'─'*60}")
    print(f"{prefix}RESTAURATION NOTEBOOKS")
    print(f"{'─'*60}")

    try:
        all_nb_files = list_notebook_files(notebooks_backup_root)
    except Exception as e:
        print(f"[ERROR] Impossible de lister les notebooks : {e}")
        all_nb_files = []

    # Filtrer
    if notebook_filter:
        filter_norm = notebook_filter.rstrip("/")
        all_nb_files = [f for f in all_nb_files
                        if adls_path_to_workspace_path(f, notebooks_backup_root).startswith(filter_norm)]

    print(f"[INFO] {len(all_nb_files)} notebook(s) trouvé(s) dans le backup")

    for adls_path in all_nb_files:
        # Déterminer extension et langage
        ext = next((e for e in lang_map if adls_path.endswith(e)), ".py")
        lang = lang_map[ext]

        # Chemin workspace
        ws_path_orig = adls_path_to_workspace_path(adls_path, notebooks_backup_root)
        if target_workspace_path:
            # Remplace la racine par le dossier cible
            parts = ws_path_orig.strip("/").split("/", 1)
            ws_path = f"{target_workspace_path}/{parts[1]}" if len(parts) > 1 else f"{target_workspace_path}/{parts[0]}"
        else:
            ws_path = ws_path_orig

        print(f"\n  {'──' if dry_run else '▶ '} {ws_path}")

        if not dry_run:
            try:
                content = _uc_read_binary(adls_path)
                content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")

                # Créer le dossier parent si nécessaire
                parent = "/".join(ws_path.split("/")[:-1])
                requests.post(
                    f"{host}/api/2.0/workspace/mkdirs",
                    headers=headers,
                    json={"path": parent},
                    timeout=10
                )

                # Importer le notebook
                r = requests.post(
                    f"{host}/api/2.0/workspace/import",
                    headers=headers,
                    json={
                        "path": ws_path,
                        "format": "SOURCE",
                        "language": lang,
                        "content": content_b64,
                        "overwrite": True,
                    },
                    timeout=30
                )
                if r.ok:
                    print(f"     [OK] Importé")
                    nb_ok += 1
                else:
                    print(f"     [WARN] {r.status_code} — {r.text[:100]}")
                    nb_error += 1
            except Exception as e:
                print(f"     [ERROR] {e}")
                nb_error += 1
        else:
            print(f"     Source  : {adls_path}")
            print(f"     Dest    : {ws_path}  [{lang}]")
            nb_ok += 1

    print(f"\n[OK] Notebooks : {nb_ok} {'simulés' if dry_run else 'restaurés'}, {nb_error} erreurs")

# COMMAND ----------
# MAGIC %md ## 8.2 — Restauration ACLs Workspace (notebooks, dossiers)

# COMMAND ----------
acl_nb_ok    = 0
acl_nb_skip  = 0
acl_nb_error = 0

type_map_inv = {"NOTEBOOK": "notebooks", "DIRECTORY": "directories", "REPO": "repos", "FILE": "files"}

if restore_type in ("acls", "both"):
    print(f"\n{'─'*60}")
    print(f"{prefix}RESTAURATION ACLs WORKSPACE")
    print(f"{'─'*60}")

    try:
        acls_data = json.loads(_uc_head(acls_backup_path))
        print(f"[INFO] {len(acls_data)} entrées ACL trouvées dans le backup")
    except Exception as e:
        print(f"[ERROR] Impossible de lire workspace_acls.json : {e}")
        acls_data = []

    for entry in acls_data:
        ws_path   = entry.get("path", "")
        obj_type  = entry.get("object_type", "")
        acl       = entry.get("acl", [])
        perm_type = type_map_inv.get(obj_type)

        # Appliquer le filtre notebook si renseigné
        if notebook_filter and not ws_path.startswith(notebook_filter):
            acl_nb_skip += 1
            continue

        if not perm_type or not ws_path or not acl:
            acl_nb_skip += 1
            continue

        print(f"\n  {'──' if dry_run else '▶ '} {ws_path}  [{obj_type}]")

        if not dry_run:
            # Résoudre l'object_id courant depuis le chemin workspace
            # (l'ID stocké dans le backup est périmé si l'objet a été recréé)
            try:
                status_r = requests.get(
                    f"{host}/api/2.0/workspace/get-status",
                    headers=headers,
                    params={"path": ws_path},
                    timeout=10,
                )
                if status_r.status_code == 404:
                    print(f"     [SKIP] Objet introuvable dans le workspace : {ws_path}")
                    acl_nb_skip += 1
                    continue
                status_r.raise_for_status()
                current_id = status_r.json().get("object_id")
                if not current_id:
                    print(f"     [SKIP] object_id introuvable via get-status pour {ws_path}")
                    acl_nb_skip += 1
                    continue
            except Exception as e:
                print(f"     [SKIP] get-status échoué pour {ws_path} : {e}")
                acl_nb_skip += 1
                continue

            try:
                r = requests.put(
                    f"{host}/api/2.0/permissions/{perm_type}/{current_id}",
                    headers=headers,
                    json={"access_control_list": acl},
                    timeout=15
                )
                if r.ok:
                    print(f"     [OK] ACL restaurée (object_id={current_id})")
                    acl_nb_ok += 1
                else:
                    print(f"     [WARN] {r.status_code} — {r.text[:150]}")
                    acl_nb_error += 1
            except Exception as e:
                print(f"     [ERROR] {e}")
                acl_nb_error += 1
        else:
            for a in acl:
                user  = a.get("user_name") or a.get("group_name") or a.get("service_principal_name", "?")
                perms = [p["permission_level"] for p in a.get("all_permissions", []) if not p.get("inherited")]
                print(f"     {user} → {perms}")
            acl_nb_ok += 1

    print(f"\n[OK] ACLs workspace : {acl_nb_ok} {'simulées' if dry_run else 'restaurées'}, {acl_nb_error} erreurs, {acl_nb_skip} ignorées")

# COMMAND ----------
# MAGIC %md ## 8.3 — Restauration ACLs Repos Git

# COMMAND ----------
acl_repo_ok    = 0
acl_repo_error = 0

if restore_type in ("acls", "both"):
    print(f"\n{'─'*60}")
    print(f"{prefix}RESTAURATION ACLs REPOS")
    print(f"{'─'*60}")

    try:
        repos_data = json.loads(_uc_head(repos_acls_path))
        print(f"[INFO] {len(repos_data)} repos trouvés dans le backup")
    except Exception as e:
        print(f"[ERROR] Impossible de lire repos_acls.json : {e}")
        repos_data = []

    for repo in repos_data:
        repo_id  = repo.get("repo_id")
        path     = repo.get("path", "")
        url      = repo.get("url", "")
        acl      = repo.get("acl", [])

        if not acl:
            continue

        print(f"\n  {'──' if dry_run else '▶ '} {path}  ({url})")

        if not dry_run:
            try:
                r = requests.put(
                    f"{host}/api/2.0/permissions/repos/{repo_id}",
                    headers=headers,
                    json={"access_control_list": acl},
                    timeout=15
                )
                if r.ok:
                    print(f"     [OK] ACL repo restaurée")
                    acl_repo_ok += 1
                else:
                    print(f"     [WARN] {r.status_code} — {r.text[:100]}")
                    acl_repo_error += 1
            except Exception as e:
                print(f"     [ERROR] {e}")
                acl_repo_error += 1
        else:
            for a in acl:
                user  = a.get("user_name") or a.get("group_name") or a.get("service_principal_name", "?")
                perms = [p["permission_level"] for p in a.get("all_permissions", []) if not p.get("inherited")]
                print(f"     {user} → {perms}")
            acl_repo_ok += 1

    print(f"\n[OK] ACLs repos : {acl_repo_ok} {'simulées' if dry_run else 'restaurées'}, {acl_repo_error} erreurs")

# COMMAND ----------
# MAGIC %md ## Résumé

# COMMAND ----------
summary_lines = [
    f"  Date backup     : {backup_date}",
    f"  Type restauré   : {restore_type}",
]

if restore_type in ("notebooks", "both"):
    summary_lines.append(f"  Notebooks       : {nb_ok} {'simulés' if dry_run else 'restaurés'}, {nb_error} erreurs")
if restore_type in ("acls", "both"):
    summary_lines.append(f"  ACLs workspace  : {acl_nb_ok} {'simulées' if dry_run else 'restaurées'}, {acl_nb_error} erreurs")
    summary_lines.append(f"  ACLs repos      : {acl_repo_ok} {'simulées' if dry_run else 'restaurées'}, {acl_repo_error} erreurs")

print(f"""
╔══════════════════════════════════════════════════════╗
║    {prefix}WORKSPACE RESTORE — RÉSUMÉ
╠══════════════════════════════════════════════════════╣
""" + "\n".join(summary_lines) + """
╚══════════════════════════════════════════════════════╝
""")

if dry_run:
    print("Mode DRY-RUN : aucune modification effectuée.")
    print("Repasser dry_run = false pour exécuter la restauration.")

dbutils.notebook.exit(json.dumps({
    "backup_date":   backup_date,
    "restore_type":  restore_type,
    "dry_run":       dry_run,
    "nb_ok":         nb_ok,
    "nb_error":      nb_error,
    "acl_nb_ok":     acl_nb_ok,
    "acl_nb_error":  acl_nb_error,
    "acl_repo_ok":   acl_repo_ok,
    "acl_repo_error":acl_repo_error,
}))
