# Databricks notebook source
# notebooks/demo/06_restore_workspace.py

# COMMAND ----------
# MAGIC %md
# MAGIC # 🔄 Acte 7 — Restauration Jobs & Notebooks
# MAGIC
# MAGIC Ce notebook restaure depuis ADLS (sans téléchargement local) :
# MAGIC - **Jobs Databricks** : recréation via REST API depuis `jobs.json`
# MAGIC - **SQL Warehouses** : recréation depuis `sql_warehouses.json`
# MAGIC - **Notebooks** : ré-import depuis le backup workspace
# MAGIC
# MAGIC Modes disponibles : `dry_run=True` (simulation) ou `dry_run=False` (applique)
# MAGIC
# MAGIC **Durée estimée : ~3 minutes**

# COMMAND ----------
# MAGIC %md ## 7.1 — Paramètres

# COMMAND ----------
dbutils.widgets.text("backup_root", "abfss://uc-data@st10keyitdpdrpdevwe00.dfs.core.windows.net/dr-backup", "Backup root")
dbutils.widgets.text("backup_date", "", "Date du backup (vide = dernier)")
dbutils.widgets.dropdown("dry_run", "true", ["true", "false"], "Mode dry-run (simulation)")
dbutils.widgets.dropdown("restore_scope", "jobs_only", ["jobs_only", "jobs_and_notebooks", "all"], "Périmètre de restauration")

backup_root  = dbutils.widgets.get("backup_root")
backup_date  = dbutils.widgets.get("backup_date")
dry_run      = dbutils.widgets.get("dry_run") == "true"
restore_scope = dbutils.widgets.get("restore_scope")

import json
import requests

# Auth session courante
token   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
headers = {"Authorization": f"Bearer {token}"}

if not backup_date:
    latest = json.loads(dbutils.fs.head(f"{backup_root}/latest.json"))
    backup_date = latest["date"]
    print(f"[AUTO] Date backup : {backup_date}")

workspace_backup = f"{backup_root}/{backup_date}/workspace"
print(f"[OK] Backup source   : {workspace_backup}")
print(f"[OK] Mode dry-run    : {dry_run}")
print(f"[OK] Périmètre       : {restore_scope}")

# COMMAND ----------
# MAGIC %md ## 7.2 — Helpers

# COMMAND ----------
def list_all_jobs():
    """Liste tous les jobs existants (avec pagination)."""
    jobs = []
    params = {"limit": 100}
    while True:
        r = requests.get(f"{host}/api/2.1/jobs/list", headers=headers, params=params, timeout=30)
        r.raise_for_status()
        body = r.json()
        jobs.extend(body.get("jobs", []))
        if not body.get("has_more"):
            break
        params["page_token"] = body["next_page_token"]
    return jobs


def is_dab_job(settings):
    """Détecte un job géré par Databricks Asset Bundles."""
    tags = settings.get("tags", {})
    return "bundle" in tags or any(k.startswith("databricks.bundle") for k in tags)


def clean_job_settings(settings):
    """Supprime les champs read-only avant création."""
    skip = {"job_id", "creator_user_name", "created_time",
            "run_as_user_name", "effective_budget_policy_id",
            "run_as", "settings_schema_version"}
    return {k: v for k, v in settings.items() if k not in skip}


# COMMAND ----------
# MAGIC %md ## 7.3 — Restauration des Jobs

# COMMAND ----------
print("=" * 60)
print("RESTAURATION JOBS DATABRICKS")
print("=" * 60)
if dry_run:
    print("⚠️  MODE DRY-RUN — aucun job ne sera créé\n")

jobs_path = f"{workspace_backup}/jobs.json"

try:
    jobs_backup = json.loads(dbutils.fs.head(jobs_path, 10_000_000))
    print(f"  {len(jobs_backup)} jobs trouvés dans le backup\n")

    # Index des jobs existants
    existing = {j["settings"]["name"]: j["job_id"] for j in list_all_jobs()}
    print(f"  {len(existing)} jobs actuellement dans le workspace\n")

    ok = skip_dab = skip_exists = err = 0

    for job_entry in jobs_backup:
        settings = job_entry.get("settings", job_entry)
        name = settings.get("name", f"job_{job_entry.get('job_id', '?')}")

        # Skip jobs DAB
        if is_dab_job(settings):
            print(f"  ⏭ [DAB]   {name}  → géré par bundle, ignoré")
            skip_dab += 1
            continue

        # Skip si déjà existant
        if name in existing:
            print(f"  ⏭ [EXIST] {name}  → déjà présent (id={existing[name]})")
            skip_exists += 1
            continue

        if dry_run:
            print(f"  🔵 [DRY]  {name}  → serait créé")
            ok += 1
            continue

        payload = clean_job_settings(settings)
        resp = requests.post(f"{host}/api/2.1/jobs/create", headers=headers, json=payload, timeout=30)

        if resp.ok:
            new_id = resp.json().get("job_id")
            print(f"  ✅ [OK]   {name}  → créé (id={new_id})")
            ok += 1
        else:
            try:
                detail = resp.json().get("message", resp.text[:150])
            except Exception:
                detail = resp.text[:150]
            print(f"  ❌ [ERR]  {name}  → {detail}")
            err += 1

    print(f"""
  ─────────────────────────────────────
  {"[DRY-RUN] " if dry_run else ""}Résultat jobs :
    {"Créeraient" if dry_run else "Créés"}       : {ok}
    DAB (ignorés) : {skip_dab}
    Déjà présents : {skip_exists}
    Erreurs       : {err}
  ─────────────────────────────────────""")

except Exception as e:
    print(f"[ERROR] Impossible de charger jobs.json : {e}")

# COMMAND ----------
# MAGIC %md ## 7.4 — Restauration SQL Warehouses

# COMMAND ----------
print("=" * 60)
print("RESTAURATION SQL WAREHOUSES")
print("=" * 60)
if dry_run:
    print("⚠️  MODE DRY-RUN — aucun warehouse ne sera créé\n")

wh_path = f"{workspace_backup}/sql_warehouses.json"

try:
    wh_backup = json.loads(dbutils.fs.head(wh_path, 1_000_000))

    # Warehouses existants
    wh_existing = {w["name"]: w["id"]
                   for w in requests.get(f"{host}/api/2.0/sql/warehouses",
                                         headers=headers, timeout=30
                                        ).json().get("warehouses", [])}

    skip_fields = {"id", "state", "num_active_sessions", "creator_name",
                   "jdbc_url", "odbc_params", "health", "num_clusters"}

    ok = skip = err = 0
    for wh in wh_backup:
        name = wh.get("name", "unknown")

        if name in wh_existing:
            print(f"  ⏭ [EXIST] {name}  → déjà présent")
            skip += 1
            continue

        if dry_run:
            print(f"  🔵 [DRY]  {name} ({wh.get('cluster_size', '?')})  → serait créé")
            ok += 1
            continue

        payload = {k: v for k, v in wh.items() if k not in skip_fields and v is not None}
        resp = requests.post(f"{host}/api/2.0/sql/warehouses", headers=headers, json=payload, timeout=30)

        if resp.ok:
            print(f"  ✅ [OK]   {name}  → créé (id={resp.json().get('id')})")
            ok += 1
        else:
            print(f"  ❌ [ERR]  {name}  → {resp.text[:100]}")
            err += 1

    print(f"\n  {'[DRY-RUN] ' if dry_run else ''}Résultat warehouses : {ok} {'créeraient' if dry_run else 'créés'} | {skip} ignorés | {err} erreurs")

except Exception as e:
    print(f"[INFO] sql_warehouses.json non disponible : {e}")

# COMMAND ----------
# MAGIC %md ## 7.5 — Restauration Notebooks (via REST API)

# COMMAND ----------
if restore_scope in ("jobs_and_notebooks", "all"):
    print("=" * 60)
    print("RESTAURATION NOTEBOOKS WORKSPACE")
    print("=" * 60)
    if dry_run:
        print("⚠️  MODE DRY-RUN — aucun notebook ne sera importé\n")

    nb_backup_path = f"{workspace_backup}/notebooks"
    target_dir = "/Shared/dr-restore"

    try:
        def list_notebooks_recursive(path, depth=0):
            if depth > 5:
                return []
            try:
                items = dbutils.fs.ls(path)
            except Exception:
                return []
            result = []
            for item in items:
                if item.name.endswith("/"):
                    result.extend(list_notebooks_recursive(item.path, depth + 1))
                else:
                    result.append(item)
            return result

        nb_files = list_notebooks_recursive(nb_backup_path)
        print(f"  {len(nb_files)} notebooks trouvés dans le backup\n")

        if dry_run:
            for f in nb_files[:10]:
                rel = f.path.replace(nb_backup_path, "")
                print(f"  🔵 [DRY]  {rel}  → {target_dir}{rel}")
            if len(nb_files) > 10:
                print(f"  ... ({len(nb_files) - 10} fichiers supplémentaires)")
            print(f"\n  [DRY-RUN] {len(nb_files)} notebooks seraient importés vers {target_dir}")
        else:
            # Import via API workspace/import (base64)
            import base64
            ok = skip = err = 0
            for nb_file in nb_files:
                rel_path = nb_file.path.replace(nb_backup_path, "").rstrip("/")
                dest_path = f"{target_dir}{rel_path}"
                try:
                    content_bytes = dbutils.fs.head(nb_file.path, 10_000_000).encode("utf-8")
                    content_b64 = base64.b64encode(content_bytes).decode("utf-8")
                    resp = requests.post(
                        f"{host}/api/2.0/workspace/import",
                        headers=headers,
                        json={
                            "path": dest_path,
                            "content": content_b64,
                            "overwrite": True,
                            "format": "SOURCE",
                        },
                        timeout=30,
                    )
                    if resp.ok:
                        ok += 1
                    else:
                        print(f"  ❌ {rel_path}: {resp.text[:80]}")
                        err += 1
                except Exception as e:
                    print(f"  ❌ {rel_path}: {e}")
                    err += 1

            print(f"\n  Résultat notebooks : {ok} importés | {err} erreurs → {target_dir}")

    except Exception as e:
        print(f"[INFO] Backup notebooks non disponible : {e}")
else:
    print("[INFO] Restauration notebooks ignorée (restore_scope=jobs_only)")
    print("       Changer restore_scope = 'jobs_and_notebooks' pour l'activer")

# COMMAND ----------
# MAGIC %md ## 7.6 — Synthèse

# COMMAND ----------
print(f"""
╔══════════════════════════════════════════════════════════╗
║        RESTAURATION WORKSPACE — SYNTHÈSE                ║
╠══════════════════════════════════════════════════════════╣
║                                                          ║
║  Mode     : {"DRY-RUN (simulation)" if dry_run else "RÉEL (changements appliqués)"}{"         " if dry_run else "         "}║
║  Backup   : {backup_date:<46} ║
║  Périmètre: {restore_scope:<46} ║
║                                                          ║
║  Pour appliquer réellement :                             ║
║    → Changer dry_run = false                             ║
║    → Relancer le notebook                                ║
║                                                          ║
║  Pour la restauration UC + données :                     ║
║    → Notebook 03_dr_scenario                             ║
║  Pour les grants UC + ACLs workspace :                   ║
║    → Notebook 05_workspace_acl                           ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝
""")
