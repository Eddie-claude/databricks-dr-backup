# Databricks notebook source
# notebooks/09_restore_jobs.py

# COMMAND ----------
# MAGIC %md # 09 — Restauration Jobs Databricks
# MAGIC
# MAGIC Ce notebook restaure les définitions de jobs sauvegardées par `05_workspace_config` (section 5.4).
# MAGIC
# MAGIC | Étape | Action |
# MAGIC |-------|--------|
# MAGIC | 9.1 | Lister les dates de backup disponibles |
# MAGIC | 9.2 | Afficher les jobs disponibles dans le backup |
# MAGIC | 9.3 | Filtrer les jobs à restaurer |
# MAGIC | 9.4 | Recréer les jobs via l'API Databricks 2.1 |
# MAGIC | 9.5 | Résumé avec mapping ancien job_id → nouveau job_id |
# MAGIC
# MAGIC **Remarques importantes :**
# MAGIC - Les jobs sont recrées avec un **nouveau `job_id`** (l'ancien ID n'est pas réutilisable).
# MAGIC - Si un job portant le même nom existe déjà, le comportement dépend du paramètre `conflict_mode`.
# MAGIC - **dry_run = true** : affiche les actions sans rien modifier — toujours commencer par là.

# COMMAND ----------
import json
import re
import requests
from datetime import date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def _uc_head(path: str) -> str:
    """Lit un fichier texte depuis ADLS (UC-aware)."""
    return "\n".join(r.value for r in spark.read.text(path).collect())

token   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
headers = {"Authorization": f"Bearer {token}"}

# COMMAND ----------
# MAGIC %md ## Paramètres

# COMMAND ----------
from datetime import date

dbutils.widgets.text(     "backup_root",    "",             "Backup root (abfss://...)")
dbutils.widgets.text(     "backup_date",    str(date.today()), "Date du backup (YYYY-MM-DD)")
dbutils.widgets.text(     "job_filter",     "",             "Filtre nom de job (sous-chaîne, vide = tous)")
dbutils.widgets.dropdown( "conflict_mode",  "skip",         ["skip", "recreate"], "Si job existe déjà : skip / recreate")
dbutils.widgets.dropdown( "dry_run",        "true",         ["true", "false"],    "Dry-run (true = simulation)")

backup_root   = dbutils.widgets.get("backup_root").strip().rstrip("/")
backup_date   = dbutils.widgets.get("backup_date").strip()
job_filter    = dbutils.widgets.get("job_filter").strip().lower()
conflict_mode = dbutils.widgets.get("conflict_mode")
dry_run       = dbutils.widgets.get("dry_run").lower() == "true"

jobs_backup_path = f"{backup_root}/{backup_date}/jobs/jobs_all.json"
prefix = "[DRY-RUN] " if dry_run else ""

print(f"[OK] backup_root    = {backup_root or '(vide — À RENSEIGNER)'}")
print(f"[OK] backup_date    = {backup_date}")
print(f"[OK] job_filter     = {job_filter or '(tous)'}")
print(f"[OK] conflict_mode  = {conflict_mode}")
print(f"[OK] dry_run        = {dry_run}")

if not backup_root:
    raise ValueError("backup_root est vide. Renseignez le widget 'Backup root' avec le chemin abfss://...")

# COMMAND ----------
# MAGIC %md ## Étape 1 — Dates de backup disponibles

# COMMAND ----------
DATE_PAT = re.compile(r"^\d{4}-\d{2}-\d{2}$")

def list_backup_dates(root):
    try:
        return sorted([
            f.name.rstrip("/") for f in dbutils.fs.ls(root)
            if DATE_PAT.match(f.name.rstrip("/"))
        ], reverse=True)
    except Exception as e:
        print(f"  [WARN] Impossible de lister les dates : {e}")
        return []

available_dates = list_backup_dates(backup_root)
print(f"── Dates de backup disponibles ({len(available_dates)}) ──────────────")
for d in available_dates[:15]:
    marker = " ← sélectionné" if d == backup_date else ""
    has_jobs = ""
    try:
        dbutils.fs.ls(f"{backup_root}/{d}/jobs")
        has_jobs = " [jobs OK]"
    except Exception:
        has_jobs = " [pas de jobs]"
    print(f"  {d}{has_jobs}{marker}")
if len(available_dates) > 15:
    print(f"  ... et {len(available_dates) - 15} autres")

# COMMAND ----------
# MAGIC %md ## Étape 2 — Jobs disponibles dans le backup

# COMMAND ----------
def load_jobs_from_backup(path):
    """Charge jobs_all.json depuis ADLS. Retourne une liste de jobs."""
    try:
        raw = _uc_head(path)
        return json.loads(raw)
    except Exception as e:
        print(f"[WARN] Impossible de lire {path} : {e}")
        return []

def load_jobs_from_individual_files(jobs_dir):
    """Fallback : lit les fichiers individuels {id}_{name}.json."""
    jobs = []
    try:
        files = [f for f in dbutils.fs.ls(jobs_dir)
                 if f.name.endswith(".json") and f.name != "jobs_all.json"]
        for f in files:
            try:
                raw = _uc_head(f.path)
                jobs.append(json.loads(raw))
            except Exception as e:
                print(f"  [WARN] Impossible de lire {f.path} : {e}")
    except Exception as e:
        print(f"[WARN] Impossible de lister {jobs_dir} : {e}")
    return jobs

# Chargement : jobs_all.json en priorité, fichiers individuels en fallback
all_backup_jobs = load_jobs_from_backup(jobs_backup_path)
if not all_backup_jobs:
    print("[INFO] jobs_all.json vide ou absent — lecture des fichiers individuels...")
    all_backup_jobs = load_jobs_from_individual_files(f"{backup_root}/{backup_date}/jobs")

print(f"\n── {len(all_backup_jobs)} job(s) trouvé(s) dans le backup du {backup_date} ──")
for j in all_backup_jobs:
    job_id   = j.get("job_id", "?")
    job_name = j.get("settings", {}).get("name", "(sans nom)")
    tasks    = j.get("settings", {}).get("tasks", [])
    schedule = "programmé" if j.get("settings", {}).get("schedule") else "manuel"
    print(f"  [{job_id}] {job_name}  ({len(tasks)} tâche(s), {schedule})")

if not all_backup_jobs:
    print("[ERROR] Aucun job trouvé dans le backup. Vérifiez backup_root et backup_date.")
    dbutils.notebook.exit("Aucun job dans le backup")

# COMMAND ----------
# MAGIC %md ## Étape 3 — Sélection des jobs à restaurer

# COMMAND ----------
if job_filter:
    jobs_to_restore = [
        j for j in all_backup_jobs
        if job_filter in j.get("settings", {}).get("name", "").lower()
    ]
    print(f"[INFO] Filtre '{job_filter}' → {len(jobs_to_restore)} job(s) sélectionné(s)")
else:
    jobs_to_restore = all_backup_jobs
    print(f"[INFO] Aucun filtre — {len(jobs_to_restore)} job(s) sélectionné(s)")

print()
for j in jobs_to_restore:
    print(f"  ✓ [{j.get('job_id', '?')}] {j.get('settings', {}).get('name', '(sans nom)')}")

if not jobs_to_restore:
    print("[WARN] Aucun job ne correspond au filtre.")
    dbutils.notebook.exit("Aucun job sélectionné")

# COMMAND ----------
# MAGIC %md ## Étape 4 — Récupération des jobs existants sur le workspace

# COMMAND ----------
def list_existing_jobs(host, headers):
    """Retourne un dict {nom: job_id} des jobs existants sur le workspace."""
    existing = {}
    params = {"limit": 100}
    while True:
        r = requests.get(f"{host}/api/2.1/jobs/list",
                         headers=headers, params=params, timeout=30)
        if not r.ok:
            print(f"[WARN] Impossible de lister les jobs existants : {r.status_code}")
            break
        data = r.json()
        for j in data.get("jobs", []):
            name = j.get("settings", {}).get("name", "")
            if name:
                existing[name] = j.get("job_id")
        if not data.get("has_more"):
            break
        params["page_token"] = data.get("next_page_token", "")
    return existing

existing_jobs = list_existing_jobs(host, headers)
print(f"[OK] {len(existing_jobs)} job(s) existant(s) sur le workspace")

# COMMAND ----------
# MAGIC %md ## Étape 5 — Restauration

# COMMAND ----------
def strip_readonly_fields(settings: dict) -> dict:
    """
    Supprime les champs calculés que l'API de création n'accepte pas.
    On ne garde que les champs configurables (name, tasks, schedule, etc.).
    """
    readonly = {
        "created_time", "creator_user_name", "run_as_user_name",
        "run_as_service_principal_name",
    }
    return {k: v for k, v in settings.items() if k not in readonly}

results   = []
ok_count  = 0
skip_count = 0
error_count = 0

print(f"\n{'─'*60}")
print(f"{prefix}RESTAURATION JOBS — conflict_mode={conflict_mode}")
print(f"{'─'*60}\n")

for job in jobs_to_restore:
    old_id   = job.get("job_id", "?")
    settings = job.get("settings", {})
    job_name = settings.get("name", f"job_{old_id}")
    tasks    = settings.get("tasks", [])

    print(f"{'── ' if dry_run else '▶  '}{job_name}  (ancien id={old_id}, {len(tasks)} tâche(s))")

    # Vérifier si le job existe déjà
    existing_id = existing_jobs.get(job_name)
    if existing_id:
        if conflict_mode == "skip":
            print(f"   [SKIP] Job '{job_name}' existe déjà (id={existing_id}) — conflict_mode=skip\n")
            results.append({
                "job_name": job_name, "old_id": old_id,
                "new_id": existing_id, "status": "skipped",
            })
            skip_count += 1
            continue
        else:  # recreate
            print(f"   [INFO] Job '{job_name}' existe déjà (id={existing_id}) — conflict_mode=recreate → nouveau job créé en parallèle")

    if dry_run:
        schedule_info = ""
        if settings.get("schedule"):
            cron = settings["schedule"].get("quartz_cron_expression", "")
            tz   = settings["schedule"].get("timezone_id", "")
            schedule_info = f"schedule={cron} ({tz})"
        print(f"   Tâches : {', '.join(t.get('task_key', '?') for t in tasks)}")
        if schedule_info:
            print(f"   {schedule_info}")
        print(f"   → Créerait le job via POST /api/2.1/jobs/create\n")
        results.append({
            "job_name": job_name, "old_id": old_id, "new_id": None, "status": "dry_run",
        })
        ok_count += 1
        continue

    # Création effective
    payload = strip_readonly_fields(settings)
    try:
        r = requests.post(
            f"{host}/api/2.1/jobs/create",
            headers=headers,
            json=payload,
            timeout=30,
        )
        if r.ok:
            new_id = r.json().get("job_id")
            print(f"   [OK] Job créé — nouveau id={new_id}\n")
            results.append({
                "job_name": job_name, "old_id": old_id, "new_id": new_id, "status": "created",
            })
            ok_count += 1
        else:
            err_msg = r.json().get("message", r.text[:200])
            print(f"   [ERROR] {r.status_code} — {err_msg}\n")
            results.append({
                "job_name": job_name, "old_id": old_id, "new_id": None,
                "status": "error", "error": err_msg,
            })
            error_count += 1
    except Exception as e:
        print(f"   [ERROR] Exception : {e}\n")
        results.append({
            "job_name": job_name, "old_id": old_id, "new_id": None,
            "status": "error", "error": str(e),
        })
        error_count += 1

# COMMAND ----------
# MAGIC %md ## Étape 6 — Mapping ancien ID → nouveau ID

# COMMAND ----------
if not dry_run:
    created = [r for r in results if r["status"] == "created"]
    if created:
        print("── Mapping job_id ─────────────────────────────────────")
        print(f"  {'Nom':<40} {'Ancien ID':>10} {'Nouveau ID':>10}")
        print(f"  {'─'*40} {'─'*10} {'─'*10}")
        for r in created:
            print(f"  {r['job_name']:<40} {str(r['old_id']):>10} {str(r['new_id']):>10}")
        print()

# COMMAND ----------
# MAGIC %md ## Résumé

# COMMAND ----------
prefix_label = "DRY-RUN — " if dry_run else ""
print(f"""
╔══════════════════════════════════════════════════════╗
║       {prefix_label}RESTAURATION JOBS — RÉSUMÉ
╠══════════════════════════════════════════════════════╣
║  Date backup      : {backup_date:<33} ║
║  Filtre           : {(job_filter or '(tous)'):<33} ║
║  Conflict mode    : {conflict_mode:<33} ║
║  Jobs sélect.     : {len(jobs_to_restore):<33} ║
║  Créés OK         : {ok_count:<33} ║
║  Ignorés (skip)   : {skip_count:<33} ║
║  Erreurs          : {error_count:<33} ║
╚══════════════════════════════════════════════════════╝
""")

if dry_run:
    print("Mode DRY-RUN : aucun job n'a été créé.")
    print("Pour exécuter la restauration, repasser dry_run = false.")

dbutils.notebook.exit(json.dumps({
    "backup_date":   backup_date,
    "job_filter":    job_filter,
    "conflict_mode": conflict_mode,
    "dry_run":       dry_run,
    "selected":      len(jobs_to_restore),
    "ok":            ok_count,
    "skipped":       skip_count,
    "errors":        error_count,
    "results":       results,
}))
