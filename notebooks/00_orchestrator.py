# Databricks notebook source
# notebooks/00_orchestrator.py

# COMMAND ----------
# MAGIC %md # 00 — DR Backup Orchestrator

# COMMAND ----------
import json
import time
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
dbutils.widgets.text("backup_root",    "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",    str(date.today()), "Date backup YYYY-MM-DD")
dbutils.widgets.text("lib_path",       "/Workspace/Shared/dr-backup/lib", "Chemin vers lib/")
dbutils.widgets.text("retain_daily",   "30",    "Rétention quotidienne (jours)")
dbutils.widgets.text("retain_weekly",  "2",     "Rétention hebdomadaire (semaines) — legacy, purge uniquement")
dbutils.widgets.text("retain_monthly", "1",     "Rétention mensuelle (mois)")
dbutils.widgets.text("dry_run",        "false", "Dry-run retention (true = simulation)")
dbutils.widgets.text("max_parallel",   "8",     "Opérations simultanées (clones dans 02, VACUUM dans 06)")
dbutils.widgets.text("vacuum_dow",     "7",     "Jour du VACUUM : 1=lundi … 7=dimanche, 0=tous les jours")

backup_root    = dbutils.widgets.get("backup_root")
# Vide = aujourd'hui. Renseigner explicitement la date d'un run interrompu permet de
# retrouver son checkpoint (_checkpoints/{backup_date}.json, indexé par date) et donc de
# reprendre exactement où il s'est arrêté, y compris un jour plus tard.
backup_date    = dbutils.widgets.get("backup_date").strip() or str(date.today())
lib_path       = dbutils.widgets.get("lib_path")
retain_daily   = dbutils.widgets.get("retain_daily")
retain_weekly  = dbutils.widgets.get("retain_weekly")
retain_monthly = dbutils.widgets.get("retain_monthly")
dry_run        = dbutils.widgets.get("dry_run")
max_parallel   = dbutils.widgets.get("max_parallel")
vacuum_dow     = dbutils.widgets.get("vacuum_dow")
# Snapshot mensuel désactivé dans le job daily — géré par le job dr-backup-monthly
ENABLE_MONTHLY = "false"
# Snapshot weekly abandonné (Option C) — SHALLOW CLONE non supporté sur tables non-MANAGED UC.
# retain_weekly n'est plus transmis qu'à 06_retention, pour purger les anciens snapshots weekly
# déjà existants au fil du temps (aucun nouveau n'est créé).
ENABLE_WEEKLY  = "false"

print(f"[INFO] backup_date={backup_date} | backup_root={backup_root} | retain_daily={retain_daily}j")

steps = []
global_status = "success"

# COMMAND ----------
# DBTITLE 1, Helper run_step

def run_step(name, notebook_path, params, critical=False):
    """
    Exécute un notebook. Si critical=True et que le notebook échoue,
    lève une exception pour interrompre le pipeline.
    """
    global global_status
    start = time.time()
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds=28800, arguments=params)
        duration = int(time.time() - start)
        steps.append({"name": name, "status": "success", "duration_s": duration})
        print(f"[OK] {name} ({duration}s)")
        return json.loads(result) if result and result.startswith("{") else {}
    except Exception as e:
        duration = int(time.time() - start)
        steps.append({"name": name, "status": "error", "duration_s": duration, "error": str(e)})
        global_status = "degraded"
        print(f"[ERROR] {name}: {e}")
        if critical:
            raise RuntimeError(f"Étape critique '{name}' échouée — pipeline interrompu: {e}")
        return {}

# COMMAND ----------
# DBTITLE 1, Étape 1 — UC Metadata (critique)

base_params = {
    "backup_root": backup_root,
    "backup_date": backup_date,
    "lib_path":    lib_path,
}
uc_result = run_step("uc_metadata", "./01_uc_metadata", base_params, critical=True)

# COMMAND ----------
# DBTITLE 1, Étape 2 — Data Clone (critique)

clone_result = run_step("data_clone", "./02_data_clone", {
    **base_params,
    "uc_metadata_result": json.dumps(uc_result),
    "retain_daily":       retain_daily,
    "max_parallel":       max_parallel,
}, critical=True)

# COMMAND ----------
# DBTITLE 1, Écrire le manifest courant dans ADLS (avant diff)

table_names = uc_result.get("table_names", [])
current_manifest = {
    "date": backup_date,
    "tables": table_names,
    "jobs": [],
    "notebooks": [],
}

manifest_path = f"{backup_root}/{backup_date}/manifest.json"
_uc_put(manifest_path, json.dumps(current_manifest, indent=2))
print(f"[OK] Manifest écrit : {manifest_path}")

# COMMAND ----------
# DBTITLE 1, Étape 3 — Workspace Config (non critique)

# DOIT rester avant la complétion du manifest ci-dessous : c'est cette étape qui écrit
# jobs/jobs_all.json et notebooks/ pour la date courante. Placée après, la complétion ne
# trouvait ces fichiers que si le job avait déjà tourné le même jour — donc jamais au premier
# run d'une journée, laissant jobs et notebooks vides dans le manifest et faussant le diff.
run_step("workspace_config", "./05_workspace_config", base_params, critical=False)

# COMMAND ----------
# DBTITLE 1, Compléter le manifest avec les assets workspace (exportés par 05_workspace_config)

# Ces chemins sont produits par 05_workspace_config.py, qui tourne juste avant (étape 3,
# ci-dessus) — entièrement à l'intérieur de Databricks via ce même orchestrateur. Ne pas les
# confondre avec scripts/export_workspace.py (workflow GitHub Actions dr_backup.yml), qui écrit
# à un chemin différent (backup/{date}/workspace/...) et ne tourne que sur l'infra CI/CD interne
# KeyIT — jamais chez un client qui déploie uniquement via Databricks Asset Bundles.
workspace_jobs_path          = f"{backup_root}/{backup_date}/jobs/jobs_all.json"
workspace_notebooks_manifest = f"{backup_root}/{backup_date}/notebooks"

try:
    jobs_raw = json.loads(dbutils.fs.head(workspace_jobs_path, 1_000_000))
    job_names = [j.get("settings", j).get("name", str(j.get("job_id", ""))) for j in jobs_raw]
    current_manifest["jobs"] = job_names
    print(f"[OK] {len(job_names)} jobs chargés dans le manifest")
except Exception as e:
    print(f"[WARN] Impossible de charger jobs.json: {e}")

def _list_notebooks_recursive(path):
    """05_workspace_config préserve l'arborescence workspace d'origine (ex: Shared/dr-backup/...)
    — dbutils.fs.ls seul ne listerait que le premier niveau de dossiers, pas les fichiers imbriqués."""
    files = []
    try:
        for item in dbutils.fs.ls(path):
            if item.name.endswith("/"):
                files.extend(_list_notebooks_recursive(item.path.rstrip("/")))
            else:
                files.append(item.path)
    except Exception:
        pass
    return files

def _notebook_relative_path(full_path: str) -> str:
    """Chemin relatif à {backup_date}/notebooks/, ex: 'Shared/AdminScript/ControleTags.py'.

    Indispensable pour le diff : un chemin absolu contient la date du backup, donc change
    mécaniquement chaque jour. Comparés tels quels, 100 % des notebooks apparaissaient
    ajoutés ET supprimés à chaque run, avec 0 inchangé — indéfiniment.
    Le découpage se fait sur le marqueur plutôt que par str.replace du préfixe, pour rester
    insensible à une éventuelle normalisation de l'URI par dbutils.fs.ls.
    """
    marker = f"/{backup_date}/notebooks/"
    return full_path.split(marker, 1)[1] if marker in full_path else full_path

try:
    nb_files = sorted(
        _notebook_relative_path(p)
        for p in _list_notebooks_recursive(workspace_notebooks_manifest)
    )
    current_manifest["notebooks"] = nb_files
    print(f"[OK] {len(nb_files)} notebooks listés dans le manifest")
except Exception as e:
    print(f"[WARN] Impossible de lister les notebooks: {e}")

# Réécrire le manifest complété
_uc_put(manifest_path, json.dumps(current_manifest, indent=2))
print(f"[OK] Manifest complété : {manifest_path}")

# COMMAND ----------
# DBTITLE 1, Étape 4 — Diff (non critique)

diff_result = run_step("diff", "./03_diff", base_params, critical=False)

# COMMAND ----------
# DBTITLE 1, Étape 4 — Rapport (non critique)

stats = {
    "total_tables": len(table_names),
    "total_jobs": len(current_manifest.get("jobs", [])),
    "total_notebooks": len(current_manifest.get("notebooks", [])),
    "data_size_gb": clone_result.get("total_size_gb", 0),
}

run_step("report", "./04_report", {
    **base_params,
    # diff_json laissé vide : 04_report lit le diff depuis ADLS
    # (évite la limite de taille des paramètres widget)
    "diff_json": "{}",
    "stats_json": json.dumps(stats),
    "steps_json": json.dumps(steps),
}, critical=False)

# COMMAND ----------
# DBTITLE 1, Étape 5 — Retention (non critique)

run_step("retention", "./06_retention", {
    "backup_root":    backup_root,
    "backup_date":    backup_date,
    "retain_daily":   retain_daily,
    "retain_weekly":  retain_weekly,
    "retain_monthly": retain_monthly,
    "dry_run":        dry_run,
    "enable_monthly": ENABLE_MONTHLY,
    "enable_weekly":  ENABLE_WEEKLY,
    # Le job daily est le seul propriétaire du VACUUM sur incremental/ — le job monthly
    # passe enable_vacuum=false pour ne pas le dupliquer.
    "enable_vacuum":  "true",
    "vacuum_dow":     vacuum_dow,
    "max_parallel":   max_parallel,
}, critical=False)

# COMMAND ----------
# DBTITLE 1, Mettre à jour latest.json avec statut global

latest = {
    "date": backup_date,
    "manifest_path": manifest_path,
    "status": global_status,
    "steps_summary": {s["name"]: s["status"] for s in steps},
}
_uc_put(f"{backup_root}/latest.json", json.dumps(latest, indent=2))

print(f"\n[DR Backup] Terminé — {backup_date} — statut: {global_status}")
