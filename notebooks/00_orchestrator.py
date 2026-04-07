# Databricks notebook source
# notebooks/00_orchestrator.py

# COMMAND ----------
# MAGIC %md # 00 — DR Backup Orchestrator

# COMMAND ----------
import json
import time
from datetime import date

# COMMAND ----------
dbutils.widgets.text("backup_root", "abfss://uc-data@st10keyitdpdrpdevwe00.dfs.core.windows.net/dr-backup", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup YYYY-MM-DD")
dbutils.widgets.text("lib_path", "/Workspace/Shared/dr-backup/lib", "Chemin vers lib/")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")
lib_path = dbutils.widgets.get("lib_path")

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
        result = dbutils.notebook.run(notebook_path, timeout_seconds=7200, arguments=params)
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

base_params = {"backup_root": backup_root, "backup_date": backup_date, "lib_path": lib_path}
uc_result = run_step("uc_metadata", "./01_uc_metadata", base_params, critical=True)

# COMMAND ----------
# DBTITLE 1, Étape 2 — Data Clone (critique)

clone_result = run_step("data_clone", "./02_data_clone", {
    **base_params,
    "uc_metadata_result": json.dumps(uc_result),
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
dbutils.fs.put(manifest_path, json.dumps(current_manifest, indent=2), overwrite=True)
print(f"[OK] Manifest écrit : {manifest_path}")

# COMMAND ----------
# DBTITLE 1, Compléter le manifest avec les assets workspace (exportés par CI/CD)

workspace_jobs_path = f"{backup_root}/{backup_date}/workspace/jobs.json"
workspace_notebooks_manifest = f"{backup_root}/{backup_date}/workspace/notebooks"

try:
    jobs_raw = json.loads(dbutils.fs.head(workspace_jobs_path, 1_000_000))
    job_names = [j.get("settings", j).get("name", str(j.get("job_id", ""))) for j in jobs_raw]
    current_manifest["jobs"] = job_names
    print(f"[OK] {len(job_names)} jobs chargés dans le manifest")
except Exception as e:
    print(f"[WARN] Impossible de charger jobs.json: {e}")

try:
    nb_files = [f.path for f in dbutils.fs.ls(workspace_notebooks_manifest)]
    current_manifest["notebooks"] = nb_files
    print(f"[OK] {len(nb_files)} notebooks listés dans le manifest")
except Exception as e:
    print(f"[WARN] Impossible de lister les notebooks: {e}")

# Réécrire le manifest complété
dbutils.fs.put(manifest_path, json.dumps(current_manifest, indent=2), overwrite=True)
print(f"[OK] Manifest complété : {manifest_path}")

# COMMAND ----------
# DBTITLE 1, Étape 3 — Workspace Config (non critique)

run_step("workspace_config", "./05_workspace_config", base_params, critical=False)

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
# DBTITLE 1, Mettre à jour latest.json avec statut global

latest = {
    "date": backup_date,
    "manifest_path": manifest_path,
    "status": global_status,
    "steps_summary": {s["name"]: s["status"] for s in steps},
}
dbutils.fs.put(f"{backup_root}/latest.json", json.dumps(latest, indent=2), overwrite=True)

print(f"\n[DR Backup] Terminé — {backup_date} — statut: {global_status}")
