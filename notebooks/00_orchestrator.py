# Databricks notebook source
# notebooks/00_orchestrator.py

# COMMAND ----------
# MAGIC %md # 00 — DR Backup Orchestrator

# COMMAND ----------
import json
import time
from datetime import date

# COMMAND ----------
dbutils.widgets.text("backup_root", "abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net", "Backup root")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup YYYY-MM-DD")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")

steps = []

def run_step(name, notebook_path, params):
    start = time.time()
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds=7200, arguments=params)
        duration = int(time.time() - start)
        steps.append({"name": name, "status": "success", "duration_s": duration})
        print(f"[OK] {name} ({duration}s)")
        return json.loads(result) if result and result != "ok" else {}
    except Exception as e:
        duration = int(time.time() - start)
        steps.append({"name": name, "status": "error", "duration_s": duration, "error": str(e)})
        print(f"[ERROR] {name}: {e}")
        return {}

# COMMAND ----------
# DBTITLE 1, Étape 1 — UC Metadata

base_params = {"backup_root": backup_root, "backup_date": backup_date}
uc_result = run_step("uc_metadata", "./01_uc_metadata", base_params)

# COMMAND ----------
# DBTITLE 1, Étape 2 — Data Clone

clone_result = run_step("data_clone", "./02_data_clone", {
    **base_params,
    "uc_metadata_result": json.dumps(uc_result),
})

# COMMAND ----------
# DBTITLE 1, Construire le manifest courant

table_names = uc_result.get("table_names", [])
current_manifest = {
    "date": backup_date,
    "tables": table_names,
    "jobs": [],
    "notebooks": [],
}

manifest_path = f"{backup_root}/{backup_date}/manifest.json"
dbutils.fs.put(manifest_path, json.dumps(current_manifest, indent=2), overwrite=True)

# COMMAND ----------
# DBTITLE 1, Étape 3 — Diff

diff_result = run_step("diff", "./03_diff", {
    **base_params,
    "current_manifest": json.dumps(current_manifest),
})

# COMMAND ----------
# DBTITLE 1, Étape 4 — Rapport

stats = {
    "total_tables": len(table_names),
    "total_jobs": len(current_manifest.get("jobs", [])),
    "total_notebooks": len(current_manifest.get("notebooks", [])),
    "data_size_gb": clone_result.get("total_size_gb", 0),
}

run_step("report", "./04_report", {
    **base_params,
    "diff_json": json.dumps(diff_result),
    "stats_json": json.dumps(stats),
    "steps_json": json.dumps(steps),
})

# COMMAND ----------
# DBTITLE 1, Mettre à jour latest.json

latest = {"date": backup_date, "manifest_path": manifest_path}
dbutils.fs.put(f"{backup_root}/latest.json", json.dumps(latest, indent=2), overwrite=True)

print(f"\n[DR Backup] Terminé — {backup_date}")
