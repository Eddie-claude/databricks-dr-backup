# Databricks notebook source
# notebooks/03_diff.py

# COMMAND ----------
# MAGIC %md # 03 — Diff backup J vs J-1

# COMMAND ----------
import json
import sys
from datetime import date, timedelta

# Ajouter lib/ au path (chemin relatif dans le repo importé dans le workspace)
sys.path.insert(0, "/Workspace/Shared/dr-backup/lib")
from diff import compute_diff, BackupManifest

# COMMAND ----------
dbutils.widgets.text("backup_root", "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup J")
dbutils.widgets.text("current_manifest", "{}", "Manifest JSON du backup courant")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")
current_manifest_raw = json.loads(dbutils.widgets.get("current_manifest"))

# COMMAND ----------
# DBTITLE 1, Charger le manifest J-1

date_j = date.fromisoformat(backup_date)
date_j1 = (date_j - timedelta(days=1)).isoformat()
manifest_j1_path = f"{backup_root}/{date_j1}/manifest.json"

try:
    prev_raw = json.loads(dbutils.fs.head(manifest_j1_path, 1_000_000))
    print(f"[OK] Manifest J-1 chargé depuis {manifest_j1_path}")
except Exception as e:
    print(f"[WARN] Pas de manifest J-1 trouvé ({e}), diff depuis zéro")
    prev_raw = {"date": date_j1, "tables": [], "jobs": [], "notebooks": []}

# COMMAND ----------
# DBTITLE 1, Calculer le diff

prev_manifest = BackupManifest.from_dict(prev_raw)
curr_manifest = BackupManifest.from_dict(current_manifest_raw)

diff = compute_diff(prev_manifest, curr_manifest)

diff_path = f"{backup_root}/{backup_date}/diff/diff_{backup_date}.json"
dbutils.fs.put(diff_path, json.dumps(diff, indent=2), overwrite=True)

print(f"[Diff] Tables ajoutées: {diff['tables']['added']}")
print(f"[Diff] Tables supprimées: {diff['tables']['removed']}")
print(f"[Diff] Jobs ajoutés: {diff['jobs']['added']}")
print(f"[Diff] Notebooks ajoutés: {diff['notebooks']['added']}")

# COMMAND ----------
dbutils.notebook.exit(json.dumps(diff))
