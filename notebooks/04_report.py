# Databricks notebook source
# notebooks/04_report.py

# COMMAND ----------
# MAGIC %md # 04 — Génération du rapport DR

# COMMAND ----------
import json
import sys
from datetime import date

# COMMAND ----------
dbutils.widgets.text("backup_root", "", "Backup root")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup")
dbutils.widgets.text("diff_json", "{}", "Diff JSON")
dbutils.widgets.text("stats_json", "{}", "Stats JSON")
dbutils.widgets.text("steps_json", "[]", "Steps JSON")
dbutils.widgets.text("lib_path", "/Workspace/Shared/dr-backup/lib", "Chemin vers lib/")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")
lib_path = dbutils.widgets.get("lib_path")

# Diff: lire depuis ADLS (chemin canonique), fallback sur le widget
diff_json_widget = dbutils.widgets.get("diff_json")
if diff_json_widget and diff_json_widget != "{}":
    diff = json.loads(diff_json_widget)
else:
    diff_path = f"{backup_root}/{backup_date}/diff/diff_{backup_date}.json"
    try:
        diff = json.loads(dbutils.fs.head(diff_path, 10_000_000))
        print(f"[OK] Diff chargé depuis {diff_path}")
    except Exception as e:
        print(f"[WARN] Diff non disponible ({e}), rapport sans diff")
        diff = {}

stats = json.loads(dbutils.widgets.get("stats_json"))
steps = json.loads(dbutils.widgets.get("steps_json"))

sys.path.insert(0, lib_path)
from report import generate_report

# COMMAND ----------
# DBTITLE 1, Générer le rapport HTML

html = generate_report(diff=diff, stats=stats, steps=steps)
report_path = f"{backup_root}/{backup_date}/report/dr_report_{backup_date}.html"
dbutils.fs.put(report_path, html, overwrite=True)
print(f"[OK] Rapport écrit : {report_path}")

# COMMAND ----------
dbutils.notebook.exit(json.dumps({"status": "ok", "report_path": report_path}))
