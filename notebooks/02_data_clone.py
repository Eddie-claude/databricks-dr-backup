# Databricks notebook source
# notebooks/02_data_clone.py

# COMMAND ----------
# MAGIC %md # 02 — Data Clone (DEEP CLONE + external copy)

# COMMAND ----------
import json
from datetime import date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
dbutils.widgets.text("backup_root", "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup YYYY-MM-DD")
dbutils.widgets.text("uc_metadata_result", "{}", "JSON result from 01_uc_metadata")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")
uc_result = json.loads(dbutils.widgets.get("uc_metadata_result"))
table_names = uc_result.get("table_names", [])

data_backup_root = f"{backup_root}/{backup_date}/data"

# COMMAND ----------
# DBTITLE 1, DEEP CLONE toutes les tables

clone_results = []
total_size_gb = 0.0

for fqn in table_names:
    catalog, schema, table = fqn.split(".")
    dest = f"{data_backup_root}/{catalog}/{schema}/{table}"
    try:
        result = spark.sql(f"CREATE OR REPLACE TABLE delta.`{dest}` DEEP CLONE `{catalog}`.`{schema}`.`{table}`")
        metrics = result.collect()[0].asDict()
        size_gb = metrics.get("num_output_bytes", 0) / (1024**3)
        total_size_gb += size_gb
        clone_results.append({"table": fqn, "status": "success", "size_gb": round(size_gb, 4)})
        print(f"[OK] DEEP CLONE {fqn} → {dest} ({size_gb:.2f} GB)")
    except Exception as e:
        clone_results.append({"table": fqn, "status": "error", "error": str(e)})
        print(f"[ERROR] {fqn}: {e}")

# COMMAND ----------
# DBTITLE 1, Sauvegarder le manifest des clones

clone_manifest_path = f"{backup_root}/{backup_date}/data/_clone_manifest.json"
dbutils.fs.put(clone_manifest_path, json.dumps(clone_results, indent=2), overwrite=True)

print(f"\n[Résumé] {len([r for r in clone_results if r['status']=='success'])} / {len(table_names)} tables clonées")
print(f"[Résumé] Volume total : {total_size_gb:.2f} GB")

# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "clone_results": clone_results,
    "total_size_gb": round(total_size_gb, 3),
}))
