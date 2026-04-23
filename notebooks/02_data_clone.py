# Databricks notebook source
# notebooks/02_data_clone.py

# COMMAND ----------
# MAGIC %md # 02 — Data Clone (DEEP CLONE + checkpoint + resume)

# COMMAND ----------
import json
import time
from datetime import date, datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
dbutils.widgets.text("backup_root",        "",              "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",        str(date.today()),"Date backup YYYY-MM-DD")
dbutils.widgets.text("uc_metadata_result", "{}",            "JSON result from 01_uc_metadata")
dbutils.widgets.text("resume",             "true",          "Reprendre depuis checkpoint (true/false)")

backup_root  = dbutils.widgets.get("backup_root")
backup_date  = dbutils.widgets.get("backup_date")
uc_result    = json.loads(dbutils.widgets.get("uc_metadata_result"))
resume_mode  = dbutils.widgets.get("resume").lower() == "true"
table_names  = uc_result.get("table_names", [])

data_backup_root     = f"{backup_root}/{backup_date}/data"
clone_manifest_path  = f"{backup_root}/{backup_date}/data/_clone_manifest.json"
checkpoint_path      = f"{backup_root}/{backup_date}/data/_checkpoint.json"

# COMMAND ----------
# DBTITLE 1, Charger checkpoint si resume
def load_checkpoint():
    try:
        content = dbutils.fs.head(checkpoint_path, 1_000_000)
        return json.loads(content)
    except Exception:
        return {}

def save_checkpoint(done: dict):
    dbutils.fs.put(checkpoint_path, json.dumps(done, indent=2), overwrite=True)

already_done = {}
if resume_mode:
    already_done = load_checkpoint()
    if already_done:
        print(f"[RESUME] {len(already_done)} tables déjà clonées trouvées dans le checkpoint")

# COMMAND ----------
# DBTITLE 1, DEEP CLONE toutes les tables (avec checkpoint)
clone_results  = list(already_done.values())   # réinjecter les résultats précédents
total_size_gb  = sum(r.get("size_gb", 0) for r in clone_results)
pending_tables = [t for t in table_names if t not in already_done]

print(f"[INFO] {len(table_names)} tables au total — {len(pending_tables)} à traiter")

for i, fqn in enumerate(pending_tables, 1):
    catalog, schema, table = fqn.split(".")
    dest = f"{data_backup_root}/{catalog}/{schema}/{table}"
    ts   = datetime.utcnow().strftime("%H:%M:%S")

    print(f"[{ts}] ({i}/{len(pending_tables)}) Clonage de {fqn} ...")
    t0 = time.time()

    try:
        result  = spark.sql(
            f"CREATE OR REPLACE TABLE delta.`{dest}` DEEP CLONE `{catalog}`.`{schema}`.`{table}`"
        )
        metrics  = result.collect()[0].asDict()
        elapsed  = time.time() - t0
        size_gb  = metrics.get("num_output_bytes", 0) / (1024**3)
        total_size_gb += size_gb

        entry = {
            "table":    fqn,
            "status":   "success",
            "size_gb":  round(size_gb, 4),
            "duration_s": round(elapsed, 1),
        }
        print(f"  ✓ OK — {size_gb:.2f} GB en {elapsed:.0f}s")

    except Exception as e:
        elapsed = time.time() - t0
        err_str = str(e)
        # Vue ou format non supporté → skip (pas une vraie erreur de backup)
        if "DELTA_CLONE_UNSUPPORTED_SOURCE" in err_str or "format is View" in err_str:
            entry = {
                "table":      fqn,
                "status":     "skipped",
                "reason":     "format non cloneable (vue ou non-Delta)",
                "duration_s": round(elapsed, 1),
            }
            print(f"  [SKIP] Format non cloneable — {fqn}")
        else:
            entry = {
                "table":      fqn,
                "status":     "error",
                "error":      err_str,
                "duration_s": round(elapsed, 1),
            }
            print(f"  ✗ ERREUR — {e}")

    clone_results.append(entry)
    already_done[fqn] = entry

    # Checkpoint après chaque table
    save_checkpoint(already_done)

# COMMAND ----------
# DBTITLE 1, Sauvegarder le manifest final
dbutils.fs.put(clone_manifest_path, json.dumps(clone_results, indent=2), overwrite=True)

success_count = len([r for r in clone_results if r["status"] == "success"])
skip_count    = len([r for r in clone_results if r["status"] == "skipped"])
error_count   = len([r for r in clone_results if r["status"] == "error"])

print(f"\n[Résumé] {success_count} / {len(table_names)} tables clonées avec succès")
print(f"[Résumé] {skip_count} tables ignorées (vues / format non supporté)")
print(f"[Résumé] {error_count} erreurs réelles")
print(f"[Résumé] Volume total : {total_size_gb:.2f} GB")

if error_count > 0:
    errors = [r for r in clone_results if r["status"] == "error"]
    print("\n[Tables en erreur]")
    for r in errors:
        print(f"  - {r['table']}: {r.get('error', '?')}")

# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "clone_results":  clone_results,
    "total_size_gb":  round(total_size_gb, 3),
    "success_count":  success_count,
    "skip_count":     skip_count,
    "error_count":    error_count,
}))