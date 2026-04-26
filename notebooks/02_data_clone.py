# Databricks notebook source
# notebooks/02_data_clone.py

# COMMAND ----------
# MAGIC %md # 02 — Data Clone (DEEP CLONE + checkpoint + resume + parallel)

# COMMAND ----------
import concurrent.futures
import json
import threading
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
dbutils.widgets.text("backup_root",        "",               "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",        str(__import__('datetime').date.today()), "Date backup YYYY-MM-DD")
dbutils.widgets.text("uc_metadata_result", "{}",             "JSON result from 01_uc_metadata")
dbutils.widgets.text("resume",             "true",           "Reprendre depuis checkpoint (true/false)")
dbutils.widgets.text("max_parallel",       "4",              "Clones simultanés (1 = séquentiel)")

backup_root  = dbutils.widgets.get("backup_root")
backup_date  = dbutils.widgets.get("backup_date")
uc_result    = json.loads(dbutils.widgets.get("uc_metadata_result"))
resume_mode  = dbutils.widgets.get("resume").lower() == "true"
max_parallel = max(1, int(dbutils.widgets.get("max_parallel")))

# Schémas UC système : vues uniquement, non cloneables par DEEP CLONE
_EXCLUDED_SCHEMAS = {"information_schema"}

table_names = [
    t for t in uc_result.get("table_names", [])
    if len(t.split(".")) == 3 and t.split(".")[1] not in _EXCLUDED_SCHEMAS
]

# Fallback : si lancé sans orchestrateur, auto-découverte depuis Unity Catalog
if not table_names:
    print("[INFO] uc_metadata_result vide — auto-découverte des tables depuis Unity Catalog")
    _EXCLUDED_CATALOGS = {"hive_metastore", "system", "samples"}
    for _cat in [r.catalog for r in spark.sql("SHOW CATALOGS").collect()
                 if r.catalog not in _EXCLUDED_CATALOGS]:
        for _sch in [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{_cat}`").collect()
                     if r.databaseName not in _EXCLUDED_SCHEMAS]:
            try:
                _tables = {
                    r.table_name for r in spark.sql(f"""
                        SELECT table_name FROM `{_cat}`.information_schema.tables
                        WHERE table_schema = '{_sch}'
                        AND table_type IN ('MANAGED', 'EXTERNAL')
                    """).collect()
                }
                table_names += [f"{_cat}.{_sch}.{t}" for t in sorted(_tables)]
            except Exception as _e:
                print(f"[WARN] {_cat}.{_sch}: {_e}")
    print(f"[INFO] {len(table_names)} tables découvertes automatiquement")

data_backup_root    = f"{backup_root}/{backup_date}/data"
clone_manifest_path = f"{backup_root}/{backup_date}/data/_clone_manifest.json"
checkpoint_path     = f"{backup_root}/{backup_date}/data/_checkpoint.json"

print(f"[INFO] max_parallel={max_parallel} | tables={len(table_names)} | resume={resume_mode}")

# COMMAND ----------
# DBTITLE 1, Checkpoint (lecture + écriture thread-safe)

_checkpoint_lock = threading.Lock()

def load_checkpoint():
    try:
        return json.loads(dbutils.fs.head(checkpoint_path, 1_000_000))
    except Exception:
        return {}

def save_checkpoint(done: dict):
    with _checkpoint_lock:
        dbutils.fs.put(checkpoint_path, json.dumps(done, indent=2), overwrite=True)

already_done: dict = {}
if resume_mode:
    already_done = load_checkpoint()
    if already_done:
        print(f"[RESUME] {len(already_done)} tables déjà clonées trouvées dans le checkpoint")

# COMMAND ----------
# DBTITLE 1, DEEP CLONE (parallèle)

pending_tables = [t for t in table_names if t not in already_done]
total          = len(table_names)
pending_total  = len(pending_tables)

print(f"[INFO] {total} tables au total — {pending_total} à traiter")

def clone_one(args: tuple) -> dict:
    idx, fqn = args
    catalog, schema, table = fqn.split(".")
    dest = f"{data_backup_root}/{catalog}/{schema}/{table}"
    ts   = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] ({idx}/{pending_total}) → {fqn}")
    t0   = time.time()

    try:
        result  = spark.sql(
            f"CREATE OR REPLACE TABLE delta.`{dest}` DEEP CLONE `{catalog}`.`{schema}`.`{table}`"
        )
        metrics = result.collect()[0].asDict()
        elapsed = time.time() - t0
        size_gb = metrics.get("num_output_bytes", 0) / (1024**3)
        entry   = {
            "table":      fqn,
            "status":     "success",
            "size_gb":    round(size_gb, 4),
            "duration_s": round(elapsed, 1),
        }
        print(f"  ✓ {fqn} — {size_gb:.2f} GB en {elapsed:.0f}s")

    except Exception as e:
        elapsed = time.time() - t0
        err_str = str(e)
        if "DELTA_CLONE_UNSUPPORTED_SOURCE" in err_str or "format is View" in err_str:
            entry = {
                "table":      fqn,
                "status":     "skipped",
                "reason":     "vue ou format non cloneable",
                "duration_s": round(elapsed, 1),
            }
            print(f"  [SKIP] {fqn}")
        else:
            entry = {
                "table":      fqn,
                "status":     "error",
                "error":      err_str,
                "duration_s": round(elapsed, 1),
            }
            print(f"  ✗ ERREUR {fqn} — {e}")

    # Mise à jour checkpoint thread-safe
    already_done[fqn] = entry
    save_checkpoint(already_done)
    return entry

args_list = [(i + 1, fqn) for i, fqn in enumerate(pending_tables)]

with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as executor:
    new_results = list(executor.map(clone_one, args_list))

# Résultats complets (checkpoint précédent + nouveau run)
clone_results = list(already_done.values())
total_size_gb = sum(r.get("size_gb", 0) for r in clone_results)

# COMMAND ----------
# DBTITLE 1, Manifest final

dbutils.fs.put(clone_manifest_path, json.dumps(clone_results, indent=2), overwrite=True)

success_count = len([r for r in clone_results if r["status"] == "success"])
skip_count    = len([r for r in clone_results if r["status"] == "skipped"])
error_count   = len([r for r in clone_results if r["status"] == "error"])

print(f"""
╔══════════════════════════════════════════╗
║         DATA CLONE — RÉSUMÉ             ║
╠══════════════════════════════════════════╣
║  Succès   : {success_count:<29} ║
║  Ignorées : {skip_count:<29} ║
║  Erreurs  : {error_count:<29} ║
║  Volume   : {total_size_gb:<25.2f} GB ║
╚══════════════════════════════════════════╝
""")

if error_count > 0:
    print("[Tables en erreur]")
    for r in clone_results:
        if r["status"] == "error":
            print(f"  - {r['table']}: {r.get('error', '?')[:120]}")

# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "clone_results":  clone_results,
    "total_size_gb":  round(total_size_gb, 3),
    "success_count":  success_count,
    "skip_count":     skip_count,
    "error_count":    error_count,
}))
