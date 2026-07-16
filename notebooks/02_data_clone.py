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

# dbutils.fs.put/head bypasse UC External Locations et exige une clé ABFS cluster.
# Ces helpers passent par Spark (UC credentials) pour écrire/lire sur ADLS.
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

def _uc_head(path: str) -> str:
    return "\n".join(r.value for r in spark.read.text(path).collect())

# COMMAND ----------
dbutils.widgets.text("backup_root",        "",               "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",        str(__import__('datetime').date.today()), "Date backup YYYY-MM-DD")
dbutils.widgets.text("uc_metadata_result", "{}",             "JSON result from 01_uc_metadata")
dbutils.widgets.text("resume",             "true",           "Reprendre depuis checkpoint (true/false)")
dbutils.widgets.text("max_parallel",       "4",              "Clones simultanés (1 = séquentiel)")
dbutils.widgets.text("retain_daily",       "30",             "Rétention quotidienne (jours) — Delta log")

backup_root        = dbutils.widgets.get("backup_root")
backup_date        = dbutils.widgets.get("backup_date")
uc_result          = json.loads(dbutils.widgets.get("uc_metadata_result"))
resume_mode        = dbutils.widgets.get("resume").lower() == "true"
max_parallel       = max(1, int(dbutils.widgets.get("max_parallel")))
retain_daily       = max(1, int(dbutils.widgets.get("retain_daily")))
# Fichiers supprimés conservés assez longtemps pour couvrir la fenêtre de restauration point-in-time.
# Plus de terme "weekly" : le snapshot weekly (SHALLOW CLONE) est abandonné — il n'est de toute
# façon plus supporté par Unity Catalog sur des tables non-MANAGED (CANNOT_SHALLOW_CLONE_...).
max_file_retention = retain_daily + 2

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

data_backup_root    = f"{backup_root}/incremental"
clone_manifest_path = f"{backup_root}/incremental/_manifests/{backup_date}.json"
checkpoint_path     = f"{backup_root}/incremental/_checkpoints/{backup_date}.json"

print(f"[INFO] max_parallel={max_parallel} | tables={len(table_names)} | resume={resume_mode}")

# COMMAND ----------
# DBTITLE 1, Checkpoint (lecture + écriture thread-safe)

_checkpoint_lock  = threading.Lock()
_checkpoint_dirty = threading.Event()
_checkpoint_count = 0
CHECKPOINT_EVERY  = 5  # écrire sur ADLS toutes les N tables

def load_checkpoint():
    try:
        return json.loads(_uc_head(checkpoint_path))
    except Exception:
        return {}

def save_checkpoint(done: dict):
    global _checkpoint_count
    with _checkpoint_lock:
        _checkpoint_count += 1
        if _checkpoint_count % CHECKPOINT_EVERY == 0:
            _uc_put(checkpoint_path, json.dumps(done, indent=2))

def flush_checkpoint(done: dict):
    with _checkpoint_lock:
        _uc_put(checkpoint_path, json.dumps(done, indent=2))

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
        size_gb = (
            metrics.get("copied_files_size") or
            metrics.get("num_output_bytes") or
            metrics.get("source_table_size") or 0
        ) / (1024**3)
        num_files = metrics.get("num_copied_files", 0)
        entry = {
            "table":        fqn,
            "status":       "success",
            "size_gb":      round(size_gb, 4),
            "num_files":    num_files,
            "duration_s":   round(elapsed, 1),
            "incremental":  num_files == 0,
        }
        suffix = " (aucun changement)" if num_files == 0 else f" — {size_gb:.2f} GB ({num_files} fichiers)"
        print(f"  ✓ {fqn}{suffix} en {elapsed:.0f}s")
        try:
            spark.sql(f"""
              ALTER TABLE delta.`{dest}` SET TBLPROPERTIES (
                'delta.logRetentionDuration'         = 'interval {retain_daily + 1} days',
                'delta.deletedFileRetentionDuration' = 'interval {max_file_retention} days'
              )
            """)
        except Exception as _e:
            print(f"  [WARN] Propriétés Delta non définies sur {dest}: {_e}")

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

flush_checkpoint(already_done)  # garantit l'écriture finale

# Résultats complets (checkpoint précédent + nouveau run)
clone_results = list(already_done.values())
total_size_gb = sum(r.get("size_gb", 0) for r in clone_results)

# COMMAND ----------
# DBTITLE 1, Manifest final

_uc_put(clone_manifest_path, json.dumps(clone_results, indent=2))

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
