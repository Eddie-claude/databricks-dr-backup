# Databricks notebook source
# notebooks/diag_01_audit.py
#
# AUDIT PRÉ-BACKUP — À lancer AVANT le premier backup
# Scanne tout Unity Catalog et produit :
#   - Inventaire tables (taille, fichiers, format)
#   - Tables nécessitant OPTIMIZE (petits fichiers → backup lent)
#   - Estimation durée backup J1 et J2+ (incrémental)
#   - Estimation coût stockage mensuel
# Aucune écriture, aucune modification — lecture seule.

# COMMAND ----------
# MAGIC %md # Audit Pré-Backup — Unity Catalog
# MAGIC
# MAGIC **Lecture seule — aucune modification de données.**
# MAGIC
# MAGIC | Paramètre | Description |
# MAGIC |-----------|-------------|
# MAGIC | `max_parallel` | Threads parallèles pour DESCRIBE DETAIL (8 recommandé) |
# MAGIC | `optimize_files_thresh` | Seuil fichiers/table au-delà duquel OPTIMIZE est recommandé |
# MAGIC | `output_json` | Afficher le JSON complet en fin de notebook (debug) |

# COMMAND ----------
import concurrent.futures
import json
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
dbutils.widgets.text("max_parallel",           "8",     "Threads parallèles pour DESCRIBE DETAIL")
dbutils.widgets.text("optimize_files_per_gb",  "50",   "Seuil fichiers/GB → OPTIMIZE recommandé")
dbutils.widgets.text("optimize_files_min",     "100",  "Nb fichiers minimum pour déclencher le flag (évite les faux positifs sur tables vides)")
dbutils.widgets.text("output_json",            "false", "Afficher JSON complet (true/false)")

max_parallel          = max(1, int(dbutils.widgets.get("max_parallel")))
optimize_files_per_gb = float(dbutils.widgets.get("optimize_files_per_gb"))
optimize_files_min    = int(dbutils.widgets.get("optimize_files_min"))
output_json           = dbutils.widgets.get("output_json").lower() == "true"

EXCLUDED_CATALOGS = {"hive_metastore", "system", "samples", "__databricks_internal"}
EXCLUDED_SCHEMAS  = {"information_schema"}

print(f"[OK] max_parallel={max_parallel} | seuil OPTIMIZE={optimize_files_per_gb} fichiers/GB (min {optimize_files_min} fichiers)")
print(f"[OK] Démarré à {datetime.now().strftime('%H:%M:%S')}")

# COMMAND ----------
# MAGIC %md ## 1 — Découverte des catalogs et tables

# COMMAND ----------
catalogs = [
    r.catalog for r in spark.sql("SHOW CATALOGS").collect()
    if r.catalog not in EXCLUDED_CATALOGS
]
print(f"[OK] {len(catalogs)} catalog(s) à analyser : {catalogs}")

table_fqns = []
for cat in catalogs:
    try:
        schemas = [
            r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{cat}`").collect()
            if r.databaseName not in EXCLUDED_SCHEMAS
        ]
    except Exception as e:
        print(f"[WARN] Impossible de lister les schemas de {cat}: {e}")
        continue

    for sch in schemas:
        try:
            rows = spark.sql(f"""
                SELECT table_name, table_type
                FROM `{cat}`.information_schema.tables
                WHERE table_schema = '{sch}'
                AND table_type IN ('MANAGED', 'EXTERNAL')
            """).collect()
            table_fqns += [(cat, sch, r.table_name, r.table_type) for r in rows]
        except Exception as e:
            try:
                rows = spark.sql(f"SHOW TABLES IN `{cat}`.`{sch}`").collect()
                table_fqns += [(cat, sch, r.tableName, "UNKNOWN") for r in rows]
            except Exception as e2:
                print(f"[WARN] {cat}.{sch}: {e2}")

print(f"[OK] {len(table_fqns)} tables découvertes")

# COMMAND ----------
# MAGIC %md ## 2 — DESCRIBE DETAIL (taille + fichiers par table)

# COMMAND ----------
def describe_table(args: tuple) -> dict:
    cat, sch, tbl, tbl_type = args
    fqn = f"`{cat}`.`{sch}`.`{tbl}`"
    try:
        d = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0].asDict()
        size_bytes = d.get("sizeInBytes") or 0
        num_files  = d.get("numFiles")    or 0
        return {
            "catalog":    cat,
            "schema":     sch,
            "table":      tbl,
            "fqn":        f"{cat}.{sch}.{tbl}",
            "table_type": tbl_type,
            "format":     d.get("format", "unknown"),
            "size_gb":    round(size_bytes / 1073741824, 4),
            "num_files":  num_files,
            "files_per_gb": round(num_files / max(size_bytes / 1073741824, 0.001), 1),
            "location":   d.get("location", ""),
            "status":     "ok",
        }
    except Exception as e:
        return {
            "catalog": cat, "schema": sch, "table": tbl,
            "fqn": f"{cat}.{sch}.{tbl}",
            "table_type": tbl_type, "format": "unknown",
            "size_gb": 0, "num_files": 0, "files_per_gb": 0,
            "location": "", "status": "error", "error": str(e),
        }

print(f"[INFO] Analyse de {len(table_fqns)} tables avec {max_parallel} threads...")
with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
    results = list(ex.map(describe_table, table_fqns))

ok        = [r for r in results if r["status"] == "ok"]
errors    = [r for r in results if r["status"] == "error"]
delta_ok  = [r for r in ok if r["format"] == "delta"]
non_delta = [r for r in ok if r["format"] != "delta"]

print(f"[OK] Analyse terminée — {len(ok)} succès, {len(errors)} erreurs")

# COMMAND ----------
# MAGIC %md ## 3 — Calcul des métriques et recommandations

# COMMAND ----------
total_size_gb  = sum(r["size_gb"]  for r in ok)
total_files    = sum(r["num_files"] for r in ok)
delta_size_gb  = sum(r["size_gb"]  for r in delta_ok)
delta_files    = sum(r["num_files"] for r in delta_ok)

# Tables nécessitant OPTIMIZE
# Double condition : ratio fichiers/GB élevé ET nombre minimum absolu
# → fonctionne aussi bien pour les petites que les très grosses tables
optimize_needed = [
    r for r in delta_ok
    if r["files_per_gb"] > optimize_files_per_gb and r["num_files"] >= optimize_files_min
]
optimize_size_gb = sum(r["size_gb"]  for r in optimize_needed)
optimize_files   = sum(r["num_files"] for r in optimize_needed)

# Durée estimée (base : 209 MB/s mesuré après OPTIMIZE sur ADLS Gen2 Switzerland North)
SPEED_MBPS = 209

# J1 sans OPTIMIZE : les tables à fort nombre de fichiers seront très lentes
# (les petits fichiers = overhead S3/ADLS massif → vitesse effective ~5-20 MB/s)
j1_no_opt_h = (
    (delta_size_gb - optimize_size_gb) * 1024 / SPEED_MBPS +   # tables OK
    optimize_size_gb * 1024 / 15                                 # tables small-files ~15 MB/s
) / 3600

# J1 après OPTIMIZE : tout tourne à 209 MB/s
j1_opt_h = (delta_size_gb * 1024 / SPEED_MBPS) / 3600

# J2+ incrémental : 2-5% de changement quotidien estimé
j2_low_h  = j1_opt_h * 0.02
j2_high_h = j1_opt_h * 0.05

# Estimation coût stockage (ADLS Gen2 Switzerland North LRS ~0.023 $/GB/mois)
COST_PER_GB = 0.023
# Incrémental : base + 15j de deltas (2% changement/j) + 1 monthly (17 TB)
storage_incremental_gb = (
    total_size_gb +                          # incremental/ base
    total_size_gb * 0.02 * 15 +             # deltas daily (15j × 2%)
    total_size_gb                            # monthly archive
)
cost_incremental = storage_incremental_gb * COST_PER_GB

# COMMAND ----------
# MAGIC %md ## 4 — Rapport

# COMMAND ----------
print(f"""
╔══════════════════════════════════════════════════════════════════╗
║            AUDIT PRÉ-BACKUP — UNITY CATALOG                    ║
╠══════════════════════════════════════════════════════════════════╣
║  INVENTAIRE                                                     ║
║    Catalogs analysés       : {len(catalogs):<5}                           ║
║    Tables totales          : {len(table_fqns):<5}  ({len(ok)} OK / {len(errors)} erreurs)    ║
║    Tables Delta (backup)   : {len(delta_ok):<5}                           ║
║    Tables non-Delta        : {len(non_delta):<5}  (non cloneables — ignorées)   ║
╠══════════════════════════════════════════════════════════════════╣
║  VOLUME                                                         ║
║    Total toutes tables     : {total_size_gb:>10.2f} GB                    ║
║    Total tables Delta      : {delta_size_gb:>10.2f} GB                    ║
║    Nombre de fichiers total : {total_files:>10,}                   ║
╠══════════════════════════════════════════════════════════════════╣
║  OPTIMIZE — PETITS FICHIERS (seuil : >{optimize_files_per_gb} fichiers/GB et >={optimize_files_min} fichiers)
║    Tables à optimiser      : {len(optimize_needed):<5}                           ║
║    Volume concerné         : {optimize_size_gb:>10.2f} GB                    ║
║    Fichiers à compacter    : {optimize_files:>10,}                   ║
╠══════════════════════════════════════════════════════════════════╣
║  DURÉE ESTIMÉE (ADLS Gen2 CH-North, 4 workers, parallel=8)     ║
║    J1 SANS OPTIMIZE        : {j1_no_opt_h:>7.1f} h (small-files ~15 MB/s)  ║
║    J1 APRÈS OPTIMIZE       : {j1_opt_h:>7.1f} h (cible 209 MB/s)       ║
║    J2+ (incrémental)       : {j2_low_h:.1f} - {j2_high_h:.1f} h (2-5% changement/j)  ║
╠══════════════════════════════════════════════════════════════════╣
║  COÛT STOCKAGE ESTIMÉ (stratégie incrémentale)                 ║
║    Stockage total (base+deltas+monthly) : {storage_incremental_gb:>8.0f} GB           ║
║    Coût mensuel estimé      : ${cost_incremental:>8.0f} /mois (ADLS LRS)  ║
╚══════════════════════════════════════════════════════════════════╝
""")

# ── Top 30 tables par taille ──────────────────────────────────────────────────
top_tables = sorted(ok, key=lambda r: r["size_gb"], reverse=True)[:30]
print(f"\n{'─'*90}")
print(f"{'TOP 30 TABLES PAR TAILLE':^90}")
print(f"{'─'*90}")
print(f"{'Catalog.Schema.Table':<52} {'Format':<8} {'GB':>8} {'Fichiers':>10} {'Fichiers/GB':>12} {'OPTIMIZE':>9}")
print(f"{'─'*90}")
for r in top_tables:
    flag = "⚠ OUI" if (r["files_per_gb"] > optimize_files_per_gb and r["num_files"] >= optimize_files_min) else ""
    print(f"{r['fqn']:<52} {r['format']:<8} {r['size_gb']:>8.2f} {r['num_files']:>10,} {r['files_per_gb']:>12.1f} {flag:>9}")

# ── Tables nécessitant OPTIMIZE ───────────────────────────────────────────────
if optimize_needed:
    print(f"\n{'─'*80}")
    print(f"{'TABLES NÉCESSITANT OPTIMIZE (>' + str(optimize_files_per_gb) + ' fichiers/GB, >=' + str(optimize_files_min) + ' fichiers)':^80}")
    print(f"{'─'*80}")
    print(f"{'Catalog.Schema.Table':<52} {'GB':>8} {'Fichiers':>10} {'Fichiers/GB':>10}")
    print(f"{'─'*80}")
    for r in sorted(optimize_needed, key=lambda x: x["num_files"], reverse=True):
        print(f"{r['fqn']:<52} {r['size_gb']:>8.2f} {r['num_files']:>10,} {r['files_per_gb']:>10.1f}")
    print(f"\n  → Commande à exécuter sur chaque table avant le backup initial :")
    print(f"     OPTIMIZE <catalog>.<schema>.<table>;")
    print(f"     ALTER TABLE <catalog>.<schema>.<table>")
    print(f"       SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',")
    print(f"                          'delta.autoOptimize.autoCompact'   = 'true');")

# ── Erreurs ───────────────────────────────────────────────────────────────────
if errors:
    print(f"\n[WARN] {len(errors)} tables inaccessibles (DESCRIBE DETAIL échoué) :")
    for r in errors:
        print(f"  - {r['fqn']}: {r.get('error', '')[:100]}")

# ── JSON complet (optionnel) ──────────────────────────────────────────────────
if output_json:
    print("\n[JSON complet — toutes les tables]")
    print(json.dumps(sorted(results, key=lambda r: r.get("size_gb", 0), reverse=True), indent=2))
