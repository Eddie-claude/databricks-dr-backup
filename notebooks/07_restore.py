# Databricks notebook source
# notebooks/07_restore.py

# COMMAND ----------
# MAGIC %md # 07 — Restauration DR Backup
# MAGIC
# MAGIC Ce notebook guide la restauration de tables depuis le backup ADLS.
# MAGIC
# MAGIC | Niveau | Source | Point de restauration |
# MAGIC |--------|--------|----------------------|
# MAGIC | `incremental` | `backup/incremental/` | N'importe quel timestamp dans les 15 derniers jours |
# MAGIC | `weekly` | `backup/snapshots/weekly/` | Une semaine précise (ex: `2026-W19`) |
# MAGIC | `monthly` | `backup/snapshots/monthly/` | Un mois précis (ex: `2026-05`) |
# MAGIC
# MAGIC **Mode dry_run = true** : affiche les commandes sans exécuter — recommandé avant toute restauration.

# COMMAND ----------
import json
import re
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# MAGIC %md ## Paramètres

# COMMAND ----------
dbutils.widgets.text(     "backup_root",    "", "Backup root (abfss://...)")
dbutils.widgets.dropdown( "restore_level",  "incremental", ["incremental", "weekly", "monthly"], "Niveau de restauration")
dbutils.widgets.text(     "source_table",   "", "Table(s) source : catalog.schema.table  |  catalog.schema.*  |  catalog.*.*")
dbutils.widgets.text(     "restore_point",  "", "Point de restauration : timestamp (2026-05-10T14:00:00) ou label (2026-W19 / 2026-05)")
dbutils.widgets.text(     "target_catalog", "", "Catalog cible (vide = même que la source)")
dbutils.widgets.text(     "target_schema",  "", "Schema cible (vide = même que la source)")
dbutils.widgets.dropdown( "dry_run",        "true", ["true", "false"], "Dry-run (true = simulation)")

backup_root    = dbutils.widgets.get("backup_root").rstrip("/")
restore_level  = dbutils.widgets.get("restore_level")
source_table   = dbutils.widgets.get("source_table").strip()
restore_point  = dbutils.widgets.get("restore_point").strip()
target_catalog = dbutils.widgets.get("target_catalog").strip()
target_schema  = dbutils.widgets.get("target_schema").strip()
dry_run        = dbutils.widgets.get("dry_run").lower() == "true"

print(f"[OK] backup_root    = {backup_root}")
print(f"[OK] restore_level  = {restore_level}")
print(f"[OK] source_table   = {source_table}")
print(f"[OK] restore_point  = {restore_point}")
print(f"[OK] target_catalog = {target_catalog or '(même que source)'}")
print(f"[OK] target_schema  = {target_schema  or '(même que source)'}")
print(f"[OK] dry_run        = {dry_run}")

# COMMAND ----------
# MAGIC %md ## Étape 1 — Snapshots disponibles

# COMMAND ----------
incremental_root = f"{backup_root}/incremental"
weekly_root      = f"{backup_root}/snapshots/weekly"
monthly_root     = f"{backup_root}/snapshots/monthly"

WEEK_PAT  = re.compile(r"^\d{4}-W\d{2}$")
MONTH_PAT = re.compile(r"^\d{4}-\d{2}$")

def list_dirs(path, pattern=None):
    try:
        items = [f.name.rstrip("/") for f in dbutils.fs.ls(path)
                 if not f.name.startswith("_") and not f.name.startswith(".")]
        if pattern:
            items = [i for i in items if pattern.match(i)]
        return sorted(items)
    except Exception:
        return []

weekly_available  = list_dirs(weekly_root,  WEEK_PAT)
monthly_available = list_dirs(monthly_root, MONTH_PAT)

print("── Snapshots disponibles ─────────────────────────────")
print(f"  Incremental : point-in-time sur les 15 derniers jours")
if weekly_available:
    print(f"  Weekly      : {', '.join(weekly_available)}")
else:
    print("  Weekly      : (aucun snapshot)")
if monthly_available:
    print(f"  Monthly     : {', '.join(monthly_available)}")
else:
    print("  Monthly     : (aucun snapshot)")

# COMMAND ----------
# MAGIC %md ## Étape 2 — Tables disponibles dans le backup

# COMMAND ----------
def list_tables_in_backup(root_path):
    """Retourne la liste des tables (catalog/schema/table) présentes sous root_path."""
    tables = []
    try:
        for cat in list_dirs(root_path):
            for sch in list_dirs(f"{root_path}/{cat}"):
                for tbl in list_dirs(f"{root_path}/{cat}/{sch}"):
                    tables.append({"catalog": cat, "schema": sch, "table": tbl,
                                   "path": f"{root_path}/{cat}/{sch}/{tbl}"})
    except Exception as e:
        print(f"[WARN] Impossible de lister les tables : {e}")
    return tables

if restore_level == "incremental":
    scan_root = incremental_root
elif restore_level == "weekly":
    if not restore_point:
        print("[WARN] Renseignez restore_point avec un label weekly (ex: 2026-W19)")
        dbutils.notebook.exit("restore_point manquant")
    scan_root = f"{weekly_root}/{restore_point}"
else:  # monthly
    if not restore_point:
        print("[WARN] Renseignez restore_point avec un label monthly (ex: 2026-05)")
        dbutils.notebook.exit("restore_point manquant")
    scan_root = f"{monthly_root}/{restore_point}"

all_backup_tables = list_tables_in_backup(scan_root)

print(f"\n── {len(all_backup_tables)} tables disponibles dans [{restore_level}] ──")
for t in all_backup_tables[:30]:
    print(f"  {t['catalog']}.{t['schema']}.{t['table']}")
if len(all_backup_tables) > 30:
    print(f"  ... et {len(all_backup_tables) - 30} autres")

# COMMAND ----------
# MAGIC %md ## Étape 3 — Sélection des tables à restaurer

# COMMAND ----------
def parse_source_pattern(source_table):
    """
    Accepte :
      catalog.schema.table   → une table précise
      catalog.schema.*        → toutes les tables du schéma
      catalog.*.*             → toutes les tables du catalog
    """
    parts = source_table.split(".")
    if len(parts) != 3:
        raise ValueError(f"Format invalide : '{source_table}'. Attendu : catalog.schema.table")
    return parts[0], parts[1], parts[2]

if not source_table:
    print("[INFO] source_table vide — toutes les tables du backup seront restaurées.")
    tables_to_restore = all_backup_tables
else:
    try:
        src_cat, src_sch, src_tbl = parse_source_pattern(source_table)
        tables_to_restore = [
            t for t in all_backup_tables
            if (src_cat == "*" or t["catalog"] == src_cat)
            and (src_sch == "*" or t["schema"]  == src_sch)
            and (src_tbl == "*" or t["table"]   == src_tbl)
        ]
    except ValueError as e:
        print(f"[ERROR] {e}")
        dbutils.notebook.exit(str(e))

print(f"\n── {len(tables_to_restore)} table(s) sélectionnée(s) pour restauration ──")
for t in tables_to_restore:
    print(f"  {t['catalog']}.{t['schema']}.{t['table']}")

if not tables_to_restore:
    print("[WARN] Aucune table ne correspond à la sélection.")
    dbutils.notebook.exit("Aucune table sélectionnée")

# COMMAND ----------
# MAGIC %md ## Étape 4 — Génération et exécution des commandes

# COMMAND ----------
def build_restore_sql(t, restore_level, restore_point, target_catalog, target_schema, backup_root):
    src_path  = t["path"]
    tgt_cat   = target_catalog if target_catalog else t["catalog"]
    tgt_sch   = target_schema  if target_schema  else t["schema"]
    tgt_tbl   = t["table"]
    target_uc = f"`{tgt_cat}`.`{tgt_sch}`.`{tgt_tbl}`"

    if restore_level == "incremental":
        if restore_point:
            # DEEP CLONE avec timestamp — recrée la table dans l'état voulu
            return (
                f"CREATE OR REPLACE TABLE {target_uc}\n"
                f"  DEEP CLONE delta.`{src_path}`\n"
                f"  TIMESTAMP AS OF '{restore_point}';"
            )
        else:
            # Sans timestamp → état du dernier backup
            return (
                f"CREATE OR REPLACE TABLE {target_uc}\n"
                f"  DEEP CLONE delta.`{src_path}`;"
            )
    else:
        # weekly ou monthly : DEEP CLONE simple depuis le snapshot
        return (
            f"CREATE OR REPLACE TABLE {target_uc}\n"
            f"  DEEP CLONE delta.`{src_path}`;"
        )

results = []
ok_count    = 0
error_count = 0

prefix = "[DRY-RUN] " if dry_run else ""

print(f"\n{'─'*60}")
print(f"{prefix}Restauration — niveau={restore_level} | point={restore_point or 'dernier backup'}")
print(f"{'─'*60}\n")

for t in tables_to_restore:
    sql = build_restore_sql(t, restore_level, restore_point, target_catalog, target_schema, backup_root)
    tgt_cat = target_catalog if target_catalog else t["catalog"]
    tgt_sch = target_schema  if target_schema  else t["schema"]
    label   = f"{tgt_cat}.{tgt_sch}.{t['table']}"

    print(f"{'── ' if dry_run else '▶  '}{label}")
    print(f"   {sql}\n")

    if not dry_run:
        try:
            spark.sql(sql)
            results.append({"table": label, "status": "success", "sql": sql})
            ok_count += 1
            print(f"   [OK] Restaurée\n")
        except Exception as e:
            results.append({"table": label, "status": "error", "error": str(e), "sql": sql})
            error_count += 1
            print(f"   [ERROR] {e}\n")
    else:
        results.append({"table": label, "status": "dry_run", "sql": sql})

# COMMAND ----------
# MAGIC %md ## Étape 5 — Vérification post-restauration

# COMMAND ----------
if not dry_run and ok_count > 0:
    print("── Vérification des tables restaurées ──────────────────")
    for r in results:
        if r["status"] == "success":
            tgt_cat, tgt_sch, tgt_tbl = r["table"].split(".")
            try:
                count = spark.sql(f"SELECT COUNT(*) AS n FROM `{tgt_cat}`.`{tgt_sch}`.`{tgt_tbl}`").collect()[0]["n"]
                print(f"  [OK] {r['table']} — {count:,} lignes")
            except Exception as e:
                print(f"  [WARN] {r['table']} — impossible de compter : {e}")

# COMMAND ----------
# MAGIC %md ## Résumé

# COMMAND ----------
prefix_label = "DRY-RUN — " if dry_run else ""
print(f"""
╔══════════════════════════════════════════════════════╗
║          {prefix_label}RESTAURATION — RÉSUMÉ
╠══════════════════════════════════════════════════════╣
║  Niveau          : {restore_level:<34} ║
║  Point           : {(restore_point or 'dernier backup'):<34} ║
║  Tables sélect.  : {len(tables_to_restore):<34} ║
║  Restaurées OK   : {ok_count:<34} ║
║  Erreurs         : {error_count:<34} ║
╚══════════════════════════════════════════════════════╝
""")

if dry_run:
    print("Mode DRY-RUN : aucune table n'a été modifiée.")
    print("Pour exécuter la restauration, repasser dry_run = false.")

summary = {
    "restore_level":  restore_level,
    "restore_point":  restore_point,
    "tables_count":   len(tables_to_restore),
    "ok":             ok_count,
    "errors":         error_count,
    "dry_run":        dry_run,
    "results":        results,
}
dbutils.notebook.exit(json.dumps(summary))
