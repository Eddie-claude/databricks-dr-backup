# Databricks notebook source
# notebooks/12_restore_uc_objects.py

# COMMAND ----------
# MAGIC %md # 12 — Restauration Volumes + Fonctions Unity Catalog
# MAGIC
# MAGIC Rejoue les DDL sauvegardés par `01_uc_metadata` :
# MAGIC
# MAGIC | Fichier | Statements |
# MAGIC |---------|-----------|
# MAGIC | `uc_metadata/05_volumes.sql`   | `CREATE [EXTERNAL] VOLUME IF NOT EXISTS ...` |
# MAGIC | `uc_metadata/06_functions.sql` | `CREATE FUNCTION IF NOT EXISTS ...` (SQL et Python) |
# MAGIC
# MAGIC **Remarques :**
# MAGIC - À lancer **après** la restauration des tables (une fonction SQL peut lire une table)
# MAGIC   et **avant** celle des grants (les grants VOLUME/FUNCTION visent ces objets).
# MAGIC - Les objets déjà existants ne sont pas modifiés (`IF NOT EXISTS`).
# MAGIC - Seule la définition des volumes est restaurée ici. Un volume EXTERNAL retrouve ses fichiers
# MAGIC   (ils sont restés sur son stockage, l'External Location doit exister) ; un volume MANAGED est
# MAGIC   recréé vide, ses fichiers se restaurent ensuite avec `15_restore_volume_files` (scope `volume_files`).
# MAGIC - Les fonctions qui en appellent d'autres sont rejouées en plusieurs passes.
# MAGIC - **dry_run = true** : affiche les statements sans les exécuter.

# COMMAND ----------
import json
import re
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def _uc_head(path: str) -> str:
    # spark.read.text(path) peut lire 0 octet sur ce chemin même juste après une réécriture
    # complète dans la même session (observé en prod, confirmé via le panneau performance —
    # "Bytes read: 0 B") — dbutils.fs.head est plus fiable ici.
    return dbutils.fs.head(path, 10_000_000)

# COMMAND ----------
# MAGIC %md ## Paramètres

# COMMAND ----------
dbutils.widgets.text(    "backup_root",    "", "Backup root (abfss://...)")
dbutils.widgets.text(    "backup_date",    "", "Date du backup (YYYY-MM-DD)")
dbutils.widgets.text(    "catalog_filter", "", "Catalogs à restaurer (séparés par virgule, vide = tous)")
# lib/ est déployé par le bundle à côté de notebooks/ : …/files/notebooks/x → …/files/lib
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_path = _nb_path if _nb_path.startswith("/Workspace") else "/Workspace" + _nb_path
dbutils.widgets.text("lib_path", _nb_path.rsplit("/notebooks/", 1)[0] + "/lib", "Chemin vers lib/")
dbutils.widgets.dropdown("dry_run",        "true", ["true", "false"], "Dry-run (true = simulation)")

backup_root    = dbutils.widgets.get("backup_root").strip().rstrip("/")
backup_date    = dbutils.widgets.get("backup_date").strip()
catalog_filter = dbutils.widgets.get("catalog_filter").strip()
lib_path       = dbutils.widgets.get("lib_path").strip()
dry_run        = dbutils.widgets.get("dry_run").lower() == "true"

if not backup_root:
    raise ValueError("backup_root est vide — renseignez le chemin abfss://...")
if not backup_date:
    raise ValueError("backup_date est vide — renseignez la date du backup (YYYY-MM-DD)")

sys.path.insert(0, lib_path)
from uc_ddl import split_sql_statements

metadata_path       = f"{backup_root}/{backup_date}/uc_metadata"
catalogs_to_restore = {c.strip().lower() for c in catalog_filter.split(",") if c.strip()}
prefix              = "[DRY-RUN] " if dry_run else ""

print(f"[OK] backup_root     = {backup_root}")
print(f"[OK] backup_date     = {backup_date}")
print(f"[OK] catalog_filter  = {catalog_filter or '(tous)'}")
print(f"[OK] dry_run         = {dry_run}")

# COMMAND ----------
# MAGIC %md ## Étape 1 — Chargement des DDL

# COMMAND ----------
CREATE_PAT  = re.compile(r"^CREATE\s+(?:EXTERNAL\s+)?(?:VOLUME|FUNCTION)\s+IF\s+NOT\s+EXISTS\s+`([^`]+)`", re.IGNORECASE)
FQN_PAT     = re.compile(r"`[^`]+`\.`[^`]+`\.`[^`]+`")

def load_statements(file_name: str) -> list:
    path = f"{metadata_path}/{file_name}"
    try:
        raw = _uc_head(path)
    except Exception as e:
        # Backups antérieurs à l'ajout des volumes/fonctions : fichier absent.
        print(f"[WARN] {file_name} introuvable ({e.__class__.__name__}) — backup antérieur à l'export volumes/fonctions ?")
        return []
    selected, skipped = [], 0
    for stmt in split_sql_statements(raw):
        m = CREATE_PAT.match(stmt)
        if not m:
            # Sécurité : uniquement les CREATE attendus dans ces fichiers
            print(f"  [SKIP-INVALID] {stmt[:80]}")
            skipped += 1
            continue
        if catalogs_to_restore and m.group(1).lower() not in catalogs_to_restore:
            skipped += 1
            continue
        selected.append(stmt)
    print(f"[OK] {file_name} : {len(selected)} statement(s) retenu(s), {skipped} ignoré(s)")
    return selected

volume_stmts   = load_statements("05_volumes.sql")
function_stmts = load_statements("06_functions.sql")

# COMMAND ----------
# MAGIC %md ## Étape 2 — Exécution

# COMMAND ----------
results = []

def label_of(stmt: str) -> str:
    m = FQN_PAT.search(stmt)
    return m.group(0).replace("`", "") if m else stmt[:60]

def execute(stmts: list, kind: str, max_passes: int = 1) -> None:
    """
    Exécute les statements ; ceux en échec sont rejoués à la passe suivante tant
    qu'au moins un a réussi (dépendances entre fonctions dans un ordre quelconque).
    """
    if dry_run:
        for stmt in stmts:
            print(f"  ── {stmt}\n")
            results.append({"kind": kind, "object": label_of(stmt), "status": "dry_run"})
        return

    pending = list(stmts)
    for pass_no in range(1, max_passes + 1):
        failed = []
        for stmt in pending:
            try:
                spark.sql(stmt)
                print(f"  [OK] {kind} {label_of(stmt)}")
                results.append({"kind": kind, "object": label_of(stmt), "status": "success"})
            except Exception as e:
                failed.append((stmt, str(e)))
        # Arrêt : tout est passé, ou plus aucun progrès possible
        if not failed or len(failed) == len(pending) or pass_no == max_passes:
            break
        print(f"  [RETRY] passe {pass_no + 1} : {len(failed)} {kind}(s) en attente de dépendances")
        pending = [s for s, _ in failed]

    for stmt, err in failed:
        print(f"  [ERROR] {kind} {label_of(stmt)}\n          → {err[:200]}")
        results.append({"kind": kind, "object": label_of(stmt), "status": "error", "error": err[:300]})

print(f"\n{'─'*60}\n{prefix}VOLUMES — {len(volume_stmts)} statement(s)\n{'─'*60}")
execute(volume_stmts, "volume")

print(f"\n{'─'*60}\n{prefix}FONCTIONS — {len(function_stmts)} statement(s)\n{'─'*60}")
execute(function_stmts, "function", max_passes=5)

# COMMAND ----------
# MAGIC %md ## Résumé

# COMMAND ----------
def count(kind, status):
    return sum(1 for r in results if r["kind"] == kind and r["status"] == status)

ok_status = "dry_run" if dry_run else "success"
prefix_label = "DRY-RUN — " if dry_run else ""
print(f"""
╔══════════════════════════════════════════════════════╗
║    {prefix_label}RESTAURATION VOLUMES + FONCTIONS
╠══════════════════════════════════════════════════════╣
║  Date backup      : {backup_date:<33} ║
║  Filtre catalog   : {(catalog_filter or '(tous)'):<33} ║
║  Volumes OK       : {count('volume', ok_status):<33} ║
║  Volumes erreur   : {count('volume', 'error'):<33} ║
║  Fonctions OK     : {count('function', ok_status):<33} ║
║  Fonctions erreur : {count('function', 'error'):<33} ║
╚══════════════════════════════════════════════════════╝
""")

if dry_run:
    print("Mode DRY-RUN : aucun objet n'a été créé.")
    print("Pour exécuter la restauration, repasser dry_run = false.")

errors = [r for r in results if r["status"] == "error"]
summary = {
    "backup_date":    backup_date,
    "catalog_filter": catalog_filter,
    "dry_run":        dry_run,
    "volumes":        len(volume_stmts),
    "functions":      len(function_stmts),
    "ok":             sum(1 for r in results if r["status"] == ok_status),
    "errors":         len(errors),
    "results":        results,
}
dbutils.notebook.exit(json.dumps(summary))
