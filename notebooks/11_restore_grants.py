# Databricks notebook source
# notebooks/11_restore_grants.py

# COMMAND ----------
# MAGIC %md # 11 — Restauration Grants Unity Catalog
# MAGIC
# MAGIC Ce notebook restaure les permissions Unity Catalog sauvegardées par `01_uc_metadata`
# MAGIC dans `uc_metadata/04_grants.sql`.
# MAGIC
# MAGIC | Niveau | Statements restaurés |
# MAGIC |--------|---------------------|
# MAGIC | Catalog | `GRANT ... ON CATALOG` |
# MAGIC | Schema  | `GRANT ... ON SCHEMA` |
# MAGIC | Table   | `GRANT ... ON TABLE` |
# MAGIC
# MAGIC **Remarques :**
# MAGIC - Les grants déjà existants sont idempotents (rejouer `GRANT` ne provoque pas d'erreur).
# MAGIC - Les grants vers des principals ou objets introuvables génèrent un avertissement, pas un arrêt.
# MAGIC - **dry_run = true** : affiche les statements sans les exécuter.

# COMMAND ----------
import json
import re
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def _uc_head(path: str) -> str:
    """Lit un fichier texte depuis ADLS (UC-aware)."""
    # spark.read.text(path) peut lire 0 octet sur ce chemin même juste après une réécriture
    # complète dans la même session (observé en prod, confirmé via le panneau performance —
    # "Bytes read: 0 B") — dbutils.fs.head est plus fiable ici.
    return dbutils.fs.head(path, 10_000_000)

# COMMAND ----------
# MAGIC %md ## Paramètres

# COMMAND ----------
dbutils.widgets.text(    "backup_root",     "", "Backup root (abfss://...)")
dbutils.widgets.text(    "backup_date",     "", "Date du backup (YYYY-MM-DD)")
dbutils.widgets.text(    "catalog_filter",  "", "Catalogs à restaurer (séparés par virgule, vide = tous)")
dbutils.widgets.dropdown("dry_run",         "true", ["true", "false"], "Dry-run (true = simulation)")

backup_root    = dbutils.widgets.get("backup_root").strip().rstrip("/")
backup_date    = dbutils.widgets.get("backup_date").strip()
catalog_filter = dbutils.widgets.get("catalog_filter").strip()
dry_run        = dbutils.widgets.get("dry_run").lower() == "true"

grants_path = f"{backup_root}/{backup_date}/uc_metadata/04_grants.sql"
prefix      = "[DRY-RUN] " if dry_run else ""

if not backup_root:
    raise ValueError("backup_root est vide — renseignez le chemin abfss://...")
if not backup_date:
    raise ValueError("backup_date est vide — renseignez la date du backup (YYYY-MM-DD)")

catalogs_to_restore = {c.strip().lower() for c in catalog_filter.split(",") if c.strip()} if catalog_filter else set()

print(f"[OK] backup_root     = {backup_root}")
print(f"[OK] backup_date     = {backup_date}")
print(f"[OK] catalog_filter  = {catalog_filter or '(tous)'}")
print(f"[OK] dry_run         = {dry_run}")
print(f"[OK] grants_path     = {grants_path}")

# COMMAND ----------
# MAGIC %md ## Étape 1 — Chargement du fichier grants

# COMMAND ----------
try:
    raw = _uc_head(grants_path)
    all_statements = [
        line.strip()
        for line in raw.splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]
    print(f"[OK] {len(all_statements)} statement(s) trouvé(s) dans 04_grants.sql")
except Exception as e:
    print(f"[ERROR] Impossible de lire {grants_path} : {e}")
    dbutils.notebook.exit(json.dumps({"error": str(e), "ok": 0, "skipped": 0, "errors": 1}))

# COMMAND ----------
# MAGIC %md ## Étape 2 — Filtrage et validation

# COMMAND ----------
GRANT_PAT = re.compile(r"^GRANT\s+", re.IGNORECASE)

def extract_catalog_from_statement(stmt: str) -> str:
    """Extrait le nom du catalog depuis un statement GRANT."""
    m = re.search(r"ON\s+(?:CATALOG|SCHEMA|TABLE)\s+`([^`]+)`", stmt, re.IGNORECASE)
    return m.group(1).lower() if m else ""

valid_statements = []
skipped_invalid  = 0

for stmt in all_statements:
    # Sécurité : uniquement des GRANT
    if not GRANT_PAT.match(stmt):
        print(f"  [SKIP-INVALID] Statement non-GRANT ignoré : {stmt[:80]}")
        skipped_invalid += 1
        continue

    # Filtre catalog
    if catalogs_to_restore:
        cat = extract_catalog_from_statement(stmt)
        if cat not in catalogs_to_restore:
            skipped_invalid += 1
            continue

    valid_statements.append(stmt)

print(f"\n── {len(valid_statements)} grant(s) à restaurer ({skipped_invalid} ignorés par filtre/validation) ──")

# Grouper par niveau pour l'affichage
by_level = {"CATALOG": [], "SCHEMA": [], "TABLE": []}
for s in valid_statements:
    m = re.search(r"ON\s+(CATALOG|SCHEMA|TABLE)", s, re.IGNORECASE)
    level = m.group(1).upper() if m else "OTHER"
    by_level.get(level, by_level["TABLE"]).append(s)

for level, stmts in by_level.items():
    if stmts:
        print(f"  {level:<8} : {len(stmts)} grant(s)")

if not valid_statements:
    print("[WARN] Aucun grant à restaurer.")
    dbutils.notebook.exit(json.dumps({
        "backup_date": backup_date, "dry_run": dry_run,
        "ok": 0, "skipped": skipped_invalid, "errors": 0,
        "statements_total": len(all_statements),
    }))

# COMMAND ----------
# MAGIC %md ## Étape 3 — Exécution des grants

# COMMAND ----------
ok_count    = 0
skip_count  = 0
error_count = 0
errors_list = []

print(f"\n{'─'*60}")
print(f"{prefix}RESTAURATION GRANTS — {len(valid_statements)} statement(s)")
print(f"{'─'*60}\n")

for stmt in valid_statements:
    # Retirer le point-virgule final si présent (spark.sql ne l'accepte pas toujours)
    sql = stmt.rstrip(";").strip()

    if dry_run:
        print(f"  ── {sql}")
        ok_count += 1
        continue

    try:
        spark.sql(sql)
        print(f"  [OK] {sql}")
        ok_count += 1
    except Exception as e:
        err_msg = str(e)
        # Grants déjà existants → pas une erreur réelle
        if any(kw in err_msg.lower() for kw in ["already", "already exists", "already granted"]):
            print(f"  [SKIP-EXISTS] {sql}")
            skip_count += 1
        # Principal introuvable → warning
        elif any(kw in err_msg.lower() for kw in ["principal", "user", "group", "service principal", "not found"]):
            print(f"  [WARN-PRINCIPAL] {sql}")
            print(f"               → {err_msg[:120]}")
            skip_count += 1
        # Objet introuvable → warning
        elif any(kw in err_msg.lower() for kw in ["table", "schema", "catalog", "does not exist", "not found"]):
            print(f"  [WARN-OBJECT] {sql}")
            print(f"             → {err_msg[:120]}")
            skip_count += 1
        else:
            print(f"  [ERROR] {sql}")
            print(f"        → {err_msg[:150]}")
            error_count += 1
            errors_list.append({"stmt": sql, "error": err_msg[:200]})

# COMMAND ----------
# MAGIC %md ## Résumé

# COMMAND ----------
prefix_label = "DRY-RUN — " if dry_run else ""
print(f"""
╔══════════════════════════════════════════════════════╗
║    {prefix_label}RESTAURATION GRANTS — RÉSUMÉ
╠══════════════════════════════════════════════════════╣
║  Date backup      : {backup_date:<33} ║
║  Filtre catalog   : {(catalog_filter or '(tous)'):<33} ║
║  Grants total     : {len(all_statements):<33} ║
║  Grants filtrés   : {len(valid_statements):<33} ║
║  Exécutés OK      : {ok_count:<33} ║
║  Ignorés (skip)   : {skip_count:<33} ║
║  Erreurs          : {error_count:<33} ║
╚══════════════════════════════════════════════════════╝
""")

if dry_run:
    print("Mode DRY-RUN : aucun grant n'a été appliqué.")
    print("Pour exécuter la restauration, repasser dry_run = false.")

if errors_list:
    print("\n── Erreurs détaillées ──")
    for e in errors_list:
        print(f"  {e['stmt'][:80]}")
        print(f"  → {e['error']}")

summary = {
    "backup_date":       backup_date,
    "catalog_filter":    catalog_filter,
    "dry_run":           dry_run,
    "statements_total":  len(all_statements),
    "statements_valid":  len(valid_statements),
    "ok":                ok_count,
    "skipped":           skip_count,
    "errors":            error_count,
}
dbutils.notebook.exit(json.dumps(summary))
