# Databricks notebook source
# notebooks/demo/07_gameday.py

# COMMAND ----------
# MAGIC %md # 07 — Game Day DR automatisé
# MAGIC
# MAGIC Test de restauration automatisé et non-interactif sur le catalog de démo
# MAGIC `source_demo01` : crée un baseline connu, simule un sinistre, restaure depuis
# MAGIC le dernier backup réel de production, vérifie que les données restaurées
# MAGIC correspondent au baseline, puis nettoie l'environnement — que la vérification
# MAGIC réussisse ou échoue.
# MAGIC
# MAGIC **Aucune intervention manuelle requise.** En cas d'échec, l'exception remonte
# MAGIC jusqu'au job Databricks, qui déclenche l'email de notification configuré
# MAGIC (`email_notifications.on_failure`). Silence = tout va bien.

# COMMAND ----------
import json
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# MAGIC %md ## Paramètres

# COMMAND ----------
dbutils.widgets.text("backup_root", "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup", "Backup root (abfss://...)")
dbutils.widgets.text("lib_path",    "/Workspace/Shared/dr-backup/lib", "Chemin vers lib/")

backup_root = dbutils.widgets.get("backup_root").rstrip("/")
lib_path    = dbutils.widgets.get("lib_path")

sys.path.insert(0, lib_path)
from gameday import compare_row_counts

catalog      = "source_demo01"
schema_sales = "sales_demo"
schema_hr    = "hr_demo"

print(f"[OK] backup_root = {backup_root}")
print(f"[OK] catalog     = {catalog}")

# COMMAND ----------
# MAGIC %md ## Étape 1 — Setup (baseline connu, idempotent)

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_sales} COMMENT 'Données ventes — game day DR'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_hr} COMMENT 'Données RH — game day DR'")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_sales}.clients (
  client_id   INT,
  nom         STRING,
  email       STRING,
  region      STRING,
  created_at  DATE
) COMMENT 'Table clients — game day DR'
""")
spark.sql(f"""
INSERT INTO {catalog}.{schema_sales}.clients VALUES
  (1, 'Groupe Mutuelle',   'contact@groupemutuelle.ch', 'Romandie',    '2024-01-15'),
  (2, 'KeyIT Consulting',  'info@keyit.ch',             'Berne',       '2024-03-01'),
  (3, 'Helvetia Data',     'data@helvetia.ch',          'Zurich',      '2024-06-10'),
  (4, 'Swiss Analytics',   'hello@swissanalytics.ch',   'Genève',      '2025-01-20'),
  (5, 'BioTech Romandie',  'rd@biotech-romandie.ch',    'Lausanne',    '2025-04-05')
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_sales}.transactions (
  txn_id      INT,
  client_id   INT,
  montant     DOUBLE,
  produit     STRING,
  statut      STRING,
  txn_date    DATE
) COMMENT 'Table transactions — game day DR'
""")
spark.sql(f"""
INSERT INTO {catalog}.{schema_sales}.transactions VALUES
  (1001, 1, 15000.0, 'Plateforme Data',   'validé',    '2025-10-01'),
  (1002, 2, 8500.0,  'Support Premium',   'validé',    '2025-10-15'),
  (1003, 3, 23000.0, 'Migration Cloud',   'en_cours',  '2025-11-01'),
  (1004, 4, 4200.0,  'Licence BI',        'validé',    '2025-11-20'),
  (1005, 5, 11000.0, 'Databricks Setup',  'validé',    '2025-12-01'),
  (1006, 1, 6000.0,  'Formation Spark',   'en_cours',  '2026-01-10')
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_sales}.produits (
  produit_id  INT,
  nom         STRING,
  categorie   STRING,
  prix_base   DOUBLE,
  actif       BOOLEAN
) COMMENT 'Catalogue produits — game day DR'
""")
spark.sql(f"""
INSERT INTO {catalog}.{schema_sales}.produits VALUES
  (1, 'Plateforme Data',   'Infrastructure', 15000.0, true),
  (2, 'Support Premium',   'Services',        8500.0, true),
  (3, 'Migration Cloud',   'Projet',         23000.0, true),
  (4, 'Licence BI',        'Logiciel',        4200.0, true),
  (5, 'Formation Spark',   'Formation',       6000.0, true)
""")

spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_hr}.employes (
  emp_id      INT,
  nom         STRING,
  role        STRING,
  departement STRING,
  date_entree DATE
) COMMENT 'Table employés — game day DR'
""")
spark.sql(f"""
INSERT INTO {catalog}.{schema_hr}.employes VALUES
  (1, 'Alice Martin',   'Data Engineer',    'IT',  '2023-03-01'),
  (2, 'Bob Dupont',     'Data Scientist',   'IT',  '2023-06-15'),
  (3, 'Claire Favre',   'DBA',              'IT',  '2024-01-10'),
  (4, 'David Keller',   'Analyste BI',      'Finance', '2024-09-01')
""")

print(f"[OK] Baseline créé dans {catalog}.{schema_sales} et {catalog}.{schema_hr}")

# COMMAND ----------
# MAGIC %md ## Étape 2 — Capture du baseline (comptage réel, pas de constantes en dur)

# COMMAND ----------
def count_tables(catalog: str, schema: str) -> dict:
    """Retourne {table_complet: row_count} pour toutes les tables d'un schéma."""
    counts = {}
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
    for t in tables:
        full_name = f"{catalog}.{schema}.{t.tableName}"
        counts[full_name] = spark.sql(f"SELECT COUNT(*) AS n FROM {full_name}").collect()[0]["n"]
    return counts

expected_counts = count_tables(catalog, schema_sales)
print(f"[OK] Baseline capturé — {len(expected_counts)} table(s) :")
for table, count in expected_counts.items():
    print(f"  {table} : {count} lignes")

# COMMAND ----------
# MAGIC %md ## Étape 3 — Sinistre, restauration, vérification (cleanup garanti)

# COMMAND ----------
# Le cleanup doit s'exécuter que la vérification réussisse ou échoue : le bloc
# `finally` couvre donc sinistre+restauration+vérification. Si la vérification
# échoue, `raise` propage l'exception après le cleanup, ce qui interrompt le
# script ici — la cellule "Résumé" ci-dessous n'est alors jamais atteinte et le
# job Databricks passe en FAILED (déclenche l'email on_failure).
try:
    print("=" * 60)
    print("SIMULATION DU SINISTRE")
    print("=" * 60)
    tables_avant = [t.tableName for t in spark.sql(f"SHOW TABLES IN {catalog}.{schema_sales}").collect()]
    for table in tables_avant:
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema_sales}.{table}")
        print(f"  [DROP TABLE] {catalog}.{schema_sales}.{table}")
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema_sales}")
    print(f"  [DROP SCHEMA] {catalog}.{schema_sales}")

    print("\n" + "=" * 60)
    print("RESTAURATION DEPUIS LE DERNIER BACKUP RÉEL")
    print("=" * 60)
    latest = json.loads(dbutils.fs.head(f"{backup_root}/latest.json"))
    backup_date = latest["date"]
    print(f"[AUTO] Dernier backup détecté : {backup_date}")

    for sql_file in ["02_schemas.sql", "03_tables.sql"]:
        sql_path = f"{backup_root}/{backup_date}/uc_metadata/{sql_file}"
        sql_content = dbutils.fs.head(sql_path, 10_000_000)
        statements = [s.strip() for s in sql_content.split(";") if s.strip()]
        ok_count, skip_count = 0, 0
        for stmt in statements:
            if catalog not in stmt and schema_sales not in stmt:
                continue
            if "information_schema" in stmt.lower():
                continue
            try:
                spark.sql(stmt)
                ok_count += 1
            except Exception as e:
                if "already exists" in str(e).lower():
                    skip_count += 1
                else:
                    print(f"  [WARN] {sql_file}: {str(e)[:150]}")
        print(f"[OK] {sql_file} rejoué — {ok_count} statement(s), {skip_count} déjà existant(s)")

    clone_manifest_path = f"{backup_root}/{backup_date}/data/_clone_manifest.json"
    clone_manifest = json.loads(dbutils.fs.head(clone_manifest_path, 10_000_000))
    results = clone_manifest if isinstance(clone_manifest, list) else clone_manifest.get("clone_results", [])
    demo_tables = [r for r in results
                   if r.get("table", "").startswith(f"{catalog}.{schema_sales}.")
                   and r.get("status") == "success"]

    for entry in demo_tables:
        table_name = entry["table"]
        parts = table_name.split(".")
        clone_path = f"{backup_root}/{backup_date}/data/{'/'.join(parts)}"
        spark.sql(f"CREATE OR REPLACE TABLE {table_name} DEEP CLONE delta.`{clone_path}`")
        print(f"  [RESTORE] {table_name}")

    print("\n" + "=" * 60)
    print("VÉRIFICATION POST-RESTAURATION")
    print("=" * 60)
    actual_counts = count_tables(catalog, schema_sales)
    mismatches = compare_row_counts(expected_counts, actual_counts)

    if mismatches:
        for m in mismatches:
            print(f"  [FAIL] {m}")
        raise RuntimeError(
            f"Game day DR ÉCHOUÉ — {len(mismatches)} écart(s) : " + " | ".join(mismatches)
        )

    for table, count in actual_counts.items():
        print(f"  [OK] {table} : {count} lignes — conforme au baseline")
    print("\n[SUCCÈS] Toutes les tables restaurées correspondent au baseline.")

finally:
    print("\n" + "=" * 60)
    print("CLEANUP")
    print("=" * 60)
    try:
        for schema in (schema_sales, schema_hr):
            tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
            for t in tables:
                spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{t.tableName}")
            spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema}")
            print(f"  [OK] {catalog}.{schema} nettoyé")
    except Exception as cleanup_error:
        print(f"  [WARN] Cleanup incomplet : {cleanup_error}")

# COMMAND ----------
# MAGIC %md ## Résumé (atteint uniquement si la vérification a réussi)

# COMMAND ----------
summary = {
    "status": "success",
    "catalog": catalog,
    "schema": schema_sales,
    "tables_checked": list(expected_counts.keys()),
}
print(json.dumps(summary, indent=2, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(summary))
