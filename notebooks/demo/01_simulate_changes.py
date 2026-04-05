# Databricks notebook source
# notebooks/demo/01_simulate_changes.py

# COMMAND ----------
# MAGIC %md
# MAGIC # 🔄 Acte 2 — Simulation de changements métier (J+1)
# MAGIC
# MAGIC Ce notebook simule ce qui se passe entre deux backups consécutifs :
# MAGIC - ✅ Ajout d'une nouvelle table (`new_products`)
# MAGIC - ❌ Suppression d'une table existante (`produits`)
# MAGIC - ✏️ Ajout de lignes dans une table existante (`clients`)
# MAGIC
# MAGIC Après ce notebook, relancer le backup avec une date J+1 pour capturer le diff.
# MAGIC
# MAGIC **Durée estimée : ~2 minutes**

# COMMAND ----------
# MAGIC %md ## 2.1 — Paramètres

# COMMAND ----------
catalog = "source_demo01"
schema_sales = "sales_demo"

print(f"Catalogue : {catalog}")
print(f"Schéma    : {schema_sales}")

# COMMAND ----------
# MAGIC %md ## 2.2 — Ajout d'une nouvelle table

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_sales}.new_products (
  product_id  INT,
  nom         STRING,
  version     STRING,
  release_date DATE,
  prix        DOUBLE
) COMMENT 'Nouveaux produits — ajoutés après le backup baseline'
""")

spark.sql(f"""
INSERT INTO {catalog}.{schema_sales}.new_products VALUES
  (10, 'DR Backup Pro',      'v1.0', '2026-04-01', 12000.0),
  (11, 'Data Observability', 'v2.1', '2026-04-05', 9500.0),
  (12, 'Lakehouse Monitor',  'v1.5', '2026-04-10', 7800.0)
""")
print(f"[+] Table AJOUTÉE : {catalog}.{schema_sales}.new_products (3 lignes)")

# COMMAND ----------
# MAGIC %md ## 2.3 — Suppression d'une table existante

# COMMAND ----------
spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema_sales}.produits")
print(f"[-] Table SUPPRIMÉE : {catalog}.{schema_sales}.produits")

# COMMAND ----------
# MAGIC %md ## 2.4 — Modification d'une table existante (nouvelles lignes)

# COMMAND ----------
spark.sql(f"""
INSERT INTO {catalog}.{schema_sales}.clients VALUES
  (6, 'MediData Suisse',   'data@medidata.ch',    'Bâle',        '2026-04-01'),
  (7, 'FinTech Geneva',    'ops@fintech-ge.ch',   'Genève',      '2026-04-03')
""")

count = spark.sql(f"SELECT COUNT(*) as n FROM {catalog}.{schema_sales}.clients").collect()[0].n
print(f"[✏] Table MODIFIÉE : {catalog}.{schema_sales}.clients ({count} lignes au total)")

# COMMAND ----------
# MAGIC %md ## 2.5 — État après changements

# COMMAND ----------
print("=== ÉTAT APRÈS CHANGEMENTS (J+1) ===\n")

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema_sales}").collect()
print(f"Tables dans {schema_sales}:")
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as n FROM {catalog}.{schema_sales}.{t.tableName}").collect()[0].n
    print(f"  • {t.tableName} ({count} lignes)")

print("""
Résumé des changements :
  [+] new_products  → nouvelle table (3 lignes)
  [-] produits      → supprimée
  [✏] clients       → 7 lignes (était 5)

[NEXT] Lancer le backup J+1 puis afficher le diff (notebook 02_show_report)
""")
