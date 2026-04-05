# Databricks notebook source
# notebooks/demo/00_setup.py

# COMMAND ----------
# MAGIC %md
# MAGIC # 🛠️ Acte 1 — Setup : Création des données de démo
# MAGIC
# MAGIC Ce notebook prépare l'environnement de démonstration dans `source_demo01`.
# MAGIC Il crée des schémas, des tables Delta avec données, et des grants UC.
# MAGIC
# MAGIC **Durée estimée : ~3 minutes**

# COMMAND ----------
# MAGIC %md ## 1.1 — Paramètres

# COMMAND ----------
catalog = "source_demo01"
schema_sales = "sales_demo"
schema_hr = "hr_demo"

print(f"Catalogue cible : {catalog}")
print(f"Schémas : {schema_sales}, {schema_hr}")

# COMMAND ----------
# MAGIC %md ## 1.2 — Création des schémas

# COMMAND ----------
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_sales} COMMENT 'Données ventes — démo DR'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_hr} COMMENT 'Données RH — démo DR'")
print(f"[OK] Schémas créés : {schema_sales}, {schema_hr}")

# COMMAND ----------
# MAGIC %md ## 1.3 — Création des tables Delta

# COMMAND ----------
# Table 1 : clients
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_sales}.clients (
  client_id   INT,
  nom         STRING,
  email       STRING,
  region      STRING,
  created_at  DATE
) COMMENT 'Table clients — démo DR'
""")

spark.sql(f"""
INSERT INTO {catalog}.{schema_sales}.clients VALUES
  (1, 'Groupe Mutuelle',   'contact@groupemutuelle.ch', 'Romandie',    '2024-01-15'),
  (2, 'KeyIT Consulting',  'info@keyit.ch',             'Berne',       '2024-03-01'),
  (3, 'Helvetia Data',     'data@helvetia.ch',          'Zurich',      '2024-06-10'),
  (4, 'Swiss Analytics',   'hello@swissanalytics.ch',   'Genève',      '2025-01-20'),
  (5, 'BioTech Romandie',  'rd@biotech-romandie.ch',    'Lausanne',    '2025-04-05')
""")
print(f"[OK] Table {catalog}.{schema_sales}.clients créée avec 5 lignes")

# COMMAND ----------
# Table 2 : transactions
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_sales}.transactions (
  txn_id      INT,
  client_id   INT,
  montant     DOUBLE,
  produit     STRING,
  statut      STRING,
  txn_date    DATE
) COMMENT 'Table transactions — démo DR'
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
print(f"[OK] Table {catalog}.{schema_sales}.transactions créée avec 6 lignes")

# COMMAND ----------
# Table 3 : produits
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_sales}.produits (
  produit_id  INT,
  nom         STRING,
  categorie   STRING,
  prix_base   DOUBLE,
  actif       BOOLEAN
) COMMENT 'Catalogue produits — démo DR'
""")

spark.sql(f"""
INSERT INTO {catalog}.{schema_sales}.produits VALUES
  (1, 'Plateforme Data',   'Infrastructure', 15000.0, true),
  (2, 'Support Premium',   'Services',        8500.0, true),
  (3, 'Migration Cloud',   'Projet',         23000.0, true),
  (4, 'Licence BI',        'Logiciel',        4200.0, true),
  (5, 'Formation Spark',   'Formation',       6000.0, true)
""")
print(f"[OK] Table {catalog}.{schema_sales}.produits créée avec 5 lignes")

# COMMAND ----------
# Table 4 : employés (schéma HR)
spark.sql(f"""
CREATE OR REPLACE TABLE {catalog}.{schema_hr}.employes (
  emp_id      INT,
  nom         STRING,
  role        STRING,
  departement STRING,
  date_entree DATE
) COMMENT 'Table employés — démo DR'
""")

spark.sql(f"""
INSERT INTO {catalog}.{schema_hr}.employes VALUES
  (1, 'Alice Martin',   'Data Engineer',    'IT',  '2023-03-01'),
  (2, 'Bob Dupont',     'Data Scientist',   'IT',  '2023-06-15'),
  (3, 'Claire Favre',   'DBA',              'IT',  '2024-01-10'),
  (4, 'David Keller',   'Analyste BI',      'Finance', '2024-09-01')
""")
print(f"[OK] Table {catalog}.{schema_hr}.employes créée avec 4 lignes")

# COMMAND ----------
# MAGIC %md ## 1.4 — Attribution de grants UC

# COMMAND ----------
spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `account users`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema_sales} TO `account users`")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.{schema_sales} TO `account users`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema_hr} TO `account users`")
# Note: SELECT sur hr_demo intentionnellement NON accordé (données sensibles)
print("[OK] Grants attribués (sales: SELECT, hr: accès restreint)")

# COMMAND ----------
# MAGIC %md ## 1.5 — Vérification

# COMMAND ----------
print("=== ÉTAT INITIAL (Baseline J) ===\n")

tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema_sales}").collect()
print(f"Tables dans {schema_sales}:")
for t in tables:
    count = spark.sql(f"SELECT COUNT(*) as n FROM {catalog}.{schema_sales}.{t.tableName}").collect()[0].n
    print(f"  ✓ {t.tableName} ({count} lignes)")

tables_hr = spark.sql(f"SHOW TABLES IN {catalog}.{schema_hr}").collect()
print(f"\nTables dans {schema_hr}:")
for t in tables_hr:
    count = spark.sql(f"SELECT COUNT(*) as n FROM {catalog}.{schema_hr}.{t.tableName}").collect()[0].n
    print(f"  ✓ {t.tableName} ({count} lignes)")

print(f"\n[PRÊT] Environnement de démo initialisé dans {catalog}")
print("[NEXT] Lancer le job dr-backup-daily pour capturer ce baseline")
