# Databricks notebook source
# notebooks/demo/04_cleanup.py

# COMMAND ----------
# MAGIC %md
# MAGIC # 🧹 Cleanup — Remise en état après démo
# MAGIC
# MAGIC Ce notebook supprime toutes les ressources créées pendant la démonstration.
# MAGIC
# MAGIC ⚠️ **À exécuter UNIQUEMENT après la démo.**
# MAGIC
# MAGIC **Durée estimée : ~1 minute**

# COMMAND ----------
# MAGIC %md ## Suppression des schémas de démo dans source_demo01

# COMMAND ----------
catalog = "source_demo01"
schemas_to_drop = ["sales_demo", "hr_demo"]

for schema in schemas_to_drop:
    try:
        # Supprimer toutes les tables du schéma
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
        for t in tables:
            spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.{t.tableName}")
            print(f"  [-] DROP TABLE {catalog}.{schema}.{t.tableName}")
        # Supprimer le schéma
        spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema}")
        print(f"[OK] DROP SCHEMA {catalog}.{schema}")
    except Exception as e:
        print(f"[INFO] {schema}: {e}")

print("\n[OK] Environnement de démo nettoyé.")
print("     Les données dans ADLS (backup) sont conservées.")
