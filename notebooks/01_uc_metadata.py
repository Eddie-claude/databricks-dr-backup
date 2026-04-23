# Databricks notebook source
# notebooks/01_uc_metadata.py

# COMMAND ----------
# MAGIC %md # 01 — Export Unity Catalog Metadata

# COMMAND ----------
import json
from datetime import date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# DBTITLE 1, Paramètres
dbutils.widgets.text("backup_root", "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup YYYY-MM-DD")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")

assert backup_root.startswith("abfss://"), "backup_root doit commencer par abfss://"

output_path = f"{backup_root}/{backup_date}/uc_metadata"

# COMMAND ----------
# DBTITLE 1, Export catalogs

# information_schema = schéma système UC (vues uniquement, non cloneable)
EXCLUDED_SCHEMAS = {"information_schema"}

catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect() if r.catalog not in ("hive_metastore", "system")]
catalog_ddl = "\n".join([f"CREATE CATALOG IF NOT EXISTS `{c}`;" for c in catalogs])

dbutils.fs.put(f"{output_path}/01_catalogs.sql", catalog_ddl, overwrite=True)
print(f"[01_catalogs] {len(catalogs)} catalogs exportés : {catalogs}")

# COMMAND ----------
# DBTITLE 1, Export schemas + tables (avec cache schemas)

schema_ddls = []
table_ddls = []
table_names = []

for catalog in catalogs:
    schemas = [
        r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        if r.databaseName not in EXCLUDED_SCHEMAS
    ]
    for schema in schemas:
        schema_ddls.append(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`;")

        # Récupérer uniquement les tables cloneables (MANAGED/EXTERNAL, pas les vues)
        try:
            cloneable_tables = {
                r.table_name for r in spark.sql(f"""
                    SELECT table_name FROM `{catalog}`.information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_type IN ('MANAGED', 'EXTERNAL')
                """).collect()
            }
        except Exception as e:
            print(f"[WARN] Fallback table_type pour {catalog}.{schema}: {e}")
            cloneable_tables = None  # inclure tout, les vues seront filtrées dans 02_data_clone

        tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
        for t in tables:
            fqn = f"`{catalog}`.`{schema}`.`{t.tableName}`"
            try:
                ddl_row = spark.sql(f"SHOW CREATE TABLE {fqn}").collect()[0][0]
                table_ddls.append(ddl_row + ";")
                # Ajouter à la liste de clone uniquement si c'est une table réelle
                if cloneable_tables is None or t.tableName in cloneable_tables:
                    table_names.append(f"{catalog}.{schema}.{t.tableName}")
                else:
                    print(f"[SKIP] Vue ignorée pour le clone : {catalog}.{schema}.{t.tableName}")
            except Exception as e:
                print(f"[WARN] Impossible d'exporter {fqn}: {e}")

dbutils.fs.put(f"{output_path}/02_schemas.sql", "\n".join(schema_ddls), overwrite=True)
print(f"[02_schemas] {len(schema_ddls)} schemas exportés")

dbutils.fs.put(f"{output_path}/03_tables.sql", "\n\n".join(table_ddls), overwrite=True)
print(f"[03_tables] {len(table_ddls)} tables exportées")

# COMMAND ----------
# DBTITLE 1, Export grants (catalogs, schemas, tables)

grant_statements = []

for catalog in catalogs:
    # Grants catalog
    try:
        for g in spark.sql(f"SHOW GRANTS ON CATALOG `{catalog}`").collect():
            grant_statements.append(f"GRANT {g.ActionType} ON CATALOG `{catalog}` TO `{g.Principal}`;")
    except Exception as e:
        print(f"[WARN] Grants catalog {catalog}: {e}")

    schemas = [ddl.split("`")[3] for ddl in schema_ddls if ddl.startswith(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`")]
    for schema in schemas:
        # Grants schema
        try:
            for g in spark.sql(f"SHOW GRANTS ON SCHEMA `{catalog}`.`{schema}`").collect():
                grant_statements.append(f"GRANT {g.ActionType} ON SCHEMA `{catalog}`.`{schema}` TO `{g.Principal}`;")
        except Exception as e:
            print(f"[WARN] Grants schema {catalog}.{schema}: {e}")

        # Grants tables
        tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
        for t in tables:
            fqn_plain = f"{catalog}.{schema}.{t.tableName}"
            fqn = f"`{catalog}`.`{schema}`.`{t.tableName}`"
            try:
                for g in spark.sql(f"SHOW GRANTS ON TABLE {fqn}").collect():
                    grant_statements.append(f"GRANT {g.ActionType} ON TABLE {fqn} TO `{g.Principal}`;")
            except Exception as e:
                print(f"[WARN] Grants table {fqn_plain}: {e}")

dbutils.fs.put(f"{output_path}/04_grants.sql", "\n".join(grant_statements), overwrite=True)
print(f"[04_grants] {len(grant_statements)} grants exportés")

# COMMAND ----------
# DBTITLE 1, Retourner le manifest partiel

result = {
    "catalogs": catalogs,
    "table_names": table_names,
    "grant_count": len(grant_statements),
}
dbutils.notebook.exit(json.dumps(result))
