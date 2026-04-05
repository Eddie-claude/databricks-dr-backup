# Databricks notebook source
# notebooks/demo/03_dr_scenario.py

# COMMAND ----------
# MAGIC %md
# MAGIC # 🚨 Acte 4 — Scénario DR : Sinistre et Restauration
# MAGIC
# MAGIC Ce notebook simule un sinistre complet sur `source_demo01.sales_demo` :
# MAGIC 1. **Avant** : on vérifie l'état des tables
# MAGIC 2. **Sinistre** : DROP TABLE + DROP SCHEMA
# MAGIC 3. **Restauration** : replay du dump SQL UC depuis le backup
# MAGIC 4. **Vérification** : les tables sont revenues avec leurs données
# MAGIC
# MAGIC ⚠️ Ce notebook modifie réellement le catalogue `source_demo01`.
# MAGIC
# MAGIC **Durée estimée : ~5 minutes**

# COMMAND ----------
# MAGIC %md ## 4.1 — Paramètres

# COMMAND ----------
dbutils.widgets.text("backup_root", "abfss://uc-data@st10keyitdpdrpdevwe00.dfs.core.windows.net/dr-backup", "Backup root")
dbutils.widgets.text("backup_date", "", "Date du backup à restaurer (vide = dernier)")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")

import json

if not backup_date:
    latest = json.loads(dbutils.fs.head(f"{backup_root}/latest.json"))
    backup_date = latest["date"]
    print(f"[AUTO] Date backup détectée : {backup_date}")
else:
    print(f"[OK] Date backup : {backup_date}")

catalog = "source_demo01"
schema_sales = "sales_demo"

# COMMAND ----------
# MAGIC %md ## 4.2 — État AVANT sinistre

# COMMAND ----------
print("=" * 60)
print("ÉTAT AVANT SINISTRE")
print("=" * 60)

try:
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema_sales}").collect()
    print(f"\nTables dans {catalog}.{schema_sales} :")
    for t in tables:
        count = spark.sql(f"SELECT COUNT(*) as n FROM {catalog}.{schema_sales}.{t.tableName}").collect()[0].n
        print(f"  ✅ {t.tableName} ({count} lignes)")
except Exception as e:
    print(f"  [INFO] {e}")

# COMMAND ----------
# MAGIC %md ## 4.3 — 💥 Simulation du sinistre

# COMMAND ----------
print("=" * 60)
print("⚠️  SIMULATION DU SINISTRE EN COURS...")
print("=" * 60)

# Sauvegarder la liste des tables avant suppression
try:
    tables_avant = [t.tableName for t in spark.sql(f"SHOW TABLES IN {catalog}.{schema_sales}").collect()]
except:
    tables_avant = []

# DROP de toutes les tables
for table in tables_avant:
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema_sales}.{table}")
    print(f"  ❌ DROP TABLE {catalog}.{schema_sales}.{table}")

# DROP du schéma
spark.sql(f"DROP SCHEMA IF EXISTS {catalog}.{schema_sales}")
print(f"\n  ❌ DROP SCHEMA {catalog}.{schema_sales}")

print("\n💥 SINISTRE SIMULÉ — toutes les données sont perdues!")

# COMMAND ----------
# MAGIC %md ## 4.4 — Vérification de la perte

# COMMAND ----------
print("=" * 60)
print("VÉRIFICATION DE LA PERTE")
print("=" * 60)

try:
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema_sales}").collect()
    print(f"Tables restantes : {len(tables)}")
except Exception as e:
    print(f"  ❌ Le schéma n'existe plus : {e}")

# COMMAND ----------
# MAGIC %md ## 4.5 — 🔄 Restauration depuis le backup

# COMMAND ----------
print("=" * 60)
print("🔄 RESTAURATION DEPUIS LE BACKUP")
print("=" * 60)
print(f"\nSource : {backup_root}/{backup_date}/uc_metadata/")

# Charger le dump SQL
sql_files = ["02_schemas.sql", "03_tables.sql"]

for sql_file in sql_files:
    sql_path = f"{backup_root}/{backup_date}/uc_metadata/{sql_file}"
    print(f"\n📄 Exécution de {sql_file}...")

    try:
        sql_content = dbutils.fs.head(sql_path, 10_000_000)
        statements = [s.strip() for s in sql_content.split(";") if s.strip()]

        ok_count = 0
        skip_count = 0
        err_count = 0

        for stmt in statements:
            # Filtrer uniquement les statements pour source_demo01.sales_demo
            # Exclure information_schema (vues système non recréables)
            if catalog not in stmt and schema_sales not in stmt:
                continue
            if "information_schema" in stmt.lower():
                continue
            try:
                spark.sql(stmt)
                ok_count += 1
            except Exception as e:
                err_str = str(e).lower()
                if "already exists" in err_str:
                    skip_count += 1
                else:
                    print(f"  [WARN] {str(e)[:100]}")
                    err_count += 1

        print(f"  ✅ {ok_count} statements OK | ⏭ {skip_count} déjà existants | ⚠️ {err_count} erreurs")

    except Exception as e:
        print(f"  [ERROR] Impossible de charger {sql_file}: {e}")

# COMMAND ----------
# MAGIC %md ## 4.6 — Restauration des données via Delta CLONE

# COMMAND ----------
print("\n🔄 Restauration des données Delta depuis les clones...")

clone_manifest_path = f"{backup_root}/{backup_date}/data/_clone_manifest.json"

try:
    clone_manifest = json.loads(dbutils.fs.head(clone_manifest_path, 10_000_000))
    # Le manifest est une liste directe d'entrées {table, status, size_gb}
    results = clone_manifest if isinstance(clone_manifest, list) else clone_manifest.get("clone_results", [])

    # Filtrer les tables du catalog/schema de démo
    demo_tables = [r for r in results
                   if r.get("table", "").startswith(f"{catalog}.{schema_sales}.")
                   and r.get("status") == "success"]

    print(f"\nTables à restaurer depuis les clones : {len(demo_tables)}")

    for entry in demo_tables:
        table_name = entry["table"]
        # Reconstruire le chemin du clone : data/{catalog}/{schema}/{table}
        parts = table_name.split(".")   # [catalog, schema, table]
        clone_path = f"{backup_root}/{backup_date}/data/{'/'.join(parts)}"
        try:
            spark.sql(f"""
                CREATE OR REPLACE TABLE {table_name}
                DEEP CLONE delta.`{clone_path}`
            """)
            count = spark.sql(f"SELECT COUNT(*) as n FROM {table_name}").collect()[0].n
            print(f"  ✅ {table_name} restaurée ({count} lignes)")
        except Exception as e:
            print(f"  ❌ {table_name}: {e}")

except Exception as e:
    print(f"[WARN] Clone manifest non disponible: {e}")
    print("       La restauration des données via CLONE n'est pas possible pour cette démo.")
    print("       Les schémas et tables (vides) ont été recréés depuis le dump SQL.")

# COMMAND ----------
# MAGIC %md ## 4.7 — ✅ Vérification post-restauration

# COMMAND ----------
print("=" * 60)
print("✅ VÉRIFICATION POST-RESTAURATION")
print("=" * 60)

try:
    tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema_sales}").collect()
    print(f"\nTables dans {catalog}.{schema_sales} :")
    for t in tables:
        try:
            count = spark.sql(f"SELECT COUNT(*) as n FROM {catalog}.{schema_sales}.{t.tableName}").collect()[0].n
            print(f"  ✅ {t.tableName} ({count} lignes) — RESTAURÉE")
        except Exception as e:
            print(f"  ⚠️ {t.tableName} — structure OK, données: {e}")
except Exception as e:
    print(f"  ❌ Erreur : {e}")

print(f"""
═══════════════════════════════════════════════════════
✅ RESTAURATION COMPLÈTE
   Backup source   : {backup_date}
   Catalogue       : {catalog}
   Schéma          : {schema_sales}
═══════════════════════════════════════════════════════

[NEXT] Acte 5 — Validation des grants (notebook 05_workspace_acl)
""")
