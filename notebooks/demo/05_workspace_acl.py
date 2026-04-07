# Databricks notebook source
# notebooks/demo/05_workspace_acl.py

# COMMAND ----------
# MAGIC %md
# MAGIC # 🔐 Acte 6 — Workspace Config & ACLs
# MAGIC
# MAGIC Ce notebook valide que les grants Unity Catalog sont correctement :
# MAGIC 1. **Capturés** dans le backup (`04_grants.sql`)
# MAGIC 2. **Restorables** via `restore_uc.py`
# MAGIC 3. **Vérifiables** après restauration
# MAGIC
# MAGIC Il met également en évidence les **limites** de la solution (transparence client).
# MAGIC
# MAGIC **Durée estimée : ~3 minutes**

# COMMAND ----------
# MAGIC %md ## 6.1 — Paramètres

# COMMAND ----------
dbutils.widgets.text("backup_root", "abfss://uc-data@st10keyitdpdrpdevwe00.dfs.core.windows.net/dr-backup", "Backup root")
dbutils.widgets.text("backup_date", "", "Date du backup (vide = dernier)")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")

import json

if not backup_date:
    latest = json.loads(dbutils.fs.head(f"{backup_root}/latest.json"))
    backup_date = latest["date"]
    print(f"[AUTO] Date backup : {backup_date}")

catalog = "source_demo01"
schema_sales = "sales_demo"

# COMMAND ----------
# MAGIC %md ## 6.2 — Grants actuels sur source_demo01

# COMMAND ----------
print("=" * 60)
print("GRANTS UC ACTUELS")
print("=" * 60)

try:
    print(f"\nGrants sur CATALOG {catalog}:")
    display(spark.sql(f"SHOW GRANTS ON CATALOG {catalog}"))
except Exception as e:
    print(f"  [INFO] {e}")

# COMMAND ----------
try:
    print(f"\nGrants sur SCHEMA {catalog}.{schema_sales}:")
    display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.{schema_sales}"))
except Exception as e:
    print(f"  [INFO] {e}")

# COMMAND ----------
try:
    print(f"\nGrants sur TABLE {catalog}.{schema_sales}.clients:")
    display(spark.sql(f"SHOW GRANTS ON TABLE {catalog}.{schema_sales}.clients"))
except Exception as e:
    print(f"  [INFO] {e}")

# COMMAND ----------
# MAGIC %md ## 6.3 — Contenu du fichier grants sauvegardé

# COMMAND ----------
print("=" * 60)
print("GRANTS SAUVEGARDÉS DANS LE BACKUP")
print("=" * 60)

grants_path = f"{backup_root}/{backup_date}/uc_metadata/04_grants.sql"

try:
    grants_sql = dbutils.fs.head(grants_path, 50_000)
    size_kb = round(len(grants_sql.encode()) / 1024, 1)
    lines = [l for l in grants_sql.split("\n") if l.strip().startswith("GRANT")]
    print(f"\nFichier : {grants_path}")
    print(f"Taille  : {size_kb} KB")
    print(f"Nombre de GRANT statements : {len(lines)}\n")

    # Afficher un extrait
    print("Extrait (10 premiers grants) :")
    for line in lines[:10]:
        print(f"  {line}")
    if len(lines) > 10:
        print(f"  ... ({len(lines) - 10} grants supplémentaires)")

except Exception as e:
    print(f"[ERROR] {e}")

# COMMAND ----------
# MAGIC %md ## 6.4 — Simulation : Révocation d'un grant

# COMMAND ----------
print("=" * 60)
print("SIMULATION : PERTE DE GRANTS")
print("=" * 60)

try:
    spark.sql(f"REVOKE SELECT ON SCHEMA {catalog}.{schema_sales} FROM `account users`")
    print(f"  ❌ REVOKE SELECT ON SCHEMA {catalog}.{schema_sales} FROM `account users`")
    print("\n  → Les utilisateurs n'ont plus accès aux données !")
except Exception as e:
    print(f"  [INFO] {e}")

# COMMAND ----------
# MAGIC %md ## 6.5 — Restauration des grants depuis le backup

# COMMAND ----------
print("=" * 60)
print("RESTAURATION DES GRANTS")
print("=" * 60)

try:
    grants_sql = dbutils.fs.head(grants_path, 10_000_000)
    statements = [s.strip() for s in grants_sql.split(";") if s.strip().upper().startswith("GRANT")]

    # Filtrer uniquement les grants sur source_demo01.sales_demo
    demo_grants = [s for s in statements
                   if catalog in s
                   and (schema_sales in s or catalog + "." not in s.lower())
                   and "information_schema" not in s.lower()]

    ok, skip, err = 0, 0, 0
    for stmt in demo_grants:
        try:
            spark.sql(stmt)
            ok += 1
        except Exception as e:
            err_str = str(e).lower()
            if "already" in err_str or "no change" in err_str:
                skip += 1
            else:
                print(f"  [WARN] {str(e)[:120]}")
                err += 1

    print(f"\n  ✅ {ok} grants restaurés | ⏭ {skip} déjà présents | ⚠️ {err} erreurs")

except Exception as e:
    print(f"[ERROR] {e}")

# COMMAND ----------
# MAGIC %md ## 6.6 — Vérification post-restauration des grants

# COMMAND ----------
print("=" * 60)
print("VÉRIFICATION POST-RESTAURATION")
print("=" * 60)

try:
    print(f"\nGrants sur SCHEMA {catalog}.{schema_sales} :")
    display(spark.sql(f"SHOW GRANTS ON SCHEMA {catalog}.{schema_sales}"))
except Exception as e:
    print(f"  [INFO] {e}")

# COMMAND ----------
# MAGIC %md ## 6.7 — Vérification du backup Workspace Config

# COMMAND ----------
import json

print("=" * 60)
print("BACKUP WORKSPACE CONFIG (ACLs + CLUSTER POLICIES)")
print("=" * 60)

try:
    files = dbutils.fs.ls(f"{backup_root}/{backup_date}/workspace_config")
    print(f"\nFichiers sauvegardés dans workspace_config/ :")
    for f in files:
        size_kb = round(f.size / 1024, 1)
        print(f"  📄 {f.name} ({size_kb} KB)")

    # Afficher un résumé du contenu
    print()

    policies = json.loads(dbutils.fs.head(
        f"{backup_root}/{backup_date}/workspace_config/cluster_policies.json", 100_000))
    print(f"  Cluster policies sauvegardées : {len(policies)}")
    for p in policies[:5]:
        print(f"    • {p.get('name')}")

    clusters = json.loads(dbutils.fs.head(
        f"{backup_root}/{backup_date}/workspace_config/clusters.json", 100_000))
    print(f"\n  Clusters sauvegardés : {len(clusters)}")
    for c in clusters[:5]:
        print(f"    • {c.get('cluster_name')} [{c.get('state')}]")

    acls = json.loads(dbutils.fs.head(
        f"{backup_root}/{backup_date}/workspace_config/workspace_acls.json", 1_000_000))
    print(f"\n  ACLs workspace sauvegardées : {len(acls)} objets avec permissions explicites")
    for a in acls[:5]:
        print(f"    • {a.get('object_type')} : {a.get('path')}")

    repos = json.loads(dbutils.fs.head(
        f"{backup_root}/{backup_date}/workspace_config/repos_acls.json", 100_000))
    print(f"\n  Repos sauvegardés : {len(repos)}")
    for r in repos[:3]:
        print(f"    • {r.get('path')} — {r.get('url','')[:50]}")

except Exception as e:
    print(f"\n[INFO] Backup workspace_config pas encore disponible : {e}")
    print("       Relancer le job dr-backup-daily pour l'inclure")

# COMMAND ----------
# MAGIC %md ## 6.8 — Périmètre couvert et limites de la solution

# COMMAND ----------
print("""
╔══════════════════════════════════════════════════════════╗
║           PÉRIMÈTRE DE BACKUP — SYNTHÈSE                ║
╠══════════════════════════════════════════════════════════╣
║                                                          ║
║  ✅ COUVERT                                              ║
║     • UC Metadata : CATALOG, SCHEMA, TABLE (DDL)         ║
║     • Grants UC : CATALOG / SCHEMA / TABLE               ║
║     • Données Delta : DEEP CLONE (full + incrémental)    ║
║     • Jobs Databricks : export JSON via REST API         ║
║     • Notebooks workspace : export via Databricks CLI    ║
║     • ACLs workspace : notebooks, dossiers, repos        ║
║     • Cluster policies et configurations cluster         ║
║     • Diff J/J-1 : tables, jobs, notebooks               ║
║     • Rapport HTML automatique                           ║
║                                                          ║
║  ⚠️  PARTIELLEMENT COUVERT                               ║
║     • Vues (VIEW) : DDL capturé, pas les données         ║
║     • Tables externes : structure OK, données hors scope ║
║                                                          ║
║  ❌ NON COUVERT (limites identifiées)                    ║
║     • Secrets Databricks                                 ║
║     • Delta Sharing configurations                       ║
║     • MLflow models & experiments                        ║
║                                                          ║
╚══════════════════════════════════════════════════════════╝
""")
