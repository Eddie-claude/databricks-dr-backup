# Databricks notebook source
# notebooks/diag_02_connectivity.py
#
# TEST DE CONNECTIVITÉ — À lancer AVANT le premier backup
# Vérifie que toute la chaîne technique fonctionne :
#   1. Accès en lecture au répertoire ADLS (External Location)
#   2. Écriture UC-aware sur ADLS (via Spark, pas dbutils.fs.put)
#   3. DEEP CLONE d'une petite table Delta vers le backup ADLS
#   4. Nettoyage automatique des données de test
# En cas d'échec, le message d'erreur indique précisément le problème.

# COMMAND ----------
# MAGIC %md # Test de Connectivité Backup
# MAGIC
# MAGIC **Renseigner `backup_root` avant d'exécuter.**
# MAGIC
# MAGIC | Paramètre | Exemple |
# MAGIC |-----------|---------|
# MAGIC | `backup_root` | `abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup-dev` |
# MAGIC | `test_table` | `my_catalog.my_schema.my_table` (vide = auto-sélection de la plus petite table Delta) |

# COMMAND ----------
import time
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
dbutils.widgets.text("backup_root", "", "Backup root ADLS (abfss://...)")
dbutils.widgets.text("test_table",  "", "Table test FQN (vide = auto-sélection)")

backup_root = dbutils.widgets.get("backup_root").strip()
test_table  = dbutils.widgets.get("test_table").strip()

assert backup_root.startswith("abfss://"), "backup_root doit commencer par abfss://"

EXCLUDED_CATALOGS = {"hive_metastore", "system", "samples", "__databricks_internal"}
EXCLUDED_SCHEMAS  = {"information_schema"}
TEST_PREFIX       = f"{backup_root}/_diag_test"

results = {}
print(f"[OK] Démarré à {datetime.now().strftime('%H:%M:%S')}")
print(f"[OK] Target : {backup_root}")

# COMMAND ----------
# MAGIC %md ## Test 1 — Accès en lecture (External Location)

# COMMAND ----------
print("\n[TEST 1] Accès en lecture au répertoire backup ADLS...")
try:
    items = dbutils.fs.ls(backup_root)
    print(f"  ✓ {len(items)} objet(s) trouvé(s) — External Location accessible")
    results["adls_read"] = "OK"
except Exception as e:
    err = str(e)
    print(f"  ✗ ÉCHEC : {err}")
    if "PERMISSION_DENIED" in err or "AuthorizationPermissionMismatch" in err:
        print("  → Vérifier : External Location + Storage Credential (Managed Identity)")
    elif "RESOURCE_DOES_NOT_EXIST" in err or "404" in err:
        print("  → Vérifier : nom du container ADLS et chemin backup_root")
    else:
        print("  → Vérifier : réseau (Private Endpoint, VNET injection) et droits IAM")
    results["adls_read"] = f"FAIL: {err[:120]}"

# COMMAND ----------
# MAGIC %md ## Test 2 — Écriture UC-aware (via Spark)

# COMMAND ----------
print("\n[TEST 2] Écriture et lecture via Spark (UC-aware)...")
write_path = f"{TEST_PREFIX}/write_test"
try:
    probe = f"connectivity-probe-{int(time.time())}"
    (spark.createDataFrame([(probe,)], "value STRING")
          .coalesce(1)
          .write.mode("overwrite")
          .text(write_path))
    parts = [f.path for f in dbutils.fs.ls(write_path)
             if not f.name.startswith("_") and not f.name.startswith(".")]
    assert parts, "Aucun fichier écrit trouvé"
    read_back = "\n".join(r.value for r in spark.read.text(parts[0]).collect())
    assert probe in read_back, f"Contenu inattendu : {read_back}"
    print(f"  ✓ Écriture et lecture OK")
    results["adls_write"] = "OK"
except Exception as e:
    print(f"  ✗ ÉCHEC : {e}")
    print("  → Vérifier : droits WRITE sur l'External Location")
    results["adls_write"] = f"FAIL: {str(e)[:120]}"

# COMMAND ----------
# MAGIC %md ## Test 3 — Auto-sélection de la table de test (si non fournie)

# COMMAND ----------
if not test_table:
    print("\n[TEST 3] Recherche de la plus petite table Delta disponible...")
    candidates = []
    for cat in [r.catalog for r in spark.sql("SHOW CATALOGS").collect()
                if r.catalog not in EXCLUDED_CATALOGS]:
        for sch in [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{cat}`").collect()
                    if r.databaseName not in EXCLUDED_SCHEMAS]:
            try:
                rows = spark.sql(f"""
                    SELECT table_name FROM `{cat}`.information_schema.tables
                    WHERE table_schema = '{sch}' AND table_type = 'MANAGED'
                """).collect()
                for r in rows:
                    try:
                        d = spark.sql(
                            f"DESCRIBE DETAIL `{cat}`.`{sch}`.`{r.table_name}`"
                        ).collect()[0].asDict()
                        if d.get("format") == "delta":
                            size = d.get("sizeInBytes") or 0
                            candidates.append((size, f"{cat}.{sch}.{r.table_name}"))
                    except Exception:
                        pass
            except Exception:
                pass

    if candidates:
        _, test_table = sorted(candidates)[0]
        print(f"  → Table sélectionnée : {test_table}")
    else:
        print("  ✗ Aucune table Delta MANAGED trouvée")
        print("  → Renseigner test_table manuellement (catalog.schema.table)")
        results["deep_clone"] = "SKIP — aucune table trouvée"
        test_table = None
else:
    print(f"\n[TEST 3] Table fournie : {test_table}")

# COMMAND ----------
# MAGIC %md ## Test 4 — DEEP CLONE vers ADLS

# COMMAND ----------
if test_table:
    print(f"\n[TEST 4] DEEP CLONE → {test_table} ...")
    parts  = test_table.split(".")
    if len(parts) != 3:
        print(f"  ✗ Format invalide : attendu catalog.schema.table, reçu '{test_table}'")
        results["deep_clone"] = "FAIL — format FQN invalide"
    else:
        cat, sch, tbl = parts
        dest = f"{TEST_PREFIX}/clone/{cat}/{sch}/{tbl}"
        t0   = time.time()
        try:
            res     = spark.sql(
                f"CREATE OR REPLACE TABLE delta.`{dest}` DEEP CLONE `{cat}`.`{sch}`.`{tbl}`"
            ).collect()[0].asDict()
            elapsed = time.time() - t0
            size_gb = (res.get("copied_files_size") or res.get("num_output_bytes") or 0) / 1073741824
            n_files = res.get("num_copied_files", 0)
            speed   = (size_gb * 1024 / elapsed) if elapsed > 0 and size_gb > 0 else 0
            print(f"  ✓ Succès — {size_gb:.3f} GB, {n_files} fichiers en {elapsed:.1f}s", end="")
            print(f" ({speed:.1f} MB/s)" if speed > 0 else "")
            results["deep_clone"] = f"OK — {size_gb:.3f} GB, {n_files} fichiers, {elapsed:.1f}s"
            if speed > 0:
                results["deep_clone"] += f" ({speed:.1f} MB/s)"
        except Exception as e:
            elapsed = time.time() - t0
            err = str(e)
            print(f"  ✗ ÉCHEC après {elapsed:.1f}s : {err}")
            if "DELTA_CLONE_UNSUPPORTED_SOURCE" in err:
                print("  → La table n'est pas au format Delta — fournir une autre table")
            elif "PERMISSION" in err or "Forbidden" in err:
                print("  → Vérifier : cluster SINGLE_USER mode + droits SELECT sur la table")
            elif "EXTERNAL_LOCATION" in err:
                print("  → Vérifier : External Location configurée pour le chemin backup")
            results["deep_clone"] = f"FAIL: {err[:120]}"

# COMMAND ----------
# MAGIC %md ## Nettoyage des données de test

# COMMAND ----------
print("\n[CLEANUP] Suppression des données de test...")
try:
    dbutils.fs.rm(TEST_PREFIX, recurse=True)
    print(f"  ✓ {TEST_PREFIX} supprimé")
except Exception as e:
    print(f"  [WARN] Nettoyage partiel : {e}")

# COMMAND ----------
# MAGIC %md ## Résumé

# COMMAND ----------
status_icons = {True: "✓ OK", False: "✗ FAIL"}

adls_read_ok   = results.get("adls_read",   "").startswith("OK")
adls_write_ok  = results.get("adls_write",  "").startswith("OK")
deep_clone_ok  = results.get("deep_clone",  "").startswith("OK")
deep_clone_skip= results.get("deep_clone",  "").startswith("SKIP")

all_ok = adls_read_ok and adls_write_ok and (deep_clone_ok or deep_clone_skip)
global_status = "✓ PRÊT POUR BACKUP" if all_ok else "✗ PROBLÈME(S) DÉTECTÉ(S)"

print(f"""
╔══════════════════════════════════════════════════════════════════╗
║           CONNECTIVITY TEST — RÉSUMÉ                           ║
╠══════════════════════════════════════════════════════════════════╣
║  Test 1 — Accès ADLS (ls)       : {'✓ OK' if adls_read_ok  else '✗ FAIL':<30} ║
║  Test 2 — Écriture UC-aware     : {'✓ OK' if adls_write_ok else '✗ FAIL':<30} ║
║  Test 3 — Sélection table test  : {'✓ ' + test_table if test_table else '✗ non trouvée':<30} ║
║  Test 4 — DEEP CLONE            : {('✓ OK' if deep_clone_ok else ('— SKIP' if deep_clone_skip else '✗ FAIL')):<30} ║
╠══════════════════════════════════════════════════════════════════╣
║  Statut global : {global_status:<46} ║
╚══════════════════════════════════════════════════════════════════╝
""")

if not all_ok:
    print("[ACTION REQUISE] Corriger les erreurs ci-dessus avant de lancer le backup.")
    print("Points de contrôle :")
    if not adls_read_ok:
        print("  • External Location : vérifier Storage Credential (Managed Identity ou SP)")
        print("  • Droits IAM : Storage Blob Data Contributor sur le compte ADLS")
    if not adls_write_ok:
        print("  • Droits WRITE sur External Location dans Unity Catalog")
    if not deep_clone_ok and not deep_clone_skip:
        print("  • Cluster : utiliser SINGLE_USER mode (obligatoire pour UC credential passthrough)")
        print("  • Droits SELECT sur les tables source")
else:
    print("[INFO] Environnement validé — vous pouvez lancer le backup.")
