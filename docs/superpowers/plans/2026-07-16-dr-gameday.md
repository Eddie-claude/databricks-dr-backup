# DR Game Day automatisé — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ajouter un test de restauration automatisé et non-interactif ("game day") sur le catalog de démo `source_demo01`, qui prouve que le dernier backup réel de production est restaurable, sans intervention manuelle, avec alerte email uniquement en cas d'échec.

**Architecture:** Un notebook orchestrateur unique (`notebooks/demo/07_gameday.py`) réutilise la logique déjà éprouvée des notebooks `demo/00_setup.py` et `demo/03_dr_scenario.py` (setup idempotent, sinistre, restauration depuis le dernier backup réel via `latest.json`), délègue la comparaison des row counts à une fonction pure et testable (`lib/gameday.py`), et garantit un cleanup via `finally`. Un nouveau job Databricks `dr_gameday` (dans `databricks.yml`) expose ce notebook, sans `schedule` (déclenchement à la demande uniquement), avec `email_notifications.on_failure`.

**Tech Stack:** PySpark / Databricks notebooks, Databricks Asset Bundles (DAB), pytest pour la logique pure de `lib/`.

## Global Constraints

- Environnement testé : uniquement `source_demo01` (catalog de démo), jamais un catalog de production.
- Périmètre validé : données uniquement (tables Delta), pas d'ACLs/grants/jobs/notebooks dans cette itération.
- Source du backup restauré : le **vrai dernier backup de production**, auto-détecté via `{backup_root}/latest.json` — aucune nouvelle logique de backup, aucun préfixe ADLS dédié au test.
- Déclenchement : à la demande uniquement. Le job **ne doit pas** avoir de bloc `schedule` dans cette itération (pas de cron).
- Reporting : email uniquement en cas d'échec, via `email_notifications.on_failure` sur la variable `notification_email` déjà définie dans `databricks.yml` — aucun code de notification à écrire.
- Cluster : identique à celui du job `dr_backup_daily` existant (`data_security_mode: SINGLE_USER`, Single Node) — requis pour le credential passthrough Unity Catalog sur ADLS.
- Cleanup (`DROP SCHEMA` sur `sales_demo` et `hr_demo`) doit s'exécuter que la vérification réussisse ou échoue (bloc `finally`), sans jamais masquer l'exception originale de vérification si le cleanup lui-même échoue.
- Toute exception de vérification doit se propager jusqu'au job (pas de `try/except` silencieux autour de la vérification) pour que le job passe en `FAILED` et déclenche l'email natif Databricks.

---

## Contexte de fichiers existants (à connaître avant de commencer)

- `notebooks/demo/00_setup.py` — crée `source_demo01.sales_demo` (tables `clients` 5 lignes, `transactions` 6 lignes, `produits` 5 lignes) et `source_demo01.hr_demo` (table `employes` 4 lignes), avec `CREATE OR REPLACE` (idempotent).
- `notebooks/demo/03_dr_scenario.py` — simule le sinistre (`DROP TABLE`/`DROP SCHEMA` sur `sales_demo`) puis restaure depuis `{backup_root}/{backup_date}/uc_metadata/02_schemas.sql` + `03_tables.sql` (replay SQL filtré) et `{backup_root}/{backup_date}/data/_clone_manifest.json` (DEEP CLONE).
- `notebooks/demo/04_cleanup.py` — `DROP SCHEMA` sur `sales_demo` et `hr_demo`.
- `lib/diff.py` + `tests/test_diff.py` — pattern existant du projet pour la logique pure testée par pytest, importée dans les notebooks via `sys.path.insert(0, lib_path); from diff import ...` (widget `lib_path`, défaut `/Workspace/Shared/dr-backup/lib`).
- `databricks.yml` — jobs `dr_backup_daily` (cluster `Standard_DS3_v2`, `num_workers: 0`, `data_security_mode: SINGLE_USER`, `spark.master: local[*, 8]`, `spark.databricks.cluster.profile: singleNode`) et `dr_backup_monthly`. Variables globales : `backup_root`, `lib_path`, `notification_email` (`efi@keyit.ch`).
- CLI Databricks déjà authentifié dans cet environnement sur `https://adb-2547670924000766.6.azuredatabricks.net` (profil `dev` par défaut) — `databricks bundle validate --target dev` fonctionne et est sûr (lecture seule, aucun déploiement).

---

### Task 1: Logique de comparaison des row counts (`lib/gameday.py`)

**Files:**
- Create: `lib/gameday.py`
- Test: `tests/test_gameday.py`

**Interfaces:**
- Produces: `compare_row_counts(expected: dict[str, int], actual: dict[str, int]) -> list[str]` — retourne la liste des écarts sous forme de messages lisibles ; liste vide = tout correspond. Consommée par `notebooks/demo/07_gameday.py` (Task 2).

- [ ] **Step 1: Write the failing tests**

Create `tests/test_gameday.py`:

```python
# tests/test_gameday.py
from lib.gameday import compare_row_counts


def test_compare_row_counts_all_match():
    expected = {"cat.sch.clients": 5, "cat.sch.transactions": 6}
    actual = {"cat.sch.clients": 5, "cat.sch.transactions": 6}
    assert compare_row_counts(expected, actual) == []


def test_compare_row_counts_mismatch():
    expected = {"cat.sch.clients": 5}
    actual = {"cat.sch.clients": 4}
    result = compare_row_counts(expected, actual)
    assert result == ["cat.sch.clients: 4 lignes après restauration, 5 attendues"]


def test_compare_row_counts_missing_table():
    expected = {"cat.sch.clients": 5, "cat.sch.transactions": 6}
    actual = {"cat.sch.clients": 5}
    result = compare_row_counts(expected, actual)
    assert result == ["cat.sch.transactions: table manquante après restauration (attendu 6 lignes)"]


def test_compare_row_counts_ignores_extra_tables_in_actual():
    expected = {"cat.sch.clients": 5}
    actual = {"cat.sch.clients": 5, "cat.sch.unrelated": 99}
    assert compare_row_counts(expected, actual) == []


def test_compare_row_counts_multiple_mismatches_sorted_by_table_name():
    expected = {"cat.sch.b": 2, "cat.sch.a": 1}
    actual = {"cat.sch.b": 99, "cat.sch.a": 99}
    result = compare_row_counts(expected, actual)
    assert result == [
        "cat.sch.a: 99 lignes après restauration, 1 attendues",
        "cat.sch.b: 99 lignes après restauration, 2 attendues",
    ]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `python -m pytest tests/test_gameday.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'lib.gameday'` (or `ImportError`).

- [ ] **Step 3: Write minimal implementation**

Create `lib/gameday.py`:

```python
# lib/gameday.py
from typing import Dict, List


def compare_row_counts(expected: Dict[str, int], actual: Dict[str, int]) -> List[str]:
    """Compare les row counts attendus (baseline pré-sinistre) aux row counts
    réels (post-restauration). Retourne la liste des écarts ; liste vide = conforme.
    Les tables présentes dans `actual` mais absentes de `expected` sont ignorées.
    """
    mismatches: List[str] = []
    for table in sorted(expected):
        expected_count = expected[table]
        if table not in actual:
            mismatches.append(
                f"{table}: table manquante après restauration (attendu {expected_count} lignes)"
            )
            continue
        actual_count = actual[table]
        if actual_count != expected_count:
            mismatches.append(
                f"{table}: {actual_count} lignes après restauration, {expected_count} attendues"
            )
    return mismatches
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `python -m pytest tests/test_gameday.py -v`
Expected: `5 passed`

- [ ] **Step 5: Run the full existing test suite to check for regressions**

Run: `python -m pytest tests/ -q`
Expected: `14 passed` (9 tests déjà existants + 5 nouveaux)

- [ ] **Step 6: Commit**

```bash
git add lib/gameday.py tests/test_gameday.py
git commit -m "feat: add row-count comparison logic for DR game day"
```

---

### Task 2: Notebook orchestrateur (`notebooks/demo/07_gameday.py`)

**Files:**
- Create: `notebooks/demo/07_gameday.py`

**Interfaces:**
- Consumes: `compare_row_counts(expected: dict[str, int], actual: dict[str, int]) -> list[str]` from Task 1, importé via `sys.path.insert(0, lib_path); from gameday import compare_row_counts` (même pattern que `notebooks/03_diff.py:22-23`).
- Produces: notebook exécutable par le job `dr_gameday` (Task 3) avec les widgets `backup_root` et `lib_path`. En cas de succès, appelle `dbutils.notebook.exit(json.dumps({"status": "success", ...}))`. En cas d'échec de vérification, laisse l'exception se propager (pas de `dbutils.notebook.exit` atteint) pour que le job passe en `FAILED`.

- [ ] **Step 1: Write the notebook**

Create `notebooks/demo/07_gameday.py`:

```python
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
```

- [ ] **Step 2: Validate syntax locally**

Run: `python -m py_compile "notebooks/demo/07_gameday.py"`
Expected: no output, exit code 0 (les commentaires `# MAGIC` sont des commentaires Python valides — même technique déjà utilisée sur les 14 autres notebooks du projet).

- [ ] **Step 3: Commit**

```bash
git add notebooks/demo/07_gameday.py
git commit -m "feat: add automated DR game day notebook"
```

---

### Task 3: Job Databricks `dr_gameday` (`databricks.yml`)

**Files:**
- Modify: `databricks.yml` (ajouter un nouveau job après `dr_backup_monthly`, avant la section `# ── Targets ──`)

**Interfaces:**
- Consumes: `notebooks/demo/07_gameday.py` (Task 2), variables globales existantes `backup_root`, `lib_path`, `notification_email` (déjà définies en tête de `databricks.yml`).
- Produces: job bundle `dr_gameday`, déclenchable via `databricks bundle run dr_gameday --target dev`.

- [ ] **Step 1: Add the job definition**

In `databricks.yml`, after the `dr_backup_monthly` job block (which ends just before the `# ── Targets ──` comment), insert:

```yaml
    # ── Job 3 : Game Day DR — test de restauration automatisé (à la demande) ──
    dr_gameday:
      name: dr-gameday
      description: "DR Game Day — test de restauration automatisé et non-interactif sur le catalog de démo (source_demo01), sans intervention manuelle. Déclenchement à la demande uniquement (pas de schedule)."

      tags:
        project: databricks-dr
        managed_by: dab

      max_concurrent_runs: 1

      tasks:
        - task_key: gameday
          new_cluster:
            spark_version: "17.3.x-scala2.13"
            node_type_id: "Standard_DS3_v2"
            num_workers: 0
            data_security_mode: "SINGLE_USER"
            spark_conf:
              spark.master: "local[*, 8]"
              spark.databricks.cluster.profile: "singleNode"
            custom_tags:
              ResourceClass: "SingleNode"
          timeout_seconds: 3600   # 1h — largement suffisant pour un jeu de données de démo (~20 lignes)
          notebook_task:
            notebook_path: ./notebooks/demo/07_gameday.py
            base_parameters:
              backup_root: ${var.backup_root}
              lib_path:    ${var.lib_path}
          email_notifications:
            on_failure:
              - ${var.notification_email}
```

Notez l'absence volontaire de bloc `schedule:` — le job n'est déclenchable qu'à la demande (`databricks bundle run dr_gameday --target <dev|prod>` ou depuis l'UI Databricks Jobs). Pour activer une planification récurrente plus tard, ajouter un bloc `schedule:` identique à celui de `dr_backup_daily` (`quartz_cron_expression`, `timezone_id`, `pause_status: UNPAUSED`).

- [ ] **Step 2: Validate the bundle (safe, read-only — no deployment)**

Run: `databricks bundle validate --target dev`
Expected: output ending with `Validation OK!` (confirmé fonctionnel dans cet environnement — CLI déjà authentifié sur `https://adb-2547670924000766.6.azuredatabricks.net`).

- [ ] **Step 3: Commit**

```bash
git add databricks.yml
git commit -m "feat: add dr_gameday job (on-demand DR restore test)"
```

---

### Task 4: Validation end-to-end sur le workspace dev (nécessite confirmation explicite avant exécution)

> **Cette tâche exécute de vraies opérations (déploiement + DROP TABLE/SCHEMA + DEEP CLONE) sur le workspace Databricks réel `https://adb-2547670924000766.6.azuredatabricks.net`, cible `dev`.** Contrairement aux tâches précédentes (écriture de fichiers locaux, tests pytest locaux, `bundle validate` en lecture seule), ceci modifie un état partagé réel. **Ne pas exécuter automatiquement sans confirmation explicite de l'utilisateur**, même en exécution par sous-agent — demander avant de lancer les commandes ci-dessous.

**Files:** aucun (validation opérationnelle uniquement)

- [ ] **Step 1: Deploy the bundle to dev**

Run: `databricks bundle deploy --target dev`
Expected: déploiement réussi, le job `dr_gameday` apparaît dans l'UI Databricks Jobs du workspace dev.

- [ ] **Step 2: Run the game day job (nominal case)**

Run: `databricks bundle run dr_gameday --target dev`
Expected: le run se termine en `SUCCEEDED`. Consulter les logs du run : baseline créé (5 clients, 6 transactions, 5 produits), sinistre simulé, restauration effectuée, vérification conforme, cleanup effectué, résumé JSON en sortie.

- [ ] **Step 3: Confirm no leftover state**

Run (depuis un notebook ou `databricks sql` — ou consulter les logs du Step 2) : vérifier que `source_demo01.sales_demo` et `source_demo01.hr_demo` n'existent plus après le run (cleanup effectif).
Expected: les deux schémas sont absents (le cleanup a bien tourné).

- [ ] **Step 4: Negative test — confirm failure detection and email**

Modifier temporairement une valeur dans `notebooks/demo/07_gameday.py` pour forcer un écart (par exemple, changer une des lignes `INSERT INTO ... clients VALUES` de l'étape Setup pour insérer une ligne de moins que ce que le backup restaurera — ou plus simplement, insérer une ligne supplémentaire dans `clients` juste après la capture du baseline et avant le sinistre, pour créer un écart artificiel). Redéployer (`databricks bundle deploy --target dev`) et relancer (`databricks bundle run dr_gameday --target dev`).
Expected: le run se termine en `FAILED`, l'exception `RuntimeError` contenant le détail de l'écart est visible dans les logs du run, et un email est reçu à l'adresse configurée dans `notification_email`. **Annuler la modification temporaire et redéployer la version correcte avant de terminer.**

- [ ] **Step 5: Restore the correct notebook version and redeploy**

```bash
git checkout notebooks/demo/07_gameday.py
databricks bundle deploy --target dev
```

Expected: `git status` ne montre plus de modification sur `notebooks/demo/07_gameday.py`, et le bundle redéployé correspond au commit de Task 2.

---

## Self-Review

**1. Spec coverage:**
- Notebook orchestrateur réutilisant 00_setup + 03_dr_scenario → Task 2. ✓
- Vérification stricte avec assertion/raise → Task 2 (Étape 3, `compare_row_counts` + `raise RuntimeError`). ✓
- Cleanup garanti en `finally` → Task 2 (Étape 3, bloc `finally`). ✓
- Job Databricks `dr_gameday`, cluster identique au daily, pas de schedule, email on_failure → Task 3. ✓
- Réutilisation du vrai dernier backup via `latest.json` (Approche A) → Task 2 (Étape 3, restauration). ✓
- Validation par exécution réelle + test négatif → Task 4. ✓
- Pas de suite pytest pour le notebook lui-même, mais logique de comparaison testée → Task 1. ✓

**2. Placeholder scan:** aucun `TBD`/`TODO` — toutes les étapes contiennent du code complet et des commandes exactes.

**3. Type consistency:** `compare_row_counts(expected: Dict[str, int], actual: Dict[str, int]) -> List[str]` défini dans Task 1 et utilisé à l'identique (mêmes noms de paramètres positionnels) dans Task 2, Étape 3 : `compare_row_counts(expected_counts, actual_counts)`.
