# DR Backup Databricks — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Mettre en place un backup DR journalier complet d'un workspace Azure Databricks (notebooks, jobs, clusters, UC metadata, Delta tables) vers un ADLS Gen2 DR avec Private Endpoint, incluant diff J/J-1, reconstruction catalogue et compte rendu.

**Architecture:** Pipeline hybride — CI/CD (GitHub Actions) exporte les assets workspace via Databricks CLI + REST API et déclenche un Databricks Job qui gère l'export UC metadata, le DEEP CLONE des tables Delta, le diff et la génération du rapport HTML.

**Tech Stack:** Python 3.10+, Databricks SDK (`databricks-sdk`), Databricks CLI v0.200+, PySpark (Databricks Runtime), pytest, GitHub Actions, ADLS Gen2 (abfss://), Terraform (infra déjà provisionnée).

---

## Structure de fichiers cible

```
files/
├── notebooks/
│   ├── 00_orchestrator.py
│   ├── 01_uc_metadata.py
│   ├── 02_data_clone.py
│   ├── 03_diff.py
│   └── 04_report.py
├── scripts/
│   ├── export_workspace.py
│   ├── restore_uc.py
│   └── restore_workspace.py
├── jobs/
│   └── dr_backup_job.json
├── lib/
│   ├── diff.py
│   └── report.py
├── tests/
│   ├── test_diff.py
│   ├── test_report.py
│   └── test_export_workspace.py
├── .github/workflows/
│   └── dr_backup.yml
├── docs/plans/         ← déjà existant
├── main.tf             ← déjà existant
├── variables.tf        ← déjà existant
├── outputs.tf          ← déjà existant
└── terraform.tfvars.example ← déjà existant
```

---

## Task 1: Structure projet + dépendances

**Files:**
- Create: `requirements.txt`
- Create: `requirements-dev.txt`
- Create: `lib/__init__.py`
- Create: `tests/__init__.py`

**Step 1: Créer requirements.txt**

```
databricks-sdk>=0.20.0
requests>=2.31.0
jinja2>=3.1.0
```

**Step 2: Créer requirements-dev.txt**

```
-r requirements.txt
pytest>=7.4.0
pytest-mock>=3.12.0
```

**Step 3: Créer les fichiers __init__.py vides**

```bash
touch lib/__init__.py tests/__init__.py
```

**Step 4: Installer les dépendances dev**

```bash
pip install -r requirements-dev.txt
```

**Step 5: Commit**

```bash
git add requirements.txt requirements-dev.txt lib/__init__.py tests/__init__.py
git commit -m "chore: setup project structure and dependencies"
```

---

## Task 2: Logique de diff (TDD)

**Files:**
- Create: `lib/diff.py`
- Create: `tests/test_diff.py`

### Step 1: Écrire les tests

```python
# tests/test_diff.py
import pytest
from lib.diff import compute_diff, BackupManifest

def make_manifest(date, tables, jobs, notebooks):
    return BackupManifest(
        date=date,
        tables=tables,
        jobs=jobs,
        notebooks=notebooks,
    )

def test_diff_no_changes():
    prev = make_manifest("2026-04-01", {"cat.sch.t1"}, {"job1"}, {"nb1"})
    curr = make_manifest("2026-04-02", {"cat.sch.t1"}, {"job1"}, {"nb1"})
    diff = compute_diff(prev, curr)
    assert diff["tables"]["added"] == []
    assert diff["tables"]["removed"] == []
    assert diff["jobs"]["added"] == []
    assert diff["notebooks"]["removed"] == []

def test_diff_added_table():
    prev = make_manifest("2026-04-01", {"cat.sch.t1"}, set(), set())
    curr = make_manifest("2026-04-02", {"cat.sch.t1", "cat.sch.t2"}, set(), set())
    diff = compute_diff(prev, curr)
    assert diff["tables"]["added"] == ["cat.sch.t2"]
    assert diff["tables"]["removed"] == []

def test_diff_removed_job():
    prev = make_manifest("2026-04-01", set(), {"job1", "job2"}, set())
    curr = make_manifest("2026-04-02", set(), {"job1"}, set())
    diff = compute_diff(prev, curr)
    assert diff["jobs"]["removed"] == ["job2"]
    assert diff["jobs"]["added"] == []

def test_diff_from_dicts():
    prev = {"date": "2026-04-01", "tables": ["a", "b"], "jobs": ["j1"], "notebooks": []}
    curr = {"date": "2026-04-02", "tables": ["a", "c"], "jobs": ["j1", "j2"], "notebooks": ["nb1"]}
    diff = compute_diff(
        BackupManifest.from_dict(prev),
        BackupManifest.from_dict(curr),
    )
    assert "b" not in diff["tables"]["added"]
    assert "b" in diff["tables"]["removed"]
    assert "c" in diff["tables"]["added"]
    assert "j2" in diff["jobs"]["added"]
    assert "nb1" in diff["notebooks"]["added"]
```

**Step 2: Lancer les tests — vérifier qu'ils échouent**

```bash
pytest tests/test_diff.py -v
```
Attendu : `ImportError: cannot import name 'compute_diff'`

**Step 3: Implémenter lib/diff.py**

```python
# lib/diff.py
from dataclasses import dataclass, field
from typing import Set, Dict, List, Any


@dataclass
class BackupManifest:
    date: str
    tables: Set[str] = field(default_factory=set)
    jobs: Set[str] = field(default_factory=set)
    notebooks: Set[str] = field(default_factory=set)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BackupManifest":
        return cls(
            date=d.get("date", ""),
            tables=set(d.get("tables", [])),
            jobs=set(d.get("jobs", [])),
            notebooks=set(d.get("notebooks", [])),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "tables": sorted(self.tables),
            "jobs": sorted(self.jobs),
            "notebooks": sorted(self.notebooks),
        }


def _set_diff(prev: Set[str], curr: Set[str]) -> Dict[str, List[str]]:
    return {
        "added": sorted(curr - prev),
        "removed": sorted(prev - curr),
        "unchanged": sorted(prev & curr),
    }


def compute_diff(prev: BackupManifest, curr: BackupManifest) -> Dict[str, Any]:
    return {
        "date_prev": prev.date,
        "date_curr": curr.date,
        "tables": _set_diff(prev.tables, curr.tables),
        "jobs": _set_diff(prev.jobs, curr.jobs),
        "notebooks": _set_diff(prev.notebooks, curr.notebooks),
    }
```

**Step 4: Lancer les tests — vérifier qu'ils passent**

```bash
pytest tests/test_diff.py -v
```
Attendu : `5 passed`

**Step 5: Commit**

```bash
git add lib/diff.py tests/test_diff.py
git commit -m "feat: add diff logic for backup comparison"
```

---

## Task 3: Génération du rapport HTML (TDD)

**Files:**
- Create: `lib/report.py`
- Create: `lib/templates/dr_report.html.j2`
- Create: `tests/test_report.py`

**Step 1: Écrire les tests**

```python
# tests/test_report.py
import pytest
from lib.report import generate_report


def test_report_contains_date():
    diff = {
        "date_prev": "2026-04-01",
        "date_curr": "2026-04-02",
        "tables": {"added": ["t1"], "removed": [], "unchanged": ["t2"]},
        "jobs": {"added": [], "removed": [], "unchanged": ["j1"]},
        "notebooks": {"added": [], "removed": [], "unchanged": ["nb1"]},
    }
    stats = {"total_tables": 2, "total_jobs": 1, "total_notebooks": 1, "data_size_gb": 12.5}
    steps = [
        {"name": "uc_metadata", "status": "success", "duration_s": 45},
        {"name": "data_clone", "status": "success", "duration_s": 320},
    ]
    html = generate_report(diff=diff, stats=stats, steps=steps)
    assert "2026-04-02" in html
    assert "t1" in html
    assert "success" in html
    assert "12.5" in html


def test_report_shows_errors():
    diff = {
        "date_prev": "2026-04-01",
        "date_curr": "2026-04-02",
        "tables": {"added": [], "removed": [], "unchanged": []},
        "jobs": {"added": [], "removed": [], "unchanged": []},
        "notebooks": {"added": [], "removed": [], "unchanged": []},
    }
    stats = {"total_tables": 0, "total_jobs": 0, "total_notebooks": 0, "data_size_gb": 0}
    steps = [
        {"name": "data_clone", "status": "error", "duration_s": 10, "error": "Timeout connecting to storage"},
    ]
    html = generate_report(diff=diff, stats=stats, steps=steps)
    assert "error" in html.lower()
    assert "Timeout connecting to storage" in html
```

**Step 2: Lancer les tests — vérifier qu'ils échouent**

```bash
pytest tests/test_report.py -v
```
Attendu : `ImportError`

**Step 3: Créer le template Jinja2**

```bash
mkdir -p lib/templates
```

```html
{# lib/templates/dr_report.html.j2 #}
<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <title>DR Backup Report — {{ diff.date_curr }}</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 2rem; }
    h1 { color: #1a1a2e; }
    .ok { color: green; font-weight: bold; }
    .error { color: red; font-weight: bold; }
    table { border-collapse: collapse; width: 100%; margin: 1rem 0; }
    th, td { border: 1px solid #ccc; padding: 8px; text-align: left; }
    th { background: #f0f0f0; }
    .added { background: #e6ffe6; }
    .removed { background: #ffe6e6; }
  </style>
</head>
<body>
  <h1>DR Backup Report</h1>
  <p><strong>Date backup :</strong> {{ diff.date_curr }}</p>
  <p><strong>Backup précédent :</strong> {{ diff.date_prev }}</p>

  <h2>Statistiques</h2>
  <table>
    <tr><th>Asset</th><th>Total</th></tr>
    <tr><td>Tables Delta</td><td>{{ stats.total_tables }}</td></tr>
    <tr><td>Jobs</td><td>{{ stats.total_jobs }}</td></tr>
    <tr><td>Notebooks</td><td>{{ stats.total_notebooks }}</td></tr>
    <tr><td>Volume données</td><td>{{ stats.data_size_gb }} GB</td></tr>
  </table>

  <h2>Étapes</h2>
  <table>
    <tr><th>Étape</th><th>Statut</th><th>Durée (s)</th><th>Erreur</th></tr>
    {% for step in steps %}
    <tr>
      <td>{{ step.name }}</td>
      <td class="{{ step.status }}">{{ step.status }}</td>
      <td>{{ step.duration_s }}</td>
      <td>{{ step.error | default('') }}</td>
    </tr>
    {% endfor %}
  </table>

  <h2>Diff vs J-1</h2>
  {% for category in ['tables', 'jobs', 'notebooks'] %}
  <h3>{{ category | capitalize }}</h3>
  <table>
    <tr><th>Type</th><th>Éléments</th></tr>
    <tr class="added"><td>Ajoutés</td><td>{{ diff[category].added | join(', ') or '—' }}</td></tr>
    <tr class="removed"><td>Supprimés</td><td>{{ diff[category].removed | join(', ') or '—' }}</td></tr>
    <tr><td>Inchangés</td><td>{{ diff[category].unchanged | length }} éléments</td></tr>
  </table>
  {% endfor %}
</body>
</html>
```

**Step 4: Implémenter lib/report.py**

```python
# lib/report.py
import os
from typing import Any, Dict, List
from jinja2 import Environment, FileSystemLoader


_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


def generate_report(
    diff: Dict[str, Any],
    stats: Dict[str, Any],
    steps: List[Dict[str, Any]],
) -> str:
    env = Environment(loader=FileSystemLoader(_TEMPLATES_DIR), autoescape=True)
    template = env.get_template("dr_report.html.j2")
    return template.render(diff=diff, stats=stats, steps=steps)
```

**Step 5: Lancer les tests — vérifier qu'ils passent**

```bash
pytest tests/test_report.py -v
```
Attendu : `2 passed`

**Step 6: Commit**

```bash
git add lib/report.py lib/templates/dr_report.html.j2 tests/test_report.py
git commit -m "feat: add HTML report generation with Jinja2 template"
```

---

## Task 4: Notebook 01_uc_metadata — Export DDL Unity Catalog

**Files:**
- Create: `notebooks/01_uc_metadata.py`

Ce notebook s'exécute sur un cluster Databricks avec Unity Catalog activé. Il exporte tous les DDL et grants en fichiers SQL vers ADLS DR.

**Step 1: Créer le notebook**

```python
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
output_path = f"{backup_root}/{backup_date}/uc_metadata"

assert backup_root.startswith("abfss://"), "backup_root doit commencer par abfss://"

# COMMAND ----------
# DBTITLE 1, Export catalogs

catalogs = [r.catalog for r in spark.sql("SHOW CATALOGS").collect() if r.catalog not in ("hive_metastore", "system")]
catalog_ddl = "\n".join([f"CREATE CATALOG IF NOT EXISTS `{c}`;" for c in catalogs])

dbutils.fs.put(f"{output_path}/01_catalogs.sql", catalog_ddl, overwrite=True)
print(f"[01_catalogs] {len(catalogs)} catalogs exportés : {catalogs}")

# COMMAND ----------
# DBTITLE 1, Export schemas

schema_ddls = []
for catalog in catalogs:
    schemas = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()]
    for schema in schemas:
        schema_ddls.append(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`;")

dbutils.fs.put(f"{output_path}/02_schemas.sql", "\n".join(schema_ddls), overwrite=True)
print(f"[02_schemas] {len(schema_ddls)} schemas exportés")

# COMMAND ----------
# DBTITLE 1, Export tables DDL

table_ddls = []
table_names = []
for catalog in catalogs:
    schemas = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()]
    for schema in schemas:
        tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
        for t in tables:
            fqn = f"`{catalog}`.`{schema}`.`{t.tableName}`"
            try:
                ddl_row = spark.sql(f"SHOW CREATE TABLE {fqn}").collect()[0][0]
                table_ddls.append(ddl_row + ";")
                table_names.append(f"{catalog}.{schema}.{t.tableName}")
            except Exception as e:
                print(f"[WARN] Impossible d'exporter {fqn}: {e}")

dbutils.fs.put(f"{output_path}/03_tables.sql", "\n\n".join(table_ddls), overwrite=True)
print(f"[03_tables] {len(table_ddls)} tables exportées")

# COMMAND ----------
# DBTITLE 1, Export grants

grant_statements = []
for catalog in catalogs:
    try:
        grants = spark.sql(f"SHOW GRANTS ON CATALOG `{catalog}`").collect()
        for g in grants:
            grant_statements.append(f"GRANT {g.ActionType} ON CATALOG `{catalog}` TO `{g.Principal}`;")
    except Exception as e:
        print(f"[WARN] Grants catalog {catalog}: {e}")

    schemas = [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()]
    for schema in schemas:
        try:
            grants = spark.sql(f"SHOW GRANTS ON SCHEMA `{catalog}`.`{schema}`").collect()
            for g in grants:
                grant_statements.append(f"GRANT {g.ActionType} ON SCHEMA `{catalog}`.`{schema}` TO `{g.Principal}`;")
        except Exception as e:
            print(f"[WARN] Grants schema {catalog}.{schema}: {e}")

dbutils.fs.put(f"{output_path}/05_grants.sql", "\n".join(grant_statements), overwrite=True)
print(f"[05_grants] {len(grant_statements)} grants exportés")

# COMMAND ----------
# DBTITLE 1, Retourner le manifest partiel

result = {
    "catalogs": catalogs,
    "table_names": table_names,
    "grant_count": len(grant_statements),
}
dbutils.notebook.exit(json.dumps(result))
```

**Step 2: Commit**

```bash
git add notebooks/01_uc_metadata.py
git commit -m "feat: add UC metadata export notebook (DDL + grants)"
```

---

## Task 5: Notebook 02_data_clone — DEEP CLONE Delta tables

**Files:**
- Create: `notebooks/02_data_clone.py`

**Step 1: Créer le notebook**

```python
# Databricks notebook source
# notebooks/02_data_clone.py

# COMMAND ----------
# MAGIC %md # 02 — Data Clone (DEEP CLONE + external copy)

# COMMAND ----------
import json
from datetime import date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
dbutils.widgets.text("backup_root", "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup YYYY-MM-DD")
dbutils.widgets.text("uc_metadata_result", "{}", "JSON result from 01_uc_metadata")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")
uc_result = json.loads(dbutils.widgets.get("uc_metadata_result"))
table_names = uc_result.get("table_names", [])

data_backup_root = f"{backup_root}/{backup_date}/data"

# COMMAND ----------
# DBTITLE 1, DEEP CLONE toutes les tables

clone_results = []
total_size_gb = 0.0

for fqn in table_names:
    catalog, schema, table = fqn.split(".")
    dest = f"{data_backup_root}/{catalog}/{schema}/{table}"
    try:
        result = spark.sql(f"CREATE OR REPLACE TABLE delta.`{dest}` DEEP CLONE `{catalog}`.`{schema}`.`{table}`")
        metrics = result.collect()[0].asDict()
        size_gb = metrics.get("num_output_bytes", 0) / (1024**3)
        total_size_gb += size_gb
        clone_results.append({"table": fqn, "status": "success", "size_gb": round(size_gb, 4)})
        print(f"[OK] DEEP CLONE {fqn} → {dest} ({size_gb:.2f} GB)")
    except Exception as e:
        clone_results.append({"table": fqn, "status": "error", "error": str(e)})
        print(f"[ERROR] {fqn}: {e}")

# COMMAND ----------
# DBTITLE 1, Sauvegarder le manifest des clones

clone_manifest_path = f"{backup_root}/{backup_date}/data/_clone_manifest.json"
dbutils.fs.put(clone_manifest_path, json.dumps(clone_results, indent=2), overwrite=True)

print(f"\n[Résumé] {len([r for r in clone_results if r['status']=='success'])} / {len(table_names)} tables clonées")
print(f"[Résumé] Volume total : {total_size_gb:.2f} GB")

# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "clone_results": clone_results,
    "total_size_gb": round(total_size_gb, 3),
}))
```

**Step 2: Commit**

```bash
git add notebooks/02_data_clone.py
git commit -m "feat: add Delta DEEP CLONE notebook for data backup"
```

---

## Task 6: Notebook 03_diff — Comparaison J vs J-1

**Files:**
- Create: `notebooks/03_diff.py`

**Step 1: Créer le notebook**

```python
# Databricks notebook source
# notebooks/03_diff.py

# COMMAND ----------
# MAGIC %md # 03 — Diff backup J vs J-1

# COMMAND ----------
import json
import sys
from datetime import date, timedelta

# Ajouter lib/ au path (chemin relatif dans le repo importé dans le workspace)
sys.path.insert(0, "/Workspace/Shared/dr-backup/lib")
from diff import compute_diff, BackupManifest

# COMMAND ----------
dbutils.widgets.text("backup_root", "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup J")
dbutils.widgets.text("current_manifest", "{}", "Manifest JSON du backup courant")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")
current_manifest_raw = json.loads(dbutils.widgets.get("current_manifest"))

# COMMAND ----------
# DBTITLE 1, Charger le manifest J-1

date_j = date.fromisoformat(backup_date)
date_j1 = (date_j - timedelta(days=1)).isoformat()
manifest_j1_path = f"{backup_root}/{date_j1}/manifest.json"

try:
    prev_raw = json.loads(dbutils.fs.head(manifest_j1_path, 1_000_000))
    print(f"[OK] Manifest J-1 chargé depuis {manifest_j1_path}")
except Exception as e:
    print(f"[WARN] Pas de manifest J-1 trouvé ({e}), diff depuis zéro")
    prev_raw = {"date": date_j1, "tables": [], "jobs": [], "notebooks": []}

# COMMAND ----------
# DBTITLE 1, Calculer le diff

prev_manifest = BackupManifest.from_dict(prev_raw)
curr_manifest = BackupManifest.from_dict(current_manifest_raw)

diff = compute_diff(prev_manifest, curr_manifest)

diff_path = f"{backup_root}/{backup_date}/diff/diff_{backup_date}.json"
dbutils.fs.put(diff_path, json.dumps(diff, indent=2), overwrite=True)

print(f"[Diff] Tables ajoutées: {diff['tables']['added']}")
print(f"[Diff] Tables supprimées: {diff['tables']['removed']}")
print(f"[Diff] Jobs ajoutés: {diff['jobs']['added']}")
print(f"[Diff] Notebooks ajoutés: {diff['notebooks']['added']}")

# COMMAND ----------
dbutils.notebook.exit(json.dumps(diff))
```

**Step 2: Commit**

```bash
git add notebooks/03_diff.py
git commit -m "feat: add diff notebook using lib/diff.py"
```

---

## Task 7: Notebook 04_report + Notebook 00_orchestrator

**Files:**
- Create: `notebooks/04_report.py`
- Create: `notebooks/00_orchestrator.py`

**Step 1: Créer le notebook rapport**

```python
# Databricks notebook source
# notebooks/04_report.py

# COMMAND ----------
# MAGIC %md # 04 — Génération du rapport DR

# COMMAND ----------
import json
import sys
from datetime import date

sys.path.insert(0, "/Workspace/Shared/dr-backup/lib")
from report import generate_report

# COMMAND ----------
dbutils.widgets.text("backup_root", "", "Backup root")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup")
dbutils.widgets.text("diff_json", "{}", "Diff JSON")
dbutils.widgets.text("stats_json", "{}", "Stats JSON")
dbutils.widgets.text("steps_json", "[]", "Steps JSON")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")
diff = json.loads(dbutils.widgets.get("diff_json"))
stats = json.loads(dbutils.widgets.get("stats_json"))
steps = json.loads(dbutils.widgets.get("steps_json"))

# COMMAND ----------
# DBTITLE 1, Générer le rapport HTML

html = generate_report(diff=diff, stats=stats, steps=steps)
report_path = f"{backup_root}/{backup_date}/report/dr_report_{backup_date}.html"
dbutils.fs.put(report_path, html, overwrite=True)
print(f"[OK] Rapport écrit : {report_path}")

# COMMAND ----------
dbutils.notebook.exit("ok")
```

**Step 2: Créer l'orchestrateur**

```python
# Databricks notebook source
# notebooks/00_orchestrator.py

# COMMAND ----------
# MAGIC %md # 00 — DR Backup Orchestrator

# COMMAND ----------
import json
import time
from datetime import date

# COMMAND ----------
dbutils.widgets.text("backup_root", "abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net", "Backup root")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup YYYY-MM-DD")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")

steps = []

def run_step(name, notebook_path, params):
    start = time.time()
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds=7200, arguments=params)
        duration = int(time.time() - start)
        steps.append({"name": name, "status": "success", "duration_s": duration})
        print(f"[OK] {name} ({duration}s)")
        return json.loads(result) if result and result != "ok" else {}
    except Exception as e:
        duration = int(time.time() - start)
        steps.append({"name": name, "status": "error", "duration_s": duration, "error": str(e)})
        print(f"[ERROR] {name}: {e}")
        return {}

# COMMAND ----------
# DBTITLE 1, Étape 1 — UC Metadata

base_params = {"backup_root": backup_root, "backup_date": backup_date}
uc_result = run_step("uc_metadata", "./01_uc_metadata", base_params)

# COMMAND ----------
# DBTITLE 1, Étape 2 — Data Clone

clone_result = run_step("data_clone", "./02_data_clone", {
    **base_params,
    "uc_metadata_result": json.dumps(uc_result),
})

# COMMAND ----------
# DBTITLE 1, Construire le manifest courant

table_names = uc_result.get("table_names", [])
current_manifest = {
    "date": backup_date,
    "tables": table_names,
    "jobs": [],       # rempli par le CI/CD côté workspace
    "notebooks": [],  # rempli par le CI/CD côté workspace
}

manifest_path = f"{backup_root}/{backup_date}/manifest.json"
dbutils.fs.put(manifest_path, json.dumps(current_manifest, indent=2), overwrite=True)

# COMMAND ----------
# DBTITLE 1, Étape 3 — Diff

diff_result = run_step("diff", "./03_diff", {
    **base_params,
    "current_manifest": json.dumps(current_manifest),
})

# COMMAND ----------
# DBTITLE 1, Étape 4 — Rapport

stats = {
    "total_tables": len(table_names),
    "total_jobs": len(current_manifest.get("jobs", [])),
    "total_notebooks": len(current_manifest.get("notebooks", [])),
    "data_size_gb": clone_result.get("total_size_gb", 0),
}

run_step("report", "./04_report", {
    **base_params,
    "diff_json": json.dumps(diff_result),
    "stats_json": json.dumps(stats),
    "steps_json": json.dumps(steps),
})

# COMMAND ----------
# DBTITLE 1, Mettre à jour latest.json

latest = {"date": backup_date, "manifest_path": manifest_path}
dbutils.fs.put(f"{backup_root}/latest.json", json.dumps(latest, indent=2), overwrite=True)

print(f"\n[DR Backup] Terminé — {backup_date}")
```

**Step 3: Commit**

```bash
git add notebooks/04_report.py notebooks/00_orchestrator.py
git commit -m "feat: add report and orchestrator notebooks"
```

---

## Task 8: Job Databricks — Configuration JSON

**Files:**
- Create: `jobs/dr_backup_job.json`

**Step 1: Créer la définition du job**

```json
{
  "name": "dr-backup-daily",
  "description": "DR Backup journalier — UC metadata + Delta DEEP CLONE + diff + rapport",
  "tags": {
    "project": "databricks-dr",
    "managed_by": "terraform"
  },
  "schedule": {
    "quartz_cron_expression": "0 0 2 * * ?",
    "timezone_id": "UTC",
    "pause_status": "UNPAUSED"
  },
  "tasks": [
    {
      "task_key": "orchestrator",
      "notebook_task": {
        "notebook_path": "/Shared/dr-backup/notebooks/00_orchestrator",
        "base_parameters": {
          "backup_root": "abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net",
          "backup_date": "{{tasks.orchestrator.start_time | strftime('%Y-%m-%d')}}"
        }
      },
      "existing_cluster_id": "__SET_CLUSTER_ID__",
      "timeout_seconds": 28800,
      "email_notifications": {
        "on_failure": ["__SET_EMAIL__"]
      }
    }
  ],
  "max_concurrent_runs": 1
}
```

**Step 2: Commit**

```bash
git add jobs/dr_backup_job.json
git commit -m "feat: add Databricks job definition for daily DR backup"
```

---

## Task 9: Script export workspace (CI/CD)

**Files:**
- Create: `scripts/export_workspace.py`
- Create: `tests/test_export_workspace.py`

**Step 1: Écrire les tests**

```python
# tests/test_export_workspace.py
import pytest
from unittest.mock import MagicMock, patch
from scripts.export_workspace import build_export_commands, write_json_asset


def test_build_export_commands_notebooks():
    cmds = build_export_commands(
        workspace_host="https://adb-123.azuredatabricks.net",
        backup_path="/tmp/backup/2026-04-02/workspace",
    )
    # Doit contenir la commande d'export notebooks
    assert any("workspace export-dir" in " ".join(c) for c in cmds)


def test_write_json_asset(tmp_path):
    data = {"id": 1, "name": "test-job"}
    out_file = tmp_path / "jobs.json"
    write_json_asset(data, str(out_file))
    import json
    assert json.loads(out_file.read_text()) == data
```

**Step 2: Vérifier que les tests échouent**

```bash
pytest tests/test_export_workspace.py -v
```
Attendu : `ImportError`

**Step 3: Implémenter scripts/export_workspace.py**

```python
# scripts/export_workspace.py
"""
Export des assets workspace Databricks via CLI + REST API.
Utilisé par le CI/CD pipeline avant de déclencher le Databricks Job.
"""
import json
import os
import subprocess
import sys
from datetime import date
from typing import Any, Dict, List, Tuple


def build_export_commands(workspace_host: str, backup_path: str) -> List[List[str]]:
    """Retourne la liste des commandes CLI à exécuter pour l'export workspace."""
    return [
        ["databricks", "workspace", "export-dir", "/", f"{backup_path}/notebooks", "--overwrite"],
    ]


def write_json_asset(data: Any, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def export_jobs(host: str, token: str, output_path: str) -> List[str]:
    import requests
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(f"{host}/api/2.1/jobs/list", headers=headers, params={"limit": 100})
    resp.raise_for_status()
    jobs = resp.json().get("jobs", [])
    write_json_asset(jobs, output_path)
    return [str(j["job_id"]) for j in jobs]


def export_clusters(host: str, token: str, output_path: str) -> None:
    import requests
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(f"{host}/api/2.0/clusters/list", headers=headers)
    resp.raise_for_status()
    write_json_asset(resp.json().get("clusters", []), output_path)


def export_policies(host: str, token: str, output_path: str) -> None:
    import requests
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(f"{host}/api/2.0/policies/clusters/list", headers=headers)
    resp.raise_for_status()
    write_json_asset(resp.json().get("policies", []), output_path)


def export_warehouses(host: str, token: str, output_path: str) -> None:
    import requests
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(f"{host}/api/2.0/sql/warehouses", headers=headers)
    resp.raise_for_status()
    write_json_asset(resp.json().get("warehouses", []), output_path)


def trigger_databricks_job(host: str, token: str, job_name: str, backup_date: str) -> int:
    import requests
    headers = {"Authorization": f"Bearer {token}"}
    # Chercher le job par nom
    resp = requests.get(f"{host}/api/2.1/jobs/list", headers=headers)
    resp.raise_for_status()
    jobs = resp.json().get("jobs", [])
    job = next((j for j in jobs if j["settings"]["name"] == job_name), None)
    if not job:
        raise ValueError(f"Job '{job_name}' introuvable dans le workspace")
    job_id = job["job_id"]
    run_resp = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=headers,
        json={"job_id": job_id, "notebook_params": {"backup_date": backup_date}},
    )
    run_resp.raise_for_status()
    run_id = run_resp.json()["run_id"]
    print(f"[OK] Job '{job_name}' déclenché — run_id={run_id}")
    return run_id


def main() -> None:
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    backup_date = os.environ.get("BACKUP_DATE", str(date.today()))
    local_backup_dir = os.environ.get("LOCAL_BACKUP_DIR", f"/tmp/dr-backup/{backup_date}")
    workspace_dir = f"{local_backup_dir}/workspace"

    os.makedirs(workspace_dir, exist_ok=True)

    print(f"[CI/CD] Export workspace — date={backup_date}")

    # 1. Export notebooks via CLI
    cmds = build_export_commands(host, workspace_dir)
    for cmd in cmds:
        print(f"  → {' '.join(cmd)}")
        subprocess.run(cmd, check=True, env={**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token})

    # 2. Export assets via REST API
    export_jobs(host, token, f"{workspace_dir}/jobs.json")
    export_clusters(host, token, f"{workspace_dir}/clusters.json")
    export_policies(host, token, f"{workspace_dir}/policies.json")
    export_warehouses(host, token, f"{workspace_dir}/sql_warehouses.json")

    print(f"[CI/CD] Export terminé → {workspace_dir}")

    # 3. Upload vers ADLS DR (via azcopy ou Azure CLI)
    backup_root = os.environ.get("BACKUP_ROOT", "")
    if backup_root:
        subprocess.run([
            "azcopy", "sync", workspace_dir,
            f"{backup_root}/{backup_date}/workspace",
            "--recursive"
        ], check=True)
        print(f"[CI/CD] Upload ADLS terminé → {backup_root}/{backup_date}/workspace")

    # 4. Déclencher le Databricks Job
    trigger_databricks_job(host, token, "dr-backup-daily", backup_date)


if __name__ == "__main__":
    main()
```

**Step 4: Lancer les tests — vérifier qu'ils passent**

```bash
pytest tests/test_export_workspace.py -v
```
Attendu : `2 passed`

**Step 5: Commit**

```bash
git add scripts/export_workspace.py tests/test_export_workspace.py
git commit -m "feat: add workspace export script for CI/CD pipeline"
```

---

## Task 10: GitHub Actions Workflow

**Files:**
- Create: `.github/workflows/dr_backup.yml`

**Step 1: Créer le workflow**

```yaml
# .github/workflows/dr_backup.yml
name: DR Backup Daily

on:
  schedule:
    - cron: '0 2 * * *'   # 02h00 UTC chaque jour
  workflow_dispatch:        # déclenchement manuel possible
    inputs:
      backup_date:
        description: 'Date backup (YYYY-MM-DD, défaut: aujourd'hui)'
        required: false
        default: ''

env:
  DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
  DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
  BACKUP_ROOT: ${{ secrets.BACKUP_ROOT }}    # abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net

jobs:
  export-workspace:
    name: Export workspace assets + trigger Databricks Job
    runs-on: ubuntu-latest

    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: pip install -r requirements.txt requests

      - name: Install Databricks CLI
        run: |
          curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
          databricks version

      - name: Install azcopy
        run: |
          wget -q https://aka.ms/downloadazcopy-v10-linux -O azcopy.tar.gz
          tar -xzf azcopy.tar.gz --strip-components=1 --wildcards '*/azcopy'
          sudo mv azcopy /usr/local/bin/
          azcopy --version

      - name: Set backup date
        run: |
          DATE="${{ github.event.inputs.backup_date }}"
          if [ -z "$DATE" ]; then DATE=$(date -u +%Y-%m-%d); fi
          echo "BACKUP_DATE=$DATE" >> $GITHUB_ENV
          echo "LOCAL_BACKUP_DIR=/tmp/dr-backup/$DATE" >> $GITHUB_ENV

      - name: Export workspace assets
        run: python scripts/export_workspace.py

      - name: Upload summary
        if: always()
        run: |
          echo "## DR Backup ${{ env.BACKUP_DATE }}" >> $GITHUB_STEP_SUMMARY
          echo "- Workspace export: terminé" >> $GITHUB_STEP_SUMMARY
          echo "- Databricks Job déclenché" >> $GITHUB_STEP_SUMMARY
```

**Step 2: Ajouter les secrets GitHub requis**

Dans GitHub → Settings → Secrets → Actions, créer :
- `DATABRICKS_HOST` : ex `https://adb-xxx.azuredatabricks.net`
- `DATABRICKS_TOKEN` : PAT Databricks avec droits `jobs:run`, `workspace:read`, `clusters:list`
- `BACKUP_ROOT` : `abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net`

**Step 3: Commit**

```bash
mkdir -p .github/workflows
git add .github/workflows/dr_backup.yml
git commit -m "feat: add GitHub Actions workflow for daily DR backup"
```

---

## Task 11: Script restore UC

**Files:**
- Create: `scripts/restore_uc.py`

**Step 1: Créer le script**

```python
# scripts/restore_uc.py
"""
Restauration du Unity Catalog depuis les dumps SQL.
Usage: python scripts/restore_uc.py --backup-date 2026-04-02 --backup-root abfss://...
Requiert: databricks CLI configuré + droits metastore admin
"""
import argparse
import os
import subprocess
import tempfile


SQL_FILES_ORDER = [
    "01_catalogs.sql",
    "02_schemas.sql",
    "03_tables.sql",
    "04_external_locations.sql",
    "05_grants.sql",
]


def download_sql_files(backup_root: str, backup_date: str, local_dir: str) -> None:
    """Télécharge les fichiers SQL depuis ADLS vers un dossier local."""
    src = f"{backup_root}/{backup_date}/uc_metadata"
    subprocess.run(
        ["azcopy", "sync", src, local_dir, "--recursive"],
        check=True
    )


def run_sql_file(sql_path: str, host: str, token: str) -> None:
    """Exécute un fichier SQL via le Databricks CLI (SQL exec)."""
    with open(sql_path, "r", encoding="utf-8") as f:
        statements = [s.strip() for s in f.read().split(";") if s.strip()]

    for stmt in statements:
        result = subprocess.run(
            ["databricks", "sql", "execute", "--statement", stmt],
            capture_output=True, text=True,
            env={**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
        )
        if result.returncode != 0 and "already exists" not in result.stderr.lower():
            print(f"[WARN] {stmt[:80]}... → {result.stderr.strip()}")
        else:
            print(f"[OK] {stmt[:80]}...")


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore Unity Catalog from SQL dump")
    parser.add_argument("--backup-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--backup-root", required=True, help="abfss://...")
    args = parser.parse_args()

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"[Restore UC] Téléchargement des dumps SQL depuis {args.backup_root}...")
        download_sql_files(args.backup_root, args.backup_date, tmpdir)

        for sql_file in SQL_FILES_ORDER:
            path = os.path.join(tmpdir, sql_file)
            if not os.path.exists(path):
                print(f"[SKIP] {sql_file} introuvable")
                continue
            print(f"\n[Restore UC] Exécution {sql_file}...")
            run_sql_file(path, host, token)

    print("\n[Restore UC] Terminé.")


if __name__ == "__main__":
    main()
```

**Step 2: Commit**

```bash
git add scripts/restore_uc.py
git commit -m "feat: add UC restore script from SQL dump"
```

---

## Task 12: Script restore workspace

**Files:**
- Create: `scripts/restore_workspace.py`

**Step 1: Créer le script**

```python
# scripts/restore_workspace.py
"""
Restauration des assets workspace (notebooks, jobs, clusters) depuis le backup.
Usage: python scripts/restore_workspace.py --backup-date 2026-04-02 --local-dir /tmp/backup
"""
import argparse
import json
import os
import subprocess
import requests


def restore_notebooks(notebooks_dir: str, host: str, token: str) -> None:
    """Re-importe les notebooks .dbc dans le workspace."""
    result = subprocess.run(
        ["databricks", "workspace", "import-dir", notebooks_dir, "/", "--overwrite"],
        capture_output=True, text=True,
        env={**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
    )
    if result.returncode != 0:
        print(f"[ERROR] Import notebooks: {result.stderr}")
    else:
        print(f"[OK] Notebooks importés depuis {notebooks_dir}")


def restore_jobs(jobs_json_path: str, host: str, token: str) -> None:
    """Recrée les jobs depuis jobs.json."""
    headers = {"Authorization": f"Bearer {token}"}
    with open(jobs_json_path, "r") as f:
        jobs = json.load(f)
    for job in jobs:
        settings = job.get("settings", job)
        resp = requests.post(f"{host}/api/2.1/jobs/create", headers=headers, json=settings)
        if resp.status_code == 200:
            print(f"[OK] Job créé: {settings.get('name', 'unknown')}")
        else:
            print(f"[WARN] Job {settings.get('name')}: {resp.text}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore workspace assets")
    parser.add_argument("--backup-date", required=True)
    parser.add_argument("--local-dir", required=True, help="Dossier local contenant le backup workspace/")
    args = parser.parse_args()

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    workspace_dir = os.path.join(args.local_dir, "workspace")

    print(f"[Restore Workspace] Date: {args.backup_date}")

    notebooks_dir = os.path.join(workspace_dir, "notebooks")
    if os.path.exists(notebooks_dir):
        restore_notebooks(notebooks_dir, host, token)

    jobs_path = os.path.join(workspace_dir, "jobs.json")
    if os.path.exists(jobs_path):
        restore_jobs(jobs_path, host, token)

    print("[Restore Workspace] Terminé.")


if __name__ == "__main__":
    main()
```

**Step 2: Lancer tous les tests**

```bash
pytest tests/ -v
```
Attendu : tous les tests passent.

**Step 3: Commit final**

```bash
git add scripts/restore_workspace.py
git commit -m "feat: add workspace restore script (notebooks + jobs)"
```

---

## Task 13: Validation end-to-end

**Prérequis :** Terraform apply déjà effectué, workspace Databricks accessible.

**Step 1: Importer les notebooks dans le workspace Databricks**

```bash
databricks workspace mkdir /Shared/dr-backup/notebooks
databricks workspace mkdir /Shared/dr-backup/lib
databricks workspace import-dir notebooks/ /Shared/dr-backup/notebooks --overwrite
databricks workspace import-dir lib/ /Shared/dr-backup/lib --overwrite
```

**Step 2: Créer le job dans Databricks**

```bash
databricks jobs create --json @jobs/dr_backup_job.json
```
Mettre à jour `existing_cluster_id` avec un cluster valide au préalable.

**Step 3: Tester manuellement le backup**

```bash
databricks jobs run-now --job-name "dr-backup-daily" \
  --notebook-params '{"backup_date": "2026-04-02"}'
```

**Step 4: Vérifier le rapport généré**

```bash
azcopy copy \
  "abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net/2026-04-02/report/dr_report_2026-04-02.html" \
  /tmp/dr_report.html
open /tmp/dr_report.html
```

**Step 5: Vérifier le diff**

```bash
azcopy copy \
  "abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net/2026-04-02/diff/diff_2026-04-02.json" \
  /tmp/diff.json
cat /tmp/diff.json
```

**Step 6: Tester la restauration UC (env de test uniquement)**

```bash
python scripts/restore_uc.py \
  --backup-date 2026-04-02 \
  --backup-root "abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net"
```

---

## Récapitulatif des secrets requis

| Secret | Valeur | Où |
|---|---|---|
| `DATABRICKS_HOST` | `https://adb-xxx.azuredatabricks.net` | GitHub Secrets + env local |
| `DATABRICKS_TOKEN` | PAT Databricks | GitHub Secrets + env local |
| `BACKUP_ROOT` | `abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net` | GitHub Secrets |

---

## Commandes utiles

```bash
# Lancer tous les tests
pytest tests/ -v

# Test du diff seul
pytest tests/test_diff.py -v

# Export workspace manuel
DATABRICKS_HOST=https://... DATABRICKS_TOKEN=... python scripts/export_workspace.py

# Restauration UC
DATABRICKS_HOST=https://... DATABRICKS_TOKEN=... \
  python scripts/restore_uc.py --backup-date 2026-04-02 \
  --backup-root "abfss://dr-backup@st10keyitdpdrpdevchn00.dfs.core.windows.net"
```
