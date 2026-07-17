# Databricks notebook source
# notebooks/10_restore_orchestrator.py

# COMMAND ----------
# MAGIC %md # 10 — DR Restore Orchestrator
# MAGIC
# MAGIC Notebook principal de restauration — sélectionner le périmètre via les widgets,
# MAGIC puis exécuter tout ou partie de la chaîne de restore.
# MAGIC
# MAGIC | Scope | Notebook appelé | Ce qui est restauré |
# MAGIC |-------|----------------|---------------------|
# MAGIC | `tables` | `07_restore` | Tables Delta (Unity Catalog) |
# MAGIC | `grants` | `11_restore_grants` | Permissions UC (GRANT sur catalogs, schemas, tables) |
# MAGIC | `jobs` | `09_restore_jobs` | Définitions de jobs Databricks |
# MAGIC | `notebooks` | `08_restore_workspace` | Sources notebooks (.py/.sql/.scala) |
# MAGIC | `acls` | `08_restore_workspace` | ACLs workspace + ACLs repos Git |
# MAGIC
# MAGIC > ⚠️ **`grants` et `acls` sont deux choses distinctes :**
# MAGIC > - `grants` = permissions Unity Catalog (`GRANT SELECT ON CATALOG ...`)
# MAGIC > - `acls`   = permissions workspace Databricks (notebooks, dossiers)
# MAGIC
# MAGIC **Toujours commencer avec `dry_run = true` pour valider le plan avant d'appliquer.**

# COMMAND ----------
import json
import time
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def _uc_head(path: str) -> str:
    # Invalide le cache de listing Spark pour ce chemin — sans ça, sur un cluster resté chaud,
    # une lecture antérieure (fichier vide/absent à ce moment-là) peut rester en cache même
    # après une réécriture complète du fichier.
    try:
        spark.catalog.refreshByPath(path)
    except Exception:
        pass
    return "\n".join(r.value for r in spark.read.text(path).collect())

# COMMAND ----------
# MAGIC %md ## Paramètres

# COMMAND ----------
from datetime import date

dbutils.widgets.text(       "backup_root",    "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup", "Backup root (abfss://...)")
dbutils.widgets.text(       "backup_date",    "",          "Date du backup (vide = dernier backup)")
dbutils.widgets.multiselect("restore_scope",  "jobs",      ["tables", "grants", "jobs", "notebooks", "acls"], "Périmètre de restauration")
dbutils.widgets.dropdown(   "dry_run",        "true",      ["true", "false"], "Dry-run (true = simulation)")

# Paramètres tables (utilisés si scope contient 'tables')
dbutils.widgets.dropdown(   "restore_level",  "incremental", ["incremental", "weekly", "monthly"], "Niveau restauration tables")
dbutils.widgets.text(       "restore_point",  "",          "Point de restauration (timestamp ou label, vide = dernier)")
dbutils.widgets.text(       "source_table",   "",          "Table(s) à restaurer (vide = toutes)")
dbutils.widgets.text(       "target_catalog", "",          "Catalog cible tables (vide = même que source)")

# Paramètres grants (utilisés si scope contient 'grants')
dbutils.widgets.text(       "catalog_filter", "",          "Catalogs à restaurer pour les grants (vide = tous)")

# Paramètres jobs (utilisés si scope contient 'jobs')
dbutils.widgets.text(       "job_filter",     "",          "Filtre nom de job (vide = tous)")
dbutils.widgets.dropdown(   "conflict_mode",  "skip",      ["skip", "recreate"], "Jobs existants : skip / recreate")

backup_root    = dbutils.widgets.get("backup_root").strip().rstrip("/")
backup_date    = dbutils.widgets.get("backup_date").strip()
restore_scope  = set(s.strip() for s in dbutils.widgets.get("restore_scope").split(",") if s.strip())
dry_run        = dbutils.widgets.get("dry_run")

restore_level  = dbutils.widgets.get("restore_level")
restore_point  = dbutils.widgets.get("restore_point").strip()
source_table   = dbutils.widgets.get("source_table").strip()
target_catalog = dbutils.widgets.get("target_catalog").strip()

catalog_filter = dbutils.widgets.get("catalog_filter").strip()

job_filter     = dbutils.widgets.get("job_filter").strip()
conflict_mode  = dbutils.widgets.get("conflict_mode")

if not backup_root:
    raise ValueError("backup_root est vide — renseignez le chemin abfss://...")

# Auto-détection de la dernière date de backup
if not backup_date:
    try:
        latest = json.loads(_uc_head(f"{backup_root}/latest.json"))
        backup_date = latest["date"]
        print(f"[AUTO] Dernière date de backup détectée : {backup_date}")
    except Exception as e:
        raise ValueError(f"backup_date vide et latest.json introuvable : {e}")

print(f"""
[OK] backup_root    = {backup_root}
[OK] backup_date    = {backup_date}
[OK] restore_scope  = {sorted(restore_scope)}
[OK] dry_run        = {dry_run}
""")

if "tables" in restore_scope:
    print(f"[OK] restore_level  = {restore_level}")
    print(f"[OK] restore_point  = {restore_point or '(dernier backup)'}")
    print(f"[OK] source_table   = {source_table or '(toutes)'}")
    print(f"[OK] target_catalog = {target_catalog or '(même que source)'}")

if "grants" in restore_scope:
    print(f"[OK] catalog_filter = {catalog_filter or '(tous)'}")

if "jobs" in restore_scope:
    print(f"[OK] job_filter     = {job_filter or '(tous)'}")
    print(f"[OK] conflict_mode  = {conflict_mode}")

# COMMAND ----------
# MAGIC %md ## Helper — run_step

# COMMAND ----------
steps         = []
global_status = "success"

def run_step(name: str, notebook_path: str, params: dict, critical: bool = False) -> dict:
    """
    Exécute un notebook enfant via dbutils.notebook.run().
    Si critical=True et que le notebook échoue, interrompt le pipeline.
    """
    global global_status
    start = time.time()
    print(f"\n{'═'*60}")
    print(f"  DÉMARRAGE : {name}")
    print(f"{'═'*60}")
    try:
        result = dbutils.notebook.run(notebook_path, timeout_seconds=28800, arguments=params)
        duration = int(time.time() - start)
        steps.append({"name": name, "status": "success", "duration_s": duration})
        print(f"\n[OK] {name} terminé en {duration}s")
        return json.loads(result) if result and result.strip().startswith("{") else {}
    except Exception as e:
        duration = int(time.time() - start)
        steps.append({"name": name, "status": "error", "duration_s": duration, "error": str(e)})
        global_status = "degraded"
        print(f"\n[ERROR] {name} échoué après {duration}s : {e}")
        if critical:
            raise RuntimeError(f"Étape critique '{name}' échouée — pipeline interrompu : {e}")
        return {}

# COMMAND ----------
# MAGIC %md ## Étape 1 — Restauration Tables Delta

# COMMAND ----------
tables_result = {}

if "tables" in restore_scope:
    tables_result = run_step(
        name          = "restore_tables",
        notebook_path = "./07_restore",
        params        = {
            "backup_root":    backup_root,
            "restore_level":  restore_level,
            "source_table":   source_table,
            "restore_point":  restore_point,
            "target_catalog": target_catalog,
            "target_schema":  "",
            "dry_run":        dry_run,
        },
        critical = False,
    )
else:
    print("[SKIP] Tables — non sélectionné dans restore_scope")

# COMMAND ----------
# MAGIC %md ## Étape 2 — Restauration Grants Unity Catalog

# COMMAND ----------
grants_result = {}

if "grants" in restore_scope:
    grants_result = run_step(
        name          = "restore_grants",
        notebook_path = "./11_restore_grants",
        params        = {
            "backup_root":    backup_root,
            "backup_date":    backup_date,
            "catalog_filter": catalog_filter,
            "dry_run":        dry_run,
        },
        critical = False,
    )
else:
    print("[SKIP] Grants UC — non sélectionné dans restore_scope")

# COMMAND ----------
# MAGIC %md ## Étape 3 — Restauration Jobs

# COMMAND ----------
jobs_result = {}

if "jobs" in restore_scope:
    jobs_result = run_step(
        name          = "restore_jobs",
        notebook_path = "./09_restore_jobs",
        params        = {
            "backup_root":   backup_root,
            "backup_date":   backup_date,
            "job_filter":    job_filter,
            "conflict_mode": conflict_mode,
            "dry_run":       dry_run,
        },
        critical = False,
    )
else:
    print("[SKIP] Jobs — non sélectionné dans restore_scope")

# COMMAND ----------
# MAGIC %md ## Étape 4 — Restauration Notebooks (sources)

# COMMAND ----------
notebooks_result = {}

if "notebooks" in restore_scope:
    notebooks_result = run_step(
        name          = "restore_notebooks",
        notebook_path = "./08_restore_workspace",
        params        = {
            "backup_root":           backup_root,
            "backup_date":           backup_date,
            "restore_type":          "notebooks",
            "notebook_filter":       "",
            "target_workspace_path": "",
            "dry_run":               dry_run,
        },
        critical = False,
    )
else:
    print("[SKIP] Notebooks — non sélectionné dans restore_scope")

# COMMAND ----------
# MAGIC %md ## Étape 5 — Restauration ACLs (workspace + repos)

# COMMAND ----------
acls_result = {}

if "acls" in restore_scope:
    acls_result = run_step(
        name          = "restore_acls",
        notebook_path = "./08_restore_workspace",
        params        = {
            "backup_root":           backup_root,
            "backup_date":           backup_date,
            "restore_type":          "acls",
            "notebook_filter":       "",
            "target_workspace_path": "",
            "dry_run":               dry_run,
        },
        critical = False,
    )
else:
    print("[SKIP] ACLs — non sélectionné dans restore_scope")

# COMMAND ----------
# MAGIC %md ## Résumé final

# COMMAND ----------
dry_label = "DRY-RUN — " if dry_run == "true" else ""

print(f"""
╔══════════════════════════════════════════════════════════════╗
║         {dry_label}DR RESTORE — RÉSUMÉ FINAL
╠══════════════════════════════════════════════════════════════╣
║  Date backup  : {backup_date:<45} ║
║  Périmètre    : {str(sorted(restore_scope)):<45} ║
║  Statut global: {global_status:<45} ║
╠══════════════════════════════════════════════════════════════╣
║  Étape                   Statut       Durée                  ║
║  ────────────────────    ──────────   ──────                 ║""")

for s in steps:
    status_icon = "OK" if s["status"] == "success" else "ERROR"
    print(f"║  {s['name']:<24}  [{status_icon:<6}]    {s['duration_s']:>4}s                 ║")

if not steps:
    print(f"║  (aucune étape exécutée)                                     ║")

print(f"""╠══════════════════════════════════════════════════════════════╣""")

# Détails par périmètre
if tables_result:
    t_ok  = tables_result.get("ok", 0)
    t_err = tables_result.get("errors", 0)
    print(f"║  Tables    : {t_ok} restaurées, {t_err} erreurs{'':<33}║")

if grants_result:
    g_ok   = grants_result.get("ok", 0)
    g_skip = grants_result.get("skipped", 0)
    g_err  = grants_result.get("errors", 0)
    print(f"║  Grants UC : {g_ok} appliqués, {g_skip} ignorés, {g_err} erreurs{'':<25}║")

if jobs_result:
    j_ok   = jobs_result.get("ok", 0)
    j_skip = jobs_result.get("skipped", 0)
    j_err  = jobs_result.get("errors", 0)
    print(f"║  Jobs      : {j_ok} créés, {j_skip} ignorés, {j_err} erreurs{'':<26}║")

if notebooks_result:
    nb_ok  = notebooks_result.get("nb_ok", 0)
    nb_err = notebooks_result.get("nb_error", 0)
    print(f"║  Notebooks : {nb_ok} restaurés, {nb_err} erreurs{'':<31}║")

if acls_result:
    a_ok  = acls_result.get("acl_nb_ok", 0) + acls_result.get("acl_repo_ok", 0)
    a_err = acls_result.get("acl_nb_error", 0) + acls_result.get("acl_repo_error", 0)
    print(f"║  ACLs      : {a_ok} restaurées, {a_err} erreurs{'':<31}║")

print(f"""╠══════════════════════════════════════════════════════════════╣""")

if dry_run == "true":
    print(f"║  MODE DRY-RUN : aucune modification appliquée               ║")
    print(f"║  → Repasser dry_run = false pour exécuter                   ║")
else:
    if global_status == "success":
        print(f"║  Restauration appliquée avec succès                         ║")
    else:
        print(f"║  Restauration terminée avec des erreurs — vérifier les logs ║")

print(f"╚══════════════════════════════════════════════════════════════╝")

dbutils.notebook.exit(json.dumps({
    "backup_date":    backup_date,
    "restore_scope":  sorted(restore_scope),
    "dry_run":        dry_run == "true",
    "global_status":  global_status,
    "steps":          steps,
    "tables":         tables_result,
    "grants":         grants_result,
    "jobs":           jobs_result,
    "notebooks":      notebooks_result,
    "acls":           acls_result,
}))
