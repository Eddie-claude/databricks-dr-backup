# Databricks notebook source
# notebooks/demo/00_demo_guide.py

# COMMAND ----------
# MAGIC %md
# MAGIC # 🎯 DR Backup — Guide de Démonstration Client
# MAGIC
# MAGIC **Projet** : Solution DR Backup Databricks — Groupe Mutuelle
# MAGIC **Périmètre** : Unity Catalog + Delta DEEP CLONE + Workspace Assets
# MAGIC **Durée totale** : ~22 minutes
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Architecture de la solution
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                          DATABRICKS WORKSPACE                                    │
# MAGIC │                                                                                  │
# MAGIC │   SOURCES DE BACKUP                  JOB dr-backup-daily (2h UTC — DAB prod)    │
# MAGIC │   ┌──────────────────┐               ┌──────────────────────────────────────┐   │
# MAGIC │   │ Unity Catalog    │               │ 00_orchestrator                      │   │
# MAGIC │   │ ├ Catalogs       │──────────────▶│   ├─ 01_uc_metadata  (SQL DDL dump)  │   │
# MAGIC │   │ ├ Schemas        │               │   ├─ 02_data_clone   (DEEP CLONE)    │   │
# MAGIC │   │ ├ Tables/Vues    │               │   ├─ 03_diff         (J vs J-1)      │   │
# MAGIC │   │ └ Grants         │               │   ├─ 04_report       (HTML)          │   │
# MAGIC │   └──────────────────┘               │   └─ 05_workspace_config             │   │
# MAGIC │                                      └──────────────────┬───────────────────┘   │
# MAGIC │   ┌──────────────────┐                                  │                       │
# MAGIC │   │ Delta Tables     │──────────────▶ (DEEP CLONE)      │                       │
# MAGIC │   │ (données métier) │                                  │                       │
# MAGIC │   └──────────────────┘                                  ▼                       │
# MAGIC │                                      ┌──────────────────────────────────────┐   │
# MAGIC │   ┌──────────────────┐               │  ADLS Gen2  (West Europe)            │   │
# MAGIC │   │ Workspace Config │               │  st10keyitdpdrpdevwe00 / uc-data     │   │
# MAGIC │   │ ├ ACLs notebooks │──────────────▶│                                      │   │
# MAGIC │   │ └ ACLs repos     │  REST API     │  dr-backup/                          │   │
# MAGIC │   └──────────────────┘               │  ├── latest.json                     │   │
# MAGIC │                                      │  └── YYYY-MM-DD/                     │   │
# MAGIC │   ┌──────────────────┐               │      ├── uc_metadata/  *.sql         │   │
# MAGIC │   │ Jobs / Notebooks │──────────────▶│      ├── data/         Delta clones  │   │
# MAGIC │   └──────────────────┘  REST API     │      ├── diff/         *.json        │   │
# MAGIC │                                      │      ├── report/       *.html        │   │
# MAGIC │                                      │      └── workspace_config/  *.json   │   │
# MAGIC │                                      └──────────────────────────────────────┘   │
# MAGIC │                                                          ▲                       │
# MAGIC └──────────────────────────────────────────────────────────┼───────────────────────┘
# MAGIC                                                            │
# MAGIC            ┌──────────────────────────────────────────────┐│
# MAGIC            │  GitHub Actions  (CI/CD)                     ││
# MAGIC            │  ├─ databricks bundle deploy  (DAB prod)     ││
# MAGIC            │  ├─ export notebooks  (workspace export-dir) ││
# MAGIC            │  └─ export jobs  (REST API)               ───┘│
# MAGIC            └──────────────────────────────────────────────┘
# MAGIC
# MAGIC ┌──────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                           RESTAURATION (DR)                                      │
# MAGIC │                                                                                  │
# MAGIC │  restore_uc.py            → SQL DDL replay  (catalogs / schemas / tables / grants)│
# MAGIC │  03_dr_scenario           → Delta DEEP CLONE depuis ADLS                         │
# MAGIC │  restore_workspace_config → ACLs notebooks / repos               (REST API)       │
# MAGIC │  restore_workspace.py     → Jobs / Notebooks                     (REST API + CLI) │
# MAGIC │  06_restore_workspace     → Demo dry-run depuis notebook (sans download local)   │
# MAGIC └──────────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## 📋 Plan de la démonstration
# MAGIC
# MAGIC | # | Acte | Notebook | Durée |
# MAGIC |---|------|----------|-------|
# MAGIC | 1 | Setup — Création des données de démo | `00_setup` | ~3 min |
# MAGIC | 2 | Simulation de changements métier (J+1) | `01_simulate_changes` | ~2 min |
# MAGIC | 3 | Diff J/J-1 et rapport HTML | `02_show_report` | ~2 min |
# MAGIC | 4 | Scénario DR : sinistre + restauration | `03_dr_scenario` | ~5 min |
# MAGIC | 5 | Workspace Config & ACLs | `05_workspace_acl` | ~3 min |
# MAGIC | 6 | Restauration Jobs & Notebooks (dry-run) | `06_restore_workspace` | ~3 min |
# MAGIC | 7 | Monitoring & job planifié | *(ce notebook)* | ~2 min |
# MAGIC | — | Cleanup | `04_cleanup` | ~1 min |

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## Acte 5 (dans ce notebook) — Monitoring & job planifié

# COMMAND ----------
# MAGIC %md ### 5.1 — Statut du dernier backup

# COMMAND ----------
import json

backup_root = "abfss://uc-data@st10keyitdpdrpdevwe00.dfs.core.windows.net/dr-backup"

latest = json.loads(dbutils.fs.head(f"{backup_root}/latest.json"))

status_icon = {"success": "✅", "degraded": "⚠️"}.get(latest["status"], "❌")
print(f"""
╔══════════════════════════════════════════╗
║         DERNIER BACKUP DR               ║
╠══════════════════════════════════════════╣
║  Date    : {latest['date']:<30} ║
║  Statut  : {status_icon} {latest['status']:<27} ║
╠══════════════════════════════════════════╣""")

for step, result in latest.get("steps_summary", {}).items():
    icon = "✅" if result == "success" else "❌"
    print(f"║  {step:<12}: {icon} {result:<22} ║")
print("╚══════════════════════════════════════════╝")

# COMMAND ----------
# MAGIC %md ### 5.2 — Historique des backups disponibles

# COMMAND ----------
print("Backups disponibles dans ADLS :\n")
dirs = dbutils.fs.ls(backup_root)
backup_dirs = sorted([d for d in dirs if d.name.startswith("20")], key=lambda x: x.name, reverse=True)

for d in backup_dirs:
    date_str = d.name.rstrip("/")
    try:
        files = dbutils.fs.ls(f"{backup_root}/{date_str}")
        file_names = [f.name for f in files]
        has_clone  = "data/" in file_names
        has_uc     = "uc_metadata/" in file_names
        has_diff   = "diff/" in file_names
        has_report = "report/" in file_names

        icons = (
            ("✅" if has_uc else "❌") + " UC  " +
            ("✅" if has_clone else "❌") + " Clone  " +
            ("✅" if has_diff else "❌") + " Diff  " +
            ("✅" if has_report else "❌") + " Rapport"
        )
        print(f"  📅 {date_str}  |  {icons}")
    except:
        print(f"  📅 {date_str}  |  (inaccessible)")

# COMMAND ----------
# MAGIC %md ### 5.3 — Job planifié (2h UTC tous les jours)

# COMMAND ----------
import requests

token = dbutils.secrets.get(scope="dr-backup", key="databricks-token")
host  = spark.conf.get("spark.databricks.workspaceUrl")

resp = requests.get(
    f"{host}/api/2.1/jobs/get?job_id=273594527444521",
    headers={"Authorization": f"Bearer {token}"},
    timeout=15
)
job = resp.json()
settings = job.get("settings", {})
schedule = settings.get("schedule", {})

print(f"""
Job DR Backup :
  Nom      : {settings.get('name')}
  Schedule : {schedule.get('quartz_cron_expression')} ({schedule.get('timezone_id')})
  Statut   : {schedule.get('pause_status')}
  Timeout  : {settings.get('tasks', [{}])[0].get('timeout_seconds', 0) // 3600}h
""")

# COMMAND ----------
# MAGIC %md ### 5.4 — Mode dégradé (résilience)

# COMMAND ----------
print("""
COMPORTEMENT EN CAS D'ERREUR PARTIELLE :
─────────────────────────────────────────
  ✅ uc_metadata  → CRITIQUE  : échec = pipeline arrêté
  ✅ data_clone   → CRITIQUE  : échec = pipeline arrêté
  ⚠️  diff        → NON CRIT. : échec = pipeline continue, statut = "degraded"
  ⚠️  report      → NON CRIT. : échec = pipeline continue, statut = "degraded"

  → Notification email à efi@keyit.ch en cas d'échec du job
  → latest.json toujours mis à jour avec le statut réel
  → Les données critiques (UC + clone) sont toujours protégées
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ---
# MAGIC ## ✅ Résumé de la démonstration
# MAGIC
# MAGIC | Fonctionnalité | Validé |
# MAGIC |----------------|--------|
# MAGIC | Backup UC Metadata (DDL + grants) | ✅ |
# MAGIC | Backup Delta DEEP CLONE | ✅ |
# MAGIC | Diff J/J-1 (tables ajoutées/supprimées) | ✅ |
# MAGIC | Rapport HTML automatique | ✅ |
# MAGIC | Export Jobs & Notebooks (CI/CD) | ✅ |
# MAGIC | Restauration UC depuis dump SQL | ✅ |
# MAGIC | Restauration données depuis clone | ✅ |
# MAGIC | Restauration grants UC | ✅ |
# MAGIC | Job planifié (2h UTC) + alertes email | ✅ |
# MAGIC | Mode dégradé (résilience partielle) | ✅ |
# MAGIC
# MAGIC ### Prochaines étapes recommandées
# MAGIC 1. **Terraform** — provisionner le storage DR en Switzerland North (`st10keyitdpdrpdevchn00`)
# MAGIC 2. **GitHub Actions** — configurer les secrets pour l'export workspace automatisé
# MAGIC 3. **Phase 2** — couvrir les secrets Databricks (scope/key backup)
