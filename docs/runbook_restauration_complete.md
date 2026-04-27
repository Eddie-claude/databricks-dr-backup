# Runbook — Restauration Complète Databricks DR

**Version :** 1.0  
**Date :** 2026-04-07  
**Contexte :** Groupe Mutuelle — Workspace Azure Databricks (Switzerland North)

---

## Périmètre

Ce runbook couvre la restauration complète de l'environnement Databricks suite à un sinistre.  
Il distingue deux scénarios :

| Scénario | Description | Durée estimée |
|----------|-------------|---------------|
| **A — Partiel** | Perte de données/schémas UC uniquement (workspace intact) | ~30 min |
| **B — Total** | Workspace Databricks détruit, tout doit être recréé | ~4–8h |

---

## Prérequis

- [ ] Accès Azure Portal (Owner sur le resource group)
- [ ] Databricks CLI installé (`pip install databricks-cli`)
- [ ] Python 3.9+ + `pip install requests`
- [ ] AzCopy installé (pour télécharger le backup depuis ADLS)
- [ ] PAT Token valide sur le workspace cible
- [ ] Accès en lecture sur `st10keyitdpdrpdevchn00` (container `uc-data`)

---

## 1. Identifier le backup à restaurer

```bash
# Lister les backups disponibles
azcopy list "https://st10keyitdpdrpdevchn00.dfs.core.windows.net/uc-data/dr-backup" \
    --recursive=false

# Vérifier le dernier backup validé
azcopy cat "https://st10keyitdpdrpdevchn00.dfs.core.windows.net/uc-data/dr-backup/latest.json"
```

Retient la date `BACKUP_DATE` (ex: `2026-04-07`).

---

## 2. Télécharger le backup localement

```bash
export BACKUP_DATE=2026-04-07
export LOCAL_BACKUP=/tmp/dr-restore/$BACKUP_DATE

azcopy sync \
    "https://st10keyitdpdrpdevchn00.dfs.core.windows.net/uc-data/dr-backup/$BACKUP_DATE" \
    "$LOCAL_BACKUP" \
    --recursive
```

Structure téléchargée :
```
$LOCAL_BACKUP/
├── uc_metadata/          # DDL SQL : catalogs, schemas, tables, grants
├── data/                 # Delta DEEP CLONE des tables
├── workspace/
│   ├── notebooks/        # Export des notebooks workspace
│   ├── jobs.json         # Définitions des jobs
│   └── sql_warehouses.json
└── workspace_config/
    ├── cluster_policies.json
    ├── clusters.json
    ├── workspace_acls.json
    └── repos_acls.json
```

---

## SCÉNARIO A — Restauration Partielle (workspace intact)

### A.1 — Restaurer la structure UC et les données

```bash
export DATABRICKS_HOST=https://adb-2547670924000766.6.azuredatabricks.net
export DATABRICKS_TOKEN=dapiXXXX

# Restaurer catalogs + schemas + tables + grants (SQL replay)
python scripts/restore_uc.py \
    --backup-date $BACKUP_DATE \
    --backup-root "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup"
```

> ⚠️ Le script utilise `databricks sql execute` statement par statement.  
> Les `already exists` sont ignorés automatiquement.

### A.2 — Restaurer les données Delta

Depuis un notebook Databricks (ou le notebook `03_dr_scenario`) :

```python
# Pour chaque table dans le manifest :
spark.sql(f"""
    CREATE OR REPLACE TABLE {catalog}.{schema}.{table}
    DEEP CLONE delta.`{backup_root}/{backup_date}/data/{catalog}/{schema}/{table}`
""")
```

### A.3 — Restaurer les configs workspace (si nécessaire)

```bash
python scripts/restore_workspace_config.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-acls \
    --restore-policies
```

---

## SCÉNARIO B — Restauration Totale (nouveau workspace)

### B.1 — Recréer l'infrastructure Azure (Terraform)

> ⚠️ Cette étape est hors scope du backup applicatif.

```bash
# Depuis le répertoire terraform du projet
terraform init
terraform apply -var-file=terraform.tfvars
```

Ressources recréées :
- Workspace Databricks (Premium)
- VNET + Private Endpoints
- ADLS Gen2 + Storage Credentials + External Locations
- Unity Catalog Metastore (si détruit)

### B.2 — Configurer le CLI sur le nouveau workspace

```bash
export DATABRICKS_HOST=https://<nouveau-workspace>.azuredatabricks.net
export DATABRICKS_TOKEN=<nouveau-PAT>

databricks configure --host $DATABRICKS_HOST --token $DATABRICKS_TOKEN
```

### B.3 — Restaurer la structure Unity Catalog

```bash
python scripts/restore_uc.py \
    --backup-date $BACKUP_DATE \
    --backup-root "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup"
```

Ordre d'exécution automatique :
1. `01_catalogs.sql` — recrée les catalogs
2. `02_schemas.sql` — recrée les schemas
3. `03_tables.sql` — recrée les tables (DDL)
4. `04_grants.sql` — restaure les permissions

### B.4 — Restaurer les données Delta

> Les données sont dans ADLS (inchangé après sinistre Databricks).

Depuis un notebook Databricks sur le nouveau workspace :

```python
backup_root = "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup"
backup_date = "2026-04-07"

clone_manifest = json.loads(dbutils.fs.head(f"{backup_root}/{backup_date}/data/_clone_manifest.json"))
for entry in clone_manifest:
    if entry["status"] != "success":
        continue
    table = entry["table"]
    parts = table.split(".")
    clone_path = f"{backup_root}/{backup_date}/data/{'/'.join(parts)}"
    spark.sql(f"CREATE OR REPLACE TABLE {table} DEEP CLONE delta.`{clone_path}`")
    print(f"  ✅ {table}")
```

### B.5 — Restaurer les Notebooks

```bash
# Dry-run d'abord pour vérifier
python scripts/restore_workspace.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-notebooks \
    --target-dir /Shared/dr-backup \
    --dry-run

# Appliquer
python scripts/restore_workspace.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-notebooks \
    --target-dir /Shared/dr-backup
```

### B.6 — Restaurer les Jobs

```bash
# Dry-run : liste les jobs qui seraient créés
python scripts/restore_workspace.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-jobs \
    --dry-run

# Appliquer (les jobs DAB sont automatiquement ignorés)
python scripts/restore_workspace.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-jobs
```

> ℹ️ Les jobs gérés par Databricks Asset Bundles (tag `bundle`) sont **ignorés** —  
> ils seront redéployés via `databricks bundle deploy` dans l'étape suivante.

### B.7 — Redéployer les jobs DAB (bundle)

```bash
# Depuis le repo GitHub, sur le nouveau workspace
export DATABRICKS_HOST=<nouveau-workspace>
export DATABRICKS_TOKEN=<nouveau-PAT>

databricks bundle deploy --target prod
```

Ou déclencher le workflow GitHub Actions `DR Backup DAB`.

### B.8 — Restaurer les cluster policies et ACLs

```bash
python scripts/restore_workspace_config.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-policies \
    --restore-clusters \
    --restore-acls
```

### B.9 — Restaurer les SQL Warehouses

```bash
python scripts/restore_workspace.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-warehouses
```

---

## 3. Vérification post-restauration

### Via notebooks Databricks (démo)

Exécuter dans l'ordre :
1. `notebooks/demo/03_dr_scenario` — vérifie tables et données
2. `notebooks/demo/05_workspace_acl` — vérifie grants et workspace_config

### Via CLI

```bash
# Vérifier les catalogs
databricks unity-catalog catalogs list

# Vérifier les jobs
databricks jobs list

# Vérifier les notebooks
databricks workspace ls /Shared
```

---

## 4. Ce qui nécessite une intervention manuelle

| Composant | Action manuelle requise |
|-----------|------------------------|
| **Secrets Databricks** | Recréer les secret scopes et valeurs manuellement |
| **Delta Sharing** | Reconfigurer les partages avec les destinataires |
| **MLflow** | Reconfigurer les experiments si nécessaire |
| **Service Principals** | Reconfigurer dans Azure Entra ID |
| **Users / Groups** | Synchronisation Entra ID → automatique à la reconnexion |

---

## 5. Contacts et ressources

- Backup ADLS : `st10keyitdpdrpdevchn00.dfs.core.windows.net/uc-data/dr-backup/`
- Workspace Databricks : `https://adb-2547670924000766.6.azuredatabricks.net`
- Repo GitHub : `https://github.com/Eddie-claude/databricks-dr-backup`
- Subscription Azure : `sub-keyIT-prd-dataplatform-01`
