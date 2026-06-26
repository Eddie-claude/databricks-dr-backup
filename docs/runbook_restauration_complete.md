# Runbook — Restauration Complète Databricks DR

**Version :** 2.0  
**Date :** 2026-06-25  
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

## Scripts et notebooks disponibles

### Notebooks Databricks (recommandé — exécution directe dans le workspace)

| Notebook | Périmètre restauré |
|----------|--------------------|
| `10_restore_orchestrator` | **Point d'entrée principal** — orchestre les notebooks ci-dessous via widgets multiselect |
| `07_restore` | Tables Delta (Unity Catalog) |
| `11_restore_grants` | Permissions Unity Catalog (GRANT sur catalogs, schemas, tables) |
| `09_restore_jobs` | Définitions de jobs Databricks |
| `08_restore_workspace` | Sources notebooks + ACLs workspace (notebooks/dossiers) + ACLs repos Git |

> ⚠️ **`grants` ≠ `acls`** : les grants Unity Catalog (`GRANT SELECT ON CATALOG …`) sont distincts des ACLs workspace Databricks (permissions sur les notebooks et dossiers). Ils sont sauvegardés dans des fichiers séparés et restaurés par des notebooks différents.

### Scripts CLI (exécution hors Databricks / CI-CD)

| Script | Périmètre restauré |
|--------|--------------------|
| `scripts/restore_uc.py` | Structure Unity Catalog : catalogs, schemas, tables DDL, grants |
| `scripts/restore_workspace.py` | Jobs + Notebooks |
| `scripts/restore_workspace_config.py` | ACLs workspace + ACLs repos Git |

---

## Prérequis

- [ ] Accès Azure Portal (Owner sur le resource group)
- [ ] Databricks CLI installé (`pip install databricks-cli`)
- [ ] Python 3.9+ + `pip install requests`
- [ ] AzCopy installé (pour télécharger le backup depuis ADLS — scripts CLI uniquement)
- [ ] PAT Token valide sur le workspace cible
- [ ] Accès en lecture sur `st10keyitdpdrpdevchn00` (container `uc-data`)

---

## 1. Identifier le backup à restaurer

```bash
# Lister les backups disponibles
azcopy list "https://st10keyitdpdrpdevchn00.dfs.core.windows.net/uc-data/backup" \
    --recursive=false

# Vérifier le dernier backup validé
azcopy cat "https://st10keyitdpdrpdevchn00.dfs.core.windows.net/uc-data/backup/latest.json"
```

Retient la date `BACKUP_DATE` (ex: `2026-06-25`).

---

## 2. Structure du backup ADLS

```
backup/
└── {BACKUP_DATE}/
    ├── uc_metadata/              # DDL SQL : catalogs, schemas, tables, grants
    │   ├── 01_catalogs.sql
    │   ├── 02_schemas.sql
    │   ├── 03_tables.sql
    │   └── 04_grants.sql
    ├── incremental/              # DEEP CLONE journalier des tables (Delta)
    │   └── {catalog}/{schema}/{table}/
    ├── snapshots/
    │   ├── weekly/{YYYY-Www}/    # Snapshot hebdomadaire
    │   └── monthly/{YYYY-MM}/   # Snapshot mensuel
    ├── jobs/
    │   ├── jobs_all.json         # Toutes les définitions de jobs
    │   └── {id}_{name}.json      # Un fichier par job
    ├── notebooks/                # Sources notebooks (.py / .sql / .scala)
    │   └── {chemin_workspace}/
    └── workspace_config/
        ├── workspace_acls.json   # ACLs notebooks et dossiers workspace
        └── repos_acls.json       # ACLs repos Git
```

---

## SCÉNARIO A — Restauration Partielle (workspace intact)

### A.1 — Restaurer la structure UC et les données

#### Option 1 : Notebook (recommandé)

Ouvrir `10_restore_orchestrator` dans le workspace et sélectionner `tables` dans le widget `restore_scope`.

| Paramètre | Valeur |
|-----------|--------|
| `backup_root` | `abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup` |
| `backup_date` | vide = auto-détection, ou `2026-06-25` |
| `restore_scope` | `tables` |
| `restore_level` | `incremental` / `weekly` / `monthly` |
| `dry_run` | `true` d'abord, puis `false` |

#### Option 2 : Script CLI

```bash
export DATABRICKS_HOST=https://adb-2547670924000766.6.azuredatabricks.net
export DATABRICKS_TOKEN=dapiXXXX

python scripts/restore_uc.py \
    --backup-date $BACKUP_DATE \
    --backup-root "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup"
```

> Les `already exists` sont ignorés automatiquement.

### A.2 — Restaurer les permissions Unity Catalog (si nécessaire)

#### Option 1 : Notebook

Sélectionner `grants` dans `restore_scope` du notebook `10_restore_orchestrator`.

| Paramètre | Valeur |
|-----------|--------|
| `restore_scope` | `grants` |
| `catalog_filter` | vide = tous les catalogs, ou `mon_catalog` pour cibler |
| `dry_run` | `true` d'abord, puis `false` |

> Restaure les GRANT sur catalogs, schemas et tables depuis `uc_metadata/04_grants.sql`.

#### Option 2 : Script CLI

```bash
python scripts/restore_uc.py \
    --backup-date $BACKUP_DATE \
    --backup-root "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup" \
    --only-grants
```

### A.3 — Restaurer les ACLs workspace (notebooks/dossiers)

#### Option 1 : Notebook

Sélectionner `acls` dans `restore_scope` du notebook `10_restore_orchestrator`.

> Restaure les permissions sur les notebooks et dossiers workspace (≠ permissions Unity Catalog).

#### Option 2 : Script CLI

```bash
export LOCAL_BACKUP=/tmp/dr-restore/$BACKUP_DATE

azcopy sync \
    "https://st10keyitdpdrpdevchn00.dfs.core.windows.net/uc-data/backup/$BACKUP_DATE" \
    "$LOCAL_BACKUP" --recursive

python scripts/restore_workspace_config.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-acls
```

### B.3.bis — Restaurer les grants Unity Catalog

```bash
python scripts/restore_uc.py \
    --backup-date $BACKUP_DATE \
    --backup-root "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup"
```

Ou depuis le notebook `10_restore_orchestrator` en sélectionnant `grants`.

---

## SCÉNARIO B — Restauration Totale (nouveau workspace)

### B.1 — Recréer l'infrastructure Azure (Terraform)

> ⚠️ Cette étape est hors scope du backup applicatif.

```bash
cd terraform/
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
1. `01_catalogs.sql` — recrée les catalogs *(critique)*
2. `02_schemas.sql` — recrée les schemas *(critique)*
3. `03_tables.sql` — recrée les tables (DDL)
4. `04_grants.sql` — restaure les permissions

### B.4 — Restaurer les données Delta

**Option 1 : Notebook `10_restore_orchestrator`**

Sélectionner `tables` dans `restore_scope`, choisir `restore_level` et `restore_point`.

**Option 2 : Notebook `07_restore` directement**

| Paramètre | Valeur |
|-----------|--------|
| `backup_root` | `abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup` |
| `restore_level` | `incremental` / `weekly` / `monthly` |
| `restore_point` | timestamp ou label (ex: `2026-W25`, `2026-06`) |
| `source_table` | `catalog.schema.*` ou vide = toutes |
| `dry_run` | `true` d'abord |

### B.5 — Restaurer les Notebooks

**Option 1 : Notebook `10_restore_orchestrator`** — sélectionner `notebooks`.

**Option 2 : Script CLI**

```bash
export LOCAL_BACKUP=/tmp/dr-restore/$BACKUP_DATE
azcopy sync \
    "https://st10keyitdpdrpdevchn00.dfs.core.windows.net/uc-data/backup/$BACKUP_DATE" \
    "$LOCAL_BACKUP" --recursive

# Dry-run
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

**Option 1 : Notebook `10_restore_orchestrator`** — sélectionner `jobs`.

**Option 2 : Notebook `09_restore_jobs`** pour restaurer des jobs spécifiques avec filtre.

**Option 3 : Script CLI**

```bash
# Dry-run
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

> ℹ️ Le `job_id` sera différent après restauration. Le notebook `09_restore_jobs` affiche  
> le mapping `ancien job_id → nouveau job_id` en fin d'exécution.

### B.7 — Redéployer les jobs DAB (bundle)

```bash
export DATABRICKS_HOST=<nouveau-workspace>
export DATABRICKS_TOKEN=<nouveau-PAT>

databricks bundle deploy --target prod
```

Ou déclencher le workflow GitHub Actions `DR Backup DAB`.

### B.8 — Restaurer les ACLs workspace et repos Git

**Option 1 : Notebook `10_restore_orchestrator`** — sélectionner `acls`.

**Option 2 : Script CLI**

```bash
python scripts/restore_workspace_config.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-acls
```

---

## 3. Utiliser l'orchestrateur (approche recommandée)

Pour une restauration interactive depuis le workspace Databricks, le notebook `10_restore_orchestrator` permet de tout piloter depuis un seul endroit.

**Widgets disponibles :**

| Widget | Description |
|--------|-------------|
| `backup_root` | Pré-rempli avec le chemin ADLS production |
| `backup_date` | Vide = auto-détection via `latest.json` |
| `restore_scope` | Multiselect : `tables`, `grants`, `jobs`, `notebooks`, `acls` |
| `dry_run` | `true` (simulation) / `false` (applique) |
| `restore_level` | Pour les tables : `incremental` / `weekly` / `monthly` |
| `restore_point` | Pour les tables : timestamp ou label (`2026-W25`) |
| `source_table` | Pour les tables : `cat.schema.table` ou vide = toutes |
| `catalog_filter` | Pour les grants UC : noms des catalogs séparés par virgule, vide = tous |
| `job_filter` | Pour les jobs : sous-chaîne du nom, vide = tous |
| `conflict_mode` | Pour les jobs : `skip` (défaut) / `recreate` |

**Procédure :**
1. Importer le notebook dans le workspace
2. Renseigner `backup_root` et sélectionner le périmètre
3. Lancer avec `dry_run = true` — vérifier le plan
4. Relancer avec `dry_run = false`

---

## 4. Vérification post-restauration

### Via notebooks Databricks (démo)

Exécuter dans l'ordre :
1. `notebooks/demo/03_dr_scenario` — vérifie tables et données
2. `notebooks/demo/05_workspace_acl` — vérifie grants et ACLs workspace

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

## 5. Ce qui nécessite une intervention manuelle

| Composant | Action manuelle requise |
|-----------|------------------------|
| **Secrets Databricks** | Recréer les secret scopes et valeurs manuellement |
| **Delta Sharing** | Reconfigurer les partages avec les destinataires |
| **MLflow** | Reconfigurer les experiments si nécessaire |
| **Service Principals** | Reconfigurer dans Azure Entra ID |
| **Users / Groups** | Synchronisation Entra ID → automatique à la reconnexion |
| **Clusters / Cluster Policies** | Recréer manuellement (non sauvegardés) |
| **SQL Warehouses** | Recréer manuellement (non sauvegardés) |

---

## 6. Contacts et ressources

- Backup ADLS : `st10keyitdpdrpdevchn00.dfs.core.windows.net/uc-data/backup/`
- Workspace Databricks : `https://adb-2547670924000766.6.azuredatabricks.net`
- Repo GitHub : `https://github.com/Eddie-claude/databricks-dr-backup`
- Subscription Azure : `sub-keyIT-prd-dataplatform-01`
