# Design — DR Backup Hybride Azure Databricks

**Date :** 2026-04-02
**Client :** Groupe Mutuelle
**Auteur :** Solution Technique

---

## Contexte

Le client exploite un workspace Azure Databricks (Unity Catalog activé) en production sur Switzerland North. Les fonctionnalités DR natives Databricks ne seront disponibles que l'an prochain. L'objectif est de mettre en place un DR fait maison, simple et efficace, couvrant une restauration complète de l'environnement après un désastre.

L'infrastructure de destination (ADLS Gen2 DR avec Private Endpoints, UC External Location, Access Connector RBAC) est déjà provisionnée via Terraform (`main.tf`).

---

## Périmètre

### Assets à sauvegarder (scope complet)

| Domaine | Assets | Outil |
|---|---|---|
| Workspace | Notebooks (.dbc), Jobs (JSON), Clusters (JSON), Cluster Policies, Instance Pools, SQL Warehouses | CI/CD + Databricks CLI / DAB |
| Unity Catalog | Catalogs, Schemas, Tables (DDL SQL), External Locations, Grants/Permissions | Notebook Databricks |
| Données | Delta tables managed (DEEP CLONE), Delta tables external (copie fichiers) | Notebook Databricks |
| Secrets | Secret scopes (noms + clés listés — valeurs non exportables par Databricks) | CI/CD + REST API |

### Fréquence
Journalière — déclenchement à 02h00 UTC.

### Format de sauvegarde
- Notebooks : `.dbc` (export natif Databricks)
- Jobs, Clusters, Policies, Warehouses : JSON (REST API)
- UC metadata : DDL SQL (`CREATE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE`, `GRANT`)
- Données Delta : format Delta natif (DEEP CLONE ou copie fichiers)
- Rapport : HTML + JSON

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  CI/CD Pipeline (GitHub Actions / Azure DevOps)          │
│  Schedule: 02h00 UTC daily                               │
│                                                          │
│  databricks bundle generate  ──► jobs YAML               │
│  databricks workspace export ──► notebooks .dbc          │
│  databricks api get          ──► clusters, policies,     │
│                                   warehouses, pools       │
│                                                          │
│  → écrit dans ADLS DR: dr-backup/YYYY-MM-DD/workspace/  │
│  → commit dans git repo (historique versionné)           │
│  → déclenche le Databricks Job de backup data via API    │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼ trigger REST API
┌─────────────────────────────────────────────────────────┐
│  Databricks Job — DR Backup Orchestrator                 │
│                                                          │
│  01_uc_metadata  → DDL SQL (catalogs, schemas,           │
│                    tables, external locations, grants)   │
│  02_data_clone   → DEEP CLONE managed tables             │
│                    copie fichiers external tables        │
│  03_diff         → compare manifestes J vs J-1           │
│  04_report       → génère rapport HTML dans ADLS         │
└─────────────────────────────────────────────────────────┘
                          │
                          ▼
┌─────────────────────────────────────────────────────────┐
│  ADLS Gen2 DR (st10keyitdpdrpdevchn00)                   │
│  Container: dr-backup/                                   │
│  Accès: Private Endpoint uniquement (dfs + blob)         │
│  Auth: Access Connector Managed Identity                 │
└─────────────────────────────────────────────────────────┘
```

---

## Structure ADLS DR

```
dr-backup/
├── YYYY-MM-DD/
│   ├── workspace/
│   │   ├── notebooks/          ← export .dbc par dossier workspace
│   │   ├── jobs.json           ← liste complète des jobs
│   │   ├── clusters.json       ← all-purpose clusters configs
│   │   ├── policies.json       ← cluster policies
│   │   ├── instance_pools.json
│   │   └── sql_warehouses.json
│   ├── uc_metadata/
│   │   ├── 01_catalogs.sql     ← CREATE CATALOG statements
│   │   ├── 02_schemas.sql      ← CREATE SCHEMA statements
│   │   ├── 03_tables.sql       ← CREATE TABLE / CREATE VIEW DDL
│   │   ├── 04_external_locations.sql
│   │   └── 05_grants.sql       ← GRANT statements (ordre garanti)
│   ├── data/
│   │   └── <catalog>/<schema>/<table>/   ← Delta natif (DEEP CLONE)
│   ├── diff/
│   │   └── diff_YYYY-MM-DD.json         ← diff vs backup J-1
│   └── report/
│       └── dr_report_YYYY-MM-DD.html    ← compte rendu DR
├── manifest.json               ← index de tous les backups (date, taille, statut)
└── latest.json                 ← pointeur vers le dernier backup réussi
```

---

## Fonctionnalités

### 1. Diff backup J vs J-1

Le notebook `03_diff` compare les manifestes des deux derniers backups :
- Nouveaux assets (tables, jobs, notebooks ajoutés)
- Assets supprimés
- Assets modifiés (hash ou timestamp différent)
- Volume de données : delta en GB
- Résultat : `diff_YYYY-MM-DD.json` + résumé dans le rapport

### 2. Reconstruction du catalogue depuis un dump

Script de restore SQL qui rejoue `uc_metadata/` dans l'ordre strict :
```
1. CREATE CATALOG
2. CREATE SCHEMA
3. CREATE EXTERNAL LOCATION (si nécessaire)
4. CREATE TABLE / CREATE VIEW
5. GRANT (après création des objets)
```
Idempotent : utilise `CREATE IF NOT EXISTS` + gestion des erreurs par asset.

### 3. Compte rendu DR

Le notebook `04_report` génère un rapport HTML avec :
- Date et durée du backup
- Nombre d'assets sauvegardés par catégorie (tables, jobs, notebooks…)
- Volume total de données (GB)
- Résumé du diff vs J-1
- Statut de chaque étape (succès / échec / skipped)
- Erreurs détaillées avec stack trace
- Instructions de restauration rapide

---

## Stratégie données Delta

| Type de table | Stratégie backup | Stratégie restore |
|---|---|---|
| **Managed** (UC managed storage) | `DEEP CLONE` vers `dr-backup/data/<catalog>/<schema>/<table>` | `CREATE TABLE ... CLONE` depuis ADLS DR |
| **External** (ADLS propre client) | Copie fichiers Delta via `dbutils.fs.cp` ou Spark | Recréer External Location + `CREATE TABLE` DDL |

Chaque backup journalier est un clone complet horodaté — pas d'incrémental pour la phase v1 (simplifie la restauration).

---

## Séquence de restauration DR

En cas de désastre (workspace perdu, région indisponible) :

```
1. terraform apply               → recrée workspace + storage DR
2. CI/CD: DAB deploy + API POST  → restaure jobs, clusters, notebooks
3. Script SQL uc_metadata/       → recrée structure UC (catalog → schema → table → grants)
4. RESTORE depuis dr-backup/data → redonne accès aux données Delta
5. Valider via rapport du dernier backup
```

---

## Infrastructure existante (Terraform)

Déjà provisionné dans `main.tf` :
- Resource Group DR : `rg-dp-drp-processing-chn-dev-00`
- Storage Account ADLS Gen2 : `st10keyitdpdrpdevchn00` (HNS, public access disabled)
- Container `dr-backup`
- Private Endpoints : `dfs` + `blob` → Private DNS Zones liées au VNet Databricks
- RBAC : Access Connector `ac-dp-drp-dev-chn-00` → Storage Blob Data Contributor
- UC Storage Credential : `sc-dr-backup`
- UC External Location : `el-dr-backup`

---

## Contraintes & points d'attention

- **Secrets Databricks** : les valeurs de secrets ne sont pas exportables via l'API — seuls les noms des scopes et clés sont sauvegardés. La restauration des secrets nécessite une intervention manuelle ou un coffre externe (Azure Key Vault).
- **Tables external** : la copie des fichiers ne sera possible que si le storage source est accessible depuis le cluster DR. À documenter par table dans le rapport.
- **Ordre des GRANT** : les grants doivent être appliqués après la création des objets UC — l'ordre dans `05_grants.sql` est critique.
- **Private Endpoint** : le cluster Databricks doit résoudre le storage DR via DNS privé (`nslookup` vérifié en post-deploy Terraform).

---

## Composants à développer

| Composant | Type | Priorité |
|---|---|---|
| CI/CD workflow (GitHub Actions ou Azure DevOps) | YAML pipeline | P1 |
| Script export workspace (CLI + REST API) | Python/Bash | P1 |
| Notebook `01_uc_metadata` | Python (Databricks) | P1 |
| Notebook `02_data_clone` | Python/SQL (Databricks) | P1 |
| Notebook `03_diff` | Python (Databricks) | P2 |
| Notebook `04_report` | Python (Databricks) | P2 |
| Script restore UC (SQL) | SQL | P1 |
| Script restore workspace (CI/CD) | Python/Bash | P1 |
| Databricks Job config (JSON/YAML) | JSON | P1 |
