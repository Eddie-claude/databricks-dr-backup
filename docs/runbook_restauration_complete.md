# Runbook — Restauration Complète Databricks DR

**Version :** 4.2

Les valeurs entre chevrons sont à remplacer par celles de l'environnement : `<compte>` (compte de stockage du backup), `<container>`, `<workspace>` (URL du workspace Databricks).

---

## Documents de référence

| Document | Contenu |
|----------|---------|
| Guide de déploiement v4.2 | Installation, prérequis et droits (§2), rapport de couverture `diag_03` et sa lecture (§3.4), contrôle des tables protégées `diag_04` (§3.5), synchronisation automatique des droits `dr-backup-grants-sync` (§8), vue d'ensemble de la restauration (§9), plan de reconstruction des tables non sauvegardées (§9.3) |
| Notes de version v4.2 | Ce qui change par rapport à la version précédente, procédure de mise à jour et vérifications |
| Ce runbook | Procédure de restauration détaillée, paramètres des notebooks de restauration, comportement quand un objet existe déjà |

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
| `12_restore_uc_objects` | Volumes + fonctions Unity Catalog (définitions) |
| `15_restore_volume_files` | Fichiers des volumes managés, état à une date donnée (après `12`) |
| `11_restore_grants` | Permissions Unity Catalog (GRANT sur catalogs, schemas, tables, volumes, fonctions) |
| `09_restore_jobs` | Définitions de jobs Databricks + leurs permissions (backups v4.2+) |
| `13_restore_pipelines` | Pipelines Lakeflow / DLT : définition + permissions (après les notebooks) |
| `08_restore_workspace` | Notebooks, fichiers et dashboards de tout le workspace + ACLs workspace + ACLs repos Git |

> ⚠️ **`grants` ≠ `acls`** : les grants Unity Catalog (`GRANT SELECT ON CATALOG …`) sont distincts des ACLs workspace Databricks (permissions sur les notebooks et dossiers). Ils sont sauvegardés dans des fichiers séparés et restaurés par des notebooks différents.

### Scripts CLI (exécution hors Databricks / CI-CD)

| Script | Périmètre restauré |
|--------|--------------------|
| `scripts/restore_uc.py` | Structure Unity Catalog : catalogs, schemas, tables, volumes, fonctions (DDL), grants — `--only-grants` pour les permissions seules |
| `scripts/restore_workspace.py` | Jobs + Notebooks |
| `scripts/restore_workspace_config.py` | ACLs workspace + ACLs repos Git |

### Avant de restaurer : savoir ce qui est restaurable

Le notebook `diag_03_backup_coverage` (lecture seule, compte admin du metastore) produit un rapport HTML + Excel qui indique pour chaque objet du metastore et du workspace s'il est restaurable par le backup, par l'IaC, par un autre moyen — ou pas du tout. À tenir à jour et à consulter avant un exercice de restauration. Toujours renseigner `backup_root` (sinon les règles v4.1 sont appliquées) et `backup_principal`.

| Verdict | Lecture | Action |
|---------|---------|--------|
| ✅ Sauvegardé et restaurable | Définition et données couvertes | — |
| 🔵 As code | Recréé par Terraform / le bundle | Vérifier l'IaC |
| ⚪ Autre moyen | Git, recalcul de pipeline, données dans la source | Documenter au plan de reprise |
| 🟡 Partiel | Définition couverte, données non (table protégée, format non clonable) | Fiche de reconstruction (§5 ci-dessous) |
| 🔴 Inaccessible au backup | Le compte de backup n'a pas les droits | `dr-backup-grants-sync` en mode `apply` |
| 🔴 Échec au dernier backup | Table en erreur / non copiée au dernier run | Lire la cause dans le rapport du backup |
| 🔴 Non couvert | Ni backup ni IaC | Couvrir en IaC ou accepter le risque |

Pour les tables protégées (filtre de lignes, masque de colonnes), `diag_04_protected_tables`, lancé **sous l'identité du job de backup** (« Run as »), indique si ce compte voit toutes les lignes et les valeurs réelles : `COMPLETE`, `LIGNES_FILTREES`, `VALEURS_MASQUEES`, `INDETERMINE`.

---

## Ordre de restauration quand une partie de la plateforme est gérée as code

Le backup reconstitue l'état réel du metastore, y compris des objets que l'IaC (Terraform, bundle) sait aussi recréer. **Le code reste la source de vérité** : il passe en premier, le backup complète.

1. **IaC** : `terraform apply` / `databricks bundle deploy` — storage credentials, external locations, connexions, catalogs (dont fédérés), volumes, fonctions, pipelines, et les grants s'ils sont en code.
2. **Données** : tables Delta (`07_restore`, scope `tables`).
3. **Compléments du backup** : volumes et fonctions créés hors code (scope `uc_objects`, `IF NOT EXISTS` : les objets déjà recréés par l'IaC ne sont pas modifiés), puis grants (scope `grants` ou `restore_uc.py --only-grants`).

> ⚠️ Ne pas inverser : un objet recréé par le backup avant le `terraform apply` n'est pas dans le state Terraform, l'apply échoue en « already exists » (il faut alors un `terraform import`).
> ⚠️ Si les grants sont gérés en Terraform de façon autoritaire (`databricks_grants`), ne pas rejouer `04_grants.sql` sur ces objets : le prochain apply retirerait les grants ajoutés.

---

## Prérequis

- [ ] Accès Azure Portal (Owner sur le resource group)
- [ ] CLI Databricks 0.2xx ou supérieure (guide de déploiement §2.7 — l'ancienne `databricks-cli` ne gère pas les bundles)
- [ ] Python 3.10+ et `pip install -r requirements.txt` (scripts CLI)
- [ ] AzCopy installé (pour télécharger le backup depuis ADLS — scripts CLI uniquement)
- [ ] PAT Token valide sur le workspace cible
- [ ] Accès en lecture sur `<compte>` (container `<container>`)

---

## 1. Identifier le backup à restaurer

```bash
# Lister les backups disponibles
azcopy list "https://<compte>.dfs.core.windows.net/<container>/backup" \
    --recursive=false

# Vérifier le dernier backup validé
azcopy cat "https://<compte>.dfs.core.windows.net/<container>/backup/latest.json"
```

Retient la date `BACKUP_DATE` (ex: `2026-06-25`).

---

## 2. Structure du backup ADLS

```
backup/
├── latest.json                          # Date et statut du dernier backup
├── {BACKUP_DATE}/                       # Un dossier par jour de backup
│   ├── manifest.json
│   ├── uc_metadata/                     # DDL : 01_catalogs, 02_schemas, 03_tables (vues comprises),
│   │                                    #       04_grants, 05_volumes, 06_functions (.sql)
│   ├── jobs/                            # jobs_all.json, {id}_{nom}.json, jobs_permissions.json
│   ├── pipelines/pipelines_all.json     # Définitions et permissions des pipelines
│   ├── workspace/                       # manifest.json + objects/ (notebooks, fichiers, dashboards)
│   ├── workspace_config/                # workspace_acls.json, repos_acls.json
│   ├── diff/                            # Différences avec la veille
│   └── report/                          # Rapport HTML du backup
├── incremental/                         # Copie Delta de chaque table (point-in-time sur la rétention)
│   ├── {catalog}/{schema}/{table}/
│   ├── _manifests/{date}.json           # Résultat de chaque table, chaque jour
│   ├── _checkpoints/{date}.json         # Reprise après interruption
│   ├── _last_versions.json
│   └── _reclone_archive/{date}/         # Anciennes copies remplacées (à purger à la main)
├── volumes/
│   ├── files/{catalog}/{schema}/{volume}/{date}/   # Fichiers des volumes managés
│   └── _index/                          # État de chaque volume, jour par jour (Delta)
└── snapshots/
    ├── monthly/{YYYY-MM}/               # Snapshot mensuel
    └── weekly/{YYYY-Www}/               # Anciens snapshots hebdomadaires (plus créés)
```

---

## SCÉNARIO A — Restauration Partielle (workspace intact)

### A.1 — Restaurer les tables

#### Option 1 : Notebook (recommandé)

Ouvrir `10_restore_orchestrator` dans le workspace et sélectionner `tables` dans le widget `restore_scope`.

| Paramètre | Valeur |
|-----------|--------|
| `backup_root` | `abfss://<container>@<compte>.dfs.core.windows.net/backup` |
| `restore_scope` | `tables` |
| `restore_point` | vide = dernier backup ; date `AAAA-MM-JJ` ou horodatage pour un état antérieur (`backup_date` ne s'applique pas aux tables) |
| `source_table` | `catalog.schema.table`, `catalog.schema.*`, `catalog.*.*` ou vide = toutes |
| `target_catalog` | vide = même catalog ; un autre catalog pour restaurer à côté et comparer |
| `restore_level` | `incremental` (défaut) / `monthly` / `weekly` (anciens snapshots seulement) |
| `dry_run` | `true` d'abord, puis `false` |

#### Option 2 : Script CLI — structure seulement

`restore_uc.py` ne rejoue que les DDL : les tables sont recréées **vides**. Les données se restaurent par l'option 1.

```bash
export DATABRICKS_HOST=https://<workspace>.azuredatabricks.net
export DATABRICKS_TOKEN=dapiXXXX

python scripts/restore_uc.py \
    --backup-date $BACKUP_DATE \
    --backup-root "abfss://<container>@<compte>.dfs.core.windows.net/backup"
```

> Les `already exists` sont ignorés automatiquement.

### A.2 — Restaurer les volumes et fonctions Unity Catalog (si nécessaire)

Sélectionner `uc_objects` dans `restore_scope` du notebook `10_restore_orchestrator` (avec `tables` et `grants` pour un schéma complet : l'orchestrateur les enchaîne dans l'ordre tables → volumes/fonctions → grants).

| Paramètre | Valeur |
|-----------|--------|
| `restore_scope` | `uc_objects` |
| `catalog_filter` | vide = tous les catalogs, ou `mon_catalog` pour cibler |
| `dry_run` | `true` d'abord, puis `false` |

> Rejoue `uc_metadata/05_volumes.sql` et `06_functions.sql` (`IF NOT EXISTS` : les objets existants ne sont pas modifiés).
> Un volume **managé** est recréé vide : ses fichiers se restaurent ensuite avec le scope `volume_files` (`15_restore_volume_files`, état à `restore_point` ou dernier). Un volume externe retrouve ses fichiers, restés sur son stockage (l'External Location doit exister).

### A.3 — Restaurer les permissions Unity Catalog (si nécessaire)

#### Option 1 : Notebook

Sélectionner `grants` dans `restore_scope` du notebook `10_restore_orchestrator`.

| Paramètre | Valeur |
|-----------|--------|
| `restore_scope` | `grants` |
| `catalog_filter` | vide = tous les catalogs, ou `mon_catalog` pour cibler |
| `dry_run` | `true` d'abord, puis `false` |

> Restaure les GRANT sur catalogs, schemas, tables, volumes et fonctions depuis `uc_metadata/04_grants.sql`.

#### Option 2 : Script CLI

```bash
python scripts/restore_uc.py \
    --backup-date $BACKUP_DATE \
    --backup-root "abfss://<container>@<compte>.dfs.core.windows.net/backup" \
    --only-grants
```

### A.4 — Restaurer les ACLs workspace (notebooks/dossiers)

#### Option 1 : Notebook

Sélectionner `acls` dans `restore_scope` du notebook `10_restore_orchestrator`.

> Restaure les permissions sur les notebooks et dossiers workspace (≠ permissions Unity Catalog).

#### Option 2 : Script CLI

```bash
export LOCAL_BACKUP=/tmp/dr-restore/$BACKUP_DATE

azcopy sync \
    "https://<compte>.dfs.core.windows.net/<container>/backup/$BACKUP_DATE" \
    "$LOCAL_BACKUP" --recursive

python scripts/restore_workspace_config.py \
    --backup-path $LOCAL_BACKUP \
    --host $DATABRICKS_HOST \
    --token $DATABRICKS_TOKEN \
    --restore-acls
```

---

## SCÉNARIO B — Restauration Totale (nouveau workspace)

### B.1 — Recréer l'infrastructure (IaC)

> ⚠️ Cette étape est hors du périmètre du backup : elle relève de votre infrastructure as code (Terraform ou équivalent).

Ressources à recréer : workspace Databricks, réseau, compte de stockage, storage credentials, external locations, metastore Unity Catalog (si détruit), catalogs et objets gérés en code. Voir l'ordre de restauration avec l'IaC en tête de ce document.

### B.2 — Configurer la CLI sur le nouveau workspace

```bash
databricks configure --host https://<nouveau-workspace>.azuredatabricks.net --profile dr
```

### B.3 — Déployer la solution (bundle)

À faire **avant** les restaurations : le bundle dépose les notebooks de restauration et `lib/`, et recrée les jobs `dr-backup-*`. Restaurés ensuite avec `conflict_mode = skip`, ces jobs ne sont pas dupliqués.

Dans `databricks.yml`, cible utilisée : mettre à jour `workspace.host` (nouveau workspace) et `variables.backup_root` (racine du backup à restaurer). Puis :

```bash
databricks bundle deploy --target prod --profile dr
```

### B.4 — Restaurer la structure Unity Catalog

```bash
python scripts/restore_uc.py \
    --backup-date $BACKUP_DATE \
    --backup-root "abfss://<container>@<compte>.dfs.core.windows.net/backup"
```

Ordre d'exécution automatique :
1. `01_catalogs.sql` — recrée les catalogs *(critique)*
2. `02_schemas.sql` — recrée les schemas *(critique)*
3. `03_tables.sql` — recrée les tables (DDL, vides) et les vues
4. `05_volumes.sql` — recrée les volumes
5. `06_functions.sql` — recrée les fonctions
6. Nouvelle tentative des vues / fonctions en échec (une vue peut appeler une fonction créée après elle)
7. `04_grants.sql` — restaure les permissions (en dernier : elles visent aussi volumes et fonctions)

### B.5 — Restaurer les données

Depuis `10_restore_orchestrator` (dossier du bundle), dans cet ordre de périmètres : `tables` (données Delta), `volume_files` (fichiers des volumes managés), `notebooks`, `jobs` (`conflict_mode = skip`), `pipelines`, `acls`. L'orchestrateur les enchaîne dans le bon ordre si plusieurs périmètres sont sélectionnés (voir §3).

> ℹ️ La date des tables et des fichiers de volumes se fixe par `restore_point` (vide = dernier backup). `backup_date` sert aux autres périmètres.

> ℹ️ Le `job_id` change à la restauration : `09_restore_jobs` affiche le mapping `ancien job_id → nouveau job_id` en fin d'exécution.

> ℹ️ Les tables dont les données ne sont pas sauvegardées (filtre de lignes, masque de colonnes) sont recréées vides : voir §5.

---

## 3. Utiliser l'orchestrateur (approche recommandée)

Pour une restauration interactive depuis le workspace Databricks, le notebook `10_restore_orchestrator` permet de tout piloter depuis un seul endroit.

**Widgets disponibles :**

| Widget | Description |
|--------|-------------|
| `backup_root` | À renseigner : racine `abfss://…/backup` du backup |
| `backup_date` | Vide = auto-détection via `latest.json` |
| `restore_scope` | Multiselect : `tables`, `uc_objects`, `volume_files`, `grants`, `jobs`, `notebooks`, `pipelines`, `acls` |
| `dry_run` | `true` (simulation) / `false` (applique) |
| `restore_level` | Pour les tables : `incremental` (défaut) / `monthly` / `weekly` (anciens snapshots seulement) |
| `restore_point` | Pour les tables et les fichiers de volumes : vide = dernier backup, date `AAAA-MM-JJ`, horodatage, ou label de snapshot (`2026-06`) |
| `source_table` | Pour les tables : `cat.schema.table` ou vide = toutes |
| `target_catalog` | Pour les tables : vide = même catalog ; un autre catalog pour restaurer à côté et comparer |
| `include_stale` | Pour les tables : `true` inclut celles absentes du backup de référence (données périmées) |
| `catalog_filter` | Pour les grants, volumes et fonctions : catalogs séparés par virgule, vide = tous |
| `job_filter` | Pour les jobs : sous-chaîne du nom, vide = tous |
| `notebook_filter` | Pour les notebooks et leurs permissions : chemin préfixe (`/Shared/projet`), vide = tout le workspace |
| `pipeline_filter` | Pour les pipelines : sous-chaîne du nom, vide = tous |
| `volume_filter` | Pour les fichiers de volumes : `catalog.schema.volume`, jokers `*` acceptés |
| `conflict_mode` | Pour les jobs et pipelines existants : `skip` (défaut, inchangés) / `replace` (définition remplacée en place) |

> ⚠️ Laissés vides, ces filtres couvrent **tout** le backup (notebooks et permissions de tous les utilisateurs, tous les pipelines, tous les volumes). Pour une restauration partielle, toujours les renseigner.

> `07_restore` ne restaure que les tables sauvegardées avec succès à la date de référence (dernier backup, ou dernier au plus tard au `restore_point`). Les tables présentes dans `incremental/` mais absentes de ce backup sont listées comme périmées ; `include_stale = true` (widget de `07_restore`) les restaure quand même.
**Si l'objet existe déjà dans l'environnement cible :**

| Objet | Notebook | Comportement |
|-------|----------|--------------|
| Tables Delta | `07_restore` | Écrasée (CREATE OR REPLACE … DEEP CLONE). L'historique Delta est conservé : RESTORE TABLE … VERSION AS OF revient à l'état d'avant la restauration. |
| Catalogues, schémas, tables, vues (DDL) | `restore_uc.py` | Inchangés (IF NOT EXISTS ou « already exists » ignoré). |
| Volumes, fonctions | `12_restore_uc_objects` | Inchangés ; signalés « DIFFÉRENT » si leur définition diffère du backup. |
| Fichiers des volumes | `15_restore_volume_files` | Écrasés ; les fichiers absents du backup sont conservés et listés. |
| Permissions Unity Catalog | `11_restore_grants` | Ajoutées ; les permissions accordées depuis le backup ne sont pas retirées. |
| Notebooks, fichiers, tableaux de bord | `08_restore_workspace` | Écrasés. |
| Permissions du workspace | `08_restore_workspace` | Remplacées : les permissions directes accordées depuis le backup sont retirées. |
| Jobs | `09_restore_jobs` | skip (défaut) : inchangé. replace : définition remplacée en place, même job et même historique ; permissions du backup ajoutées. |
| Pipelines | `13_restore_pipelines` | skip (défaut) : inchangé. replace : définition mise à jour en place, même pipeline et mêmes tables ; permissions du backup ajoutées. |

> ⚠️ `conflict_mode` (jobs, pipelines) : `skip` (défaut) ou `replace` ; `recreate` est l'ancien nom de `replace`.

**Procédure :**
1. Ouvrir `10_restore_orchestrator` dans le dossier du bundle (`…/.bundle/dr-backup/<cible>/files/notebooks`) : il y trouve `lib/`
2. Renseigner `backup_root` et sélectionner le périmètre
3. Lancer avec `dry_run = true` — vérifier le plan
4. Relancer avec `dry_run = false`

---

## 4. Vérification post-restauration

### Via les résultats des notebooks de restauration

Chaque notebook de restauration se termine par un résumé (objets restaurés, ignorés, en erreur ; pour `12_restore_uc_objects`, objets déjà présents identiques ou DIFFÉRENTS ; pour `07_restore`, tables périmées). Un run sans erreur et sans objet « DIFFÉRENT » inattendu est le premier critère.

### Via le rapport de couverture

Relancer `diag_03_backup_coverage` (compte admin, `backup_root` et `backup_principal` renseignés) : les objets restaurés doivent réapparaître, et les seuls objets « à traiter » doivent être ceux déjà connus (tables non sauvegardées, objets as code).

### Via CLI

```bash
# Catalogs et tables d'un schéma
databricks catalogs list
databricks tables list <catalog> <schema>

# Jobs et pipelines
databricks jobs list
databricks pipelines list-pipelines

# Notebooks restaurés
databricks workspace list /Shared
```

---

## 5. Ce qui nécessite une intervention manuelle

| Composant | Action manuelle requise |
|-----------|------------------------|
| **Secrets Databricks** | Recréer les secret scopes et valeurs manuellement |
| **Delta Sharing** | Reconfigurer les partages avec les destinataires ; catalogs Delta Sharing à recréer depuis le partage |
| **Catalogs fédérés** | Recréer la connexion et le catalog (IaC) : données dans la base source, non sauvegardées |
| **External locations / storage credentials / connexions** | Non sauvegardés par le script : à recréer par l'IaC |
| **Volumes managés** | Définition (`uc_objects`) puis fichiers (`volume_files`, point-in-time) |
| **Pipelines (Lakeflow / DLT)** | Définition et permissions restaurées par `13_restore_pipelines` (bundle : redéployer) ; tables recalculées par full refresh |
| **Modèles MLflow (Unity Catalog)** | Non sauvegardés : ni définition ni versions |
| **MLflow** | Reconfigurer les experiments si nécessaire |
| **Service Principals** | Reconfigurer dans Azure Entra ID |
| **Users / Groups** | Synchronisation Entra ID → automatique à la reconnexion |
| **Clusters / Cluster Policies** | Recréer manuellement (non sauvegardés) |
| **SQL Warehouses** | Recréer manuellement (non sauvegardés) |
| **Tables à filtre de lignes / masque de colonnes** | Recréées **vides** (DEEP CLONE refusé) : à reconstruire par l'équipe propriétaire, voir ci-dessous |

### Tables dont les données ne sont pas sauvegardées

Elles figurent en « Partiel » dans le rapport de couverture et dans la liste « Tables non sauvegardées » du rapport de chaque backup. La restauration recrée la table (vide) et le job ou pipeline qui l'alimente ; **l'équipe propriétaire relance ce traitement** pour la remplir depuis sa source.

Retrouver le traitement qui écrit dans la table :

```sql
SELECT DISTINCT entity_type, entity_id, created_by, max(event_time) AS derniere_ecriture
FROM system.access.table_lineage
WHERE target_table_full_name = '<catalog>.<schema>.<table>'
GROUP BY ALL ORDER BY derniere_ecriture DESC;
```

> ⚠️ Vérifier que la source permet un rechargement **complet**. Un chargement incrémental ou une source qui purge son historique ne reconstruit qu'une partie de la table : envisager alors une copie par lecture (si `diag_04` donne `COMPLETE`) ou un filtre porté par une vue.

Fiche à tenir au plan de reprise pour chacune : table · raison (filtre / masque / format) · équipe et contact · job ou pipeline à relancer et paramètres d'un chargement complet · source et profondeur d'historique · données qui seront perdues · durée estimée.

---

## 6. Contacts et ressources

À compléter pour l'environnement :

- Backup ADLS : `<compte>.dfs.core.windows.net/<container>/backup/`
- Workspace Databricks : `https://<workspace>.azuredatabricks.net`
- Souscription Azure : `<souscription>`
- Responsable de la restauration et équipe plateforme : `<noms et contacts>`
- Responsables des tables non sauvegardées : voir les fiches du plan de reprise
