# Schemas DR Backup & Restore — Databricks Unity Catalog

> **Visualisation :** Les diagrammes sont dans `docs/diagrams/` au format `.mmd` (Mermaid).
> - **VS Code** : extension "Mermaid Editor" ou "Markdown Preview Mermaid Support"
> - **En ligne** : copier-coller sur [mermaid.live](https://mermaid.live)
> - **Export PNG/SVG** : via mermaid.live > Actions > Export

---

## Diagrammes disponibles

| # | Fichier | Description |
|---|---------|-------------|
| 1 | [`01_backup_daily.mmd`](diagrams/01_backup_daily.mmd) | Backup daily : phase critique (01, 02), phase non critique (14, 05, diff, rapport), retention, VACUUM |
| 2 | [`02_backup_monthly.mmd`](diagrams/02_backup_monthly.mmd) | Backup monthly : snapshot DEEP CLONE du dernier backup termine (latest.json), puis rapport de couverture |
| 2b | [`02b_relation_daily_monthly.mmd`](diagrams/02b_relation_daily_monthly.mmd) | Enchainement des 4 jobs planifies : grants-sync, daily, monthly, coverage-report |
| 3 | [`03_restore.mmd`](diagrams/03_restore.mmd) | Restore : 8 perimetres selectionnables avec dry-run |
| 4 | [`04_vue_ensemble.mmd`](diagrams/04_vue_ensemble.mmd) | Vue d'ensemble : production -> backup -> retention -> restauration DR |

---

## Jobs planifies (databricks.yml)

| Job | Planification (UTC) | Role |
|-----|---------------------|------|
| `dr-backup-grants-sync` | 00:30 chaque nuit | Verifie (report) ou accorde (apply) les droits du compte de backup |
| `dr-backup-daily` | 01:00 chaque nuit | Orchestrateur 00 : 01, 02, 14, 05, 03, 04, 06 |
| `dr-backup-monthly` | 02:00 le 1er du mois | 06 avec enable_monthly : snapshot mensuel |
| `dr-backup-coverage-report` | 06:00 le 1er du mois | diag_03 : couverture reelle d'apres le contenu du backup (HTML + Excel) |

---

## Rappel de l'arborescence ADLS

```
backup/
  incremental/                          Tables Delta (mises a jour chaque jour)
    {catalog}/{schema}/{table}/
    _manifests/{YYYY-MM-DD}.json        Resultat du clone par table (statut, cause, taille)
    _checkpoints/{YYYY-MM-DD}.json      Point de reprise
  volumes/
    _index                              Etat complet de chaque volume, jour par jour (Delta)
    files/...                           Fichiers des volumes manages
  {YYYY-MM-DD}/                         Metadonnees du jour
    manifest.json
    report.html
    uc_metadata/                        01_catalogs.sql ... 06_functions.sql, 04_grants.sql
    jobs/jobs_all.json                  + jobs_permissions.json
    pipelines/pipelines_all.json
    workspace/manifest.json             Inventaire notebooks, fichiers, dashboards
    workspace/objects/                  Contenu brut (parquet)
    workspace_config/                   workspace_acls.json, repos_acls.json
  snapshots/
    monthly/{YYYY-MM}/                  Snapshot mensuel complet
      {catalog}/{schema}/{table}/
  latest.json                           Dernier backup termine (date + statut)
```

---

## Glossaire rapide

| Terme | Signification |
|-------|---------------|
| **DEEP CLONE** | Copie physique complete des donnees Delta (independante de la source) |
| **incremental/** | Zone mise a jour quotidiennement par DEEP CLONE ; seuls les fichiers Delta modifies sont reecrits |
| **snapshot monthly** | Copie independante prise le 1er du mois a partir du dernier backup daily termine |
| **VACUUM** | Nettoyage des fichiers Delta obsoletes (hebdomadaire, le dimanche par defaut) |
| **manifest.json** | Inventaire des tables, jobs et notebooks sauvegardes ce jour |
| **latest.json** | Pointeur vers le dernier backup termine ; reference du snapshot mensuel et du restore |
| **tables FOREIGN** | Tables federees (ex. index AI Search) : non clonees, classees « Non couvert » sauf si declarees dans `iac_file` (« As code ») |
| **dry_run** | Mode simulation : aucune modification appliquee, verification uniquement |
| **grants** | Permissions Unity Catalog (GRANT SELECT ON CATALOG...) |
| **ACLs** | Permissions workspace (notebooks, dossiers, repos Git, jobs, pipelines) |
| **mode degrade** | Le backup continue meme si une etape non critique ou une table echoue ; le statut passe a `degraded` |
