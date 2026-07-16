# DR Game Day automatisé — Design

**Date :** 2026-07-16
**Statut :** Approuvé

## Contexte et objectif

La solution DR Backup dispose déjà de notebooks de démo manuels (`notebooks/demo/00_setup.py` → `04_cleanup.py`) qui simulent un sinistre et une restauration pour les présentations client. Ces notebooks sont narratifs (prints, pas d'assertion) et exécutés à la main.

L'objectif est d'ajouter un **game day automatisé** : un test de restauration récurrent, sans intervention manuelle, qui prouve que la procédure de restauration fonctionne réellement — avant qu'un vrai sinistre ne survienne. Il doit détecter tout seul un échec (donnée non restaurée, écart de comptage) et alerter, sans qu'un humain ait à lire les logs.

## Périmètre

- **Environnement** : sandbox démo (`source_demo01`, données factices déjà utilisées par les notebooks `demo/`). Aucun risque sur les catalogs de production.
- **Validation couverte** : données uniquement (tables Delta restaurées via UC metadata SQL + DEEP CLONE). Pas d'ACLs, grants, jobs ou notebooks dans cette première itération.
- **Déclenchement** : à la demande uniquement (pas de cron pour l'instant). Une planification récurrente pourra être ajoutée plus tard en une ligne `schedule:` dans le job.
- **Reporting** : email en cas d'échec uniquement, via le mécanisme `email_notifications.on_failure` déjà utilisé par les jobs `dr_backup_daily`/`dr_backup_monthly`. Silence = tout va bien.
- **Source du backup restauré** : le **vrai dernier backup de production** (auto-détecté via `latest.json`), pas un backup isolé dédié au test. Comme `01_uc_metadata` sauvegarde déjà tous les catalogs (dont `source_demo01`), aucune nouvelle logique de backup n'est nécessaire. Cela valide directement que le backup produit chaque nuit est effectivement restaurable — c'est l'objectif même d'un game day DR.

## Architecture

Un nouveau notebook orchestrateur unique, appelé par un nouveau job Databricks, qui réutilise la logique déjà éprouvée des notebooks `demo/00_setup.py` et `demo/03_dr_scenario.py` sans la dupliquer, et y ajoute une étape de vérification stricte avec assertion ainsi qu'un cleanup garanti.

## Composants

### 1. `notebooks/demo/07_gameday.py` (nouveau)

Orchestrateur unique, séquence :

1. **Setup idempotent** — même logique que `00_setup.py` (CREATE OR REPLACE des schémas/tables de démo avec données fixes). Capture ensuite les row counts réels de chaque table juste après le setup → variable `expected_counts` (pas de constantes en dur dans le code, résilient si les données de démo évoluent plus tard).
2. **Sinistre + restauration** — même logique que `03_dr_scenario.py`, limitée à `source_demo01.sales_demo` (le schéma `hr_demo` n'est pas mis en sinistre/restauré, exactement comme `03_dr_scenario.py` aujourd'hui) :
   - `DROP TABLE` + `DROP SCHEMA` sur `source_demo01.sales_demo`
   - Auto-détection de la dernière date de backup via `latest.json`
   - Replay des dumps SQL (`02_schemas.sql`, `03_tables.sql`) filtrés sur `source_demo01.sales_demo`
   - `DEEP CLONE` des tables depuis le manifest de clone du backup
3. **Vérification** — recompte les lignes de chaque table de `sales_demo` post-restauration, compare à `expected_counts` table par table. Au moindre écart ou table manquante : `raise RuntimeError(...)` avec le détail des écarts.
4. **Cleanup** — `DROP` des schémas de démo créés à l'étape 1 (`sales_demo` et `hr_demo`), dans un bloc `finally` de sorte qu'il s'exécute que la vérification ait réussi ou échoué. Le cleanup lui-même est protégé par un `try/except` interne qui logue un warning en cas d'échec, sans jamais masquer l'exception originale de vérification (même principe critical/non-critical que dans `10_restore_orchestrator.py`).
5. `dbutils.notebook.exit(json.dumps({...}))` avec un résumé (statut, tables vérifiées, écarts) — consultable dans l'UI Databricks Jobs même en cas de succès.

### 2. Job `dr_gameday` (nouveau, dans `databricks.yml`)

- Cluster : identique au cluster du job `dr_backup_daily` (SINGLE_USER, Single Node — requis pour le credential passthrough UC lors de la lecture ADLS)
- `pause_status: PAUSED`, pas de bloc `schedule` (déclenchement à la demande uniquement pour cette itération)
- `email_notifications.on_failure` sur la variable `notification_email` déjà définie dans `databricks.yml`
- Déclenchement : `databricks bundle run dr_gameday --target dev` (ou `prod`), ou via l'UI Databricks Jobs

## Flux de données

```
00_setup (idempotent) → capture "expected" (comptage réel post-setup)
        ↓
DROP TABLE + DROP SCHEMA (sinistre simulé)
        ↓
Replay SQL UC (02_schemas.sql, 03_tables.sql)
  + DEEP CLONE depuis le dernier backup réel (latest.json)
        ↓
Recompte les lignes → compare à "expected"
   ├─ Match      → succès, résumé JSON, dbutils.notebook.exit
   └─ Mismatch   → raise RuntimeError
        ↓ (dans tous les cas, bloc finally)
Cleanup (DROP schémas démo)
```

## Gestion des erreurs

- Aucune exception silencieuse dans le chemin principal (setup / sinistre / restore / vérification) : toute erreur remonte jusqu'au job, qui passe en `FAILED` → email natif Databricks, sans code de notification custom à écrire.
- Le cleanup est isolé dans son propre `try/except` pour ne jamais masquer l'erreur de vérification originale si le nettoyage lui-même échoue.
- Le résumé JSON de fin d'exécution (`dbutils.notebook.exit`) est produit dans tous les cas de succès, pour un historique consultable dans l'UI Jobs.

## Validation

Pas de suite pytest pertinente pour ce notebook (effets de bord réels sur Unity Catalog, cohérent avec le reste des notebooks du projet — seul `lib/` a des tests unitaires). Validation par exécution réelle :

1. `databricks bundle run dr_gameday --target dev` — vérifier le comportement nominal (succès + cleanup effectif).
2. Test négatif volontaire : casser temporairement une valeur attendue (ou altérer les données restaurées) pour confirmer que l'échec est bien détecté (le job passe en FAILED) et que l'email part réellement.

## Hors périmètre (pour une itération future)

- Cron / planification récurrente (ajout trivial d'un bloc `schedule:` une fois le comportement validé manuellement)
- Extension du périmètre testé aux ACLs, grants, jobs, notebooks (couvrirait alors tout ce que `10_restore_orchestrator.py` orchestre)
- Mini-backup isolé dédié au game day (Approche B, écartée — voir ci-dessous)

## Alternative écartée

**Approche B — mini-backup isolé dédié au game day** : le game day aurait créé son propre snapshot scopé à `source_demo01` dans un préfixe ADLS dédié (`gameday/`), indépendant du backup réel de production. Écartée car elle demande plus de code à maintenir et surtout ne valide plus le vrai artefact de production — seulement le mécanisme de clone/restore en général. L'approche retenue (réutiliser le vrai dernier backup) est plus simple et plus fidèle à l'objectif du game day.
