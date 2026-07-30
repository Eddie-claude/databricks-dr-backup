# Guide de déploiement v4 — design

**Date :** 2026-07-30
**Contexte :** préparation d'un workshop client (déploiement + exécution d'un backup sur son environnement) et d'un déploiement de test sur un nouvel environnement.

## Problème

Le dernier guide de déploiement (`old/DR_Backup_Guide_Deploiement_v3.docx`, 3 juin 2026) est antérieur à l'ensemble des correctifs de juillet. Il documente une solution qui n'existe plus :

- rétention annoncée `retain_daily=15` + snapshot weekly, alors que le weekly est abandonné (Option C) et `retain_daily=30`
- aucune mention du step VACUUM (6.5), qui n'existait pas
- aucune mention des notebooks de restauration 09/10/11
- aucun dépannage, alors que quatre pièges bloquants ont été rencontrés en conditions réelles (échec Terraform, PAT à scope restreint, host ADLS confondu avec host Databricks, `SCHEMA_NOT_FOUND`)
- dimensionnement cluster silencieux, alors que le Single Node est la cause racine du timeout 8 h constaté chez le client

## Livrable

`docs/DR_Backup_Guide_Deploiement_v4.docx`, généré par `docs/generate_deployment_guide.py` (committé, adapté de `old/generate_guide.py` pour conserver la charte : Calibri 11, titres `1F497D`, blocs de code Courier New sur fond `F0F0F0`, tableaux `Table Grid` à en-tête bleu, encarts note en italique gris).

Le script est committé pour qu'une v5 soit régénérable sans repartir de zéro — c'est la méthode déjà retenue pour le guide de restauration v4.

## Décisions cadrées

| Sujet | Décision |
|---|---|
| Destinataire | Guide client, utilisé d'abord par nous pour le déploiement de test — le dogfooding valide le document avant le workshop |
| Prérequis Azure | Couverture de A à Z (container ADLS, Storage Credential, External Location UC, GRANTs, Service Principal), l'environnement de test étant neuf |
| Auth CLI/DAB | Service Principal en voie principale ; OAuth U2M (`databricks auth login`) en annexe, pour déployer sans attendre l'admin Azure |
| Cluster daily | Passage à 4 workers appliqué en code et documenté comme défaut |
| Game Day | Exclu du guide — codé mais jamais exécuté de bout en bout ; on ne documente pas une promesse non tenue |

## Structure

1. Contexte et objectifs — architecture, périmètre couvert / non couvert
2. Prérequis — informations à préparer, ADLS, Storage Credential, External Location, GRANTs, Service Principal (A/B/C)
3. Phase 1 — audit `diag_01_audit.py` et OPTIMIZE préalable (ratio fichiers/GB : idéal 4-10, critique > 50)
4. Phase 2 — ZIP, `databricks.yml`, profil CLI, `diag_02_connectivity.py`, `bundle deploy`
5. Premier backup — dimensionnement, durées J1 vs J2+, comportement en cas de timeout (checkpoint = reprise)
6. Rétention réellement appliquée — 30 j, VACUUM, abandon du weekly, vérification par `DESCRIBE DETAIL`
7. Validation post-déploiement — points de contrôle avec sortie attendue
8. Activation des schedules
9. Restauration — renvoi au guide dédié, table 07→11
10. Checklist de déploiement
- Annexe A — dépannage (Terraform, PAT 403 `all-apis`, host confondu, `SCHEMA_NOT_FOUND`, `MSYS_NO_PATHCONV=1`)
- Annexe B — OAuth U2M

## Changement de code inclus

`databricks.yml`, job `dr_backup_daily` :

- `num_workers: 0 → 4`
- `node_type_id: Standard_DS3_v2 → Standard_DS4_v2` (aligné sur le monthly, seule configuration mesurée à ~1,3 s/table contre ~17 s/table en Single Node)
- retrait de `spark.master: local[*, 8]`, du profil `singleNode` et du tag `ResourceClass: SingleNode`, incompatibles avec un cluster multi-worker

`max_parallel` reste à 8. Cette valeur avait été calée sur `local[*, 8]` ; sur 4 workers le parallélisme utile vient des exécuteurs Spark et non des threads du driver. 8 reste un compromis raisonnable, mais la valeur est à confirmer sur l'environnement de test — inscrite comme point de validation au §7, pas comme acquis.

## Critères de succès

- Le guide suffit à déployer sur un environnement neuf sans connaissance préalable du projet
- Chaque piège rencontré en conditions réelles a son entrée en annexe A avec le message d'erreur exact
- Chaque point de validation du §7 indique la sortie attendue, y compris les cas où « 0 » est le bon résultat (VACUUM dans la fenêtre de rétention)
- Le `.docx` est régénérable via `python docs/generate_deployment_guide.py`
