# DR Backup — Corrections de la version 4.2

Synthèse des corrections ; le détail est dans `DR_Backup_Notes_de_Version_v4.2.docx` (§ indiqués).
Validé par un test aller-retour complet (backup → sinistre → restauration) sur KeyIT dev le
2026-10-07, rejoué le 2026-10-09 avec les derniers correctifs, et par 247 tests automatisés. Les correctifs
§ 2.20 à 2.22 (droits hérités des groupes, dernier backup prioritaire, mensuel sur le dernier backup terminé — 2026-10-27 et 2026-10-29) sont couverts par les tests, pas encore par un run réel.

## Pertes de données silencieuses

| § | Correction |
|---|---|
| 2.8 | **Jobs sauvegardés sans leurs tâches** : une restauration recréait des jobs vides. Les backups antérieurs à la 4.2 ne permettent pas de restaurer les jobs. |
| 2.9 | **Workspace incomplet** : les objets refusés par l'API (HTTP 429, trop de requêtes) étaient perdus sans erreur — tout `/Shared` manquait sur KeyIT. Réessais, parallélisme 4, l'étape échoue si l'export est incomplet. |
| 2.12 | **Tables non sauvegardées invisibles** : le rapport affichait les tables découvertes. Il détaille désormais sauvegardées / non clonables / en erreur, avec la liste et la cause ; statut « degraded » en cas d'erreur de clone ; les vues ne partent plus au clone si `information_schema` est illisible (cause chez le client : pas de `USE CATALOG` sur `system`, droit désormais documenté et vérifié par `dr-backup-grants-sync`). |
| 2.13 | **Table supprimée puis recréée non recopiée** si sa version Delta retombait au même numéro : le backup gardait les données de l'ancienne table. |
| 2.4 | **Droits hérités exportés comme directs** : une restauration les aurait matérialisés objet par objet. |

## Restauration incorrecte ou impossible

| § | Correction |
|---|---|
| 2.10 | **Droits du workspace (dossiers, notebooks, repos) jamais restaurés** : l'API refusait le format envoyé. |
| 2.11 | **Vues recréées hors de leur catalogue**, et **catalogues non recréables** sur un metastore sans stockage racine (DDL sans emplacement). |
| 2.15 | **`07` restaurait des tables périmées** (présentes dans le stockage mais absentes du backup du jour) : sélection d'après le manifest de la date de référence, `include_stale` pour les inclure. |
| 2.16 | **L'orchestrateur de restauration reprenait tout** (workspace de tous les utilisateurs, tous les pipelines, tous les volumes) : `notebook_filter`, `pipeline_filter`, `volume_filter`. |
| 2.17 | **Metric views absentes du backup** : `SHOW CREATE TABLE` les refuse sur le runtime des jobs. Définition reconstruite depuis leur YAML (`CREATE VIEW … WITH METRICS`). |
| 2.18 | **« recreate » doublait les jobs** (second job du même nom, deux exécutions planifiées) **et supprimait les pipelines** avec leurs tables : `replace` remplace en place (`jobs/reset`, `PUT /pipelines`). |
| 2.19 | **Volumes / fonctions modifiés depuis le backup laissés en place sans signal** : comparés au backup, signalés « DIFFÉRENT ». |
| 2.6 | Le corps des fonctions Python gagnait des lignes vides à chaque cycle backup → restauration. |

## Fiabilité du job de backup

| § | Correction |
|---|---|
| 2.1 | **Premier run du daily en échec chaque jour** (mémoire du driver saturée, cache par table jamais libéré) ; driver Standard_E8s_v3. |
| 2.14 | **Table en échec chaque jour** après passage en column mapping « name » puis renommage ou suppression de colonne : l'ancienne copie est archivée, la table recopiée en entier. |
| 2.7 | **`lib/` lu dans une ancienne copie manuelle** : volumes et fonctions non exportés, plusieurs restaurations en échec. Les jobs utilisent le `lib/` déployé par le bundle. |
| 2.5 | Point de reprise écrit moins souvent (allège le driver). |
| 2.2 | Catalogues fédérés et Delta Sharing exclus (définition relevant de l'IaC). |
| 2.3 | Traces Java retirées des messages (elles tronquaient la sortie des notebooks). |
| 2.22 | **Snapshot mensuel lancé avant la fin du daily** : le manifest du jour n'existait pas encore, le mensuel parcourait tout `incremental/` (tables supprimées ou en erreur copiées, mélange d'états si le daily tournait). Référence désormais le dernier backup terminé (`latest.json`). |
| 2.20 | **`dr-backup-grants-sync` ignorait les droits hérités des groupes** : il les signalait manquants et, en mode apply, les aurait ré-accordés directement au compte de backup. Groupes lus par l'API SCIM du compte, imbrication comprise ; apply interrompu si les groupes sont illisibles. |

## Rapport de couverture (`diag_03_backup_coverage`)

Règles alignées sur la v4.2 corrigée : jobs (tâches et permissions), pipelines, tout le workspace
(fichiers compris, toute profondeur), metric views couvertes ; tables à filtre de lignes ou masque de
colonnes signalées « données non sauvegardées » ; une vue en erreur au clone n'est plus comptée comme
un échec ; alerte si le compte de backup n'a pas `USE CATALOG` sur `system`. Droits du compte de
backup lus **avec ceux de tous ses groupes**, imbrication comprise (§ 2.20, retour du test datalab) :
sans cela, des objets accessibles via des groupes de privilèges apparaissaient « inaccessibles ». Pour les
tables, **le résultat du dernier backup prime sur le calcul des droits** (§ 2.21, même retour) : une table
copiée est sauvegardée ; absente du manifest, elle est « nouvelle » (créée après le backup) ou « absente du
dernier backup ». Les règles v4.1 restent appliquées à un backup v4.1 (jobs sans tâches, seul `/Shared` exporté…).

Job `dr-backup-coverage-report` (livré en pause, le 1er du mois à 06:00 UTC) : produit le rapport
chaque mois dans `coverage_output_dir`, avec l'identité admin qui déploie le bundle. Vérifié sur
KeyIT dev (run en succès, rapport HTML + Excel écrit).

## Contrôle des tables protégées (`diag_04_protected_tables`)

Pour chaque table à filtre de lignes ou masque de colonnes, appelle les fonctions de filtre et de
masque sous l'identité qui exécute le notebook (à lancer en « Run as » du compte de backup) et rend un
verdict : COMPLETE (toutes les lignes, valeurs réelles), LIGNES_FILTREES, VALEURS_MASQUEES, INDETERMINE.
Prérequis d'une éventuelle copie par lecture (CTAS) de ces tables. Vérifié sur KeyIT dev contre les
lignes et valeurs réellement visibles (4 cas sur 4).

## Effets visibles après la mise à jour

- Le **premier run recopie toutes les tables une fois** : plus long que d'habitude.
- Le **statut « degraded »** peut apparaître : il signale un manque réel, jusqu'ici invisible.
- `incremental/_reclone_archive/` n'est **pas purgé** par la rétention : à nettoyer manuellement.
- Les tables protégées par un **filtre de lignes ou un masque de colonnes** restent non sauvegardables
  par DEEP CLONE ; elles sont désormais listées dans le rapport.
