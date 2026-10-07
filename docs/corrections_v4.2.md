# DR Backup — Corrections de la version 4.2

Synthèse des corrections ; le détail est dans `DR_Backup_Notes_de_Version_v4.2.docx` (§ indiqués).
Validé par un test aller-retour complet (backup → sinistre → restauration) sur KeyIT dev le
2026-10-07, et par 153 tests automatisés.

## Pertes de données silencieuses

| § | Correction |
|---|---|
| 2.8 | **Jobs sauvegardés sans leurs tâches** : une restauration recréait des jobs vides. Les backups antérieurs à la 4.2 ne permettent pas de restaurer les jobs. |
| 2.9 | **Workspace incomplet** : les objets refusés par l'API (HTTP 429, trop de requêtes) étaient perdus sans erreur — tout `/Shared` manquait sur KeyIT. Réessais, parallélisme 4, l'étape échoue si l'export est incomplet. |
| 2.12 | **Tables non sauvegardées invisibles** : le rapport affichait les tables découvertes. Il détaille désormais sauvegardées / non clonables / en erreur, avec la liste et la cause ; statut « degraded » en cas d'erreur de clone ; les vues ne partent plus au clone si `information_schema` est illisible. |
| 2.13 | **Table supprimée puis recréée non recopiée** si sa version Delta retombait au même numéro : le backup gardait les données de l'ancienne table. |
| 2.4 | **Droits hérités exportés comme directs** : une restauration les aurait matérialisés objet par objet. |

## Restauration incorrecte ou impossible

| § | Correction |
|---|---|
| 2.10 | **Droits du workspace (dossiers, notebooks, repos) jamais restaurés** : l'API refusait le format envoyé. |
| 2.11 | **Vues recréées hors de leur catalogue**, et **catalogues non recréables** sur un metastore sans stockage racine (DDL sans emplacement). |
| 2.15 | **`07` restaurait des tables périmées** (présentes dans le stockage mais absentes du backup du jour) : sélection d'après le manifest de la date de référence, `include_stale` pour les inclure. |
| 2.16 | **L'orchestrateur de restauration reprenait tout** (workspace de tous les utilisateurs, tous les pipelines, tous les volumes) : `notebook_filter`, `pipeline_filter`, `volume_filter`. |
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

## Effets visibles après la mise à jour

- Le **premier run recopie toutes les tables une fois** : plus long que d'habitude.
- Le **statut « degraded »** peut apparaître : il signale un manque réel, jusqu'ici invisible.
- `incremental/_reclone_archive/` n'est **pas purgé** par la rétention : à nettoyer manuellement.
- Les tables protégées par un **filtre de lignes ou un masque de colonnes** restent non sauvegardables
  par DEEP CLONE ; elles sont désormais listées dans le rapport.
