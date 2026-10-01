# Backup des fichiers des volumes managés — Design

Date : 2026-10-01
Statut : validé en conversation, en relecture

## Contexte et objectif

Depuis l'ajout de `05_volumes.sql`, le backup sauvegarde la **définition** des volumes Unity Catalog,
mais pas leur **contenu**. Un volume EXTERNAL garde ses fichiers sur le stockage du client ; un volume
MANAGED, recréé par l'IaC ou par `12_restore_uc_objects`, revient **vide**. C'est le seul manque
restant du périmètre UC.

**Objectif :** pouvoir restaurer l'état exact des fichiers de chaque volume managé **à n'importe quel
jour des `retain_daily` derniers jours** (30 par défaut), avec la même promesse que pour les tables,
plus un état par mois sur `retain_monthly` mois.

**Hors périmètre :**
- volumes EXTERNAL (données hors de la responsabilité du backup, protégées par leur stockage) ;
- catalogs exclus du backup (`system`, `samples`, `hive_metastore`) et schéma `information_schema`.

**Inconnue :** le volume de données chez le client (nombre de fichiers, taille, taux de
changement). Le design doit tenir le million de fichiers sans être surdimensionné pour quelques Go,
et une mesure est ajoutée à `diag_01_audit` (voir § Mesure).

## Approche retenue

Stockage **versionné par dossier daté** + **index Delta** de l'état quotidien de chaque volume.
Copie des fichiers depuis le driver (`dbutils.fs.cp`, en threads), qui streame les octets sans les
charger en mémoire. Le moteur de copie est isolé derrière une fonction : si la mesure révèle de très
gros volumes, il pourra être remplacé (copie distribuée sur les executors) sans changer le format.

Écartées :
- *Copie distribuée via FUSE `/Volumes` sur les executors* : non éprouvée en `SINGLE_USER` chez le
  client. À évaluer seulement si la mesure le justifie.
- *Azure natif (object replication, PIT restore, AzCopy)* : object replication et PIT restore ne sont
  pas supportés sur un compte ADLS à namespace hiérarchique ; AzCopy contournerait la gouvernance UC
  (droits RBAC directs sur le stockage managé).

## Stockage

```
{backup_root}/volumes/
  files/{catalog}/{schema}/{volume}/{YYYY-MM-DD}/{rel_path}   ← versions copiées ce jour-là
  _index/                                                     ← table Delta, partitionnée par snapshot_date
```

Colonnes de `_index` :

| Colonne | Type | Sens |
|---|---|---|
| `snapshot_date` | STRING `YYYY-MM-DD` | jour du backup (partition) |
| `catalog`, `schema`, `volume` | STRING | volume source |
| `rel_path` | STRING | chemin du fichier relatif à la racine du volume |
| `size` | BIGINT | taille en octets |
| `mtime` | BIGINT | date de modification (ms epoch) |
| `stored_in` | STRING `YYYY-MM-DD` | dossier daté qui contient cette version |

La partition `D` décrit **l'état complet** de chaque volume le jour `D`. Le fichier de la ligne se
trouve à `files/{catalog}/{schema}/{volume}/{stored_in}/{rel_path}`. Un fichier inchangé depuis
trois semaines n'est stocké qu'une fois, toutes les partitions suivantes pointent vers le même
`stored_in`. Stockage ≈ données + changements sur la période de rétention.

Un volume qui ne contient aucun fichier n'a aucune ligne : la restauration le laisse vide, ce qui est
correct.

## Backup quotidien — `notebooks/13_volume_files.py`

Appelé par `00_orchestrator` après `02_data_clone`, étape **non critique** (un échec dégrade le
statut global sans bloquer le reste). Paramètres : `backup_root`, `backup_date`, `lib_path`,
`retain_daily`, `retain_monthly`, `max_parallel`, `dry_run` (rétention seulement).

1. **Inventaire des volumes** — `information_schema.volumes` de chaque catalog sauvegardé,
   `volume_type = 'MANAGED'`, mêmes exclusions que `01_uc_metadata`.
2. **Listing des fichiers** — parcours récursif de `/Volumes/{c}/{s}/{v}/` avec `dbutils.fs.ls`,
   parallélisé par répertoire (pool de threads). Inclut les fichiers `_*` et `.*`, que les lecteurs
   Spark (`binaryFile`) ignoreraient. Résultat : `(rel_path, size, mtime)` par fichier.
3. **Calcul du plan** (fonction pure, `lib/volume_backup.py`) — comparaison avec la **dernière
   partition existante** `< D` de l'index pour ce volume (pas forcément la veille) :
   - même `rel_path`, même `size` et même `mtime` → reprend le `stored_in` précédent, pas de copie ;
   - sinon (nouveau ou modifié) → `stored_in = D`, à copier ;
   - absent aujourd'hui → simplement pas de ligne dans la partition `D`.
   Premier run : tout est à copier.
4. **Copie** — `dbutils.fs.cp(/Volumes/.../rel_path, files/.../D/rel_path)` sur `max_parallel`
   threads. **Reprise sans checkpoint** : avant de copier, le dossier `files/.../D/` est listé ; un
   fichier déjà présent avec la même taille est sauté. Relancer avec le même `backup_date` reprend
   donc là où le run s'est arrêté, y compris le lendemain (même convention que `02`).
   Un fichier disparu entre listing et copie → avertissement, retiré du plan (pas de ligne).
   Toute autre erreur de copie → le volume est marqué en erreur et **sa partition n'est pas écrite**.
5. **Écriture de l'index** — en fin de volume, `replaceWhere` sur
   `snapshot_date = D AND catalog/schema/volume = …` : idempotent, et un volume dont la copie a
   échoué ne laisse jamais de snapshot pointant vers des fichiers absents.
6. **Rétention** (voir ci-dessous), puis résultat JSON : volumes traités, fichiers listés, copiés,
   octets copiés, erreurs.

### Rétention

Fonction pure `select_kept_dates(dates, today, retain_daily, retain_monthly)` :
- garder les partitions des `retain_daily` derniers jours ;
- garder, pour chacun des `retain_monthly` derniers mois, la **plus ancienne** partition du mois
  (le 1er si le job a tourné, sinon le premier jour disponible) ;
- les autres partitions sont expirées.

Purge, calculée **depuis l'index** (pas de listing de `files/`) :
- un fichier `files/…/{stored_in}/{rel_path}` référencé par une partition expirée et par **aucune**
  partition conservée est supprimé ;
- puis les partitions expirées sont supprimées de l'index (`DELETE WHERE snapshot_date IN …`) ;
- les dossiers datés `files/…/{date}/` sans partition dans l'index et plus anciens que
  `retain_daily` jours (restes d'un run interrompu jamais repris) sont supprimés.

`dry_run = true` affiche ce qui serait purgé sans rien supprimer (même widget que `06_retention`).
Un `VACUUM` de `_index` est lancé par la même étape (rétention Delta par défaut), pour que la table
d'index elle-même ne grossisse pas indéfiniment.

## Restauration — `notebooks/14_restore_volume_files.py`

Ajouté au multiselect `restore_scope` de `10_restore_orchestrator` sous le nom `volume_files`,
exécuté après `uc_objects` (le volume cible doit exister).

Paramètres : `backup_root`, `restore_date` (vide = dernière partition disponible),
`volume_filter` (`catalog.schema.volume`, jokers `*` comme `07_restore`), `target_volume`
(optionnel, `catalog.schema.volume`, uniquement si le filtre désigne un seul volume), `dry_run`.

- Pour chaque volume sélectionné, lit la partition `restore_date`. Si ce volume n'en a pas ce
  jour-là (copie en échec ce jour), prend sa dernière partition **antérieure** et l'indique en
  avertissement. Si aucune partition `≤ restore_date` n'existe pour ce volume, il est signalé en
  erreur avec la liste de ses dates disponibles ; les autres volumes sont restaurés.
- Vérifie que le volume cible existe (sinon erreur explicite : le créer via IaC ou `12`).
- Copie `files/…/{stored_in}/{rel_path}` → `/Volumes/{cible}/{rel_path}`, en écrasant, sur
  `max_parallel` threads.
- Les fichiers présents dans la cible mais absents du snapshot **ne sont pas supprimés** : ils sont
  comptés et listés (50 premiers) dans le rapport.
- `dry_run = true` : affiche nombre de fichiers, taille totale et un échantillon, sans copier.

Effet de bord connu : un fichier restauré prend une nouvelle `mtime`, il sera donc recopié une fois
au backup suivant.

## Mesure — `diag_01_audit`

Nouvelle section : pour chaque volume managé, nombre de fichiers, taille totale, nombre et taille
des fichiers modifiés depuis 24 h, durée du listing. Permet d'estimer la durée du premier run (copie
complète) et des runs quotidiens avant activation chez le client.

## Code et tests

- `lib/volume_backup.py` — fonctions pures, testées en local (`tests/test_volume_backup.py`) :
  plan de copie, sélection des dates conservées, fichiers à purger, construction des chemins
  (`rel_path` avec espaces, accents, sous-dossiers ; refus des `..`).
- Notebooks : orchestration et I/O uniquement.
- Validation Databricks sur le workspace dev, **avec accord explicite avant toute écriture** :
  1. vérifier que `dbutils.fs.cp` copie bien de `/Volumes/...` vers `abfss://` sur le cluster
     `SINGLE_USER` (point non encore éprouvé — à tester en premier) ;
  2. backup d'un volume de test, modification/suppression de fichiers, second backup, restauration
     au jour 1 et au jour 2 dans un volume cible, comparaison.

## Documentation

Runbook (nouvelle section de restauration, arborescence `volumes/`), source du guide de déploiement
(périmètre, § « ne sauvegarde pas » mis à jour, étape 13 dans le pipeline), README si concerné.
