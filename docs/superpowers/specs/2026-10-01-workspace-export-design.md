# Export complet du workspace + pipelines — Design (brouillon)

Date : 2026-10-01
Statut : **brouillon, non validé** — deux questions ouvertes (voir fin), à reprendre.

## Constat sur `05_workspace_config` (état au commit de ce document)

| Sujet | Constat |
|---|---|
| Chemins | `workspace_paths` = `/Shared` par défaut ; `00_orchestrator` ne le transmet pas → seul `/Shared` est exporté |
| Profondeur | `list_workspace_recursive(max_depth=4)` s'arrête silencieusement au-delà de 4 niveaux |
| Types | seuls les `NOTEBOOK` sont exportés ; les fichiers workspace (`FILE` : `.sh`, `.yml`, `.py` non-notebook, `.whl`…) jamais |
| Format | export `SOURCE` : un notebook `.ipynb` devient `.py` (sorties et format perdus) |
| Binaire | `_uc_put` découpe en lignes texte → un fichier binaire serait corrompu |
| Erreurs | un dossier illisible (403) donne une liste vide, sans avertissement |
| Jobs | tous ceux visibles par l'identité du job (pas de `run_as` → celle du déployeur) ; permissions des jobs non sauvegardées |
| Pipelines | non sauvegardés |
| Restore `08` | déduit le type depuis l'extension (`lang_map`) → ambigu entre notebook `x.py` et fichier `x.py` |

Home = `/Users/<moi>`, couvert par `/Users`. `/Repos` : seuls URL/branche/ACL sont sauvegardés
(`repos_acls.json`).

## Proposition

1. **Export de tout le workspace (`05`)**
   - `workspace_paths` = `/` par défaut, exposé dans l'orchestrateur et le bundle ; `exclude_paths`
     (défaut `/Repos`).
   - Parcours récursif sans limite de profondeur ; `[WARN]` + compteur dans le rapport pour chaque
     dossier illisible.
   - Notebooks exportés en format `AUTO` (Jupyter reste `.ipynb`, source reste `.py/.sql/...`).
   - Fichiers workspace exportés en brut (octets), sans conversion.
   - `workspace_manifest.json` : chemin, type, langage, format de chaque objet.
   - Prérequis : identité du job **admin du workspace** pour lire `/Users/*` → `run_as` explicite
     (service principal) dans `databricks.yml`.
2. **Restauration (`08`)** — pilotée par le manifest (bon type, bon format, fichiers en brut) ;
   rétrocompatible avec les backups sans manifest (logique actuelle par extension).
3. **Pipelines** — `05` exporte la définition complète (`GET /api/2.0/pipelines/{id}`) et les
   permissions dans `pipelines/` ; restauration par un scope `pipelines` (dans `09` ou notebook
   dédié), modes `skip` / `recreate` comme les jobs. Limite : tables de pipeline (MV, streaming
   tables) non clonables → full refresh après restauration ; les streaming tables relisent leurs
   sources depuis le début si celles-ci ont encore les données.
4. **Permissions des jobs** — sauvegardées et restaurées sur le même mécanisme.

## À vérifier sur dev (lecture seule) avant de coder

- Ce que renvoie l'API d'export en format `AUTO` pour un notebook `.ipynb`.
- Nombre d'objets du workspace : `_uc_put` lance un job Spark par fichier, potentiellement des
  heures pour des milliers de notebooks sous `/Users` → choisir le mode d'écriture (ex. une archive
  par dossier racine plutôt qu'un fichier par objet).

## Questions ouvertes

1. Les pipelines sont-ils déjà définis en code (bundle / Terraform) ? Si oui : backup de la
   définition gardé comme filet de sécurité, ordre de restauration « IaC d'abord ».
2. Contenu des Repos / Git folders : exclu (dans Git, risque : modifications non commitées perdues)
   ou inclus ?
