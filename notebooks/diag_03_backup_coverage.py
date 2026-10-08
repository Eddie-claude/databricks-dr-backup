# Databricks notebook source
# notebooks/diag_03_backup_coverage.py
#
# RAPPORT DE COUVERTURE DU BACKUP DR — lecture seule
# Inventorie tous les objets du metastore Unity Catalog et du workspace, et indique pour
# chacun s'il est sauvegardé / restaurable, et par quel moyen (script de backup, as code,
# autre moyen) — ou s'il ne l'est pas.
# Produit un rapport HTML (présentation) et un fichier Excel (détail objet par objet).

# COMMAND ----------
# MAGIC %md # Rapport de couverture du backup DR
# MAGIC
# MAGIC **Lecture seule — aucune modification.** À lancer avec un compte **admin du metastore**
# MAGIC (il doit voir tous les objets) ; les droits du compte de backup sont vérifiés séparément
# MAGIC via `backup_principal`.
# MAGIC
# MAGIC | Paramètre | Description |
# MAGIC |-----------|-------------|
# MAGIC | `backup_principal` | Groupe ou service principal (Application ID) qui exécute le backup — vide = droits non vérifiés |
# MAGIC | `iac_identities` | Identités qui déploient l'IaC (SP Terraform, SP bundle), séparées par des virgules : un objet qu'elles ont créé est classé « as code » |
# MAGIC | `iac_file` | (optionnel) Fichier listant les objets gérés en code, un nom complet par ligne (`catalog.schema.objet`, nom d'external location…) |
# MAGIC | `excluded_catalogs` | Catalogs exclus volontairement du backup (séparés par des virgules) |
# MAGIC | `backup_root` | (optionnel) Racine du backup `abfss://…` : vérifie le dernier backup réel et détecte la version déployée |
# MAGIC | `backup_version` | `auto` (détectée depuis `backup_root`), `v4.1` ou `v4.2` |
# MAGIC | `count_workspace_objects` | Compter les notebooks / fichiers par dossier du workspace (plus long) |
# MAGIC | `output_dir` | Dossier du workspace où écrire le HTML et l'Excel (vide = dossier personnel) |

# COMMAND ----------
import concurrent.futures
import html
import json
import os
from datetime import datetime

import requests
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def _short(e) -> str:
    """Message d'erreur sans la stacktrace JVM (qui ferait tronquer la sortie du notebook)."""
    return " ".join(str(e).split("JVM stacktrace:")[0].split())[:300]

_ctx  = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST  = _ctx.apiUrl().get()
_HDRS = {"Authorization": f"Bearer {_ctx.apiToken().get()}"}

def api_get(path: str, params: dict = None) -> dict:
    r = requests.get(f"{HOST}{path}", headers=_HDRS, params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()

def api_list(path: str, key: str, params: dict = None) -> list:
    """Liste paginée (next_page_token) d'une API REST Databricks."""
    params, items = dict(params or {}), []
    while True:
        d = api_get(path, params)
        items += d.get(key, []) or []
        if not d.get("next_page_token"):
            return items
        params["page_token"] = d["next_page_token"]

def sql_rows(query: str) -> list:
    return [r.asDict() for r in spark.sql(query).collect()]

warnings = []
def warn(msg: str) -> None:
    warnings.append(msg)
    print(f"[WARN] {msg}")

# COMMAND ----------
dbutils.widgets.text("backup_principal",        "",      "Groupe / SP du backup")
dbutils.widgets.text("iac_identities",          "",      "Identités de déploiement IaC (virgules)")
dbutils.widgets.text("iac_file",                "",      "Fichier des objets as code (optionnel)")
dbutils.widgets.text("excluded_catalogs",       "",      "Catalogs exclus volontairement (virgules)")
dbutils.widgets.text("backup_root",             "",      "Racine du backup abfss://… (optionnel)")
dbutils.widgets.dropdown("backup_version",      "auto",  ["auto", "v4.1", "v4.2"], "Version du backup déployée")
dbutils.widgets.dropdown("count_workspace_objects", "true", ["true", "false"], "Compter les objets du workspace")
dbutils.widgets.text("output_dir",              "",      "Dossier de sortie (workspace)")

def _csv(name: str) -> list:
    return [x.strip() for x in dbutils.widgets.get(name).split(",") if x.strip()]

backup_principal  = dbutils.widgets.get("backup_principal").strip()
iac_identities    = {x.lower() for x in _csv("iac_identities")}
iac_file          = dbutils.widgets.get("iac_file").strip()
excluded_catalogs = {x.lower() for x in _csv("excluded_catalogs")}
backup_root       = dbutils.widgets.get("backup_root").strip().rstrip("/")
backup_version    = dbutils.widgets.get("backup_version")
count_ws_objects  = dbutils.widgets.get("count_workspace_objects") == "true"
current_user      = spark.sql("SELECT current_user()").collect()[0][0]
output_dir        = dbutils.widgets.get("output_dir").strip() or f"/Workspace/Users/{current_user}/dr-backup-rapports"
run_at            = datetime.now()

print(f"[OK] Exécuté par {current_user} sur {HOST} — {run_at:%Y-%m-%d %H:%M}")

# COMMAND ----------
# MAGIC %md ## Règles de couverture
# MAGIC Bloc pur (aucune I/O), testé hors Databricks par `tests/test_coverage_rules.py`.
# MAGIC Il traduit ce que fait **réellement** la solution de backup, version par version.

# COMMAND ----------
# >>> REGLES
# Couverture d'une définition ou de données :
#   SCRIPT = notebooks de backup DR | IAC = recréé par l'infrastructure as code
#   OTHER  = autre moyen (données hors Databricks, Git, recalcul) | NONE = non couvert | NA = sans objet
LABELS_COV = {"SCRIPT": "Script backup", "IAC": "As code", "OTHER": "Autre moyen", "NONE": "Non couvert", "NA": "—"}

VERDICTS = {  # code: (pastille, libellé, couleur, ordre d'affichage)
    "OK":           ("✅", "Sauvegardé et restaurable",           "#1a7f37", 1),
    "AS_CODE":      ("🔵", "As code (IaC)",                        "#1f6feb", 2),
    "AUTRE":        ("⚪", "Autre moyen",                          "#6e7781", 3),
    "PARTIEL":      ("🟡", "Partiel",                              "#bf8700", 4),
    "INACCESSIBLE": ("🔴", "Couvert mais inaccessible au backup",  "#cf222e", 5),
    "ECHEC":        ("🔴", "Échec au dernier backup",              "#cf222e", 6),
    "NON_COUVERT":  ("🔴", "Non couvert",                          "#cf222e", 7),
    "EXCLU":        ("⚫", "Exclu volontairement",                 "#24292f", 8),
}
TO_TREAT = {"PARTIEL", "INACCESSIBLE", "ECHEC", "NON_COUVERT"}

# Nombre de niveaux sous /Shared exportés par 05_workspace_config (list_workspace_recursive, max_depth=4)
SHARED_EXPORT_DEPTH = 5


def priv_set(privileges) -> set:
    """Normalise les privilèges ('USE CATALOG', 'use_catalog' → 'USE_CATALOG')."""
    return {p.strip().upper().replace(" ", "_") for p in privileges if p}


def can_access(kind: str, cat_privs: set, sch_privs: set, obj_privs: set) -> bool:
    """Le principal de backup a-t-il les droits nécessaires au backup de cet objet ?"""
    def has(privs, p):
        return p in privs or "ALL_PRIVILEGES" in privs
    if not has(cat_privs, "USE_CATALOG"):
        return False
    if kind == "CATALOG":
        return True
    if not has(sch_privs, "USE_SCHEMA"):
        return False
    if kind == "SCHEMA":
        return True
    if kind == "TABLE":                 # DEEP CLONE : lecture des données
        return has(obj_privs, "SELECT")
    # Volumes / fonctions : seule la définition est exportée, BROWSE (métadonnées) suffit
    needed = {"VOLUME": "READ_VOLUME", "FUNCTION": "EXECUTE"}[kind]
    return has(obj_privs, needed) or "BROWSE" in obj_privs or "BROWSE" in cat_privs


def parse_iac_file(text: str) -> set:
    names = set()
    for line in text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            names.add(line.replace("`", "").lower())
    return names


def iac_source(name: str, created_by: str, iac_ids: set, iac_names: set):
    """'créateur' / 'fichier' si l'objet est géré as code, sinon None."""
    if created_by and created_by.lower() in iac_ids:
        return "créateur"
    if name and name.replace("`", "").lower() in iac_names:
        return "fichier"
    return None


def apply_iac(def_cov: str, iac):
    """Une définition non couverte par le script mais déployée en code devient IAC."""
    return "IAC" if iac and def_cov == "NONE" else def_cov


def missing_system_access(system_privs: set) -> bool:
    """Le compte de backup lit information_schema, qui s'appuie sur le catalog system : sans USE
    CATALOG dessus, le type des tables est illisible (cas client : 4 295 vues envoyées au clone)."""
    return not ({"USE_CATALOG", "ALL_PRIVILEGES"} & set(system_privs))


def coverage(kind: str, sub: str = "", fmt: str = "", version: str = "v4.2", protected: bool = False) -> tuple:
    """(couverture définition, couverture données, explication) selon la version du backup.
    protected = table soumise à un filtre de lignes ou un masque de colonnes."""
    v42 = version == "v4.2"
    sub, fmt = (sub or "").upper(), (fmt or "").upper()
    if kind == "CATALOG":
        if sub == "FOREIGN_CATALOG":
            return "NONE", "OTHER", "Catalog fédéré : données dans la base source ; connexion et catalog à recréer (IaC)"
        if sub == "DELTASHARING_CATALOG":
            return "NONE", "OTHER", "Catalog Delta Sharing : données chez le fournisseur ; catalog à recréer depuis le partage"
        return "SCRIPT", "NA", "DDL exporté (uc_metadata/01_catalogs.sql)"
    if kind == "SCHEMA":
        return "SCRIPT", "NA", "DDL exporté (uc_metadata/02_schemas.sql)"
    if kind == "TABLE":
        if sub == "METRIC_VIEW":
            if v42:
                return "SCRIPT", "NA", "Définition YAML exportée (uc_metadata/03_tables.sql, CREATE VIEW … WITH METRICS)"
            return "NONE", "NA", "Non exportée en v4.1 : SHOW CREATE TABLE refusé sur le runtime des jobs"
        if sub == "VIEW":
            return "SCRIPT", "NA", ("DDL de la vue exporté (uc_metadata/03_tables.sql)" if v42 else
                                    "DDL exporté sans le catalogue en v4.1 : à recréer dans le bon catalogue")
        if sub in ("MATERIALIZED_VIEW", "STREAMING_TABLE"):
            return "SCRIPT", "OTHER", "DDL exporté ; données non clonables, recalculées par refresh / pipeline"
        if sub in ("MANAGED", "EXTERNAL"):
            if protected:
                return "SCRIPT", "NONE", ("Filtre de lignes ou masque de colonnes : DEEP CLONE refusé, données "
                                          "non sauvegardées, quel que soit le compte. À couvrir autrement : "
                                          "reconstruction depuis la source, copie par lecture (CTAS), ou filtre "
                                          "porté par une vue plutôt que par la table")
            if fmt == "DELTA":
                return "SCRIPT", "SCRIPT", "DDL + DEEP CLONE incrémental (point-in-time sur la rétention quotidienne)"
            return "SCRIPT", "NONE", f"Format {fmt or 'inconnu'} : DDL exporté, DEEP CLONE non garanti"
        return "SCRIPT", "NONE", f"Type {sub or 'inconnu'} : DDL exporté, données non clonées"
    if kind == "VOLUME":
        if sub == "EXTERNAL":
            return ("SCRIPT" if v42 else "NONE"), "OTHER", \
                   "Définition " + ("exportée (05_volumes.sql)" if v42 else "non exportée en v4.1") + \
                   " ; fichiers sur le stockage d'origine (protection du compte de stockage)"
        if v42:
            return "SCRIPT", "SCRIPT", ("Définition exportée (05_volumes.sql) ; fichiers sauvegardés par "
                                        "14_volume_files (point-in-time sur la rétention quotidienne)")
        return "NONE", "NONE", "Définition et fichiers non sauvegardés en v4.1"
    if kind == "FUNCTION":
        return ("SCRIPT" if v42 else "NONE"), "NA", \
               "DDL exporté (06_functions.sql)" if v42 else "Non exportée en v4.1"
    if kind in ("EXTERNAL_LOCATION", "STORAGE_CREDENTIAL", "CONNECTION", "SHARE", "RECIPIENT"):
        return "NONE", "NA", "Non sauvegardé par le script : à gérer en IaC"
    if kind == "MODEL":
        return "NONE", "NONE", "Modèle MLflow : ni définition ni versions sauvegardées"
    if kind == "JOB":
        if v42:
            return "SCRIPT", "NA", "Définition complète (tâches comprises) et permissions exportées (jobs/*.json)"
        return "NONE", "NA", "Sauvegardé sans ses tâches en v4.1 : le job serait recréé vide"
    if kind == "PIPELINE":
        if v42:
            return "SCRIPT", "OTHER", ("Définition et permissions exportées (pipelines/pipelines_all.json) ; "
                                       "tables recalculées par full refresh")
        return "NONE", "OTHER", "Définition non sauvegardée en v4.1 ; tables recalculées par full refresh"
    return "NONE", "NA", "Type non pris en charge"


def _ws_parts(path: str) -> list:
    path = path or ""
    if path.startswith("/Workspace/"):
        path = path[len("/Workspace"):]
    return [p for p in path.strip("/").split("/") if p]


def workspace_root_coverage(path: str, version: str = "v4.2") -> tuple:
    parts = _ws_parts(path)
    root = parts[0] if parts else ""
    if root == "Repos":
        return "OTHER", "NA", "Contenu dans Git ; URL, branche et ACL sauvegardées"
    if version == "v4.2":
        return "SCRIPT", "NA", ("Notebooks, fichiers et dashboards exportés (contenu brut, toute profondeur) "
                                "avec leurs permissions")
    if root == "Shared":
        return "SCRIPT", "NONE", (f"Notebooks exportés (sources, {SHARED_EXPORT_DEPTH} niveaux max) ; "
                                  "fichiers workspace (.sh, .yml, .whl…) non sauvegardés en v4.1")
    return "NONE", "NONE", "Non exporté en v4.1 (seul /Shared l'était)"


def pipeline_code_covered(paths: list, has_files: bool = False, version: str = "v4.2") -> bool:
    """Le code source du pipeline est-il dans le périmètre exporté par 05_workspace_config ?
    paths = chemins des notebooks / fichiers du pipeline ; has_files = le pipeline utilise aussi des
    fichiers workspace (.py, .sql, dossier racine). v4.2 exporte tout le workspace (fichiers compris)
    hors /Repos ; v4.1 n'exportait que les notebooks de /Shared, sur 5 niveaux."""
    if not paths:
        return False
    if version == "v4.2":
        return all((_ws_parts(p) or [""])[0] != "Repos" for p in paths)
    if has_files:
        return False
    for p in paths:
        parts = _ws_parts(p)
        if not parts or parts[0] != "Shared" or len(parts) - 1 > SHARED_EXPORT_DEPTH:
            return False
    return True


def verdict(def_cov: str, data_cov: str, excluded: bool = False, accessible=None, last_status=None,
            iac: bool = False) -> str:
    if excluded:
        return "EXCLU"
    if def_cov == "NONE":
        return "NON_COUVERT"
    if accessible is False and "SCRIPT" in (def_cov, data_cov):
        # Le script ne peut pas le lire ; l'IaC recrée la définition, mais pas des données
        if not iac or data_cov == "SCRIPT":
            return "INACCESSIBLE"
        def_cov = "IAC"
    # Un échec ou un « skipped » au clone ne compte que pour des données clonées : une vue envoyée
    # au clone (v4.1) y échouait sans conséquence, sa DDL étant exportée à part
    if last_status in ("error", "skipped") and data_cov == "SCRIPT":
        return "ECHEC"
    if data_cov == "NONE":
        return "PARTIEL"
    if "OTHER" in (def_cov, data_cov):
        return "AUTRE"
    if def_cov == "IAC":
        return "AS_CODE"
    return "OK"
# <<< REGLES

# COMMAND ----------
# MAGIC %md ## 1 — Contexte : objets as code, version et dernier backup

# COMMAND ----------
iac_names = set()
if iac_file:
    try:
        with open(iac_file, encoding="utf-8") as f:
            iac_names = parse_iac_file(f.read())
        print(f"[OK] {len(iac_names)} objet(s) déclaré(s) as code dans {iac_file}")
    except Exception as e:
        warn(f"Fichier as code illisible ({iac_file}) : {_short(e)}")

def _read_text(path: str) -> str:
    return spark.read.text(path, wholetext=True).collect()[0][0]

last_status_by_table, last_backup_date = {}, None
if backup_root:
    try:
        last_backup_date = json.loads(_read_text(f"{backup_root}/latest.json"))["date"]
        print(f"[OK] Dernier backup : {last_backup_date}")
        if backup_version == "auto":
            names = {f.name for f in dbutils.fs.ls(f"{backup_root}/{last_backup_date}/uc_metadata/")}
            backup_version = "v4.2" if "05_volumes.sql" in names else "v4.1"
        manifest = json.loads(_read_text(f"{backup_root}/incremental/_manifests/{last_backup_date}.json"))
        last_status_by_table = {e["table"].lower(): e.get("status") for e in manifest}
        print(f"[OK] Manifest du dernier backup : {len(last_status_by_table)} tables")
    except Exception as e:
        warn(f"Dernier backup non lisible dans {backup_root} : {_short(e)}")

if backup_version == "auto":
    backup_version = "v4.1"
    warn("Version du backup déployée inconnue (backup_root vide ou illisible) : hypothèse prudente v4.1")
print(f"[OK] Règles appliquées : backup {backup_version}")

# COMMAND ----------
# MAGIC %md ## 2 — Inventaire du metastore

# COMMAND ----------
SKIP_CATALOG_TYPES = {"SYSTEM_CATALOG", "INTERNAL_CATALOG"}

try:
    api_catalogs = api_list("/api/2.1/unity-catalog/catalogs", "catalogs", {"max_results": 1000})
except Exception as e:
    api_catalogs = []
    warn(f"Liste des catalogs via l'API impossible : {_short(e)}")

catalogs = {c["name"].lower(): c for c in api_catalogs if c.get("catalog_type") not in SKIP_CATALOG_TYPES}
standard_catalogs = {n for n, c in catalogs.items()
                     if c.get("catalog_type") not in ("FOREIGN_CATALOG", "DELTASHARING_CATALOG")}
print(f"[OK] {len(catalogs)} catalog(s) dont {len(standard_catalogs)} standard(s)")

def _in_scope(cat: str) -> bool:
    return (cat or "").lower() in standard_catalogs

def _query(name: str, query: str) -> list:
    try:
        rows = sql_rows(query)
        print(f"[OK] {name} : {len(rows)}")
        return rows
    except Exception as e:
        warn(f"{name} non listables : {_short(e)}")
        return []

IS = "system.information_schema"
schemas   = [r for r in _query("Schémas", f"SELECT * FROM {IS}.schemata WHERE schema_name <> 'information_schema'")
             if _in_scope(r["catalog_name"])]
tables    = [r for r in _query("Tables / vues", f"SELECT table_catalog, table_schema, table_name, table_type, "
                               f"data_source_format, table_owner, created_by FROM {IS}.tables "
                               f"WHERE table_schema <> 'information_schema'")
             if _in_scope(r["table_catalog"])]
volumes   = [r for r in _query("Volumes", f"SELECT * FROM {IS}.volumes WHERE volume_schema <> 'information_schema'")
             if _in_scope(r["volume_catalog"])]
functions = [r for r in _query("Fonctions", f"SELECT routine_catalog, routine_schema, routine_name, routine_owner, "
                               f"created_by FROM {IS}.routines WHERE routine_schema <> 'information_schema'")
             if _in_scope(r["routine_catalog"])]
protected_tables = {
    (r["table_catalog"].lower(), r["table_schema"].lower(), r["table_name"].lower())
    for view in ("row_filters", "column_masks")
    for r in _query(f"Tables protégées ({view})",
                    f"SELECT DISTINCT table_catalog, table_schema, table_name FROM {IS}.{view}")}
ext_locations = _query("External locations", f"SELECT * FROM {IS}.external_locations")
credentials   = _query("Storage credentials", f"SELECT * FROM {IS}.storage_credentials")
connections   = _query("Connexions", f"SELECT * FROM {IS}.connections")

def _api_objects(label: str, path: str, key: str, params: dict = None) -> list:
    try:
        items = api_list(path, key, params or {"max_results": 1000})
        print(f"[OK] {label} : {len(items)}")
        return items
    except Exception as e:
        warn(f"{label} non listables : {_short(e)}")
        return []

models     = [m for m in _api_objects("Modèles MLflow", "/api/2.1/unity-catalog/models", "registered_models")
              if _in_scope(m.get("catalog_name"))]
shares     = _api_objects("Shares Delta Sharing", "/api/2.1/unity-catalog/shares", "shares")
recipients = _api_objects("Recipients Delta Sharing", "/api/2.1/unity-catalog/recipients", "recipients")

# Metastore Hive legacy : hors Unity Catalog, jamais sauvegardé — signalé s'il contient des tables
hive_tables = 0
try:
    for s in spark.sql("SHOW SCHEMAS IN hive_metastore").collect():
        hive_tables += spark.sql(f"SHOW TABLES IN hive_metastore.`{s[0]}`").count()
except Exception:
    pass

# COMMAND ----------
# MAGIC %md ## 3 — Droits du principal de backup

# COMMAND ----------
access_checked = bool(backup_principal)
cat_p, sch_p, tbl_p, vol_p, fn_p = {}, {}, {}, {}, {}

if access_checked:
    # Les droits accordés à « account users » valent pour tout le monde, donc aussi pour le backup
    grantees = "('" + backup_principal.replace("'", "''") + "', 'account users')"
    def _privs(view: str, keys: tuple) -> dict:
        out = {}
        for r in _query(f"Droits ({view})", f"SELECT * FROM {IS}.{view} WHERE grantee IN {grantees}"):
            k = tuple((r[c] or "").lower() for c in keys)
            out.setdefault(k, set()).update(priv_set([r["privilege_type"]]))
        return out
    cat_p = _privs("catalog_privileges", ("catalog_name",))
    sch_p = _privs("schema_privileges",  ("catalog_name", "schema_name"))
    tbl_p = _privs("table_privileges",   ("table_catalog", "table_schema", "table_name"))
    vol_p = _privs("volume_privileges",  ("volume_catalog", "volume_schema", "volume_name"))
    fn_p  = _privs("routine_privileges", ("routine_catalog", "routine_schema", "routine_name"))
else:
    warn("backup_principal vide : les droits du compte de backup ne sont pas vérifiés")

def access(kind: str, cat: str, sch: str = "", name: str = "", owner: str = ""):
    if not access_checked:
        return None
    if owner and owner.lower() == backup_principal.lower():
        return True
    cat, sch, name = cat.lower(), (sch or "").lower(), (name or "").lower()
    obj = {"TABLE": tbl_p, "VOLUME": vol_p, "FUNCTION": fn_p}.get(kind, {}).get((cat, sch, name), set())
    return can_access(kind, cat_p.get((cat,), set()), sch_p.get((cat, sch), set()), obj)

# COMMAND ----------
# MAGIC %md ## 4 — Inventaire du workspace (jobs, pipelines, dossiers)

# COMMAND ----------
# Tailles de page maximales de ces API (au-delà : HTTP 400, constaté sur l'API pipelines)
jobs = _api_objects("Jobs", "/api/2.1/jobs/list", "jobs", {"limit": 100})

pipelines = []
for p in _api_objects("Pipelines", "/api/2.0/pipelines", "statuses", {"max_results": 100}):
    try:
        pipelines.append(api_get(f"/api/2.0/pipelines/{p['pipeline_id']}"))
    except Exception as e:
        warn(f"Pipeline {p.get('name')} non lisible : {_short(e)}")
        pipelines.append({"pipeline_id": p["pipeline_id"], "spec": {"name": p.get("name")},
                          "creator_user_name": p.get("creator_user_name")})

def _pipeline_sources(spec: dict) -> tuple:
    """(chemins des notebooks, chemins des fichiers / dossiers) du code source du pipeline."""
    notebooks, files = [], []
    for lib in spec.get("libraries", []) or []:
        if lib.get("notebook", {}).get("path"):
            notebooks.append(lib["notebook"]["path"])
        if lib.get("file", {}).get("path"):
            files.append(lib["file"]["path"])
        if lib.get("glob", {}).get("include"):
            files.append(lib["glob"]["include"])
    if spec.get("root_path"):
        files.append(spec["root_path"])
    return notebooks, files

def _ws_list(path: str) -> list:
    try:
        return api_get("/api/2.0/workspace/list", {"path": path}).get("objects", [])
    except Exception:
        return []

def _ws_count(path: str) -> dict:
    """Compte récursivement notebooks / fichiers (sans entrer dans les repos Git)."""
    counts, stack = {"NOTEBOOK": 0, "FILE": 0, "DASHBOARD": 0}, [path]
    while stack:
        for o in _ws_list(stack.pop()):
            t = o.get("object_type")
            if t == "DIRECTORY":
                stack.append(o["path"])
            elif t in counts:
                counts[t] += 1
    return counts

ws_roots = []
for o in _ws_list("/"):
    if o.get("object_type") != "DIRECTORY":
        continue
    if o["path"] == "/Users":     # une ligne par dossier personnel
        ws_roots += [u["path"] for u in _ws_list("/Users") if u.get("object_type") == "DIRECTORY"]
    else:
        ws_roots.append(o["path"])

ws_counts = {}
if count_ws_objects:
    to_count = [p for p in ws_roots if not p.startswith("/Repos")]
    print(f"[INFO] Comptage des objets de {len(to_count)} dossier(s) du workspace…")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        ws_counts = dict(zip(to_count, ex.map(_ws_count, to_count)))
print(f"[OK] {len(jobs)} jobs, {len(pipelines)} pipelines, {len(ws_roots)} dossiers racine du workspace")

# COMMAND ----------
# MAGIC %md ## 5 — Couverture objet par objet

# COMMAND ----------
TYPE_LABELS = {
    "CATALOG": "Catalog", "SCHEMA": "Schéma", "TABLE": "Table / vue", "VOLUME": "Volume",
    "FUNCTION": "Fonction", "MODEL": "Modèle MLflow", "EXTERNAL_LOCATION": "External location",
    "STORAGE_CREDENTIAL": "Storage credential", "CONNECTION": "Connexion", "SHARE": "Share Delta Sharing",
    "RECIPIENT": "Recipient Delta Sharing", "JOB": "Job", "PIPELINE": "Pipeline",
    "WORKSPACE": "Dossier workspace", "HIVE": "Metastore Hive (legacy)",
}
rows = []

def add(domain, kind, name, sub="", fmt="", catalog="", owner="", created_by="", accessible=None, protected=False,
        last_status=None, note="", def_cov=None, data_cov=None, iac_force=None, explanation=None):
    d, data, default_explanation = coverage(kind, sub, fmt, backup_version, protected=protected)
    d, data = def_cov or d, data_cov or data
    explanation = explanation or default_explanation
    iac = iac_force or iac_source(name, created_by, iac_identities, iac_names)
    d = apply_iac(d, iac)
    # Seules les tables à données clonables figurent dans le manifest du clone (pas les vues)
    clonable = kind == "TABLE" and data in ("SCRIPT", "NONE")
    # Le dernier backup réel prime : une table clonée avec succès a ses données couvertes
    if last_status == "success" and data == "NONE":
        data = "SCRIPT"
    v = verdict(d, data, excluded=(catalog or "").lower() in excluded_catalogs,
                accessible=accessible, last_status=last_status, iac=bool(iac))
    if last_status:
        last_label = last_status
    elif clonable and last_status_by_table:
        last_label = "absent"
    else:
        last_label = "—"
    rows.append({
        "Domaine": domain, "Type": TYPE_LABELS.get(kind, kind), "Sous-type": sub or "",
        "Catalog": catalog or "", "Objet": name, "Propriétaire": owner or "", "Créé par": created_by or "",
        "Définition": LABELS_COV[d], "Données": LABELS_COV[data],
        "As code": f"Oui ({iac})" if iac else "Non",
        "Accès backup": "—" if accessible is None else ("Oui" if accessible else "NON"),
        "Dernier backup": last_label,
        "Verdict": f"{VERDICTS[v][0]} {VERDICTS[v][1]}", "_code": v,
        "Explication": explanation + (f" — {note}" if note else ""),
    })

for n, c in sorted(catalogs.items()):
    add("Metastore", "CATALOG", c["name"], sub=c.get("catalog_type", ""), catalog=c["name"],
        owner=c.get("owner"), created_by=c.get("created_by"),
        accessible=access("CATALOG", n) if n in standard_catalogs else None)

for s in schemas:
    add("Metastore", "SCHEMA", f"{s['catalog_name']}.{s['schema_name']}", catalog=s["catalog_name"],
        owner=s["schema_owner"], created_by=s["created_by"],
        accessible=access("SCHEMA", s["catalog_name"], s["schema_name"]))

for t in tables:
    fqn = f"{t['table_catalog']}.{t['table_schema']}.{t['table_name']}"
    add("Metastore", "TABLE", fqn, sub=t["table_type"], fmt=t["data_source_format"], catalog=t["table_catalog"],
        owner=t["table_owner"], created_by=t["created_by"],
        accessible=access("TABLE", t["table_catalog"], t["table_schema"], t["table_name"], t["table_owner"]),
        protected=(t["table_catalog"].lower(), t["table_schema"].lower(), t["table_name"].lower()) in protected_tables,
        last_status=last_status_by_table.get(fqn.lower()))

for v in volumes:
    add("Metastore", "VOLUME", f"{v['volume_catalog']}.{v['volume_schema']}.{v['volume_name']}",
        sub=v["volume_type"], catalog=v["volume_catalog"], owner=v["volume_owner"], created_by=v["created_by"],
        accessible=access("VOLUME", v["volume_catalog"], v["volume_schema"], v["volume_name"], v["volume_owner"]))

for f in functions:
    add("Metastore", "FUNCTION", f"{f['routine_catalog']}.{f['routine_schema']}.{f['routine_name']}",
        catalog=f["routine_catalog"], owner=f["routine_owner"], created_by=f["created_by"],
        accessible=access("FUNCTION", f["routine_catalog"], f["routine_schema"], f["routine_name"], f["routine_owner"]))

for m in models:
    add("Metastore", "MODEL", m.get("full_name", ""), catalog=m.get("catalog_name"),
        owner=m.get("owner"), created_by=m.get("created_by"))

for e in ext_locations:
    add("Metastore", "EXTERNAL_LOCATION", e["external_location_name"], owner=e["external_location_owner"],
        created_by=e["created_by"], note=e.get("url") or "")
for c in credentials:
    add("Metastore", "STORAGE_CREDENTIAL", c["storage_credential_name"], owner=c["storage_credential_owner"],
        created_by=c["created_by"])
for c in connections:
    add("Metastore", "CONNECTION", c["connection_name"], sub=c.get("connection_type", ""),
        owner=c["connection_owner"], created_by=c["created_by"])
for s in shares:
    add("Metastore", "SHARE", s.get("name", ""), owner=s.get("owner"), created_by=s.get("created_by"))
for r in recipients:
    add("Metastore", "RECIPIENT", r.get("name", ""), owner=r.get("owner"), created_by=r.get("created_by"))

if hive_tables:
    add("Metastore", "HIVE", "hive_metastore", def_cov="NONE", data_cov="NONE",
        explanation="Metastore Hive legacy, hors Unity Catalog : exclu du backup",
        note=f"{hive_tables} table(s) hors Unity Catalog, jamais sauvegardées")

for j in jobs:
    s = j.get("settings", {})
    bundle = (s.get("deployment") or {}).get("kind") == "BUNDLE"
    add("Workspace", "JOB", s.get("name", str(j.get("job_id"))), owner=j.get("run_as_user_name"),
        created_by=j.get("creator_user_name"), iac_force="bundle" if bundle else None)

for p in pipelines:
    spec = p.get("spec", {}) or {}
    nb_paths, file_paths = _pipeline_sources(spec)
    paths = nb_paths + file_paths
    bundle = (spec.get("deployment") or {}).get("kind") == "BUNDLE"
    code_ok = pipeline_code_covered(nb_paths + file_paths, has_files=bool(file_paths), version=backup_version)
    add("Workspace", "PIPELINE", spec.get("name") or p.get("name", p.get("pipeline_id", "")),
        catalog=spec.get("catalog", ""), created_by=p.get("creator_user_name"),
        iac_force="bundle" if bundle else None,
        note=("code source dans le périmètre exporté" if code_ok else "code source NON sauvegardé")
             + (f" ({', '.join(paths[:3])})" if paths else ""))

for path in sorted(ws_roots):
    d, data, ws_explanation = workspace_root_coverage(path, backup_version)
    c = ws_counts.get(path)
    note = f"{c['NOTEBOOK']} notebook(s), {c['FILE']} fichier(s), {c['DASHBOARD']} dashboard(s)" if c else ""
    add("Workspace", "WORKSPACE", path, def_cov=d, data_cov=data, note=note, explanation=ws_explanation)

print(f"[OK] {len(rows)} objets évalués")

# COMMAND ----------
# MAGIC %md ## 6 — Rapport HTML + Excel

# COMMAND ----------
# >>> XLSX
# Écriture .xlsx sans dépendance (openpyxl est absent du runtime Databricks, et PyPI peut être
# inaccessible) : un .xlsx est un zip de XML. En-tête en gras, ligne figée, filtres automatiques.
import re as _re
import zipfile
from xml.sax.saxutils import escape as _xml_escape

_XML_INVALID = _re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def _col(n: int) -> str:
    s = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _cell(ref: str, value, style: int = 0) -> str:
    st = f' s="{style}"' if style else ""
    if value is None or value == "":
        return ""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        text = _xml_escape(_XML_INVALID.sub("", str(value)))
        return f'<c r="{ref}" t="inlineStr"{st}><is><t xml:space="preserve">{text}</t></is></c>'
    return f'<c r="{ref}"{st}><v>{value}</v></c>'


def _sheet_xml(headers: list, rows: list) -> str:
    widths = [min(60, max([len(str(h))] + [len(str(r[i])) for r in rows if i < len(r) and r[i] is not None]) + 2)
              for i, h in enumerate(headers)]
    cols = "".join(f'<col min="{i + 1}" max="{i + 1}" width="{w}" customWidth="1"/>' for i, w in enumerate(widths))
    data = [f'<row r="1">{"".join(_cell(f"{_col(i)}1", h, 1) for i, h in enumerate(headers))}</row>']
    for r_idx, row in enumerate(rows, start=2):
        data.append(f'<row r="{r_idx}">{"".join(_cell(f"{_col(i)}{r_idx}", v) for i, v in enumerate(row))}</row>')
    last = f"{_col(len(headers) - 1)}{len(rows) + 1}"
    return ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
            '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
            '<sheetViews><sheetView workbookViewId="0"><pane ySplit="1" topLeftCell="A2" activePane="bottomLeft" '
            'state="frozen"/></sheetView></sheetViews>'
            f'<cols>{cols}</cols><sheetData>{"".join(data)}</sheetData>'
            f'<autoFilter ref="A1:{last}"/></worksheet>')


def write_xlsx(path: str, sheets: dict) -> None:
    """sheets = {nom: (en-têtes, lignes)} ; lignes = listes de valeurs (str, nombre, None)."""
    names = []
    for name in sheets:
        clean = _re.sub(r"[\[\]:*?/\\]", "", name)[:31] or "Feuille"
        names.append(clean)
    ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
    rel = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("[Content_Types].xml",
                   '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                   '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
                   '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
                   '<Default Extension="xml" ContentType="application/xml"/>'
                   '<Override PartName="/xl/workbook.xml" '
                   'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
                   '<Override PartName="/xl/styles.xml" '
                   'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
                   + "".join(f'<Override PartName="/xl/worksheets/sheet{i + 1}.xml" '
                             'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
                             for i in range(len(names)))
                   + '</Types>')
        z.writestr("_rels/.rels",
                   '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   f'<Relationship Id="rId1" Type="{rel}/officeDocument" Target="xl/workbook.xml"/></Relationships>')
        z.writestr("xl/workbook.xml",
                   f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><workbook xmlns="{ns}" xmlns:r="{rel}"><sheets>'
                   + "".join(f'<sheet name="{_xml_escape(n)}" sheetId="{i + 1}" r:id="rId{i + 1}"/>'
                             for i, n in enumerate(names))
                   + "</sheets></workbook>")
        z.writestr("xl/_rels/workbook.xml.rels",
                   '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
                   '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
                   + "".join(f'<Relationship Id="rId{i + 1}" Type="{rel}/worksheet" Target="worksheets/sheet{i + 1}.xml"/>'
                             for i in range(len(names)))
                   + f'<Relationship Id="rId{len(names) + 1}" Type="{rel}/styles" Target="styles.xml"/></Relationships>')
        z.writestr("xl/styles.xml",
                   f'<?xml version="1.0" encoding="UTF-8" standalone="yes"?><styleSheet xmlns="{ns}">'
                   '<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font>'
                   '<font><b/><sz val="11"/><name val="Calibri"/></font></fonts>'
                   '<fills count="2"><fill><patternFill patternType="none"/></fill>'
                   '<fill><patternFill patternType="gray125"/></fill></fills>'
                   '<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>'
                   '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
                   '<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/>'
                   '<xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>'
                   '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
                   '</styleSheet>')
        for i, (headers, rows) in enumerate(sheets.values()):
            z.writestr(f"xl/worksheets/sheet{i + 1}.xml", _sheet_xml(list(headers), [list(r) for r in rows]))
# <<< XLSX

# COMMAND ----------
import pandas as pd

df = pd.DataFrame(rows)
order = sorted(VERDICTS, key=lambda k: VERDICTS[k][3])
summary = (df.pivot_table(index="Type", columns="_code", values="Objet", aggfunc="count", fill_value=0)
             .reindex(columns=[c for c in order if c in set(df["_code"])], fill_value=0))
summary["Total"] = summary.sum(axis=1)
by_catalog = (df[df["Catalog"] != ""]
              .pivot_table(index="Catalog", columns="_code", values="Objet", aggfunc="count", fill_value=0)
              .reindex(columns=[c for c in order if c in set(df["_code"])], fill_value=0))
to_treat = df[df["_code"].isin(TO_TREAT)].sort_values(["_code", "Type", "Objet"])
totals = df["_code"].value_counts().to_dict()

def e(x) -> str:
    return html.escape(str(x))

def _bar(counts: dict) -> str:
    total = sum(counts.values()) or 1
    return "".join(
        f'<span title="{e(VERDICTS[k][1])} : {counts[k]}" style="width:{100 * counts[k] / total:.2f}%;'
        f'background:{VERDICTS[k][2]}"></span>' for k in order if counts.get(k))

def _table(frame, index_name: str) -> str:
    cols = [c for c in frame.columns if c != "Total"]
    head = "".join(f"<th>{VERDICTS[c][0]} {e(VERDICTS[c][1])}</th>" for c in cols)
    body = ""
    for idx, r in frame.iterrows():
        counts = {c: int(r[c]) for c in cols}
        cells = "".join(f"<td class=n>{v or ''}</td>" for v in counts.values())
        body += (f"<tr><td>{e(idx)}</td><td class=n><b>{sum(counts.values())}</b></td>{cells}"
                 f"<td><div class=bar>{_bar(counts)}</div></td></tr>")
    return (f"<table><thead><tr><th>{index_name}</th><th>Total</th>{head}<th></th></tr></thead>"
            f"<tbody>{body}</tbody></table>")

attention = []
if backup_version == "v4.1":
    attention.append("La version déployée est la <b>v4.1</b> : volumes, fonctions, metric views et pipelines ne "
                     "sont pas sauvegardés, les jobs le sont sans leurs tâches et seul /Shared est exporté. "
                     "La v4.2 couvre ces objets.")
if access_checked and missing_system_access(cat_p.get(("system",), set())):
    attention.append(f"Le compte de backup <code>{e(backup_principal)}</code> n'a pas USE CATALOG sur le catalog "
                     "<b>system</b> : information_schema lui est illisible, le backup lit alors le type des tables "
                     "via l'API (plus lent). <code>GRANT USE CATALOG ON CATALOG system TO …</code>")
if protected_tables:
    attention.append(f"<b>{len(protected_tables)}</b> table(s) protégée(s) par un filtre de lignes ou un masque de "
                     "colonnes : DEEP CLONE les refuse, leurs données ne sont pas sauvegardées.")
if totals.get("INACCESSIBLE"):
    attention.append(f"<b>{totals['INACCESSIBLE']}</b> objet(s) seraient sauvegardés par le script mais le compte "
                     f"de backup <code>{e(backup_principal)}</code> n'a pas les droits nécessaires "
                     "(USE CATALOG, USE SCHEMA, SELECT / READ VOLUME / EXECUTE).")
if not access_checked:
    attention.append("Les droits du compte de backup n'ont pas été vérifiés (paramètre <code>backup_principal</code> vide).")
attention.append("Les permissions (GRANT) sont sauvegardées pour les catalogs, schémas et tables"
                 + (", volumes et fonctions" if backup_version == "v4.2" else "")
                 + " ; leur export complet exige le droit MANAGE (ou la propriété) pour le compte de backup.")
attention.append("Rétention : restauration des tables à n'importe quel jour de la rétention quotidienne, "
                 "plus un snapshot mensuel.")
attention += [e(w) for w in warnings]

CSS = """
body{font-family:Segoe UI,Helvetica,Arial,sans-serif;color:#1f2328;background:#fff;margin:32px;max-width:1200px}
h1{font-size:26px;margin:0 0 4px} h2{font-size:19px;margin:32px 0 10px;border-bottom:1px solid #d0d7de;padding-bottom:6px}
.meta{color:#59636e;font-size:13px} .kpis{display:flex;flex-wrap:wrap;gap:10px;margin:18px 0}
.kpi{border:1px solid #d0d7de;border-radius:8px;padding:10px 14px;min-width:150px}
.kpi b{display:block;font-size:24px} .kpi span{font-size:12px;color:#59636e}
table{border-collapse:collapse;width:100%;font-size:13px} th,td{border-bottom:1px solid #eaeef2;padding:6px 8px;text-align:left;vertical-align:top}
th{background:#f6f8fa;font-weight:600} td.n{text-align:right;font-variant-numeric:tabular-nums}
.bar{display:flex;width:160px;height:10px;border-radius:5px;overflow:hidden;background:#eaeef2}
.bar span{display:block;height:100%} ul{padding-left:20px} li{margin:4px 0}
.legend td:first-child{white-space:nowrap} code{background:#f6f8fa;padding:1px 4px;border-radius:4px}
"""

legend_rows = "".join(f"<tr><td>{v[0]} <b>{e(v[1])}</b></td><td>{e(t)}</td></tr>" for v, t in [
    (VERDICTS["OK"], "Définition et données recréées par le script de backup DR."),
    (VERDICTS["AS_CODE"], "Recréé par l'infrastructure as code (Terraform, bundle) : identité créatrice ou liste fournie."),
    (VERDICTS["AUTRE"], "Données hors Databricks (base fédérée, stockage d'un volume externe), dans Git, ou recalculées."),
    (VERDICTS["PARTIEL"], "Définition sauvegardée mais pas les données (ex. table dans un format non clonable)."),
    (VERDICTS["INACCESSIBLE"], "Le script le sauvegarderait, mais le compte de backup n'a pas les droits."),
    (VERDICTS["ECHEC"], "Prévu par le script mais en erreur lors du dernier backup."),
    (VERDICTS["NON_COUVERT"], "Aucun moyen de restauration identifié."),
    (VERDICTS["EXCLU"], "Catalog exclu volontairement du périmètre."),
])

kpis = "".join(f'<div class=kpi style="border-left:4px solid {VERDICTS[k][2]}"><b>{totals[k]}</b>'
               f'<span>{VERDICTS[k][0]} {e(VERDICTS[k][1])}</span></div>' for k in order if totals.get(k))

TREAT_MAX = 500
treat_rows = "".join(
    f"<tr><td>{e(r['Verdict'])}</td><td>{e(r['Type'])}</td><td>{e(r['Objet'])}</td><td>{e(r['Explication'])}</td></tr>"
    for _, r in to_treat.head(TREAT_MAX).iterrows())
treat_more = (f"<p class=meta>… et {len(to_treat) - TREAT_MAX} autres : voir l'onglet « À traiter » du fichier Excel.</p>"
              if len(to_treat) > TREAT_MAX else "")

report_html = f"""<!doctype html><html lang=fr><head><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>Couverture du backup DR</title><style>{CSS}</style></head><body>
<h1>Couverture du backup DR — Databricks</h1>
<div class=meta>Workspace {e(HOST)} · généré le {run_at:%d.%m.%Y %H:%M} par {e(current_user)} ·
règles du backup <b>{e(backup_version)}</b>{f" · dernier backup {e(last_backup_date)}" if last_backup_date else ""}
{f" · compte de backup <code>{e(backup_principal)}</code>" if backup_principal else ""}</div>
<div class=kpis><div class=kpi><b>{len(df)}</b><span>objets inventoriés</span></div>{kpis}</div>
<h2>Synthèse par type d'objet</h2>{_table(summary, "Type d'objet")}
<h2>Synthèse par catalog</h2>{_table(by_catalog, "Catalog") if len(by_catalog) else "<p class=meta>Aucun catalog.</p>"}
<h2>Points d'attention</h2><ul>{"".join(f"<li>{a}</li>" for a in attention)}</ul>
<h2>À traiter ({len(to_treat)})</h2>
<table><thead><tr><th>Verdict</th><th>Type</th><th>Objet</th><th>Explication</th></tr></thead><tbody>{treat_rows}</tbody></table>
{treat_more}
<h2>Légende</h2><table class=legend><tbody>{legend_rows}</tbody></table>
<p class=meta>Le détail objet par objet (définition, données, as code, accès du compte de backup, dernier backup)
figure dans le fichier Excel joint.</p>
</body></html>"""

os.makedirs(output_dir, exist_ok=True)
stamp = run_at.strftime("%Y%m%d_%H%M")
html_path = f"{output_dir}/couverture_backup_{stamp}.html"
with open(html_path, "w", encoding="utf-8") as f:
    f.write(report_html)
print(f"[OK] Rapport HTML : {html_path}")

def _sheet(frame) -> tuple:
    """(en-têtes, lignes) en types Python natifs (tolist convertit les entiers numpy)."""
    frame = frame.astype(object).where(pd.notna(frame), None)
    return [str(c) for c in frame.columns], frame.values.tolist()

synthese = summary.rename(columns=lambda c: c if c == "Total" else f"{VERDICTS[c][0]} {VERDICTS[c][1]}") \
                  .rename_axis("Type").reset_index()
xlsx_path = f"{output_dir}/couverture_backup_{stamp}.xlsx"
write_xlsx(xlsx_path, {
    "Synthèse":  _sheet(synthese),
    "Détail":    _sheet(df.drop(columns=["_code"])),
    "À traiter": _sheet(to_treat.drop(columns=["_code"])),
})
print(f"[OK] Détail Excel : {xlsx_path}")

print("Télécharger : Workspace → " + output_dir.replace("/Workspace", "") + " → ⋮ → Download")
displayHTML(report_html)
