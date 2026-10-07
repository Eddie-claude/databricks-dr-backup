# Databricks notebook source
# notebooks/01_uc_metadata.py

# COMMAND ----------
# MAGIC %md # 01 — Export Unity Catalog Metadata

# COMMAND ----------
import json
import sys
from datetime import date
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# dbutils.fs.put/head bypasse UC External Locations et exige une clé ABFS cluster.
# Ces helpers passent par Spark (UC credentials) pour écrire/lire sur ADLS.
def _uc_put(path: str, content: str, overwrite: bool = True) -> None:
    tmp = path + ".__tmp__"
    try: dbutils.fs.rm(tmp, recurse=True)
    except: pass
    spark.createDataFrame([(line,) for line in content.split("\n")], "value STRING") \
        .coalesce(1).write.mode("overwrite").text(tmp)
    parts = [f.path for f in dbutils.fs.ls(tmp)
             if not f.name.startswith("_") and not f.name.startswith(".")]
    if overwrite:
        try: dbutils.fs.rm(path)
        except: pass
    dbutils.fs.mv(parts[0], path)
    dbutils.fs.rm(tmp, recurse=True)

def _uc_head(path: str) -> str:
    # spark.read.text(path) peut lire 0 octet sur ce chemin même juste après une réécriture
    # complète dans la même session (observé en prod, confirmé via le panneau performance —
    # "Bytes read: 0 B") — dbutils.fs.head est plus fiable ici.
    return dbutils.fs.head(path, 10_000_000)

def _short(e) -> str:
    """Première ligne de l'erreur : les exceptions Spark embarquent une stacktrace JVM de
    centaines de lignes qui fait dépasser la limite de sortie du notebook (sortie tronquée)."""
    return " ".join(str(e).split("JVM stacktrace:")[0].split())[:300]

# COMMAND ----------
# DBTITLE 1, Paramètres
dbutils.widgets.text("backup_root", "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date", str(date.today()), "Date backup YYYY-MM-DD")
# lib/ est déployé par le bundle à côté de notebooks/ : …/files/notebooks/x → …/files/lib
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_path = _nb_path if _nb_path.startswith("/Workspace") else "/Workspace" + _nb_path
dbutils.widgets.text("lib_path", _nb_path.rsplit("/notebooks/", 1)[0] + "/lib", "Chemin vers lib/")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")
lib_path = dbutils.widgets.get("lib_path")

sys.path.insert(0, lib_path)
# Étape critique du backup : un lib/ pas encore redéployé ne doit pas la faire échouer,
# seul l'export volumes/fonctions est alors sauté (signalé dans le résultat).
try:
    from uc_ddl import build_catalog_ddl, build_function_ddl, build_volume_ddl, qualify_create_name
    uc_objects_error = None
except ImportError as e:
    uc_objects_error = f"lib/uc_ddl.py introuvable dans {lib_path} : {e}"
    print(f"[WARN] {uc_objects_error} — volumes et fonctions NON sauvegardés")
    # Formes historiques, sans emplacement ni catalogue dans le nom des vues
    build_catalog_ddl = lambda name, storage_root, comment: f"CREATE CATALOG IF NOT EXISTS `{name}`;"
    qualify_create_name = lambda ddl, catalog, schema, name: ddl

assert backup_root.startswith("abfss://"), "backup_root doit commencer par abfss://"

output_path = f"{backup_root}/{backup_date}/uc_metadata"

# COMMAND ----------
# DBTITLE 1, Export catalogs

# information_schema = schéma système UC (vues uniquement, non cloneable)
EXCLUDED_SCHEMAS = {"information_schema"}

# Catalogs sans données propres à sauvegarder : fédérés (Lakehouse Federation, données dans la
# base source), Delta Sharing (données chez le fournisseur), système et internes Databricks.
# Leur définition (connexion, partage) relève de l'IaC : un CREATE CATALOG simple les recréerait
# en catalog standard vide. Le type n'est pas dans information_schema, on le lit via l'API UC.
NON_BACKUP_CATALOG_TYPES = {"FOREIGN_CATALOG", "DELTASHARING_CATALOG", "SYSTEM_CATALOG", "INTERNAL_CATALOG"}

def _split_backup_catalogs(names: list, types: dict) -> tuple:
    """(catalogs à sauvegarder, {catalog exclu: type}) ; un type inconnu ou absent est gardé."""
    skipped = {n: types[n] for n in names if types.get(n) in NON_BACKUP_CATALOG_TYPES}
    return [n for n in names if n not in skipped], skipped

def _catalog_infos() -> dict:
    """{nom: définition du catalogue} via l'API Unity Catalog ; {} si elle est indisponible.
    Donne le type (absent d'information_schema) et l'emplacement géré (storage_root)."""
    import requests
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    url = f"{ctx.apiUrl().get()}/api/2.1/unity-catalog/catalogs"
    headers = {"Authorization": f"Bearer {ctx.apiToken().get()}"}
    infos, params = {}, {"max_results": 1000}
    try:
        while True:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            d = r.json()
            infos.update({c["name"]: c for c in d.get("catalogs", [])})
            if not d.get("next_page_token"):
                return infos
            params["page_token"] = d["next_page_token"]
    except Exception as e:
        print(f"[WARN] Types de catalog indisponibles (API Unity Catalog), catalogs fédérés / "
              f"Delta Sharing non filtrés : {str(e)[:200]}")
        return infos

_visible = [r.catalog for r in spark.sql("SHOW CATALOGS").collect() if r.catalog not in ("hive_metastore", "system", "samples")]
_infos = _catalog_infos()
catalogs, skipped_catalogs = _split_backup_catalogs(
    _visible, {n: i.get("catalog_type", "") for n, i in _infos.items()})
for _c, _t in sorted(skipped_catalogs.items()):
    print(f"[SKIP] Catalog {_c} ({_t}) non sauvegardé : définition à gérer en IaC")
# Avec MANAGED LOCATION : sans lui, la recréation échoue sur un metastore sans stockage racine
catalog_ddl = "\n".join(build_catalog_ddl(c, _infos.get(c, {}).get("storage_root"),
                                           _infos.get(c, {}).get("comment")) for c in catalogs)

_uc_put(f"{output_path}/01_catalogs.sql", catalog_ddl)
print(f"[01_catalogs] {len(catalogs)} catalogs exportés : {catalogs}")

# COMMAND ----------
# DBTITLE 1, Export schemas + tables (avec cache schemas)

schema_ddls = []
table_ddls = []
table_names = []

for catalog in catalogs:
    schemas = [
        r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{catalog}`").collect()
        if r.databaseName not in EXCLUDED_SCHEMAS
    ]
    for schema in schemas:
        schema_ddls.append(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`;")

        # Récupérer uniquement les tables cloneables (MANAGED/EXTERNAL, pas les vues)
        try:
            cloneable_tables = {
                r.table_name for r in spark.sql(f"""
                    SELECT table_name FROM `{catalog}`.information_schema.tables
                    WHERE table_schema = '{schema}'
                    AND table_type IN ('MANAGED', 'EXTERNAL')
                """).collect()
            }
        except Exception as e:
            print(f"[WARN] Fallback table_type pour {catalog}.{schema}: {_short(e)}")
            cloneable_tables = None  # inclure tout, les vues seront filtrées dans 02_data_clone

        tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
        for t in tables:
            fqn = f"`{catalog}`.`{schema}`.`{t.tableName}`"
            try:
                ddl_row = spark.sql(f"SHOW CREATE TABLE {fqn}").collect()[0][0]
                table_ddls.append(qualify_create_name(ddl_row, catalog, schema, t.tableName) + ";")
                # Ajouter à la liste de clone uniquement si c'est une table réelle
                if cloneable_tables is None or t.tableName in cloneable_tables:
                    table_names.append(f"{catalog}.{schema}.{t.tableName}")
                else:
                    print(f"[SKIP] Vue ignorée pour le clone : {catalog}.{schema}.{t.tableName}")
            except Exception as e:
                print(f"[WARN] Impossible d'exporter {fqn}: {_short(e)}")

_uc_put(f"{output_path}/02_schemas.sql", "\n".join(schema_ddls))
print(f"[02_schemas] {len(schema_ddls)} schemas exportés")

_uc_put(f"{output_path}/03_tables.sql", "\n\n".join(table_ddls))
print(f"[03_tables] {len(table_ddls)} tables exportées")

# COMMAND ----------
# DBTITLE 1, Export volumes + fonctions

# Pas de SHOW CREATE pour ces objets : les DDL sont reconstruits depuis information_schema
# (une requête par catalog et par vue, pas par schéma).
# Seules les métadonnées sont sauvegardées : le contenu des volumes MANAGED (fichiers)
# n'est pas copié ; celui des volumes EXTERNAL reste sur leur stockage d'origine.

def _rows(sql: str) -> list:
    return [r.asDict() for r in spark.sql(sql).collect()]

volume_ddls, volume_fqns = [], []
function_ddls, function_fqns = [], []

for catalog in (catalogs if uc_objects_error is None else []):
    excluded = ", ".join(f"'{s}'" for s in EXCLUDED_SCHEMAS)
    try:
        for v in _rows(f"""
            SELECT * FROM `{catalog}`.information_schema.volumes
            WHERE volume_schema NOT IN ({excluded})
            ORDER BY volume_schema, volume_name
        """):
            volume_ddls.append(build_volume_ddl(v) + ";")
            volume_fqns.append(f"`{catalog}`.`{v['volume_schema']}`.`{v['volume_name']}`")
    except Exception as e:
        print(f"[WARN] Volumes {catalog}: {_short(e)}")

    try:
        routines = _rows(f"""
            SELECT * FROM `{catalog}`.information_schema.routines
            WHERE routine_schema NOT IN ({excluded})
            ORDER BY routine_schema, routine_name
        """)
        params, columns = {}, {}
        if routines:
            for p in _rows(f"SELECT * FROM `{catalog}`.information_schema.parameters"):
                params.setdefault((p["specific_schema"], p["specific_name"]), []).append(p)
            for c in _rows(f"SELECT * FROM `{catalog}`.information_schema.routine_columns"):
                columns.setdefault((c["specific_schema"], c["specific_name"]), []).append(c)
        for r in routines:
            key = (r["specific_schema"], r["specific_name"])
            fqn = f"`{catalog}`.`{r['routine_schema']}`.`{r['routine_name']}`"
            ddl = build_function_ddl(r, params.get(key, []), columns.get(key))
            if ddl is None:
                print(f"[WARN] Corps illisible (droits ?), fonction non exportée : {fqn}")
                continue
            function_ddls.append(ddl + ";")
            function_fqns.append(fqn)
    except Exception as e:
        print(f"[WARN] Fonctions {catalog}: {_short(e)}")

# Fichiers absents (plutôt que vides) si l'export a été sauté : un fichier vide
# laisserait croire au restore qu'il n'y avait aucun volume ni fonction.
if uc_objects_error is None:
    _uc_put(f"{output_path}/05_volumes.sql", "\n\n".join(volume_ddls))
    print(f"[05_volumes] {len(volume_ddls)} volumes exportés")

    _uc_put(f"{output_path}/06_functions.sql", "\n\n".join(function_ddls))
    print(f"[06_functions] {len(function_ddls)} fonctions exportées")

# COMMAND ----------
# DBTITLE 1, Export grants (catalogs, schemas, tables, volumes, fonctions)

grant_statements = []

# >>> GRANTS DIRECTS
def _is_direct_grant(g, fqn: str) -> bool:
    """SHOW GRANTS ON <objet> renvoie aussi les droits hérités du catalog / schéma (ObjectKey du
    parent). Réécrits « ON <objet> », ils deviendraient des grants explicites à la restauration :
    un droit retiré plus tard au niveau du catalog subsisterait sur chaque objet. Seuls les
    grants posés sur l'objet lui-même sont exportés."""
    key = getattr(g, "ObjectKey", None)
    if key is None:
        return True
    return key.replace("`", "").lower() == fqn.replace("`", "").lower()
# <<< GRANTS DIRECTS

for catalog in catalogs:
    # Grants catalog
    try:
        for g in spark.sql(f"SHOW GRANTS ON CATALOG `{catalog}`").collect():
            if _is_direct_grant(g, f"`{catalog}`"):
                grant_statements.append(f"GRANT {g.ActionType} ON CATALOG `{catalog}` TO `{g.Principal}`;")
    except Exception as e:
        print(f"[WARN] Grants catalog {catalog}: {_short(e)}")

    schemas = [ddl.split("`")[3] for ddl in schema_ddls if ddl.startswith(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`")]
    for schema in schemas:
        # Grants schema
        try:
            for g in spark.sql(f"SHOW GRANTS ON SCHEMA `{catalog}`.`{schema}`").collect():
                if _is_direct_grant(g, f"`{catalog}`.`{schema}`"):
                    grant_statements.append(f"GRANT {g.ActionType} ON SCHEMA `{catalog}`.`{schema}` TO `{g.Principal}`;")
        except Exception as e:
            print(f"[WARN] Grants schema {catalog}.{schema}: {_short(e)}")

        # Grants tables
        tables = spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`").collect()
        for t in tables:
            fqn_plain = f"{catalog}.{schema}.{t.tableName}"
            fqn = f"`{catalog}`.`{schema}`.`{t.tableName}`"
            try:
                for g in spark.sql(f"SHOW GRANTS ON TABLE {fqn}").collect():
                    if _is_direct_grant(g, fqn):
                        grant_statements.append(f"GRANT {g.ActionType} ON TABLE {fqn} TO `{g.Principal}`;")
            except Exception as e:
                print(f"[WARN] Grants table {fqn_plain}: {_short(e)}")

# Grants volumes + fonctions
for securable, fqns in (("VOLUME", volume_fqns), ("FUNCTION", function_fqns)):
    for fqn in fqns:
        try:
            for g in spark.sql(f"SHOW GRANTS ON {securable} {fqn}").collect():
                if _is_direct_grant(g, fqn):
                    grant_statements.append(f"GRANT {g.ActionType} ON {securable} {fqn} TO `{g.Principal}`;")
        except Exception as e:
            print(f"[WARN] Grants {securable.lower()} {fqn}: {_short(e)}")

_uc_put(f"{output_path}/04_grants.sql", "\n".join(grant_statements))
print(f"[04_grants] {len(grant_statements)} grants exportés")

# COMMAND ----------
# DBTITLE 1, Retourner le manifest partiel

result = {
    "catalogs": catalogs,
    "skipped_catalogs": skipped_catalogs,
    "table_names": table_names,
    "grant_count": len(grant_statements),
    "volume_count": len(volume_ddls),
    "function_count": len(function_ddls),
    "uc_objects_error": uc_objects_error,
}
dbutils.notebook.exit(json.dumps(result))
