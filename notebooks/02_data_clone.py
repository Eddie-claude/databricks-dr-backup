# Databricks notebook source
# notebooks/02_data_clone.py

# COMMAND ----------
# MAGIC %md # 02 — Data Clone (DEEP CLONE + checkpoint + resume + parallel)

# COMMAND ----------
import concurrent.futures
import json
import threading
import time
from datetime import datetime, timezone
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# dbutils.fs.put/head bypasse UC External Locations et exige une clé ABFS cluster.
# Ces helpers passent par Spark (UC credentials) pour écrire/lire sur ADLS.
def _uc_put(path: str, content: str) -> None:
    tmp = path + ".__tmp__"
    try: dbutils.fs.rm(tmp, recurse=True)
    except: pass
    spark.createDataFrame([(line,) for line in content.split("\n")], "value STRING") \
        .coalesce(1).write.mode("overwrite").text(tmp)
    parts = [f.path for f in dbutils.fs.ls(tmp)
             if not f.name.startswith("_") and not f.name.startswith(".")]
    try: dbutils.fs.rm(path)
    except: pass
    dbutils.fs.mv(parts[0], path)
    dbutils.fs.rm(tmp, recurse=True)

def _uc_head(path: str) -> str:
    # spark.read.text(path) a montré un cache de listing par chemin qui survit à une réécriture
    # complète du fichier dans la même session (même après spark.catalog.refreshByPath) — utilise
    # dbutils.fs.head à la place (déjà utilisé avec succès ailleurs dans ce projet, ex:
    # 00_orchestrator.py, sans ce problème de cache).
    return dbutils.fs.head(path, 10_000_000)

# COMMAND ----------
dbutils.widgets.text("backup_root",        "",               "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",        str(__import__('datetime').date.today()), "Date backup YYYY-MM-DD")
dbutils.widgets.text("uc_metadata_result", "{}",             "JSON result from 01_uc_metadata")
dbutils.widgets.text("resume",             "true",           "Reprendre depuis checkpoint (true/false)")
dbutils.widgets.text("max_parallel",       "8",              "Clones simultanés (1 = séquentiel) — aligné sur spark.master local[*, 8]")
dbutils.widgets.text("retain_daily",       "30",             "Rétention quotidienne (jours) — Delta log")

backup_root        = dbutils.widgets.get("backup_root")
backup_date        = dbutils.widgets.get("backup_date")
uc_result          = json.loads(dbutils.widgets.get("uc_metadata_result"))
resume_mode        = dbutils.widgets.get("resume").lower() == "true"
max_parallel       = max(1, int(dbutils.widgets.get("max_parallel")))
retain_daily       = max(1, int(dbutils.widgets.get("retain_daily")))
# Fichiers supprimés conservés assez longtemps pour couvrir la fenêtre de restauration point-in-time.
# Plus de terme "weekly" : le snapshot weekly (SHALLOW CLONE) est abandonné — il n'est de toute
# façon plus supporté par Unity Catalog sur des tables non-MANAGED (CANNOT_SHALLOW_CLONE_...).
max_file_retention = retain_daily + 2

# Schémas UC système : vues uniquement, non cloneables par DEEP CLONE
_EXCLUDED_SCHEMAS = {"information_schema"}

# Catalogs sans données propres à sauvegarder : fédérés (Lakehouse Federation, données dans la
# base source), Delta Sharing (données chez le fournisseur), système et internes Databricks.
# Leur définition (connexion, partage) relève de l'IaC : un CREATE CATALOG simple les recréerait
# en catalog standard vide. Le type n'est pas dans information_schema, on le lit via l'API UC.
NON_BACKUP_CATALOG_TYPES = {"FOREIGN_CATALOG", "DELTASHARING_CATALOG", "SYSTEM_CATALOG", "INTERNAL_CATALOG"}

def _split_backup_catalogs(names: list, types: dict) -> tuple:
    """(catalogs à sauvegarder, {catalog exclu: type}) ; un type inconnu ou absent est gardé."""
    skipped = {n: types[n] for n in names if types.get(n) in NON_BACKUP_CATALOG_TYPES}
    return [n for n in names if n not in skipped], skipped

def _catalog_types() -> dict:
    """{nom: catalog_type} via l'API Unity Catalog ; {} si elle est indisponible."""
    import requests
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    url = f"{ctx.apiUrl().get()}/api/2.1/unity-catalog/catalogs"
    headers = {"Authorization": f"Bearer {ctx.apiToken().get()}"}
    types, params = {}, {"max_results": 1000}
    try:
        while True:
            r = requests.get(url, headers=headers, params=params, timeout=30)
            r.raise_for_status()
            d = r.json()
            types.update({c["name"]: c.get("catalog_type", "") for c in d.get("catalogs", [])})
            if not d.get("next_page_token"):
                return types
            params["page_token"] = d["next_page_token"]
    except Exception as e:
        print(f"[WARN] Types de catalog indisponibles (API Unity Catalog), catalogs fédérés / "
              f"Delta Sharing non filtrés : {str(e)[:200]}")
        return types

table_names = [
    t for t in uc_result.get("table_names", [])
    if len(t.split(".")) == 3 and t.split(".")[1] not in _EXCLUDED_SCHEMAS
]

# Fallback : si lancé sans orchestrateur, auto-découverte depuis Unity Catalog
if not table_names:
    print("[INFO] uc_metadata_result vide — auto-découverte des tables depuis Unity Catalog")
    _EXCLUDED_CATALOGS = {"hive_metastore", "system", "samples"}
    _visible = [r.catalog for r in spark.sql("SHOW CATALOGS").collect()
                if r.catalog not in _EXCLUDED_CATALOGS]
    _catalogs, _skipped = _split_backup_catalogs(_visible, _catalog_types())
    for _c, _t in sorted(_skipped.items()):
        print(f"[SKIP] Catalog {_c} ({_t}) non sauvegardé")
    for _cat in _catalogs:
        for _sch in [r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{_cat}`").collect()
                     if r.databaseName not in _EXCLUDED_SCHEMAS]:
            try:
                _tables = {
                    r.table_name for r in spark.sql(f"""
                        SELECT table_name FROM `{_cat}`.information_schema.tables
                        WHERE table_schema = '{_sch}'
                        AND table_type IN ('MANAGED', 'EXTERNAL')
                    """).collect()
                }
                table_names += [f"{_cat}.{_sch}.{t}" for t in sorted(_tables)]
            except Exception as _e:
                print(f"[WARN] {_cat}.{_sch}: {_e}")
    print(f"[INFO] {len(table_names)} tables découvertes automatiquement")

data_backup_root    = f"{backup_root}/incremental"
clone_manifest_path = f"{backup_root}/incremental/_manifests/{backup_date}.json"
checkpoint_path     = f"{backup_root}/incremental/_checkpoints/{backup_date}.json"
# Fichier persistant (pas daté) qui retient la dernière version Delta source clonée avec succès
# pour chaque table — permet de sauter le DEEP CLONE (coûteux même sans changement, puisqu'il doit
# quand même énumérer les fichiers source/dest pour le constater) quand rien n'a changé depuis hier.
last_versions_path = f"{backup_root}/incremental/_last_versions.json"

print(f"[INFO] max_parallel={max_parallel} | tables={len(table_names)} | resume={resume_mode}")

# COMMAND ----------
# DBTITLE 1, Checkpoint (lecture + écriture thread-safe)

# >>> REGLES CLONE
def should_flush(pending: int, last_flush: float, now: float, every: int = 50, max_age_s: float = 120) -> bool:
    """Écrire le checkpoint toutes les 50 tables ou toutes les 2 minutes.
    Toutes les 5 tables, le JSON complet (de plus en plus gros) était réécrit ~900 fois par run,
    un job Spark à chaque fois, sur le driver. Après un crash, au pire 50 tables sont refaites :
    sans conséquence, le DEEP CLONE (CREATE OR REPLACE) est idempotent."""
    return pending >= every or (pending > 0 and now - last_flush >= max_age_s)


def _short(e) -> str:
    """Message d'erreur sans la stacktrace JVM, borné : il est stocké dans le checkpoint et le manifest."""
    return " ".join(str(e).split("JVM stacktrace:")[0].split())[:2000]


NON_CLONABLE_REASON = ("non clonable : données NON sauvegardées (format non Delta, filtre de lignes "
                       "ou masque de colonnes, vue)")


def clone_failure(err: str) -> dict:
    """Statut d'un DEEP CLONE refusé. Une source non clonable est « skipped » (limite connue, pas
    une panne) mais garde son message : une table à filtre de lignes arrivait ici avec la raison
    « vue ou format non cloneable », sans que rien ne dise que ses données manquaient."""
    if "DELTA_CLONE_UNSUPPORTED_SOURCE" in err or "format is View" in err:
        return {"status": "skipped", "reason": NON_CLONABLE_REASON, "error": err}
    return {"status": "error", "error": err}


def source_version_key(version, commit_timestamp) -> str:
    """Identité de l'état source : version ET horodatage du commit. La version seule ne suffit
    pas : DROP + CREATE la remet à 0, et au même numéro qu'au dernier backup le clone était
    sauté — le backup gardait alors les données de l'ancienne table."""
    return f"{version}@{commit_timestamp}"


# Erreurs où la copie existante ne peut plus recevoir de clone incrémental : il faut repartir
# d'une copie neuve (sinon la table échoue tous les jours, cas DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE
# quand la source passe en column mapping « name »).
FULL_RECLONE_ERRORS = ("DELTA_UNSUPPORTED_COLUMN_MAPPING_MODE_CHANGE",)


def needs_full_reclone(err: str) -> bool:
    return any(code in err for code in FULL_RECLONE_ERRORS)


def reclone_archive_path(backup_root: str, backup_date: str, fqn: str) -> str:
    """Où déplacer l'ancienne copie avant un clone complet : son historique reste consultable
    (time travel manuel), hors de l'arborescence lue par 07_restore (dossiers « _ » ignorés)."""
    catalog, schema, table = fqn.split(".")
    return f"{backup_root}/incremental/_reclone_archive/{backup_date}/{catalog}/{schema}/{table}"
# <<< REGLES CLONE

_checkpoint_lock    = threading.Lock()
_checkpoint_pending = 0
_checkpoint_last    = time.time()

def load_checkpoint():
    try:
        return json.loads(_uc_head(checkpoint_path))
    except Exception:
        return {}

# Chaque table touchée (DESCRIBE HISTORY + DEEP CLONE) reste dans le cache des logs Delta de la
# session, avec un RDD d'état persisté jamais libéré. Mesuré sur dev (200 tables) : 1 RDD de plus
# par table et une mémoire du driver qui croît linéairement (751 → 1 373 Mo) ; en vidant ce cache
# régulièrement : 0 RDD et une mémoire stable (~500 Mo). Sur ~4 300 tables, la JVM du driver
# saturait (GC overhead limit exceeded, dans StorageStatus.addBlock). Chaque table n'étant traitée
# qu'une fois, vider ce cache est sans effet sur le résultat : au pire un clone en cours relit son log.
RELEASE_EVERY = 50
_processed    = 0
_release_off  = False
_DELTALOG_CLASSES = ("com.databricks.sql.transaction.tahoe.DeltaLog$", "org.apache.spark.sql.delta.DeltaLog$")

def release_cached_state() -> None:
    global _release_off
    if _release_off:
        return
    jvm = spark._jvm
    for cls in _DELTALOG_CLASSES:
        try:
            getattr(jvm.java.lang.Class.forName(cls).getField("MODULE$"), "get")(None).clearCache()
            return
        except Exception:
            continue
    try:   # repli : libérer au moins les RDD d'état persistés
        for rdd in spark.sparkContext._jsc.getPersistentRDDs().values():
            rdd.unpersist(False)   # non bloquant
    except Exception as e:     # ex. mode d'accès sans SparkContext : désactiver, sans bloquer le backup
        _release_off = True
        print(f"[WARN] Libération du cache Delta indisponible : {_short(e)[:200]}")

def save_checkpoint(fqn: str, entry: dict):
    """Enregistre le résultat d'une table et écrit périodiquement sur ADLS.
    L'affectation se fait SOUS le verrou : modifiée hors verrou pendant qu'un autre thread
    sérialise le dict, elle provoquait « dictionary changed size during iteration »."""
    global _checkpoint_pending, _checkpoint_last, _processed
    with _checkpoint_lock:
        already_done[fqn] = entry
        _checkpoint_pending += 1
        _processed += 1
        if should_flush(_checkpoint_pending, _checkpoint_last, time.time()):
            _uc_put(checkpoint_path, json.dumps(already_done))
            _checkpoint_pending, _checkpoint_last = 0, time.time()
        if _processed % RELEASE_EVERY == 0:
            release_cached_state()

def flush_checkpoint():
    with _checkpoint_lock:
        _uc_put(checkpoint_path, json.dumps(already_done))

already_done: dict = {}
if resume_mode:
    already_done = load_checkpoint()
    if already_done:
        print(f"[RESUME] {len(already_done)} tables déjà clonées trouvées dans le checkpoint")

# COMMAND ----------
# DBTITLE 1, Versions Delta connues (skip si source inchangée depuis le dernier clone réussi)

_last_versions_lock    = threading.Lock()
_last_versions_pending = 0
_last_versions_last    = time.time()

def load_last_versions() -> dict:
    try:
        return json.loads(_uc_head(last_versions_path))
    except Exception as e:
        print(f"[WARN] load_last_versions({last_versions_path}) a échoué : {e}")
        return {}

def _write_last_versions(versions: dict) -> None:
    """Écriture ADLS. L'appelant doit déjà détenir _last_versions_lock
    (threading.Lock n'est pas réentrant)."""
    _uc_put(last_versions_path, json.dumps(versions))

def record_last_version(fqn: str, version) -> None:
    """Mémorise la version source clonée et écrit périodiquement sur ADLS.

    Le flush périodique est indispensable : le checkpoint est indexé par date
    (_checkpoints/{backup_date}.json), donc un job tué par le timeout ne reprend
    que si on le relance le même jour. Sans écriture intermédiaire ici, la seule
    autre voie de reprise — le skip par version — serait vide elle aussi le
    lendemain, puisque l'écriture finale n'est jamais atteinte. Le run suivant
    reclonerait alors l'intégralité du périmètre.
    """
    global _last_versions_pending, _last_versions_last
    with _last_versions_lock:
        new_last_versions[fqn] = version
        _last_versions_pending += 1
        if should_flush(_last_versions_pending, _last_versions_last, time.time()):
            _write_last_versions(new_last_versions)
            _last_versions_pending, _last_versions_last = 0, time.time()

def flush_last_versions() -> None:
    """Écriture finale — garantit que les tables du dernier lot incomplet sont retenues."""
    with _last_versions_lock:
        _write_last_versions(new_last_versions)

def get_source_version(catalog: str, schema: str, table: str):
    """Dernière version Delta commitée sur la table source, ou None si indisponible
    (vue, table non-Delta, etc.) — dans ce cas le clone se fait normalement, sans skip."""
    try:
        row = spark.sql(f"DESCRIBE HISTORY `{catalog}`.`{schema}`.`{table}` LIMIT 1").collect()[0]
        return source_version_key(row["version"], row["timestamp"])
    except Exception as e:
        print(f"  [WARN] get_source_version({catalog}.{schema}.{table}) a échoué : {_short(e)[:300]}")
        return None

last_versions     = load_last_versions()
new_last_versions = dict(last_versions)
print(f"[INFO] {len(last_versions)} version(s) source connue(s) depuis le dernier run")

# COMMAND ----------
# DBTITLE 1, DEEP CLONE (parallèle)

pending_tables = [t for t in table_names if t not in already_done]
total          = len(table_names)
pending_total  = len(pending_tables)

print(f"[INFO] {total} tables au total — {pending_total} à traiter")

def clone_one(args: tuple) -> dict:
    idx, fqn = args
    catalog, schema, table = fqn.split(".")
    dest = f"{data_backup_root}/{catalog}/{schema}/{table}"
    ts   = datetime.now(timezone.utc).strftime("%H:%M:%S")
    t0   = time.time()

    # Si la version Delta de la source n'a pas bougé depuis le dernier clone réussi,
    # rien n'a changé (versions Delta strictement monotones et exhaustives) — on saute
    # le DEEP CLONE, qui coûte cher même sans changement (il doit énumérer les fichiers
    # source/dest pour le constater). Aucun risque de rater un changement : si la version
    # diffère ne serait-ce que d'une unité, le clone se fait normalement ci-dessous.
    source_version = get_source_version(catalog, schema, table)
    last_version   = last_versions.get(fqn)

    if source_version is not None and last_version is not None and source_version == last_version:
        elapsed = time.time() - t0
        entry = {
            "table":             fqn,
            "status":            "success",
            "size_gb":           0,
            "num_files":         0,
            "duration_s":        round(elapsed, 1),
            "incremental":       True,
            "skipped_unchanged": True,
            "source_version":    source_version,
        }
        print(f"[{ts}] ({idx}/{pending_total}) → {fqn} — [SKIP] version Delta inchangée ({source_version})")
        save_checkpoint(fqn, entry)
        record_last_version(fqn, source_version)
        return entry

    print(f"[{ts}] ({idx}/{pending_total}) → {fqn}")

    try:
        clone_sql = f"CREATE OR REPLACE TABLE delta.`{dest}` DEEP CLONE `{catalog}`.`{schema}`.`{table}`"
        archived_to = None
        try:
            result = spark.sql(clone_sql)
        except Exception as e_clone:
            if not needs_full_reclone(_short(e_clone)):
                raise
            archived_to = reclone_archive_path(backup_root, backup_date, fqn)
            print(f"  [WARN] {fqn} : la copie existante n'accepte plus le clone incrémental "
                  f"({_short(e_clone)[:120]}) — archivée dans {archived_to}, clone complet")
            dbutils.fs.mv(dest, archived_to, recurse=True)
            result = spark.sql(clone_sql)
        metrics = result.collect()[0].asDict()
        elapsed = time.time() - t0
        # Octets réellement copiés. Tester `is not None` et non la valeur : avec une chaîne de
        # `or`, un `copied_files_size` à 0 (clone incrémental n'ayant rien copié) était traité
        # comme absent et retombait sur `source_table_size`, soit la taille TOTALE de la table.
        # Le volume rapporté additionnait alors des tailles complètes de tables inchangées et
        # faisait paraître l'incrémental bien plus coûteux qu'il ne l'est.
        copied_bytes = 0
        for _metric in ("copied_files_size", "num_output_bytes", "source_table_size"):
            if metrics.get(_metric) is not None:
                copied_bytes = int(metrics[_metric])
                break
        size_gb   = copied_bytes / (1024**3)
        num_files = metrics.get("num_copied_files", 0)
        entry = {
            "table":        fqn,
            "status":       "success",
            "size_gb":      round(size_gb, 4),
            "num_files":    num_files,
            "duration_s":   round(elapsed, 1),
            "incremental":  num_files == 0,
            "source_version": source_version,
        }
        if archived_to:
            entry["full_reclone_archived_to"] = archived_to
        suffix = " (aucun changement)" if num_files == 0 else f" — {size_gb:.2f} GB ({num_files} fichiers)"
        print(f"  ✓ {fqn}{suffix} en {elapsed:.0f}s")
        try:
            # Delta exige logRetentionDuration >= deletedFileRetentionDuration (le log des
            # transactions doit couvrir au moins la même fenêtre que les fichiers eux-mêmes) —
            # sinon ALTER TABLE échoue avec UNSUPPORTED_TABLE_CHANGE. log_retention doit donc
            # être >= max_file_retention, jamais l'inverse.
            spark.sql(f"""
              ALTER TABLE delta.`{dest}` SET TBLPROPERTIES (
                'delta.logRetentionDuration'         = 'interval {max_file_retention + 1} days',
                'delta.deletedFileRetentionDuration' = 'interval {max_file_retention} days'
              )
            """)
        except Exception as _e:
            print(f"  [WARN] Propriétés Delta non définies sur {dest}: {_short(_e)[:300]}")

        if source_version is not None:
            record_last_version(fqn, source_version)

    except Exception as e:
        elapsed = time.time() - t0
        err_str = _short(e)
        entry = {"table": fqn, **clone_failure(err_str), "duration_s": round(elapsed, 1)}
        if entry["status"] == "skipped":
            print(f"  [SKIP] {fqn} — {NON_CLONABLE_REASON} — {err_str[:300]}")
        else:
            print(f"  ✗ ERREUR {fqn} — {err_str[:500]}")

    save_checkpoint(fqn, entry)
    return entry

args_list = [(i + 1, fqn) for i, fqn in enumerate(pending_tables)]

with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as executor:
    # Consommer l'itérateur fait remonter ici l'exception d'un thread (sinon avalée en silence)
    for _ in executor.map(clone_one, args_list):
        pass

flush_checkpoint()  # garantit l'écriture finale
flush_last_versions()

# Résultats complets (checkpoint précédent + nouveau run)
clone_results = list(already_done.values())
total_size_gb = sum(r.get("size_gb", 0) for r in clone_results)

# COMMAND ----------
# DBTITLE 1, Manifest final

_uc_put(clone_manifest_path, json.dumps(clone_results))

success_count  = len([r for r in clone_results if r["status"] == "success"])
skip_count     = len([r for r in clone_results if r["status"] == "skipped"])
error_count    = len([r for r in clone_results if r["status"] == "error"])
version_skip_count = len([r for r in clone_results if r.get("skipped_unchanged")])

print(f"""
╔══════════════════════════════════════════╗
║         DATA CLONE — RÉSUMÉ             ║
╠══════════════════════════════════════════╣
║  Succès          : {success_count:<21} ║
║    dont sans DEEP CLONE (version inchangée) : {version_skip_count:<3} ║
║  Non clonables   : {skip_count:<21} ║
║  Erreurs         : {error_count:<21} ║
║  Volume          : {total_size_gb:<17.2f} GB ║
╚══════════════════════════════════════════╝
""")

for status, title in (("error", "Tables en erreur"), ("skipped", "Tables non clonables (données NON sauvegardées)")):
    rows = [r for r in clone_results if r["status"] == status]
    if rows:
        print(f"[{title}]")
        for r in rows:
            print(f"  - {r['table']}: {r.get('error', '?')[:120]}")

# COMMAND ----------
# Sans la liste complète des résultats (~4 300 entrées) : l'orchestrateur ne lit que
# total_size_gb, et le détail est dans le manifest écrit ci-dessus.
dbutils.notebook.exit(json.dumps({
    "total_size_gb":      round(total_size_gb, 3),
    "success_count":      success_count,
    "skip_count":         skip_count,
    "error_count":        error_count,
    "version_skip_count": version_skip_count,
}))
