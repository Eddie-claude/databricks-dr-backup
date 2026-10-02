# Databricks notebook source
# notebooks/diag_01_audit.py
#
# AUDIT PRÉ-BACKUP — À lancer AVANT le premier backup
# Scanne tout Unity Catalog et produit :
#   - Inventaire tables (taille, fichiers, format)
#   - Tables nécessitant OPTIMIZE (petits fichiers → backup lent)
#   - Estimation durée backup J1 et J2+ (incrémental)
#   - Estimation coût stockage mensuel
#   - Contenu des volumes managés (fichiers, taille, changements sur 24 h)
# Aucune écriture, aucune modification — lecture seule.

# COMMAND ----------
# MAGIC %md # Audit Pré-Backup — Unity Catalog
# MAGIC
# MAGIC **Lecture seule — aucune modification de données.**
# MAGIC
# MAGIC | Paramètre | Description |
# MAGIC |-----------|-------------|
# MAGIC | `max_parallel` | Threads parallèles pour DESCRIBE DETAIL (8 recommandé) |
# MAGIC | `optimize_files_thresh` | Seuil fichiers/table au-delà duquel OPTIMIZE est recommandé |
# MAGIC | `output_json` | Afficher le JSON complet en fin de notebook (debug) |
# MAGIC | `measure_volumes` | Mesurer le contenu des volumes managés (section 5, `true` par défaut) |

# COMMAND ----------
import concurrent.futures
import json
from datetime import datetime
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
dbutils.widgets.text("max_parallel",           "8",     "Threads parallèles pour DESCRIBE DETAIL")
dbutils.widgets.text("optimize_files_per_gb",  "50",   "Seuil fichiers/GB → OPTIMIZE recommandé")
dbutils.widgets.text("optimize_files_min",     "100",  "Nb fichiers minimum pour déclencher le flag (évite les faux positifs sur tables vides)")
dbutils.widgets.text("output_json",            "false", "Afficher JSON complet (true/false)")

max_parallel          = max(1, int(dbutils.widgets.get("max_parallel")))
optimize_files_per_gb = float(dbutils.widgets.get("optimize_files_per_gb"))
optimize_files_min    = int(dbutils.widgets.get("optimize_files_min"))
output_json           = dbutils.widgets.get("output_json").lower() == "true"

EXCLUDED_CATALOGS = {"hive_metastore", "system", "samples", "__databricks_internal"}
EXCLUDED_SCHEMAS  = {"information_schema"}

# Catalogs visibles mais non lisibles par l'identité courante (USE CATALOG manquant…)
inaccessible_catalogs = set()

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

def _short(e) -> str:
    """Première ligne de l'erreur : les exceptions Spark embarquent une stacktrace JVM de
    centaines de lignes qui fait dépasser la limite de sortie du notebook (sortie tronquée)."""
    return " ".join(str(e).split("JVM stacktrace:")[0].split())[:300]

print(f"[OK] max_parallel={max_parallel} | seuil OPTIMIZE={optimize_files_per_gb} fichiers/GB (min {optimize_files_min} fichiers)")
print(f"[OK] Démarré à {datetime.now().strftime('%H:%M:%S')}")

# COMMAND ----------
# MAGIC %md ## 1 — Découverte des catalogs et tables

# COMMAND ----------
_visible = [
    r.catalog for r in spark.sql("SHOW CATALOGS").collect()
    if r.catalog not in EXCLUDED_CATALOGS
]
catalogs, skipped_catalogs = _split_backup_catalogs(_visible, _catalog_types())
if skipped_catalogs:
    print(f"[INFO] {len(skipped_catalogs)} catalog(s) fédéré(s) / Delta Sharing ignoré(s) "
          f"(non sauvegardés, voir section 6)")
print(f"[OK] {len(catalogs)} catalog(s) à analyser : {catalogs}")

table_fqns = []
for cat in catalogs:
    try:
        schemas = [
            r.databaseName for r in spark.sql(f"SHOW SCHEMAS IN `{cat}`").collect()
            if r.databaseName not in EXCLUDED_SCHEMAS
        ]
    except Exception as e:
        inaccessible_catalogs.add(cat)
        print(f"[WARN] Impossible de lister les schemas de {cat}: {_short(e)}")
        continue

    for sch in schemas:
        try:
            rows = spark.sql(f"""
                SELECT table_name, table_type
                FROM `{cat}`.information_schema.tables
                WHERE table_schema = '{sch}'
                AND table_type IN ('MANAGED', 'EXTERNAL')
            """).collect()
            table_fqns += [(cat, sch, r.table_name, r.table_type) for r in rows]
        except Exception as e:
            try:
                rows = spark.sql(f"SHOW TABLES IN `{cat}`.`{sch}`").collect()
                table_fqns += [(cat, sch, r.tableName, "UNKNOWN") for r in rows]
            except Exception as e2:
                inaccessible_catalogs.add(cat)
                print(f"[WARN] {cat}.{sch}: {_short(e2)}")

print(f"[OK] {len(table_fqns)} tables découvertes")

# COMMAND ----------
# MAGIC %md ## 2 — DESCRIBE DETAIL (taille + fichiers par table)

# COMMAND ----------
def describe_table(args: tuple) -> dict:
    cat, sch, tbl, tbl_type = args
    fqn = f"`{cat}`.`{sch}`.`{tbl}`"
    try:
        d = spark.sql(f"DESCRIBE DETAIL {fqn}").collect()[0].asDict()
        size_bytes = d.get("sizeInBytes") or 0
        num_files  = d.get("numFiles")    or 0
        return {
            "catalog":    cat,
            "schema":     sch,
            "table":      tbl,
            "fqn":        f"{cat}.{sch}.{tbl}",
            "table_type": tbl_type,
            "format":     d.get("format", "unknown"),
            "size_gb":    round(size_bytes / 1073741824, 4),
            "num_files":  num_files,
            "files_per_gb": round(num_files / max(size_bytes / 1073741824, 0.001), 1),
            "location":   d.get("location", ""),
            "status":     "ok",
        }
    except Exception as e:
        return {
            "catalog": cat, "schema": sch, "table": tbl,
            "fqn": f"{cat}.{sch}.{tbl}",
            "table_type": tbl_type, "format": "unknown",
            "size_gb": 0, "num_files": 0, "files_per_gb": 0,
            "location": "", "status": "error", "error": str(e),
        }

print(f"[INFO] Analyse de {len(table_fqns)} tables avec {max_parallel} threads...")
with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
    results = list(ex.map(describe_table, table_fqns))

ok        = [r for r in results if r["status"] == "ok"]
errors    = [r for r in results if r["status"] == "error"]
delta_ok  = [r for r in ok if r["format"] == "delta"]
non_delta = [r for r in ok if r["format"] != "delta"]

print(f"[OK] Analyse terminée — {len(ok)} succès, {len(errors)} erreurs")

# COMMAND ----------
# MAGIC %md ## 3 — Calcul des métriques et recommandations

# COMMAND ----------
total_size_gb  = sum(r["size_gb"]  for r in ok)
total_files    = sum(r["num_files"] for r in ok)
delta_size_gb  = sum(r["size_gb"]  for r in delta_ok)
delta_files    = sum(r["num_files"] for r in delta_ok)

# Tables nécessitant OPTIMIZE
# Double condition : ratio fichiers/GB élevé ET nombre minimum absolu
# → fonctionne aussi bien pour les petites que les très grosses tables
optimize_needed = [
    r for r in delta_ok
    if r["files_per_gb"] > optimize_files_per_gb and r["num_files"] >= optimize_files_min
]
optimize_size_gb = sum(r["size_gb"]  for r in optimize_needed)
optimize_files   = sum(r["num_files"] for r in optimize_needed)

# Durée estimée (base : 209 MB/s mesuré après OPTIMIZE sur ADLS Gen2 Switzerland North)
SPEED_MBPS = 209

# J1 sans OPTIMIZE : les tables à fort nombre de fichiers seront très lentes
# (les petits fichiers = overhead S3/ADLS massif → vitesse effective ~5-20 MB/s)
j1_no_opt_h = (
    (delta_size_gb - optimize_size_gb) * 1024 / SPEED_MBPS +   # tables OK
    optimize_size_gb * 1024 / 15                                 # tables small-files ~15 MB/s
) / 3600

# J1 après OPTIMIZE : tout tourne à 209 MB/s
j1_opt_h = (delta_size_gb * 1024 / SPEED_MBPS) / 3600

# J2+ incrémental : 2-5% de changement quotidien estimé
j2_low_h  = j1_opt_h * 0.02
j2_high_h = j1_opt_h * 0.05

# Estimation coût stockage (ADLS Gen2 Switzerland North LRS ~0.023 $/GB/mois)
COST_PER_GB = 0.023
# Incrémental : base + 15j de deltas (2% changement/j) + 1 monthly (17 TB)
storage_incremental_gb = (
    total_size_gb +                          # incremental/ base
    total_size_gb * 0.02 * 15 +             # deltas daily (15j × 2%)
    total_size_gb                            # monthly archive
)
cost_incremental = storage_incremental_gb * COST_PER_GB

# COMMAND ----------
# MAGIC %md ## 4 — Rapport

# COMMAND ----------
print(f"""
╔══════════════════════════════════════════════════════════════════╗
║            AUDIT PRÉ-BACKUP — UNITY CATALOG                    ║
╠══════════════════════════════════════════════════════════════════╣
║  INVENTAIRE                                                     ║
║    Catalogs analysés       : {len(catalogs):<5}                           ║
║    Tables totales          : {len(table_fqns):<5}  ({len(ok)} OK / {len(errors)} erreurs)    ║
║    Tables Delta (backup)   : {len(delta_ok):<5}                           ║
║    Tables non-Delta        : {len(non_delta):<5}  (non cloneables — ignorées)   ║
╠══════════════════════════════════════════════════════════════════╣
║  VOLUME                                                         ║
║    Total toutes tables     : {total_size_gb:>10.2f} GB                    ║
║    Total tables Delta      : {delta_size_gb:>10.2f} GB                    ║
║    Nombre de fichiers total : {total_files:>10,}                   ║
╠══════════════════════════════════════════════════════════════════╣
║  OPTIMIZE — PETITS FICHIERS (seuil : >{optimize_files_per_gb} fichiers/GB et >={optimize_files_min} fichiers)
║    Tables à optimiser      : {len(optimize_needed):<5}                           ║
║    Volume concerné         : {optimize_size_gb:>10.2f} GB                    ║
║    Fichiers à compacter    : {optimize_files:>10,}                   ║
╠══════════════════════════════════════════════════════════════════╣
║  DURÉE ESTIMÉE (ADLS Gen2 CH-North, 4 workers, parallel=8)     ║
║    J1 SANS OPTIMIZE        : {j1_no_opt_h:>7.1f} h (small-files ~15 MB/s)  ║
║    J1 APRÈS OPTIMIZE       : {j1_opt_h:>7.1f} h (cible 209 MB/s)       ║
║    J2+ (incrémental)       : {j2_low_h:.1f} - {j2_high_h:.1f} h (2-5% changement/j)  ║
╠══════════════════════════════════════════════════════════════════╣
║  COÛT STOCKAGE ESTIMÉ (stratégie incrémentale)                 ║
║    Stockage total (base+deltas+monthly) : {storage_incremental_gb:>8.0f} GB           ║
║    Coût mensuel estimé      : ${cost_incremental:>8.0f} /mois (ADLS LRS)  ║
╚══════════════════════════════════════════════════════════════════╝
""")

# ── Top 30 tables par taille ──────────────────────────────────────────────────
top_tables = sorted(ok, key=lambda r: r["size_gb"], reverse=True)[:30]
print(f"\n{'─'*90}")
print(f"{'TOP 30 TABLES PAR TAILLE':^90}")
print(f"{'─'*90}")
print(f"{'Catalog.Schema.Table':<52} {'Format':<8} {'GB':>8} {'Fichiers':>10} {'Fichiers/GB':>12} {'OPTIMIZE':>9}")
print(f"{'─'*90}")
for r in top_tables:
    flag = "⚠ OUI" if (r["files_per_gb"] > optimize_files_per_gb and r["num_files"] >= optimize_files_min) else ""
    print(f"{r['fqn']:<52} {r['format']:<8} {r['size_gb']:>8.2f} {r['num_files']:>10,} {r['files_per_gb']:>12.1f} {flag:>9}")

# ── Tables nécessitant OPTIMIZE ───────────────────────────────────────────────
if optimize_needed:
    print(f"\n{'─'*80}")
    print(f"{'TABLES NÉCESSITANT OPTIMIZE (>' + str(optimize_files_per_gb) + ' fichiers/GB, >=' + str(optimize_files_min) + ' fichiers)':^80}")
    print(f"{'─'*80}")
    print(f"{'Catalog.Schema.Table':<52} {'GB':>8} {'Fichiers':>10} {'Fichiers/GB':>10}")
    print(f"{'─'*80}")
    for r in sorted(optimize_needed, key=lambda x: x["num_files"], reverse=True):
        print(f"{r['fqn']:<52} {r['size_gb']:>8.2f} {r['num_files']:>10,} {r['files_per_gb']:>10.1f}")
    print(f"\n  → Commande à exécuter sur chaque table avant le backup initial :")
    print(f"     OPTIMIZE <catalog>.<schema>.<table>;")
    print(f"     ALTER TABLE <catalog>.<schema>.<table>")
    print(f"       SET TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true',")
    print(f"                          'delta.autoOptimize.autoCompact'   = 'true');")

# ── Erreurs ───────────────────────────────────────────────────────────────────
if errors:
    print(f"\n[WARN] {len(errors)} tables inaccessibles (DESCRIBE DETAIL échoué) :")
    for r in errors:
        print(f"  - {r['fqn']}: {r.get('error', '')[:100]}")

# ── JSON complet (optionnel) ──────────────────────────────────────────────────
if output_json:
    print("\n[JSON complet — toutes les tables]")
    print(json.dumps(sorted(results, key=lambda r: r.get("size_gb", 0), reverse=True), indent=2))

# COMMAND ----------
# MAGIC %md ## 5 — Volumes managés (contenu à sauvegarder)
# MAGIC
# MAGIC Liste récursivement les fichiers de chaque volume **MANAGED** pour dimensionner leur sauvegarde :
# MAGIC nombre de fichiers, taille, taille moyenne, fichiers modifiés sur les dernières 24 h.
# MAGIC Les volumes EXTERNAL sont seulement comptés (leurs fichiers restent sur leur propre stockage).
# MAGIC
# MAGIC Lecture seule (`dbutils.fs.ls`). Sur de gros volumes le listing peut prendre plusieurs minutes :
# MAGIC sa durée (`listing_s`) sert justement à estimer celle du backup. `measure_volumes = false` saute la section.

# COMMAND ----------
dbutils.widgets.text("measure_volumes", "true", "Mesurer le contenu des volumes managés (true/false)")
measure_volumes = dbutils.widgets.get("measure_volumes").lower() == "true"

volume_results = []
external_volumes = []

def list_volume(args: tuple) -> dict:
    cat, sch, vol = args
    since_ms = (datetime.now().timestamp() - 24 * 3600) * 1000
    files = size = recent = recent_size = unreadable = 0
    dirs, t0 = [f"/Volumes/{cat}/{sch}/{vol}/"], datetime.now()
    while dirs:
        d = dirs.pop()
        try:
            entries = dbutils.fs.ls(d)
        except Exception:
            unreadable += 1
            continue
        for f in entries:
            if f.isDir():
                dirs.append(f.path)
            else:
                files += 1
                size  += f.size
                if f.modificationTime >= since_ms:
                    recent      += 1
                    recent_size += f.size
    return {
        "fqn":            f"{cat}.{sch}.{vol}",
        "files":          files,
        "size_gb":        round(size / 1073741824, 3),
        "avg_mb":         round(size / max(files, 1) / 1048576, 2),
        "recent_files":   recent,
        "recent_gb":      round(recent_size / 1073741824, 3),
        "unreadable_dirs": unreadable,
        "listing_s":      int((datetime.now() - t0).total_seconds()),
    }

if measure_volumes:
    managed = []
    for cat in catalogs:
        try:
            for r in spark.sql(f"""
                SELECT volume_schema, volume_name, volume_type
                FROM `{cat}`.information_schema.volumes
            """).collect():
                if r.volume_schema in EXCLUDED_SCHEMAS:
                    continue
                if r.volume_type == "MANAGED":
                    managed.append((cat, r.volume_schema, r.volume_name))
                else:
                    external_volumes.append(f"{cat}.{r.volume_schema}.{r.volume_name}")
        except Exception as e:
            inaccessible_catalogs.add(cat)
            print(f"[WARN] Volumes de {cat} non listables : {_short(e)}")

    print(f"[INFO] {len(managed)} volume(s) managé(s), {len(external_volumes)} externe(s) — listing avec {max_parallel} threads...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
        volume_results = list(ex.map(list_volume, managed))

    vol_files   = sum(r["files"]        for r in volume_results)
    vol_size_gb = sum(r["size_gb"]      for r in volume_results)
    vol_rec_f   = sum(r["recent_files"] for r in volume_results)
    vol_rec_gb  = sum(r["recent_gb"]    for r in volume_results)
    vol_unread  = sum(r["unreadable_dirs"] for r in volume_results)
    # Stockage backup estimé : contenu + 30 jours de changements au rythme des dernières 24 h
    vol_storage_gb = vol_size_gb + 30 * vol_rec_gb

    print(f"""
╔══════════════════════════════════════════════════════════════════╗
║            VOLUMES MANAGÉS — CONTENU                            ║
╠══════════════════════════════════════════════════════════════════╣
║    Volumes managés         : {len(volume_results):<5}                           ║
║    Volumes externes        : {len(external_volumes):<5}  (non concernés)              ║
║    Fichiers                : {vol_files:>12,}                     ║
║    Taille totale           : {vol_size_gb:>10.2f} GB                    ║
║    Modifiés sur 24 h       : {vol_rec_f:>12,} fichiers / {vol_rec_gb:.2f} GB ║
║    Dossiers illisibles     : {vol_unread:<5}  (droits — à signaler)         ║
║    Stockage backup estimé  : {vol_storage_gb:>10.2f} GB (contenu + 30 j)     ║
╚══════════════════════════════════════════════════════════════════╝
""")

    if volume_results:
        display(spark.createDataFrame(
            [(r["fqn"], r["files"], r["size_gb"], r["avg_mb"], r["recent_files"],
              r["recent_gb"], r["unreadable_dirs"], r["listing_s"]) for r in volume_results],
            "volume STRING, fichiers LONG, taille_gb DOUBLE, taille_moy_mb DOUBLE, "
            "modifies_24h LONG, modifies_24h_gb DOUBLE, dossiers_illisibles LONG, listing_s LONG",
        ).orderBy("taille_gb", ascending=False))

    if output_json:
        print("\n[JSON complet — volumes managés]")
        print(json.dumps(volume_results, indent=2))
else:
    print("[INFO] Mesure des volumes désactivée (measure_volumes=false)")

# COMMAND ----------
# MAGIC %md ## 6 — Catalogs non couverts (fédérés, Delta Sharing, inaccessibles)

# COMMAND ----------
# Ces catalogs n'ont été ni audités ni mesurés. Lancé avec l'identité du job de backup,
# cela signifie qu'ils ne seraient pas non plus sauvegardés.
if inaccessible_catalogs:
    print(f"[WARN] {len(inaccessible_catalogs)} catalog(s) inaccessible(s) pour l'identité courante "
          f"(droit USE CATALOG / USE SCHEMA manquant) :")
    for c in sorted(inaccessible_catalogs):
        print(f"  - {c}")
else:
    print("[OK] Tous les catalogs ont pu être lus")

# Pas une anomalie : leurs données ne sont pas dans Databricks, leur définition relève de l'IaC.
if skipped_catalogs:
    print(f"[INFO] {len(skipped_catalogs)} catalog(s) hors périmètre du backup (données externes) :")
    for c, t in sorted(skipped_catalogs.items()):
        print(f"  - {c}  ({t})")

print(f"[OK] Terminé à {datetime.now().strftime('%H:%M:%S')}")
