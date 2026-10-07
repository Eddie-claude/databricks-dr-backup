# Databricks notebook source
# notebooks/14_volume_files.py

# COMMAND ----------
# MAGIC %md # 14 — Sauvegarde des fichiers des volumes managés
# MAGIC
# MAGIC Les définitions des volumes sont exportées par `01_uc_metadata` ; ce notebook sauvegarde leur
# MAGIC **contenu** (volumes MANAGED uniquement : les fichiers d'un volume EXTERNAL restent sur son stockage).
# MAGIC
# MAGIC | Élément | Emplacement |
# MAGIC |---------|-------------|
# MAGIC | Versions des fichiers | `volumes/files/{catalog}/{schema}/{volume}/{date de copie}/{chemin}` |
# MAGIC | Index (état complet de chaque volume, jour par jour) | `volumes/_index` (Delta, partition `snapshot_date`) |
# MAGIC
# MAGIC Seuls les fichiers nouveaux ou modifiés (taille ou date de modification) sont copiés ; un
# MAGIC fichier inchangé pointe vers sa version déjà stockée. Restauration possible à n'importe quel
# MAGIC jour de la rétention quotidienne, plus un état par mois (`15_restore_volume_files`).
# MAGIC
# MAGIC **Reprise** : relancé avec la même `backup_date`, un fichier déjà copié (même taille) est sauté.

# COMMAND ----------
import concurrent.futures
import json
import sys
import time
from datetime import date

import requests
from pyspark.sql import functions as F

dbutils.widgets.text("backup_root",    "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",    str(date.today()), "Date backup YYYY-MM-DD")
# lib/ est déployé par le bundle à côté de notebooks/ : …/files/notebooks/x → …/files/lib
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_path = _nb_path if _nb_path.startswith("/Workspace") else "/Workspace" + _nb_path
dbutils.widgets.text("lib_path", _nb_path.rsplit("/notebooks/", 1)[0] + "/lib", "Chemin vers lib/")
dbutils.widgets.text("retain_daily",   "30", "Rétention quotidienne (jours)")
dbutils.widgets.text("retain_monthly", "1",  "Rétention mensuelle (mois)")
dbutils.widgets.text("max_parallel",   "8",  "Copies simultanées")
dbutils.widgets.text("dry_run",        "false", "Rétention en simulation (true/false)")

backup_root    = dbutils.widgets.get("backup_root").rstrip("/")
backup_date    = dbutils.widgets.get("backup_date").strip() or str(date.today())
retain_daily   = max(1, int(dbutils.widgets.get("retain_daily")))
retain_monthly = max(0, int(dbutils.widgets.get("retain_monthly")))
max_parallel   = max(1, int(dbutils.widgets.get("max_parallel")))
dry_run        = dbutils.widgets.get("dry_run").lower() == "true"
assert backup_root.startswith("abfss://"), "backup_root doit commencer par abfss://"

sys.path.insert(0, dbutils.widgets.get("lib_path"))
from volume_backup import backup_file_path, plan_copy, safe_rel_path, select_kept_dates

INDEX = f"{backup_root}/volumes/_index"
FILES = f"{backup_root}/volumes/files"

def _short(e) -> str:
    return " ".join(str(e).split("JVM stacktrace:")[0].split())[:300]

# COMMAND ----------
# MAGIC %md ## 1 — Volumes managés à sauvegarder

# COMMAND ----------
_ctx  = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
_HOST = _ctx.apiUrl().get()
_HDRS = {"Authorization": f"Bearer {_ctx.apiToken().get()}"}

def _standard_catalogs() -> set:
    """Catalogs standards (fédérés, Delta Sharing, système exclus, comme dans 01_uc_metadata)."""
    out, params = set(), {"max_results": 1000}
    while True:
        r = requests.get(f"{_HOST}/api/2.1/unity-catalog/catalogs", headers=_HDRS, params=params, timeout=60)
        r.raise_for_status()
        d = r.json()
        out |= {c["name"] for c in d.get("catalogs", [])
                if c.get("catalog_type") not in ("FOREIGN_CATALOG", "DELTASHARING_CATALOG",
                                                  "SYSTEM_CATALOG", "INTERNAL_CATALOG")}
        if not d.get("next_page_token"):
            return out - {"hive_metastore", "samples", "system"}
        params["page_token"] = d["next_page_token"]

catalogs = _standard_catalogs()
volumes = [(r.volume_catalog, r.volume_schema, r.volume_name) for r in spark.sql("""
    SELECT volume_catalog, volume_schema, volume_name FROM system.information_schema.volumes
    WHERE volume_type = 'MANAGED' AND volume_schema <> 'information_schema'
    ORDER BY 1, 2, 3""").collect() if r.volume_catalog in catalogs]
print(f"[OK] {len(volumes)} volume(s) managé(s) à sauvegarder")

# COMMAND ----------
# MAGIC %md ## 2 — Listing, plan de copie, copie, index

# COMMAND ----------
def _norm(path: str) -> str:
    """dbutils.fs.ls renvoie « dbfs:/Volumes/… » pour un chemin « /Volumes/… »."""
    return path[len("dbfs:"):] if path.startswith("dbfs:") else path

def list_files(root: str) -> dict:
    """{chemin relatif: (taille, mtime)} — parcours récursif, fichiers « _* » et « .* » compris."""
    root = _norm(root).rstrip("/") + "/"
    out, dirs = {}, [root]
    while dirs:
        for f in dbutils.fs.ls(dirs.pop()):
            if f.isDir():
                dirs.append(f.path)
            else:
                out[_norm(f.path)[len(root):]] = (f.size, f.modificationTime)
    return out

def _list_existing(path: str) -> dict:
    try:
        return list_files(path)
    except Exception:
        return {}

index_exists = True
try:
    spark.read.format("delta").load(INDEX).limit(1).collect()
except Exception:
    index_exists = False

def previous_state(c, s, v) -> dict:
    """Dernière partition (< aujourd'hui) de ce volume : {rel: (size, mtime, stored_in)}."""
    if not index_exists:
        return {}
    idx = spark.read.format("delta").load(INDEX).where(
        (F.col("catalog") == c) & (F.col("schema") == s) & (F.col("volume") == v) & (F.col("snapshot_date") < backup_date))
    last = idx.agg(F.max("snapshot_date")).collect()[0][0]
    if not last:
        return {}
    return {r.rel_path: (r.size, r.mtime, r.stored_in)
            for r in idx.where(F.col("snapshot_date") == last).collect()}

def backup_volume(vol: tuple) -> dict:
    c, s, v = vol
    t0 = time.time()
    try:
        current = list_files(f"/Volumes/{c}/{s}/{v}")
        rows, to_copy = plan_copy([(rel, size, mtime) for rel, (size, mtime) in current.items()],
                                  previous_state(c, s, v), backup_date)
        day_dir = f"{FILES}/{c}/{s}/{v}/{backup_date}"
        already = _list_existing(day_dir)          # reprise : fichiers déjà copiés ce jour-là
        copied, copied_bytes, vanished = 0, 0, set()
        for rel in to_copy:
            safe_rel_path(rel)
            if already.get(rel, (None,))[0] == current[rel][0]:
                continue
            try:
                dbutils.fs.cp(f"/Volumes/{c}/{s}/{v}/{rel}", backup_file_path(backup_root, c, s, v, backup_date, rel))
                copied += 1
                copied_bytes += current[rel][0]
            except Exception as e:
                if "FileNotFound" in str(e) or "does not exist" in str(e) or "NOT_FOUND" in str(e):
                    vanished.add(rel)      # supprimé entre le listing et la copie
                else:
                    raise
        rows = [r for r in rows if r["rel_path"] not in vanished]
        # Partition écrite seulement si toutes les copies ont réussi : jamais d'index vers un fichier absent
        df = spark.createDataFrame(
            [(backup_date, c, s, v, r["rel_path"], r["size"], r["mtime"], r["stored_in"]) for r in rows],
            "snapshot_date STRING, catalog STRING, schema STRING, volume STRING, rel_path STRING, "
            "size BIGINT, mtime BIGINT, stored_in STRING")
        cond = (f"snapshot_date = '{backup_date}' AND catalog = '{c}' AND schema = '{s}' AND volume = '{v}'")
        df.write.format("delta").mode("overwrite").option("replaceWhere", cond) \
          .partitionBy("snapshot_date").save(INDEX)
        return {"volume": f"{c}.{s}.{v}", "status": "success", "files": len(rows), "copied": copied,
                "copied_gb": round(copied_bytes / 1073741824, 3), "vanished": len(vanished),
                "duration_s": int(time.time() - t0)}
    except Exception as e:
        return {"volume": f"{c}.{s}.{v}", "status": "error", "error": _short(e), "duration_s": int(time.time() - t0)}

# Le premier volume crée l'index ; les suivants peuvent alors écrire en parallèle (partitions disjointes)
results = []
if volumes:
    results.append(backup_volume(volumes[0]))
    index_exists = True
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
        results += list(ex.map(backup_volume, volumes[1:]))
for r in results:
    if r["status"] == "success":
        print(f"  ✓ {r['volume']} — {r['files']} fichier(s), {r['copied']} copié(s) ({r['copied_gb']} Go)"
              + (f", {r['vanished']} disparu(s) pendant la copie" if r["vanished"] else ""))
    else:
        print(f"  ✗ {r['volume']} — {r['error']}")

# COMMAND ----------
# MAGIC %md ## 3 — Rétention et purge

# COMMAND ----------
purged_files, expired = 0, []
if index_exists:
    idx = spark.read.format("delta").load(INDEX)
    dates = [r[0] for r in idx.select("snapshot_date").distinct().collect()]
    kept = select_kept_dates(dates, date.fromisoformat(backup_date), retain_daily, retain_monthly)
    expired = sorted(set(dates) - kept)
    print(f"[INFO] {len(dates)} partition(s), {len(kept)} conservée(s), {len(expired)} expirée(s)")
    if expired:
        key = ["catalog", "schema", "volume", "stored_in", "rel_path"]
        exp_refs = idx.where(F.col("snapshot_date").isin(expired)).select(*key).distinct()
        kept_refs = idx.where(F.col("snapshot_date").isin(sorted(kept))).select(*key).distinct()
        purge = [tuple(r) for r in exp_refs.join(kept_refs, key, "left_anti").collect()]
        print(f"[INFO] {len(purge)} version(s) de fichier sans référence conservée")
        if dry_run:
            for p in purge[:20]:
                print(f"  [DRY-RUN] supprimerait {backup_file_path(backup_root, *p)}")
        else:
            def _rm(p):
                try:
                    dbutils.fs.rm(backup_file_path(backup_root, *p))
                    return 1
                except Exception:
                    return 0
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
                purged_files = sum(ex.map(_rm, purge))
            in_list = ", ".join(f"'{d}'" for d in expired)
            spark.sql(f"DELETE FROM delta.`{INDEX}` WHERE snapshot_date IN ({in_list})")
            print(f"[OK] {purged_files} fichier(s) purgé(s), {len(expired)} partition(s) supprimée(s)")

    # Dossiers datés sans partition (run interrompu jamais repris), plus vieux que la rétention
    if not dry_run:
        live = set(dates) - set(expired)
        limit = date.fromisoformat(backup_date).toordinal() - retain_daily
        for c, s, v in volumes:
            try:
                dated = dbutils.fs.ls(f"{FILES}/{c}/{s}/{v}")
            except Exception:
                continue
            for d in dated:
                name = d.name.rstrip("/")
                try:
                    day = date.fromisoformat(name).toordinal()
                except ValueError:
                    continue
                if name not in live and day < limit:
                    dbutils.fs.rm(d.path, recurse=True)
                    print(f"  [OK] Dossier orphelin supprimé : {d.path}")
    try:
        spark.sql(f"VACUUM delta.`{INDEX}`")
    except Exception as e:
        print(f"[WARN] VACUUM de l'index : {_short(e)}")

# COMMAND ----------
summary = {
    "volumes": len(volumes),
    "success": sum(1 for r in results if r["status"] == "success"),
    "errors": sum(1 for r in results if r["status"] == "error"),
    "files": sum(r.get("files", 0) for r in results),
    "copied": sum(r.get("copied", 0) for r in results),
    "copied_gb": round(sum(r.get("copied_gb", 0) for r in results), 3),
    "expired_partitions": len(expired), "purged_files": purged_files,
}
print(json.dumps(summary, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(summary))
