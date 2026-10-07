# Databricks notebook source
# notebooks/15_restore_volume_files.py

# COMMAND ----------
# MAGIC %md # 15 — Restauration des fichiers des volumes managés
# MAGIC
# MAGIC Restaure le contenu d'un ou plusieurs volumes managés, dans l'état d'une date donnée, depuis
# MAGIC l'index `volumes/_index` produit par `14_volume_files`.
# MAGIC
# MAGIC - Le volume **cible doit exister** (recréé par l'IaC ou par `12_restore_uc_objects`).
# MAGIC - Si le volume n'a pas de sauvegarde à la date demandée (copie en échec ce jour-là), la dernière
# MAGIC   sauvegarde antérieure est utilisée, avec un avertissement.
# MAGIC - Les fichiers du snapshot **écrasent** ceux de la cible ; les fichiers présents dans la cible mais
# MAGIC   absents du snapshot **ne sont pas supprimés** (ils sont listés).
# MAGIC - `target_volume` (`catalog.schema.volume`) restaure ailleurs, par exemple pour comparer avant
# MAGIC   d'écraser ; uniquement si le filtre désigne un seul volume.
# MAGIC - **dry_run = true** : affiche ce qui serait restauré, sans rien copier.

# COMMAND ----------
import concurrent.futures
import fnmatch
import json
import sys

from pyspark.sql import functions as F

dbutils.widgets.text(    "backup_root",   "", "Backup root (abfss://...)")
dbutils.widgets.text(    "restore_date",  "", "Date à restaurer YYYY-MM-DD (vide = dernière sauvegarde)")
dbutils.widgets.text(    "volume_filter", "*.*.*", "Volumes : catalog.schema.volume (jokers * acceptés)")
dbutils.widgets.text(    "target_volume", "", "Volume cible catalog.schema.volume (vide = volume d'origine)")
dbutils.widgets.text(    "max_parallel",  "8", "Copies simultanées")
dbutils.widgets.dropdown("dry_run",       "true", ["true", "false"], "Dry-run (true = simulation)")
# lib/ est déployé par le bundle à côté de notebooks/ : …/files/notebooks/x → …/files/lib
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_path = _nb_path if _nb_path.startswith("/Workspace") else "/Workspace" + _nb_path
dbutils.widgets.text("lib_path", _nb_path.rsplit("/notebooks/", 1)[0] + "/lib", "Chemin vers lib/")

backup_root   = dbutils.widgets.get("backup_root").strip().rstrip("/")
restore_date  = dbutils.widgets.get("restore_date").strip()
volume_filter = dbutils.widgets.get("volume_filter").strip().lower() or "*.*.*"
target_volume = dbutils.widgets.get("target_volume").strip()
max_parallel  = max(1, int(dbutils.widgets.get("max_parallel")))
dry_run       = dbutils.widgets.get("dry_run").lower() == "true"

sys.path.insert(0, dbutils.widgets.get("lib_path"))
from volume_backup import backup_file_path, safe_rel_path

INDEX = f"{backup_root}/volumes/_index"
prefix = "[DRY-RUN] " if dry_run else ""

def _short(e) -> str:
    return " ".join(str(e).split("JVM stacktrace:")[0].split())[:300]

def _norm(path: str) -> str:
    return path[len("dbfs:"):] if path.startswith("dbfs:") else path

def list_files(root: str) -> set:
    root = _norm(root).rstrip("/") + "/"
    out, dirs = set(), [root]
    while dirs:
        for f in dbutils.fs.ls(dirs.pop()):
            if f.isDir():
                dirs.append(f.path)
            else:
                out.add(_norm(f.path)[len(root):])
    return out

# COMMAND ----------
# MAGIC %md ## Étape 1 — Volumes et dates

# COMMAND ----------
try:
    idx = spark.read.format("delta").load(INDEX)
except Exception as e:
    raise RuntimeError(f"Index des volumes introuvable ({INDEX}) : aucune sauvegarde de fichiers de volumes. {_short(e)}")

dates_by_volume = {}
for r in idx.groupBy("catalog", "schema", "volume").agg(F.collect_set("snapshot_date").alias("d")).collect():
    fqn = f"{r.catalog}.{r.schema}.{r.volume}"
    if fnmatch.fnmatch(fqn.lower(), volume_filter):
        dates_by_volume[(r.catalog, r.schema, r.volume)] = sorted(r.d)

print(f"[OK] {len(dates_by_volume)} volume(s) sélectionné(s)")
if target_volume and len(dates_by_volume) != 1:
    raise ValueError("target_volume exige un filtre qui désigne exactement un volume")

plan, errors = [], []
for (c, s, v), dates in sorted(dates_by_volume.items()):
    eligible = [d for d in dates if not restore_date or d <= restore_date]
    if not eligible:
        errors.append(f"{c}.{s}.{v} : aucune sauvegarde au {restore_date} ou avant (disponibles : {', '.join(dates[-10:])})")
        continue
    snap = eligible[-1]
    if restore_date and snap != restore_date:
        print(f"[WARN] {c}.{s}.{v} : pas de sauvegarde le {restore_date}, utilisation du {snap}")
    tc, ts, tv = target_volume.split(".") if target_volume else (c, s, v)
    plan.append(((c, s, v), snap, (tc, ts, tv)))

for err in errors:
    print(f"[ERROR] {err}")

# COMMAND ----------
# MAGIC %md ## Étape 2 — Restauration

# COMMAND ----------
results = []
for (c, s, v), snap, (tc, ts, tv) in plan:
    target_root = f"/Volumes/{tc}/{ts}/{tv}"
    rows = idx.where((F.col("catalog") == c) & (F.col("schema") == s) & (F.col("volume") == v)
                     & (F.col("snapshot_date") == snap)).select("rel_path", "size", "stored_in").collect()
    total_gb = sum(r.size for r in rows) / 1073741824
    print(f"\n{'── ' if dry_run else '▶  '}{c}.{s}.{v} @ {snap} → {tc}.{ts}.{tv} : {len(rows)} fichier(s), {total_gb:.2f} Go")
    try:
        existing = list_files(target_root)
    except Exception as e:
        msg = f"volume cible {tc}.{ts}.{tv} inaccessible ou inexistant — le créer (IaC ou 12_restore_uc_objects) : {_short(e)}"
        print(f"   [ERROR] {msg}")
        results.append({"volume": f"{c}.{s}.{v}", "status": "error", "error": msg})
        continue
    extra = sorted(existing - {r.rel_path for r in rows})
    if dry_run:
        for r in rows[:10]:
            print(f"   {r.rel_path}  ({r.size} o, version du {r.stored_in})")
        results.append({"volume": f"{c}.{s}.{v}", "status": "dry_run", "files": len(rows), "extra": len(extra)})
    else:
        def restore_one(r):
            dst = f"{target_root}/{safe_rel_path(r.rel_path)}"
            src = backup_file_path(backup_root, c, s, v, r.stored_in, r.rel_path)
            try:
                dbutils.fs.cp(src, dst)
            except Exception as e:
                if "already exists" not in str(e).lower():
                    return f"{r.rel_path} : {_short(e)}"
                dbutils.fs.rm(dst)
                dbutils.fs.cp(src, dst)
            return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
            failed = [f for f in ex.map(restore_one, rows) if f]
        for f in failed[:20]:
            print(f"   [ERROR] {f}")
        results.append({"volume": f"{c}.{s}.{v}", "status": "error" if failed else "success",
                        "files": len(rows) - len(failed), "errors": len(failed), "extra": len(extra)})
        print(f"   [{'OK' if not failed else 'ERROR'}] {len(rows) - len(failed)} restauré(s), {len(failed)} en erreur")
    if extra:
        print(f"   [INFO] {len(extra)} fichier(s) de la cible absent(s) du snapshot, conservé(s) : {', '.join(extra[:50])}")

# COMMAND ----------
ok_status = "dry_run" if dry_run else "success"
summary = {
    "restore_date": restore_date or "(dernière)", "dry_run": dry_run, "volumes": len(plan),
    "ok": sum(1 for r in results if r["status"] == ok_status),
    "errors": sum(1 for r in results if r["status"] == "error") + len(errors),
    "results": results,
}
print(json.dumps({k: v for k, v in summary.items() if k != "results"}, ensure_ascii=False))
dbutils.notebook.exit(json.dumps(summary))
