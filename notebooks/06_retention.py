# Databricks notebook source
# notebooks/06_retention.py

# COMMAND ----------
# MAGIC %md # 06 — Retention Policy (snapshots + nettoyage + VACUUM)
# MAGIC
# MAGIC | Niveau    | Paramètre        | Mécanisme                                                           |
# MAGIC |-----------|------------------|---------------------------------------------------------------------|
# MAGIC | Quotidien | `retain_daily`   | Delta log retention sur `incremental/` (positionné dans 02), 30j par défaut |
# MAGIC | Hebdo     | *(désactivé)*    | Abandonné (Option C) — SHALLOW CLONE non supporté par UC sur tables non-MANAGED. `retain_weekly` ne sert plus qu'à purger les anciens snapshots existants. |
# MAGIC | Mensuel   | `retain_monthly` | DEEP CLONE le 1er du mois → `snapshots/monthly/YYYY-MM/`           |
# MAGIC
# MAGIC `force_snapshot=true` force le snapshot monthly quel que soit le jour (tests, rattrapage).
# MAGIC
# MAGIC Un step VACUUM (6.5) purge explicitement les fichiers au-delà de `deletedFileRetentionDuration`
# MAGIC — sans lui, la propriété ne fait que fixer un seuil de sécurité, elle ne supprime rien seule.

# COMMAND ----------
import concurrent.futures
import json
import re
from datetime import date

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def _uc_head(path: str) -> str:
    return "\n".join(r.value for r in spark.read.text(path).collect())

# COMMAND ----------
dbutils.widgets.text("backup_root",     "",            "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",     str(date.today()), "Date du backup courant YYYY-MM-DD")
dbutils.widgets.text("retain_daily",    "30",          "Rétention quotidienne (jours) — info affichage")
dbutils.widgets.text("retain_weekly",   "2",           "Rétention hebdomadaire (semaines) — legacy, purge uniquement")
dbutils.widgets.text("retain_monthly",  "1",           "Rétention mensuelle (mois)")
dbutils.widgets.text("dry_run",         "false",       "Simulation (true = aucune écriture/suppression)")
dbutils.widgets.text("max_parallel",    "4",           "Clones parallèles pour les snapshots")
dbutils.widgets.text("force_snapshot",  "false",       "Forcer weekly+monthly quel que soit le jour (tests)")
dbutils.widgets.text("enable_monthly",  "false",       "Activer snapshot mensuel — false dans le job daily, true dans dr-backup-monthly")
dbutils.widgets.text("enable_weekly",   "false",       "Activer création snapshot weekly — désactivé (Option C, non supporté sur tables non-MANAGED UC)")

backup_root     = dbutils.widgets.get("backup_root")
backup_date     = dbutils.widgets.get("backup_date")
retain_daily    = int(dbutils.widgets.get("retain_daily"))
retain_weekly   = int(dbutils.widgets.get("retain_weekly"))
retain_monthly  = int(dbutils.widgets.get("retain_monthly"))
dry_run         = dbutils.widgets.get("dry_run").lower()         == "true"
max_parallel    = max(1, int(dbutils.widgets.get("max_parallel")))
force_snapshot  = dbutils.widgets.get("force_snapshot").lower()  == "true"
enable_monthly  = dbutils.widgets.get("enable_monthly").lower()  == "true"
enable_weekly   = dbutils.widgets.get("enable_weekly").lower()   == "true"

today            = date.fromisoformat(backup_date)
incremental_root = f"{backup_root}/incremental"
weekly_root      = f"{backup_root}/snapshots/weekly"
monthly_root     = f"{backup_root}/snapshots/monthly"

print(f"[OK] daily={retain_daily}j | weekly={retain_weekly}sem | monthly={retain_monthly}mois | dry_run={dry_run} | force={force_snapshot} | enable_monthly={enable_monthly}")

# COMMAND ----------
# MAGIC %md ## 6.1 — Liste des tables depuis le manifest du jour

# COMMAND ----------
manifest_path      = f"{incremental_root}/_manifests/{backup_date}.json"
incremental_tables = []

try:
    entries = json.loads(_uc_head(manifest_path))
    for e in entries:
        if e.get("status") == "success":
            parts = e["table"].split(".")
            if len(parts) == 3:
                incremental_tables.append(f"{incremental_root}/{parts[0]}/{parts[1]}/{parts[2]}")
    print(f"[OK] {len(incremental_tables)} tables lues depuis le manifest")
except Exception as e:
    print(f"[WARN] Manifest non disponible ({e}) — scan du répertoire incremental/")
    try:
        for cat in [f.path for f in dbutils.fs.ls(incremental_root) if not f.name.startswith("_")]:
            for sch in [f.path for f in dbutils.fs.ls(cat) if not f.name.startswith("_")]:
                for tbl in [f.path for f in dbutils.fs.ls(sch) if not f.name.startswith("_")]:
                    incremental_tables.append(tbl.rstrip("/"))
        print(f"[OK] {len(incremental_tables)} tables trouvées par scan ADLS")
    except Exception as e2:
        print(f"[ERROR] Impossible de lister les tables: {e2}")

# COMMAND ----------
# MAGIC %md ## 6.2 — Snapshot hebdomadaire (DÉSACTIVÉ — Option C)
# MAGIC
# MAGIC Abandonné : Unity Catalog interdit désormais le SHALLOW CLONE sur des tables qui ne sont
# MAGIC pas UC MANAGED (`CANNOT_SHALLOW_CLONE_NON_UC_MANAGED_TABLE_AS_SOURCE_OR_TARGET`) — or toutes
# MAGIC les tables de ce backup sont référencées par chemin brut, jamais enregistrées dans le catalog.
# MAGIC La couverture de restauration est reportée sur `retain_daily`, désormais étendu (30 jours par
# MAGIC défaut) — voir 6.4 pour la purge progressive des anciens snapshots weekly déjà existants.
# MAGIC
# MAGIC `enable_weekly=true` permettrait de réactiver la création (code conservé), mais nécessiterait
# MAGIC d'abord d'enregistrer ces tables comme UC MANAGED (changement architectural, non fait ici).

# COMMAND ----------
iso_year, iso_week, iso_dow = today.isocalendar()
week_label = f"{iso_year}-W{iso_week:02d}"
do_weekly  = enable_weekly and ((iso_dow == 1) or force_snapshot)

weekly_results = []

if do_weekly and incremental_tables:
    weekly_dest_root = f"{weekly_root}/{week_label}"
    print(f"[INFO] Création snapshot weekly : {weekly_dest_root}")

    def shallow_clone_one(src_path: str) -> dict:
        rel  = src_path.replace(incremental_root, "").lstrip("/")
        dest = f"{weekly_dest_root}/{rel}"
        try:
            if not dry_run:
                spark.sql(f"CREATE OR REPLACE TABLE delta.`{dest}` SHALLOW CLONE delta.`{src_path}`")
            print(f"  ✓ [SHALLOW] {rel}")
            return {"table": rel, "status": "success"}
        except Exception as e:
            print(f"  ✗ {rel}: {e}")
            return {"table": rel, "status": "error", "error": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
        weekly_results = list(ex.map(shallow_clone_one, incremental_tables))

    w_ok = sum(1 for r in weekly_results if r["status"] == "success")
    print(f"[OK] Weekly {week_label} : {w_ok}/{len(incremental_tables)} tables")
elif do_weekly:
    print("[WARN] Snapshot weekly déclenché mais aucune table disponible")
elif not enable_weekly:
    print("[INFO] Snapshot weekly désactivé (Option C — enable_weekly=false)")
else:
    print(f"[INFO] Snapshot weekly ignoré — dow={iso_dow} (pas lundi)")

# COMMAND ----------
# MAGIC %md ## 6.3 — Snapshot mensuel (DEEP CLONE)
# MAGIC
# MAGIC Créé le 1er du mois uniquement (ou si `force_snapshot=true`).
# MAGIC Le DEEP CLONE copie toutes les données → archive indépendante de `incremental/`.

# COMMAND ----------
month_label = today.strftime("%Y-%m")
# enable_monthly=false dans le job daily → mensuel jamais exécuté ici
# enable_monthly=true dans le job dr-backup-monthly → exécuté le 1er du mois (ou force_snapshot)
do_monthly  = enable_monthly and ((today.day == 1) or force_snapshot)

monthly_results = []

if do_monthly and incremental_tables:
    monthly_dest_root = f"{monthly_root}/{month_label}"
    print(f"[INFO] Création snapshot monthly : {monthly_dest_root}")

    def deep_clone_monthly(src_path: str) -> dict:
        rel  = src_path.replace(incremental_root, "").lstrip("/")
        dest = f"{monthly_dest_root}/{rel}"
        try:
            if not dry_run:
                spark.sql(f"CREATE OR REPLACE TABLE delta.`{dest}` DEEP CLONE delta.`{src_path}`")
            print(f"  ✓ [DEEP] {rel}")
            return {"table": rel, "status": "success"}
        except Exception as e:
            print(f"  ✗ {rel}: {e}")
            return {"table": rel, "status": "error", "error": str(e)}

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_parallel) as ex:
        monthly_results = list(ex.map(deep_clone_monthly, incremental_tables))

    m_ok = sum(1 for r in monthly_results if r["status"] == "success")
    print(f"[OK] Monthly {month_label} : {m_ok}/{len(incremental_tables)} tables")
elif do_monthly:
    print("[WARN] Snapshot monthly déclenché mais aucune table disponible")
else:
    print(f"[INFO] Snapshot mensuel ignoré — jour={today.day} (pas le 1er)")

# COMMAND ----------
# MAGIC %md ## 6.4 — Nettoyage snapshots expirés

# COMMAND ----------
WEEK_PATTERN  = re.compile(r"^\d{4}-W\d{2}$")
MONTH_PATTERN = re.compile(r"^\d{4}-\d{2}$")

deleted = []
errors  = []

def list_snapshot_folders(root: str, pattern) -> list:
    try:
        return sorted([
            item.name.rstrip("/")
            for item in dbutils.fs.ls(root)
            if pattern.match(item.name.rstrip("/"))
        ])
    except Exception:
        return []

# ── Weekly : garder les retain_weekly semaines les plus récentes ────────────
all_weekly  = list_snapshot_folders(weekly_root,  WEEK_PATTERN)
keep_weekly = set(sorted(all_weekly, reverse=True)[:retain_weekly])
del_weekly  = [w for w in all_weekly if w not in keep_weekly]

print(f"\n[Weekly] {len(all_weekly)} snapshots trouvés — garder {retain_weekly} — supprimer {len(del_weekly)}")
for label in del_weekly:
    path = f"{weekly_root}/{label}"
    if dry_run:
        print(f"  [DRY-RUN] weekly à supprimer : {label}")
        deleted.append(path)
    else:
        try:
            dbutils.fs.rm(path, recurse=True)
            print(f"  [DEL] {label}")
            deleted.append(path)
        except Exception as e:
            print(f"  [ERROR] {label}: {e}")
            errors.append({"path": path, "error": str(e)})

# ── Monthly : garder les retain_monthly mois les plus récents ──────────────
all_monthly  = list_snapshot_folders(monthly_root, MONTH_PATTERN)
keep_monthly = set(sorted(all_monthly, reverse=True)[:retain_monthly])
del_monthly  = [m for m in all_monthly if m not in keep_monthly]

print(f"\n[Monthly] {len(all_monthly)} snapshots trouvés — garder {retain_monthly} — supprimer {len(del_monthly)}")
for label in del_monthly:
    path = f"{monthly_root}/{label}"
    if dry_run:
        print(f"  [DRY-RUN] monthly à supprimer : {label}")
        deleted.append(path)
    else:
        try:
            dbutils.fs.rm(path, recurse=True)
            print(f"  [DEL] {label}")
            deleted.append(path)
        except Exception as e:
            print(f"  [ERROR] {label}: {e}")
            errors.append({"path": path, "error": str(e)})

# COMMAND ----------
# MAGIC %md ## 6.5 — VACUUM (purge des fichiers au-delà de la rétention configurée)
# MAGIC
# MAGIC Sans VACUUM explicite, les propriétés `delta.deletedFileRetentionDuration` définies dans
# MAGIC `02_data_clone` ne font que fixer un **seuil de sécurité** — elles ne suppriment jamais rien
# MAGIC d'elles-mêmes. `VACUUM` est appelé ici **sans `RETAIN` explicite**, pour toujours respecter
# MAGIC le seuil déjà configuré sur chaque table (jamais une valeur plus courte que la fenêtre de
# MAGIC restauration promise par `retain_daily`).

# COMMAND ----------
vacuum_results = []

print(f"\n{'─'*60}")
print(f"{'[DRY-RUN] ' if dry_run else ''}VACUUM — {len(incremental_tables)} table(s)")
print(f"{'─'*60}")

def get_vacuum_metrics(path: str) -> dict:
    """Lit operationMetrics du dernier commit (l'opération VACUUM qu'on vient de lancer)
    pour savoir combien de fichiers/dossiers ont réellement été supprimés — sans ça,
    un VACUUM qui n'a rien trouvé à purger a exactement la même sortie qu'un VACUUM
    qui a nettoyé des centaines de fichiers."""
    try:
        row     = spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").collect()[0]
        metrics = row["operationMetrics"] or {}
        return {
            "num_deleted_files":       int(metrics.get("numDeletedFiles", 0)),
            "num_vacuumed_directories": int(metrics.get("numVacuumedDirectories", 0)),
        }
    except Exception:
        return {"num_deleted_files": None, "num_vacuumed_directories": None}

for src_path in incremental_tables:
    if dry_run:
        print(f"  [DRY-RUN] VACUUM delta.`{src_path}`")
        vacuum_results.append({"table": src_path, "status": "dry_run"})
        continue
    try:
        spark.sql(f"VACUUM delta.`{src_path}`")
        vmetrics    = get_vacuum_metrics(src_path)
        num_deleted = vmetrics["num_deleted_files"]
        num_dirs    = vmetrics["num_vacuumed_directories"]

        if num_deleted is None:
            print(f"  [OK] VACUUM {src_path} (métriques indisponibles)")
        elif num_deleted == 0:
            print(f"  [OK] VACUUM {src_path} — rien à purger (dans la fenêtre de rétention)")
        else:
            print(f"  [OK] VACUUM {src_path} — {num_deleted} fichier(s) supprimé(s), {num_dirs} dossier(s)")

        vacuum_results.append({
            "table":  src_path,
            "status": "success",
            **vmetrics,
        })
    except Exception as e:
        print(f"  [ERROR] VACUUM {src_path}: {e}")
        vacuum_results.append({"table": src_path, "status": "error", "error": str(e)})

vacuum_ok           = sum(1 for r in vacuum_results if r["status"] == "success")
vacuum_error        = sum(1 for r in vacuum_results if r["status"] == "error")
vacuum_files_deleted = sum(r.get("num_deleted_files") or 0 for r in vacuum_results if r["status"] == "success")
vacuum_dirs_deleted  = sum(r.get("num_vacuumed_directories") or 0 for r in vacuum_results if r["status"] == "success")
vacuum_tables_cleaned = sum(1 for r in vacuum_results if r["status"] == "success" and (r.get("num_deleted_files") or 0) > 0)

print(f"""
[OK] VACUUM : {vacuum_ok} table(s) traitée(s), {vacuum_error} erreur(s)
     {vacuum_tables_cleaned} table(s) avec des fichiers réellement supprimés
     {vacuum_files_deleted} fichier(s) supprimé(s) au total, {vacuum_dirs_deleted} dossier(s) vidé(s)
""")

# COMMAND ----------
# MAGIC %md ## 6.6 — Résumé

# COMMAND ----------
w_ok = sum(1 for r in weekly_results  if r.get("status") == "success")
m_ok = sum(1 for r in monthly_results if r.get("status") == "success")

summary = {
    "retain_daily":      retain_daily,
    "retain_weekly":     retain_weekly,
    "retain_monthly":    retain_monthly,
    "dry_run":           dry_run,
    "weekly_snapshot":   week_label  if do_weekly  else None,
    "weekly_tables_ok":  w_ok,
    "monthly_snapshot":  month_label if do_monthly else None,
    "monthly_tables_ok": m_ok,
    "deleted_count":     len(deleted),
    "error_count":       len(errors),
    "deleted_paths":     deleted,
    "vacuum_ok":               vacuum_ok,
    "vacuum_error":            vacuum_error,
    "vacuum_tables_cleaned":   vacuum_tables_cleaned,
    "vacuum_files_deleted":    vacuum_files_deleted,
    "vacuum_dirs_deleted":     vacuum_dirs_deleted,
    "vacuum_results":          vacuum_results,
}

prefix = "[DRY-RUN] " if dry_run else ""
print(f"""
╔══════════════════════════════════════════════╗
║         RETENTION POLICY — RÉSUMÉ           ║
╠══════════════════════════════════════════════╣
║  daily    : {retain_daily}j  (Delta log sur incremental/)
║  Weekly   : {(week_label  if do_weekly  else 'désactivé (Option C)'):<20} ({w_ok} tables OK)
║  Monthly  : {(month_label if do_monthly else 'ignoré'):<20} ({m_ok} tables OK)
║  {prefix}Supprimés : {len(deleted):<4} dossiers
║  Erreurs  : {len(errors):<4}
║  {prefix}VACUUM    : {vacuum_ok:<4} tables traitées, {vacuum_error} erreurs
║             {vacuum_tables_cleaned:<4} tables nettoyées ({vacuum_files_deleted} fichiers, {vacuum_dirs_deleted} dossiers)
╚══════════════════════════════════════════════╝
""")

dbutils.notebook.exit(json.dumps(summary))
