# Databricks notebook source
# notebooks/06_retention.py

# COMMAND ----------
# MAGIC %md # 06 — Retention Policy (nettoyage des anciens backups)
# MAGIC
# MAGIC Ce notebook applique une politique de rétention à **3 niveaux** sur les dossiers de backup :
# MAGIC
# MAGIC | Niveau    | Paramètre       | Comportement                                              |
# MAGIC |-----------|-----------------|-----------------------------------------------------------|
# MAGIC | Quotidien | `retain_daily`  | Garde les N derniers jours intégralement                  |
# MAGIC | Hebdo     | `retain_weekly` | Au-delà, garde 1 backup par semaine pendant W semaines    |
# MAGIC | Mensuel   | `retain_monthly`| Au-delà, garde 1 backup par mois pendant M mois          |
# MAGIC
# MAGIC Tout ce qui dépasse ces fenêtres est supprimé.

# COMMAND ----------
import json
import re
from datetime import date, timedelta

# COMMAND ----------
dbutils.widgets.text("backup_root",    "", "Backup root (abfss://...)")
dbutils.widgets.text("backup_date",    str(date.today()), "Date du backup courant YYYY-MM-DD")
dbutils.widgets.text("retain_daily",   "7",  "Rétention quotidienne (jours)")
dbutils.widgets.text("retain_weekly",  "4",  "Rétention hebdomadaire (semaines)")
dbutils.widgets.text("retain_monthly", "3",  "Rétention mensuelle (mois)")
dbutils.widgets.text("dry_run",        "false", "Simulation (true = aucune suppression)")

backup_root    = dbutils.widgets.get("backup_root")
backup_date    = dbutils.widgets.get("backup_date")
retain_daily   = int(dbutils.widgets.get("retain_daily"))
retain_weekly  = int(dbutils.widgets.get("retain_weekly"))
retain_monthly = int(dbutils.widgets.get("retain_monthly"))
dry_run        = dbutils.widgets.get("dry_run").lower() == "true"

print(f"[OK] Politique de rétention : daily={retain_daily}j | weekly={retain_weekly}sem | monthly={retain_monthly}mois")
print(f"[OK] Dry-run : {dry_run}")

# COMMAND ----------
# MAGIC %md ## 6.1 — Inventaire des dossiers backup

# COMMAND ----------
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

try:
    all_items = dbutils.fs.ls(backup_root)
except Exception as e:
    raise RuntimeError(f"Impossible de lister {backup_root}: {e}")

# Extraire uniquement les dossiers datés (format YYYY-MM-DD)
backup_dates = sorted([
    date.fromisoformat(item.name.rstrip("/"))
    for item in all_items
    if DATE_PATTERN.match(item.name.rstrip("/"))
])

print(f"[OK] {len(backup_dates)} dossiers backup trouvés")
for d in backup_dates:
    print(f"  • {d}")

# COMMAND ----------
# MAGIC %md ## 6.2 — Calcul des dates à conserver

# COMMAND ----------
today = date.fromisoformat(backup_date)

# ── Niveau 1 : rétention quotidienne ──────────────────────────────────────────
cutoff_daily = today - timedelta(days=retain_daily)
daily_keep = {d for d in backup_dates if d >= cutoff_daily}

# ── Niveau 2 : rétention hebdomadaire ─────────────────────────────────────────
# Parmi les dates au-delà de la fenêtre daily, garder le plus récent par semaine ISO
weekly_candidates = [d for d in backup_dates if d < cutoff_daily]

weekly_best: dict = {}  # (iso_year, iso_week) → date la plus récente
for d in weekly_candidates:
    iso_year, iso_week, _ = d.isocalendar()
    key = (iso_year, iso_week)
    if key not in weekly_best or d > weekly_best[key]:
        weekly_best[key] = d

# Garder uniquement les retain_weekly semaines les plus récentes
sorted_week_keys = sorted(weekly_best.keys(), reverse=True)[:retain_weekly]
weekly_keep = {weekly_best[k] for k in sorted_week_keys}

# ── Niveau 3 : rétention mensuelle ────────────────────────────────────────────
# Parmi les dates au-delà de la fenêtre weekly, garder le plus récent par mois
weekly_cutoff = today - timedelta(days=retain_daily + retain_weekly * 7)
monthly_candidates = [d for d in backup_dates if d < weekly_cutoff]

monthly_best: dict = {}  # (year, month) → date la plus récente
for d in monthly_candidates:
    key = (d.year, d.month)
    if key not in monthly_best or d > monthly_best[key]:
        monthly_best[key] = d

sorted_month_keys = sorted(monthly_best.keys(), reverse=True)[:retain_monthly]
monthly_keep = {monthly_best[k] for k in sorted_month_keys}

# ── Ensemble final ─────────────────────────────────────────────────────────────
keep_set = daily_keep | weekly_keep | monthly_keep
to_delete = sorted([d for d in backup_dates if d not in keep_set])

print(f"\n[OK] Dates conservées ({len(keep_set)}) :")
for d in sorted(keep_set):
    tag = "daily" if d in daily_keep else ("weekly" if d in weekly_keep else "monthly")
    print(f"  ✓ {d}  [{tag}]")

print(f"\n[OK] Dates à supprimer ({len(to_delete)}) :")
for d in to_delete:
    print(f"  ✗ {d}")

# COMMAND ----------
# MAGIC %md ## 6.3 — Suppression

# COMMAND ----------
deleted = []
errors  = []

for d in to_delete:
    folder_path = f"{backup_root}/{d}"
    if dry_run:
        print(f"  [DRY-RUN] Suppression simulée : {folder_path}")
        deleted.append(str(d))
    else:
        try:
            dbutils.fs.rm(folder_path, recurse=True)
            print(f"  [DEL] {folder_path}")
            deleted.append(str(d))
        except Exception as e:
            print(f"  [ERROR] {folder_path} : {e}")
            errors.append({"date": str(d), "error": str(e)})

# COMMAND ----------
# MAGIC %md ## 6.4 — Résumé

# COMMAND ----------
summary = {
    "retain_daily":      retain_daily,
    "retain_weekly":     retain_weekly,
    "retain_monthly":    retain_monthly,
    "dry_run":           dry_run,
    "total_backups":     len(backup_dates),
    "kept_count":        len(keep_set),
    "deleted_count":     len(deleted),
    "error_count":       len(errors),
    "deleted_dates":     deleted,
}

prefix = "[DRY-RUN] " if dry_run else ""
print(f"""
╔══════════════════════════════════════════╗
║       RETENTION POLICY — RÉSUMÉ         ║
╠══════════════════════════════════════════╣
║  {prefix}Dossiers trouvés  : {len(backup_dates):<20} ║
║  {prefix}Conservés         : {len(keep_set):<20} ║
║  {prefix}Supprimés         : {len(deleted):<20} ║
║  {prefix}Erreurs           : {len(errors):<20} ║
╚══════════════════════════════════════════╝
""")

if errors:
    print("[WARN] Erreurs de suppression :")
    for e in errors:
        print(f"  • {e['date']}: {e['error']}")

dbutils.notebook.exit(json.dumps(summary))
