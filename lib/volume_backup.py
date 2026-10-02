# lib/volume_backup.py
"""
Règles de la sauvegarde des fichiers des volumes managés (spec 2026-10-01-volume-files-backup).

Stockage : {root}/volumes/files/{catalog}/{schema}/{volume}/{stored_in}/{rel_path}
Index    : {root}/volumes/_index (Delta, partition snapshot_date) — une ligne par fichier et par jour,
           qui pointe vers le dossier daté (stored_in) contenant sa version.

Fonctions pures : testables hors Databricks.
"""
from datetime import date, timedelta


def plan_copy(current: list, previous: dict, today: str) -> tuple:
    """current  = [(rel_path, size, mtime)] listés aujourd'hui
    previous = {rel_path: (size, mtime, stored_in)} de la dernière partition connue
    → (lignes de la partition du jour, rel_paths à copier dans le dossier du jour).
    Un fichier inchangé (même taille et même date de modification) reprend son stored_in."""
    rows, to_copy = [], []
    for rel, size, mtime in sorted(current):
        prev = previous.get(rel)
        if prev and prev[0] == size and prev[1] == mtime:
            stored_in = prev[2]
        else:
            stored_in = today
            to_copy.append(rel)
        rows.append({"rel_path": rel, "size": size, "mtime": mtime, "stored_in": stored_in})
    return rows, to_copy


def select_kept_dates(dates: list, today: date, retain_daily: int, retain_monthly: int) -> set:
    """Partitions conservées : celles des retain_daily derniers jours, plus la plus ancienne
    partition de chacun des retain_monthly derniers mois (le 1er si le job a tourné ce jour-là)."""
    first_daily = today - timedelta(days=retain_daily - 1)
    kept = {d for d in dates if date.fromisoformat(d) >= first_daily}
    months, y, m = [], today.year, today.month
    for _ in range(retain_monthly):
        months.append(f"{y:04d}-{m:02d}")
        y, m = (y, m - 1) if m > 1 else (y - 1, 12)
    for month in months:
        in_month = sorted(d for d in dates if d.startswith(month))
        if in_month:
            kept.add(in_month[0])
    return kept


def safe_rel_path(rel: str) -> str:
    """Refuse un chemin relatif vide, absolu ou qui sortirait de son dossier."""
    if not rel or rel.startswith("/") or ".." in rel.split("/"):
        raise ValueError(f"chemin relatif invalide : {rel!r}")
    return rel


def backup_file_path(root: str, catalog: str, schema: str, volume: str, stored_in: str, rel: str) -> str:
    return f"{root.rstrip('/')}/volumes/files/{catalog}/{schema}/{volume}/{stored_in}/{safe_rel_path(rel)}"
