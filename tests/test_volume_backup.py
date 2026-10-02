# tests/test_volume_backup.py
from datetime import date

import pytest

from lib.volume_backup import (
    backup_file_path, plan_copy, safe_rel_path, select_kept_dates,
)


# ── Plan de copie ────────────────────────────────────────────────────────

def test_first_run_copies_everything():
    rows, to_copy = plan_copy([("a.csv", 10, 100), ("d/b.json", 5, 200)], {}, "2026-10-05")
    assert to_copy == ["a.csv", "d/b.json"]
    assert {r["rel_path"]: r["stored_in"] for r in rows} == {"a.csv": "2026-10-05", "d/b.json": "2026-10-05"}


def test_unchanged_file_keeps_its_stored_in_and_is_not_copied():
    previous = {"a.csv": (10, 100, "2026-09-01")}
    rows, to_copy = plan_copy([("a.csv", 10, 100)], previous, "2026-10-05")
    assert to_copy == []
    assert rows == [{"rel_path": "a.csv", "size": 10, "mtime": 100, "stored_in": "2026-09-01"}]


@pytest.mark.parametrize("current", [("a.csv", 11, 100), ("a.csv", 10, 101)])
def test_modified_file_size_or_mtime_is_copied_again(current):
    rows, to_copy = plan_copy([current], {"a.csv": (10, 100, "2026-09-01")}, "2026-10-05")
    assert to_copy == ["a.csv"] and rows[0]["stored_in"] == "2026-10-05"


def test_deleted_file_has_no_row_today():
    rows, to_copy = plan_copy([], {"a.csv": (10, 100, "2026-09-01")}, "2026-10-05")
    assert rows == [] and to_copy == []


# ── Rétention ────────────────────────────────────────────────────────────

def test_kept_dates_daily_window_and_first_of_each_month():
    dates = ["2026-08-03", "2026-08-20", "2026-09-02", "2026-09-15", "2026-10-01", "2026-10-04", "2026-10-05"]
    kept = select_kept_dates(dates, date(2026, 10, 5), retain_daily=3, retain_monthly=2)
    # 3 derniers jours : 10-03..10-05 → 10-04, 10-05 ; mois : 2026-10 (10-01) et 2026-09 (09-02)
    assert kept == {"2026-10-04", "2026-10-05", "2026-10-01", "2026-09-02"}


def test_monthly_keeps_oldest_partition_of_the_month_even_without_the_first():
    kept = select_kept_dates(["2026-09-03", "2026-09-10"], date(2026, 10, 5), retain_daily=1, retain_monthly=2)
    assert kept == {"2026-09-03"}



# ── Chemins ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("rel", ["a.csv", "dossier/sous dossier/é été.json", ".cache", "_SUCCESS"])
def test_safe_rel_path_accepts_normal_names(rel):
    assert safe_rel_path(rel) == rel


@pytest.mark.parametrize("rel", ["../x", "a/../../x", "/abs", ""])
def test_safe_rel_path_rejects_escapes(rel):
    with pytest.raises(ValueError):
        safe_rel_path(rel)


def test_backup_file_path():
    assert backup_file_path("abfss://c@a/backup", "cat", "sch", "vol", "2026-10-05", "d/f.csv") == \
        "abfss://c@a/backup/volumes/files/cat/sch/vol/2026-10-05/d/f.csv"
