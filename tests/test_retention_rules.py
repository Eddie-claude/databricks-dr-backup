# tests/test_retention_rules.py
"""Règles de notebooks/06_retention.py (choix du backup de référence du snapshot mensuel),
extraites du notebook entre leurs marqueurs."""
from pathlib import Path

SRC = (Path(__file__).resolve().parent.parent / "notebooks" / "06_retention.py").read_text(encoding="utf-8")
R = {}
exec(SRC.split("# >>> REGLES")[1].split("# <<< REGLES")[0], R)


# Cas client (monthly du 01.09 lancé avant la fin du daily du 01.09) : la référence est le dernier
# backup terminé, pas la date du jour, dont le manifest n'existe pas encore
def test_reference_is_last_finished_backup_not_today():
    assert R["reference_backup"]({"date": "2026-08-31", "status": "success"}, "2026-09-01") == ("2026-08-31", "latest.json")


def test_reference_is_today_when_daily_already_finished():
    assert R["reference_backup"]({"date": "2026-09-01"}, "2026-09-01") == ("2026-09-01", "latest.json")


def test_reference_falls_back_to_today_without_latest():
    assert R["reference_backup"](None, "2026-09-01") == ("2026-09-01", "date du jour")
    assert R["reference_backup"]({}, "2026-09-01") == ("2026-09-01", "date du jour")
    assert R["reference_backup"]({"date": ""}, "2026-09-01") == ("2026-09-01", "date du jour")
    assert R["reference_backup"]("garbage", "2026-09-01") == ("2026-09-01", "date du jour")


def test_manifest_tables_keeps_only_successful_three_part_names():
    entries = [{"table": "c.s.ok", "status": "success"},
               {"table": "c.s.ko", "status": "error"},
               {"table": "c.s.skip", "status": "skipped"},
               {"table": "c.ok2", "status": "success"},            # nom incomplet : ignoré
               {"table": "c.s.ok3", "status": "success"}]
    assert R["manifest_tables"](entries, "abfss://x/incremental") == \
        ["abfss://x/incremental/c/s/ok", "abfss://x/incremental/c/s/ok3"]
    assert R["manifest_tables"]([], "abfss://x/incremental") == []
