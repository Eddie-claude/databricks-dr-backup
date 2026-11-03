# tests/test_report.py
import pytest
from lib.report import generate_report


def test_report_contains_date():
    diff = {
        "date_prev": "2026-04-01",
        "date_curr": "2026-04-02",
        "tables": {"added": ["t1"], "removed": [], "unchanged": ["t2"]},
        "jobs": {"added": [], "removed": [], "unchanged": ["j1"]},
        "notebooks": {"added": [], "removed": [], "unchanged": ["nb1"]},
    }
    stats = {"total_tables": 2, "total_jobs": 1, "total_notebooks": 1, "data_size_gb": 12.5}
    steps = [
        {"name": "uc_metadata", "status": "success", "duration_s": 45},
        {"name": "data_clone", "status": "success", "duration_s": 320},
    ]
    html = generate_report(diff=diff, stats=stats, steps=steps)
    assert "2026-04-02" in html
    assert "t1" in html
    assert "success" in html
    assert "12.5" in html


def test_report_shows_errors():
    diff = {
        "date_prev": "2026-04-01",
        "date_curr": "2026-04-02",
        "tables": {"added": [], "removed": [], "unchanged": []},
        "jobs": {"added": [], "removed": [], "unchanged": []},
        "notebooks": {"added": [], "removed": [], "unchanged": []},
    }
    stats = {"total_tables": 0, "total_jobs": 0, "total_notebooks": 0, "data_size_gb": 0}
    steps = [
        {"name": "data_clone", "status": "error", "duration_s": 10, "error": "Timeout connecting to storage"},
    ]
    html = generate_report(diff=diff, stats=stats, steps=steps)
    assert "error" in html.lower()
    assert "Timeout connecting to storage" in html


# ── Tables non sauvegardées ──────────────────────────────────────────────

from lib.report import non_saved_tables

_DIFF = {"date_prev": "2026-10-06", "date_curr": "2026-10-07",
         "tables": {"added": [], "removed": [], "unchanged": []},
         "jobs": {"added": [], "removed": [], "unchanged": []},
         "notebooks": {"added": [], "removed": [], "unchanged": []}}


def test_non_saved_tables_lists_errors_then_skipped():
    manifest = [
        {"table": "c.a.ok", "status": "success"},
        {"table": "c.rh.salaires", "status": "skipped", "reason": "non clonable", "error": "[DELTA_X]"},
        {"table": "c.b.t", "status": "error", "error": "[PERMISSION_DENIED]"},
    ]
    assert [t["table"] for t in non_saved_tables(manifest)] == ["c.b.t", "c.rh.salaires"]


def test_report_shows_discovered_saved_and_not_saved_tables():
    # Le rapport n'affichait que les tables découvertes (7 347 chez le client) alors que la
    # restauration n'en voit que 3 781 : l'écart doit apparaître avec la liste des tables.
    stats = {"total_tables": 3, "tables_saved": 1, "tables_skipped": 1, "tables_error": 1,
             "total_jobs": 0, "total_notebooks": 0, "data_size_gb": 0}
    non_saved = [{"table": "c.b.t", "status": "error", "error": "[PERMISSION_DENIED]"},
                 {"table": "c.rh.salaires", "status": "skipped", "reason": "non clonable", "error": "[DELTA_X]"}]
    html = generate_report(diff=_DIFF, stats=stats, steps=[], non_saved=non_saved)
    assert "Tables découvertes" in html and "Tables sauvegardées" in html
    assert "c.rh.salaires" in html and "[PERMISSION_DENIED]" in html


def test_report_without_clone_counts_still_renders():
    html = generate_report(diff=_DIFF, stats={"total_tables": 2}, steps=[])
    assert "Tables découvertes" in html


def test_report_shows_reason_and_never_an_empty_cause():
    # Cas client : table en erreur avec une cause vide dans le rapport
    non_saved = [{"table": "c.s.droits", "status": "error", "reason": "droits insuffisants du compte de backup",
                  "error": "[PERMISSION_DENIED] x"},
                 {"table": "c.s.muette", "status": "error", "error": ""}]
    html = generate_report(diff=_DIFF, stats={"total_tables": 2}, steps=[], non_saved=non_saved)
    assert "droits insuffisants du compte de backup" in html and "[PERMISSION_DENIED] x" in html
    assert "message d&#39;erreur vide" in html or "message d'erreur vide" in html


from lib.report import top_copied


def test_top_copied_ranks_by_copied_volume_with_share_of_table():
    # Cas client : 240 GB copiés pour 184 tables : voir quelles tables sont recopiées en entier
    manifest = [{"table": "c.s.rewritten", "status": "success", "size_gb": 40.0, "table_size_gb": 42.0},
                {"table": "c.s.append", "status": "success", "size_gb": 2.0, "table_size_gb": 200.0},
                {"table": "c.s.same", "status": "success", "size_gb": 0, "skipped_unchanged": True},
                {"table": "c.s.err", "status": "error", "error": "x"},
                {"table": "c.s.old", "status": "success", "size_gb": 5.0}]           # manifest antérieur à ce correctif
    top = top_copied(manifest)
    assert [t["table"] for t in top] == ["c.s.rewritten", "c.s.old", "c.s.append"]
    assert top[0]["ratio_pct"] == 95 and top[2]["ratio_pct"] == 1
    assert top[1]["table_gb"] is None and top[1]["ratio_pct"] is None
    assert len(top_copied([{"table": f"t{i}", "status": "success", "size_gb": i + 1} for i in range(30)])) == 20
    html = generate_report(diff=_DIFF, stats={"total_tables": 3}, steps=[], top=top)
    assert "Plus gros volumes copiés" in html and "95 %" in html and "c.s.rewritten" in html
