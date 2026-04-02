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
