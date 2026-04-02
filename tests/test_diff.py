# tests/test_diff.py
import pytest
from lib.diff import compute_diff, BackupManifest

def make_manifest(date, tables, jobs, notebooks):
    return BackupManifest(
        date=date,
        tables=tables,
        jobs=jobs,
        notebooks=notebooks,
    )

def test_diff_no_changes():
    prev = make_manifest("2026-04-01", {"cat.sch.t1"}, {"job1"}, {"nb1"})
    curr = make_manifest("2026-04-02", {"cat.sch.t1"}, {"job1"}, {"nb1"})
    diff = compute_diff(prev, curr)
    assert diff["tables"]["added"] == []
    assert diff["tables"]["removed"] == []
    assert diff["jobs"]["added"] == []
    assert diff["notebooks"]["removed"] == []

def test_diff_added_table():
    prev = make_manifest("2026-04-01", {"cat.sch.t1"}, set(), set())
    curr = make_manifest("2026-04-02", {"cat.sch.t1", "cat.sch.t2"}, set(), set())
    diff = compute_diff(prev, curr)
    assert diff["tables"]["added"] == ["cat.sch.t2"]
    assert diff["tables"]["removed"] == []

def test_diff_removed_job():
    prev = make_manifest("2026-04-01", set(), {"job1", "job2"}, set())
    curr = make_manifest("2026-04-02", set(), {"job1"}, set())
    diff = compute_diff(prev, curr)
    assert diff["jobs"]["removed"] == ["job2"]
    assert diff["jobs"]["added"] == []

def test_diff_from_dicts():
    prev = {"date": "2026-04-01", "tables": ["a", "b"], "jobs": ["j1"], "notebooks": []}
    curr = {"date": "2026-04-02", "tables": ["a", "c"], "jobs": ["j1", "j2"], "notebooks": ["nb1"]}
    diff = compute_diff(
        BackupManifest.from_dict(prev),
        BackupManifest.from_dict(curr),
    )
    assert "b" not in diff["tables"]["added"]
    assert "b" in diff["tables"]["removed"]
    assert "c" in diff["tables"]["added"]
    assert "j2" in diff["jobs"]["added"]
    assert "nb1" in diff["notebooks"]["added"]
