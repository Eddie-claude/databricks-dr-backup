# tests/test_export_workspace.py
import json
import pytest
from scripts.export_workspace import build_export_commands, write_json_asset


def test_build_export_commands_contains_workspace_export():
    cmds = build_export_commands(backup_path="/tmp/backup/2026-04-02/workspace")
    assert any("workspace" in " ".join(c) and "export" in " ".join(c) for c in cmds)


def test_write_json_asset(tmp_path):
    data = {"id": 1, "name": "test-job"}
    out_file = tmp_path / "jobs.json"
    write_json_asset(data, str(out_file))
    assert json.loads(out_file.read_text()) == data


def test_write_json_asset_creates_parent_dirs(tmp_path):
    data = {"key": "value"}
    out_file = tmp_path / "nested" / "dir" / "output.json"
    write_json_asset(data, str(out_file))
    assert out_file.exists()
    assert json.loads(out_file.read_text()) == data
