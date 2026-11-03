# tests/test_workspace_config_rules.py
"""Règles de notebooks/05_workspace_config.py, extraites du notebook entre leurs marqueurs."""
from pathlib import Path

SRC = (Path(__file__).resolve().parent.parent / "notebooks" / "05_workspace_config.py").read_text(encoding="utf-8")
R = {}
exec(SRC.split("# >>> REGLES WORKSPACE")[1].split("# <<< REGLES WORKSPACE")[0], R)
is_excluded, is_workspace_admin = R["is_excluded"], R["is_workspace_admin"]

NAMES = {".db_internal", ".ide"}


def test_technical_folders_are_excluded_at_any_depth():
    # Cas client : PERMISSION_DENIED sur /Users/<u>/.db_internal ; idem /Users/<u>/.ide
    assert is_excluded("/Users/favrel@groupemutuel.ch/.db_internal", ["/Repos"], NAMES)
    assert is_excluded("/Users/u/.ide/state/x", ["/Repos"], NAMES)
    assert is_excluded("/Shared/projet/.ide", ["/Repos"], NAMES)


def test_regular_paths_are_kept():
    assert not is_excluded("/Users/u/notebook", ["/Repos"], NAMES)
    assert not is_excluded("/Users/u/.db_internal_backup", ["/Repos"], NAMES)   # nom différent
    assert not is_excluded("/Users/u/my.ide.notes", ["/Repos"], NAMES)


def test_exclude_paths_still_apply():
    assert is_excluded("/Repos", ["/Repos"], NAMES)
    assert is_excluded("/Repos/u/my_repo", ["/Repos"], NAMES)
    assert not is_excluded("/Reposition", ["/Repos"], NAMES)
    assert not is_excluded("/Users/u/.ide", ["/Repos"], set())                  # exclusion désactivable


def test_workspace_admin_is_read_from_scim_me():
    assert is_workspace_admin({"groups": [{"display": "users"}, {"display": "admins"}]}) is True
    assert is_workspace_admin({"groups": [{"display": "users"}]}) is False
    assert is_workspace_admin({}) is False and is_workspace_admin(None) is False
