# tests/test_sync_grants.py
"""Règles de notebooks/admin_sync_backup_grants.py, extraites du notebook entre leurs marqueurs."""
from pathlib import Path

SRC = (Path(__file__).resolve().parent.parent / "notebooks" / "admin_sync_backup_grants.py").read_text(encoding="utf-8")
R = {}
exec(SRC.split("# >>> REGLES")[1].split("# <<< REGLES")[0], R)


def test_missing_privileges():
    required = {"USE_CATALOG", "USE_SCHEMA", "SELECT"}
    assert R["missing_privileges"](required, {"USE_CATALOG"}) == ["SELECT", "USE_SCHEMA"]
    assert R["missing_privileges"](required, {"USE_CATALOG", "USE_SCHEMA", "SELECT", "MODIFY"}) == []


def test_all_privileges_covers_everything():
    assert R["missing_privileges"]({"USE_CATALOG", "SELECT"}, {"ALL_PRIVILEGES"}) == []


def test_grant_statement_uses_sql_privilege_names():
    assert R["grant_statement"]("CATALOG", "prod", ["READ_VOLUME", "USE_CATALOG"], "dr-backup-readers") == \
        "GRANT READ VOLUME, USE CATALOG ON CATALOG `prod` TO `dr-backup-readers`"
    assert R["grant_statement"]("EXTERNAL LOCATION", "drp-loc", ["WRITE_FILES"], "dr-backup-readers") == \
        "GRANT WRITE FILES ON EXTERNAL LOCATION `drp-loc` TO `dr-backup-readers`"


def test_parse_privileges_normalizes():
    assert R["parse_privileges"]("use catalog, USE_SCHEMA ,select") == {"USE_CATALOG", "USE_SCHEMA", "SELECT"}


def test_system_catalog_use_is_required():
    # Sans USE CATALOG sur system, information_schema de chaque catalog est illisible : 01 ne
    # connaissait plus le type des tables et envoyait les vues au clone (4 295 chez le client).
    assert R["SYSTEM_CATALOG_PRIVILEGES"] == {"USE_CATALOG"}
    assert R["grant_statement"]("CATALOG", "system", ["USE_CATALOG"], "sp-backup") == \
        "GRANT USE CATALOG ON CATALOG `system` TO `sp-backup`"
