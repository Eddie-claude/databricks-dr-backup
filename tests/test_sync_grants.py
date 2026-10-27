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


def test_group_rules_identical_to_coverage_report():
    # Les deux notebooks sont livrés autonomes : mêmes règles de groupes, copie à l'identique
    cov = (Path(__file__).resolve().parent.parent / "notebooks" / "diag_03_backup_coverage.py").read_text(encoding="utf-8")
    def block(src):
        start = src.index("def is_application_id")
        end = src.index('for n in sorted(set(names))) + ")"', start)
        return src[start:src.index("\n", end)]
    assert block(SRC) == block(cov)


def test_group_closure_and_grantees():
    parents = {"g1": [("p1", "p_prd_datascience_lab_select")], "p1": []}
    groups = R["group_closure"]([("g1", "g_sps_it_dr_backup_datalab")], lambda gid: parents[gid])
    assert R["sql_in_list"]({"sp", "account users", *groups.values()}) == \
        "('account users', 'g_sps_it_dr_backup_datalab', 'p_prd_datascience_lab_select', 'sp')"
