# tests/test_uc_ddl.py
from lib.uc_ddl import build_function_ddl, build_volume_ddl, split_sql_statements


# ── Volumes ──────────────────────────────────────────────────────────────

def test_managed_volume_ddl():
    ddl = build_volume_ddl({
        "volume_catalog": "prod", "volume_schema": "raw", "volume_name": "landing",
        "volume_type": "MANAGED", "storage_location": "abfss://x@y/__unitystorage/v1",
        "comment": None,
    })
    assert ddl == "CREATE VOLUME IF NOT EXISTS `prod`.`raw`.`landing`"


def test_external_volume_ddl_keeps_location_and_escapes_comment():
    ddl = build_volume_ddl({
        "volume_catalog": "prod", "volume_schema": "raw", "volume_name": "ext",
        "volume_type": "EXTERNAL", "storage_location": "abfss://c@acc.dfs.core.windows.net/ext",
        "comment": "fichiers d'entrée",
    })
    assert ddl == (
        "CREATE EXTERNAL VOLUME IF NOT EXISTS `prod`.`raw`.`ext`\n"
        "  LOCATION 'abfss://c@acc.dfs.core.windows.net/ext'\n"
        "  COMMENT 'fichiers d\\'entrée'"
    )


# ── Fonctions ────────────────────────────────────────────────────────────

def _routine(**kw):
    base = {
        "routine_catalog": "prod", "routine_schema": "fn", "routine_name": "f",
        "data_type": "INT", "full_data_type": "INT",
        "routine_body": "SQL", "routine_definition": "x + 1",
        "external_language": None, "is_deterministic": "YES",
        "sql_data_access": "CONTAINS SQL", "comment": None,
    }
    base.update(kw)
    return base


def test_sql_scalar_function_ddl():
    ddl = build_function_ddl(
        _routine(),
        params=[{"ordinal_position": 0, "parameter_name": "x", "full_data_type": "INT",
                 "parameter_default": None, "comment": None}],
    )
    assert ddl == (
        "CREATE FUNCTION IF NOT EXISTS `prod`.`fn`.`f`(x INT)\n"
        "  RETURNS INT\n"
        "  DETERMINISTIC\n"
        "  CONTAINS SQL\n"
        "  RETURN x + 1"
    )


def test_parameters_are_ordered_and_keep_default_and_comment():
    ddl = build_function_ddl(
        _routine(routine_definition="a * b"),
        params=[
            {"ordinal_position": 1, "parameter_name": "b", "full_data_type": "INT",
             "parameter_default": "2", "comment": "facteur"},
            {"ordinal_position": 0, "parameter_name": "a", "full_data_type": "INT",
             "parameter_default": None, "comment": None},
        ],
    )
    assert "`f`(a INT, b INT DEFAULT 2 COMMENT 'facteur')" in ddl


def test_python_function_uses_dollar_quoting_and_language():
    ddl = build_function_ddl(
        _routine(routine_body="EXTERNAL", external_language="PYTHON",
                 routine_definition="return s.upper(); # ok", is_deterministic="NO",
                 sql_data_access="NO SQL", full_data_type="STRING", data_type="STRING",
                 comment="majuscules"),
        params=[{"ordinal_position": 0, "parameter_name": "s", "full_data_type": "STRING",
                 "parameter_default": None, "comment": None}],
    )
    assert ddl == (
        "CREATE FUNCTION IF NOT EXISTS `prod`.`fn`.`f`(s STRING)\n"
        "  RETURNS STRING\n"
        "  LANGUAGE PYTHON\n"
        "  NOT DETERMINISTIC\n"
        "  COMMENT 'majuscules'\n"
        "  AS $$\nreturn s.upper(); # ok\n$$"
    )


def test_python_body_containing_dollar_dollar_uses_tagged_quote():
    ddl = build_function_ddl(
        _routine(routine_body="EXTERNAL", external_language="PYTHON",
                 routine_definition="return '$$'", sql_data_access="NO SQL"),
        params=[],
    )
    assert ddl.endswith("AS $py$\nreturn '$$'\n$py$")


def test_table_function_returns_table_columns():
    ddl = build_function_ddl(
        _routine(data_type="TABLE_TYPE", full_data_type=None,
                 routine_definition="SELECT id, name FROM prod.raw.t",
                 sql_data_access="READS SQL DATA"),
        params=[],
        return_columns=[
            {"ordinal_position": 1, "column_name": "name", "full_data_type": "STRING", "comment": None},
            {"ordinal_position": 0, "column_name": "id", "full_data_type": "BIGINT", "comment": None},
        ],
    )
    assert "`f`()\n  RETURNS TABLE (id BIGINT, name STRING)\n" in ddl
    assert ddl.endswith("READS SQL DATA\n  RETURN SELECT id, name FROM prod.raw.t")


def test_function_without_definition_returns_none():
    # routine_definition NULL = l'export n'a pas le droit de lire le corps
    assert build_function_ddl(_routine(routine_definition=None), params=[]) is None


# ── Découpage des fichiers SQL ───────────────────────────────────────────

def test_split_simple_statements():
    assert split_sql_statements("CREATE A;\n\nCREATE B;\n") == ["CREATE A", "CREATE B"]


def test_split_ignores_semicolons_in_strings_and_dollar_blocks():
    content = (
        "CREATE VOLUME v COMMENT 'a;b';\n\n"
        "CREATE FUNCTION f() RETURNS INT LANGUAGE PYTHON AS $$\nx = 1; return x\n$$;\n\n"
        "CREATE FUNCTION g() RETURNS STRING LANGUAGE PYTHON AS $py$\nreturn '$$;'\n$py$;\n"
    )
    stmts = split_sql_statements(content)
    assert len(stmts) == 3
    assert stmts[1].endswith("x = 1; return x\n$$")
    assert stmts[2].endswith("return '$$;'\n$py$")


def test_split_handles_escaped_quote_and_comments():
    # Les lignes de commentaire en tête de statement sont retirées : restore_uc.py
    # filtre sur le premier mot (CREATE/GRANT...) et bloquerait un statement "--".
    content = "CREATE X COMMENT 'it\\'s; fine';\n-- note ; ici\nCREATE Y /* a;b */;\n"
    assert split_sql_statements(content) == [
        "CREATE X COMMENT 'it\\'s; fine'",
        "CREATE Y /* a;b */",
    ]


def test_split_drops_comment_only_chunks():
    assert split_sql_statements("-- en-tête\n;\nCREATE A;") == ["CREATE A"]


def test_split_keeps_trailing_statement_without_semicolon():
    assert split_sql_statements("CREATE A;\nCREATE B") == ["CREATE A", "CREATE B"]


# ── Aller-retour export → fichier → restore ──────────────────────────────

def test_generated_ddls_survive_file_round_trip():
    # Même format que 01_uc_metadata : statements suffixés ';' et séparés par une ligne vide
    ddls = [
        build_volume_ddl({"volume_catalog": "c", "volume_schema": "s", "volume_name": "v",
                          "volume_type": "EXTERNAL", "storage_location": "abfss://a@b/p;q",
                          "comment": "x; y"}),
        build_function_ddl(
            _routine(routine_body="EXTERNAL", external_language="PYTHON",
                     routine_definition="a = 1; b = ';'\n\nreturn a  # it's done;",
                     sql_data_access="NO SQL"),
            params=[]),
        build_function_ddl(_routine(routine_definition="CASE WHEN x = ';' THEN 1 END"), params=[]),
    ]
    content = "\n\n".join(d + ";" for d in ddls)
    assert split_sql_statements(content) == ddls


def test_restore_uc_replays_grants_after_volumes_and_functions():
    from scripts.restore_uc import SQL_FILES_ORDER
    assert SQL_FILES_ORDER[-1] == "04_grants.sql"
    assert SQL_FILES_ORDER.index("05_volumes.sql") > SQL_FILES_ORDER.index("02_schemas.sql")
    assert SQL_FILES_ORDER.index("06_functions.sql") > SQL_FILES_ORDER.index("03_tables.sql")


def test_information_schema_real_value_formats():
    # Valeurs observées sur Databricks (system.information_schema.routines) :
    # is_deterministic = 'true'/'false', sql_data_access = 'CONTAINS_SQL' / 'READS_SQL_DATA'
    ddl = build_function_ddl(
        _routine(is_deterministic="true", sql_data_access="READS_SQL_DATA"), params=[])
    assert "\n  DETERMINISTIC\n" in ddl
    assert "\n  READS SQL DATA\n" in ddl

    ddl = build_function_ddl(
        _routine(is_deterministic="false", sql_data_access="CONTAINS_SQL"), params=[])
    assert "\n  NOT DETERMINISTIC\n" in ddl
    assert "\n  CONTAINS SQL\n" in ddl


def test_restore_uc_only_grants_runs_grants_file_alone():
    from scripts.restore_uc import files_to_run, SQL_FILES_ORDER
    assert files_to_run(only_grants=True) == ["04_grants.sql"]
    assert files_to_run(only_grants=False) == SQL_FILES_ORDER


def test_python_body_does_not_grow_across_backup_restore_cycles():
    # Databricks renvoie le corps avec les sauts de ligne qui entouraient $$…$$ (constaté sur dev) :
    # sans nettoyage, chaque cycle backup → restore ajoute une ligne vide au début et à la fin.
    stored = "\n# commentaire avec ; et '\nr = s.upper()\nreturn r\n"
    ddl = build_function_ddl(
        _routine(routine_body="EXTERNAL", external_language="PYTHON", routine_definition=stored,
                 sql_data_access="NO SQL"),
        params=[])
    assert ddl.endswith("AS $$\n# commentaire avec ; et '\nr = s.upper()\nreturn r\n$$")


def test_python_body_keeps_its_indentation():
    stored = "\n    x = 1\n    return x\n"
    ddl = build_function_ddl(
        _routine(routine_body="EXTERNAL", external_language="PYTHON", routine_definition=stored,
                 sql_data_access="NO SQL"),
        params=[])
    assert "AS $$\n    x = 1\n    return x\n$$" in ddl
