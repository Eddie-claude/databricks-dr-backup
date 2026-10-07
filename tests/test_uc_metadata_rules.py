# tests/test_uc_metadata_rules.py
"""Règles de 01_uc_metadata sur le type des tables, extraites du notebook entre leurs marqueurs."""
from pathlib import Path

SRC = (Path(__file__).resolve().parent.parent / "notebooks" / "01_uc_metadata.py").read_text(encoding="utf-8")
NS = {}
exec(SRC.split("# >>> REGLES TABLES")[1].split("# <<< REGLES TABLES")[0], NS)
cloneable_names, api_table_types = NS["cloneable_names"], NS["api_table_types"]


def test_only_managed_and_external_tables_are_cloneable():
    types = {"t1": "MANAGED", "t2": "EXTERNAL", "v1": "VIEW", "mv": "MATERIALIZED_VIEW",
             "st": "STREAMING_TABLE", "f": "FOREIGN"}
    assert cloneable_names(types) == {"t1", "t2"}


def test_api_table_types_follows_pagination():
    # Repli quand information_schema est illisible : sans lui, les vues partaient au clone
    pages = {"": {"tables": [{"name": "t1", "table_type": "MANAGED"}], "next_page_token": "p2"},
             "p2": {"tables": [{"name": "v1", "table_type": "VIEW"}]}}
    calls = []

    def get(path, params):
        calls.append((path, dict(params)))
        return pages[params.get("page_token", "")]

    assert api_table_types(get, "cat", "sch") == {"t1": "MANAGED", "v1": "VIEW"}
    assert calls[0][0] == "/api/2.1/unity-catalog/tables"
    assert calls[0][1]["catalog_name"] == "cat" and calls[0][1]["schema_name"] == "sch"
