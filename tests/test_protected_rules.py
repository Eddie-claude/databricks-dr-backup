# tests/test_protected_rules.py
"""Règles de diag_04_protected_tables, extraites du notebook entre leurs marqueurs."""
from pathlib import Path

SRC = (Path(__file__).resolve().parent.parent / "notebooks" / "diag_04_protected_tables.py").read_text(encoding="utf-8")
R = {}
exec(SRC.split("# >>> REGLES")[1].split("# <<< REGLES")[0], R)


# ── Valeurs de test par type de colonne ──────────────────────────────────

def test_literals_by_type():
    assert R["test_literals"]("string")[:2] == ["'dr_test_a'", "'dr_test_b'"]
    assert R["test_literals"]("INT") == ["0", "-1", "123456"]
    assert R["test_literals"]("decimal(10,2)")[0] == "CAST(0 AS decimal(10,2))"
    assert R["test_literals"]("date") == ["DATE'1970-01-01'", "DATE'2999-12-31'"]
    assert R["test_literals"]("boolean") == ["true", "false"]


def test_unknown_type_is_tested_with_null_only():
    assert R["test_literals"]("struct<a:int>") == ["NULL"]


def test_calls_cover_every_value_and_a_null_row():
    calls = R["test_calls"](["string", "int"])
    assert ["'dr_test_a'", "0"] in calls and ["NULL", "NULL"] in calls
    assert len(calls) == 4          # 3 combinaisons (liste la plus longue) + ligne NULL


# ── Verdicts ─────────────────────────────────────────────────────────────

def test_filter_true_for_every_value_means_all_rows():
    assert R["filter_verdict"]([True, True, True]) == "TOUTES"


def test_filter_false_or_null_somewhere_means_partial():
    # NULL renvoyé par le filtre = ligne masquée (cas d'une colonne NULL non exemptée)
    assert R["filter_verdict"]([True, False]) == "PARTIELLE"
    assert R["filter_verdict"]([True, None]) == "PARTIELLE"


def test_filter_error_is_undetermined():
    assert R["filter_verdict"]([True, "ERR"]) == "INDETERMINE"
    assert R["filter_verdict"]([]) == "INDETERMINE"


def test_mask_verdicts():
    assert R["mask_verdict"]([True, True]) == "INTACTES"
    assert R["mask_verdict"]([True, False]) == "MASQUEES"
    assert R["mask_verdict"](["ERR"]) == "INDETERMINE"


def test_table_verdict_combines_filter_and_masks():
    tv = R["table_verdict"]
    assert tv("TOUTES", ["INTACTES"]) == "COMPLETE"
    assert tv(None, ["INTACTES", "INTACTES"]) == "COMPLETE"
    assert tv("PARTIELLE", ["INTACTES"]) == "LIGNES_FILTREES"
    assert tv("TOUTES", ["MASQUEES"]) == "VALEURS_MASQUEES"
    assert tv("PARTIELLE", ["MASQUEES"]) == "LIGNES_FILTREES"
    assert tv("TOUTES", ["INDETERMINE"]) == "INDETERMINE"
