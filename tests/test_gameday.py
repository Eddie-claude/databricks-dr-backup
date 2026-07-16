# tests/test_gameday.py
from lib.gameday import compare_row_counts


def test_compare_row_counts_all_match():
    expected = {"cat.sch.clients": 5, "cat.sch.transactions": 6}
    actual = {"cat.sch.clients": 5, "cat.sch.transactions": 6}
    assert compare_row_counts(expected, actual) == []


def test_compare_row_counts_mismatch():
    expected = {"cat.sch.clients": 5}
    actual = {"cat.sch.clients": 4}
    result = compare_row_counts(expected, actual)
    assert result == ["cat.sch.clients: 4 lignes après restauration, 5 attendues"]


def test_compare_row_counts_missing_table():
    expected = {"cat.sch.clients": 5, "cat.sch.transactions": 6}
    actual = {"cat.sch.clients": 5}
    result = compare_row_counts(expected, actual)
    assert result == ["cat.sch.transactions: table manquante après restauration (attendu 6 lignes)"]


def test_compare_row_counts_ignores_extra_tables_in_actual():
    expected = {"cat.sch.clients": 5}
    actual = {"cat.sch.clients": 5, "cat.sch.unrelated": 99}
    assert compare_row_counts(expected, actual) == []


def test_compare_row_counts_multiple_mismatches_sorted_by_table_name():
    expected = {"cat.sch.b": 2, "cat.sch.a": 1}
    actual = {"cat.sch.b": 99, "cat.sch.a": 99}
    result = compare_row_counts(expected, actual)
    assert result == [
        "cat.sch.a: 99 lignes après restauration, 1 attendues",
        "cat.sch.b: 99 lignes après restauration, 2 attendues",
    ]
