# tests/test_restore_rules.py
"""Règles de sélection de 07_restore, extraites du notebook entre leurs marqueurs."""
from pathlib import Path

SRC = (Path(__file__).resolve().parent.parent / "notebooks" / "07_restore.py").read_text(encoding="utf-8")
NS = {}
exec(SRC.split("# >>> REGLES RESTORE")[1].split("# <<< REGLES RESTORE")[0], NS)
pick_manifest_date, select_restorable = NS["pick_manifest_date"], NS["select_restorable"]

DATES = ["2026-10-05", "2026-10-06", "2026-10-08"]


def test_latest_manifest_without_restore_point():
    assert pick_manifest_date(DATES, "") == "2026-10-08"


def test_last_manifest_on_or_before_the_restore_point():
    assert pick_manifest_date(DATES, "2026-10-07T12:00:00") == "2026-10-06"
    assert pick_manifest_date(DATES, "2026-10-06") == "2026-10-06"


def test_no_manifest_before_the_restore_point():
    assert pick_manifest_date(DATES, "2026-10-01") is None
    assert pick_manifest_date([], "") is None


def _t(fqn):
    c, s, t = fqn.split(".")
    return {"catalog": c, "schema": s, "table": t, "path": f"root/{c}/{s}/{t}"}


def test_only_tables_saved_on_the_reference_date_are_restored():
    # Les dossiers d'incremental/ laissés par d'anciens runs (table en erreur, non clonable ou
    # supprimée depuis) étaient restaurés avec des données périmées : 3 781 au lieu de 3 044.
    tables = [_t("c.s.ok"), _t("c.s.inchangee"), _t("c.s.erreur"), _t("c.s.supprimee")]
    manifest = [{"table": "c.s.ok", "status": "success"},
                {"table": "c.s.inchangee", "status": "success", "skipped_unchanged": True},
                {"table": "c.s.erreur", "status": "error"}]
    selected, stale = select_restorable(tables, manifest)
    assert [t["table"] for t in selected] == ["ok", "inchangee"]
    assert {t["table"]: t["reason"] for t in stale} == {
        "erreur": "error dans le manifest", "supprimee": "absente du manifest"}
