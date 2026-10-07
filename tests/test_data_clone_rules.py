# tests/test_data_clone_rules.py
"""Règles pures de 02_data_clone, extraites du notebook entre leurs marqueurs."""
from pathlib import Path

SRC = (Path(__file__).resolve().parent.parent / "notebooks" / "02_data_clone.py").read_text(encoding="utf-8")
NS = {}
exec(SRC.split("# >>> REGLES CLONE")[1].split("# <<< REGLES CLONE")[0], NS)
should_flush, short = NS["should_flush"], NS["_short"]


def test_flush_every_50_tables():
    assert should_flush(pending=50, last_flush=0, now=1) is True
    assert should_flush(pending=49, last_flush=0, now=1) is False


def test_flush_after_two_minutes_even_with_few_tables():
    # Run lent (grosses tables) : la reprise ne doit pas perdre plus de 2 minutes de travail
    assert should_flush(pending=3, last_flush=0, now=121) is True
    assert should_flush(pending=0, last_flush=0, now=999) is False


def test_error_message_drops_jvm_stacktrace_and_is_bounded():
    err = Exception("[DELTA_X] boom\nUser does not have SELECT.\n\nJVM stacktrace:\norg.apache\n\tat x" + "y" * 5000)
    assert short(err) == "[DELTA_X] boom User does not have SELECT."
    assert len(short(Exception("z" * 5000))) == 2000


# ── DEEP CLONE refusé ────────────────────────────────────────────────────

clone_failure = NS["clone_failure"]


def test_unsupported_source_is_skipped_but_keeps_its_error_and_says_data_not_saved():
    # Une table à filtre de lignes arrivait ici avec la raison « vue ou format non cloneable » :
    # ses données n'étaient pas sauvegardées, sans que rien ne le dise.
    err = "[DELTA_CLONE_UNSUPPORTED_SOURCE] Unsupported clone source 'c.rh.salaires'"
    entry = clone_failure(err)
    assert entry["status"] == "skipped"
    assert entry["error"] == err
    assert "NON sauvegardées" in entry["reason"]


def test_other_failures_are_errors():
    assert clone_failure("[PERMISSION_DENIED] User does not have SELECT") == {
        "status": "error", "error": "[PERMISSION_DENIED] User does not have SELECT"}
