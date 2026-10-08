# tests/test_notebook_structure.py
"""Structure des notebooks au format source Databricks.

Une ligne « # MAGIC %md » dans une cellule de code n'est pas une erreur Python (c'est un
commentaire) : py_compile ne la voit pas, Databricks l'exécute comme magic et échoue
(« Line magic function `%md` not found »).
"""
from pathlib import Path

import pytest

NOTEBOOKS = sorted((Path(__file__).resolve().parent.parent / "notebooks").rglob("*.py"))
SEPARATOR = "# COMMAND ----------"


def _cells(path: Path) -> list:
    text = path.read_text(encoding="utf-8")
    return [c for c in text.split(SEPARATOR)[1:]]


@pytest.mark.parametrize("path", NOTEBOOKS, ids=lambda p: p.relative_to(p.parents[1]).as_posix())
def test_magic_cells_contain_only_magic_lines(path):
    if not path.read_text(encoding="utf-8").startswith("# Databricks notebook source"):
        pytest.skip("pas un notebook Databricks")
    for i, cell in enumerate(_cells(path), start=1):
        lines = [l for l in cell.splitlines() if l.strip()]
        if any(l.startswith("# MAGIC") for l in lines):
            stray = [l for l in lines if not l.startswith("# MAGIC")]
            assert not stray, f"cellule {i} : magic mêlée à du code ({stray[0][:80]!r})"
