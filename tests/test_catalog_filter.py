# tests/test_catalog_filter.py
"""
Le filtre des catalogs non sauvegardables est dupliqué dans les notebooks (diag_01_audit doit
rester autonome pour être envoyé au client) : on teste chaque copie, extraite du source.
"""
import re
from pathlib import Path

import pytest

NOTEBOOKS = ["notebooks/01_uc_metadata.py", "notebooks/02_data_clone.py", "notebooks/diag_01_audit.py"]
ROOT = Path(__file__).resolve().parent.parent


def _load(nb: str):
    src = (ROOT / nb).read_text(encoding="utf-8")
    m = re.search(r"^NON_BACKUP_CATALOG_TYPES = .*?^def _split_backup_catalogs\(.*?(?=^\S)", src, re.S | re.M)
    assert m, f"filtre de catalogs absent de {nb}"
    ns = {}
    exec(m.group(0), ns)
    return ns["_split_backup_catalogs"]


@pytest.mark.parametrize("nb", NOTEBOOKS)
def test_foreign_sharing_system_catalogs_are_skipped(nb):
    split = _load(nb)
    kept, skipped = split(
        ["prod", "sqlserver_prd", "partage", "system", "__databricks_internal"],
        {"prod": "MANAGED_CATALOG", "sqlserver_prd": "FOREIGN_CATALOG",
         "partage": "DELTASHARING_CATALOG", "system": "SYSTEM_CATALOG",
         "__databricks_internal": "INTERNAL_CATALOG"},
    )
    assert kept == ["prod"]
    assert skipped == {"sqlserver_prd": "FOREIGN_CATALOG", "partage": "DELTASHARING_CATALOG",
                       "system": "SYSTEM_CATALOG", "__databricks_internal": "INTERNAL_CATALOG"}


@pytest.mark.parametrize("nb", NOTEBOOKS)
def test_unknown_or_missing_type_is_kept(nb):
    # Un type inconnu (nouveau type Databricks) ou absent (API indisponible) est sauvegardé :
    # mieux vaut un [WARN] de plus qu'un catalog oublié.
    kept, skipped = _load(nb)(["a", "b"], {"a": "SOME_NEW_CATALOG"})
    assert kept == ["a", "b"]
    assert skipped == {}
