# tests/test_coverage_rules.py
"""
Règles du rapport de couverture (notebooks/diag_03_backup_coverage.py). Le notebook est
autonome (envoyé au client) : le bloc de règles pures est extrait du source entre ses marqueurs.
"""
from pathlib import Path

import pytest

SRC = (Path(__file__).resolve().parent.parent / "notebooks" / "diag_03_backup_coverage.py").read_text(encoding="utf-8")
BLOCK = SRC.split("# >>> REGLES")[1].split("# <<< REGLES")[0]
R = {}
exec(BLOCK, R)


# ── Droits du principal de backup ────────────────────────────────────────

def test_privileges_are_normalized():
    assert R["priv_set"](["USE CATALOG", "use_schema", "ALL PRIVILEGES"]) == {"USE_CATALOG", "USE_SCHEMA", "ALL_PRIVILEGES"}


@pytest.mark.parametrize("kind,obj,expected", [
    ("TABLE",    {"SELECT"},        True),
    ("TABLE",    {"MODIFY"},        False),
    ("TABLE",    {"ALL_PRIVILEGES"}, True),
    ("VOLUME",   {"READ_VOLUME"},   True),
    ("VOLUME",   {"SELECT"},        False),
    ("FUNCTION", {"EXECUTE"},       True),
])
def test_object_privilege_required(kind, obj, expected):
    assert R["can_access"](kind, {"USE_CATALOG"}, {"USE_SCHEMA"}, obj) is expected


def test_use_catalog_and_use_schema_are_required():
    assert R["can_access"]("TABLE", set(), {"USE_SCHEMA"}, {"SELECT"}) is False
    assert R["can_access"]("TABLE", {"USE_CATALOG"}, set(), {"SELECT"}) is False
    assert R["can_access"]("SCHEMA", {"USE_CATALOG"}, {"USE_SCHEMA"}, set()) is True
    assert R["can_access"]("CATALOG", {"ALL_PRIVILEGES"}, set(), set()) is True


# ── As code ──────────────────────────────────────────────────────────────

def test_iac_by_creator_then_by_file():
    ids, names = {"sp-terraform"}, {"prod.raw.landing"}
    assert R["iac_source"]("prod.x.y", "sp-terraform", ids, names) == "créateur"
    assert R["iac_source"]("prod.raw.landing", "someone", ids, names) == "fichier"
    assert R["iac_source"]("prod.x.y", "someone", ids, names) is None


def test_iac_file_names_are_case_and_backtick_insensitive():
    names = R["parse_iac_file"]("# objets Terraform\n`Prod`.`raw`.`Landing`\n\n drp-location \n")
    assert names == {"prod.raw.landing", "drp-location"}


# ── Couverture par type d'objet ──────────────────────────────────────────

@pytest.mark.parametrize("kind,sub,fmt,version,expected", [
    ("CATALOG", "MANAGED_CATALOG",      "", "v4.2", ("SCRIPT", "NA")),
    ("CATALOG", "FOREIGN_CATALOG",      "", "v4.2", ("NONE", "OTHER")),
    ("CATALOG", "DELTASHARING_CATALOG", "", "v4.2", ("NONE", "OTHER")),
    ("SCHEMA",  "",                     "", "v4.2", ("SCRIPT", "NA")),
    ("TABLE",   "MANAGED",  "DELTA",   "v4.1", ("SCRIPT", "SCRIPT")),
    ("TABLE",   "EXTERNAL", "DELTA",   "v4.1", ("SCRIPT", "SCRIPT")),
    ("TABLE",   "EXTERNAL", "CSV",     "v4.2", ("SCRIPT", "NONE")),
    ("TABLE",   "VIEW",     "UNKNOWN_DATA_SOURCE_FORMAT", "v4.2", ("SCRIPT", "NA")),
    ("TABLE",   "MATERIALIZED_VIEW", "", "v4.2", ("SCRIPT", "OTHER")),
    ("TABLE",   "STREAMING_TABLE",   "", "v4.2", ("SCRIPT", "OTHER")),
    ("VOLUME",  "MANAGED",  "", "v4.2", ("SCRIPT", "SCRIPT")),
    ("VOLUME",  "EXTERNAL", "", "v4.2", ("SCRIPT", "OTHER")),
    ("VOLUME",  "MANAGED",  "", "v4.1", ("NONE", "NONE")),
    ("VOLUME",  "EXTERNAL", "", "v4.1", ("NONE", "OTHER")),
    ("FUNCTION", "",        "", "v4.2", ("SCRIPT", "NA")),
    ("FUNCTION", "",        "", "v4.1", ("NONE", "NA")),
    ("EXTERNAL_LOCATION", "", "", "v4.2", ("NONE", "NA")),
    ("MODEL",   "",         "", "v4.2", ("NONE", "NONE")),
    ("JOB",     "",         "", "v4.2", ("SCRIPT", "NA")),
    ("PIPELINE", "",        "", "v4.2", ("NONE", "OTHER")),
])
def test_coverage_rules(kind, sub, fmt, version, expected):
    d, data, _note = R["coverage"](kind, sub, fmt, version)
    assert (d, data) == expected


def test_iac_replaces_missing_definition_but_not_script():
    assert R["apply_iac"]("NONE", "créateur") == "IAC"
    assert R["apply_iac"]("SCRIPT", "créateur") == "SCRIPT"
    assert R["apply_iac"]("NONE", None) == "NONE"


# ── Espace de travail ────────────────────────────────────────────────────

@pytest.mark.parametrize("path,expected", [
    ("/Shared",   ("SCRIPT", "NONE")),
    ("/Users",    ("NONE", "NONE")),
    ("/Repos",    ("OTHER", "NA")),
    ("/Projets",  ("NONE", "NONE")),
])
def test_workspace_root_coverage(path, expected):
    d, data, _ = R["workspace_root_coverage"](path)
    assert (d, data) == expected


def test_pipeline_code_is_covered_only_under_shared_within_depth():
    covered = R["pipeline_code_covered"]
    assert covered(["/Workspace/Shared/etl/ingest", "/Shared/etl/clean"]) is True
    assert covered(["/Shared/a/b/c/d/nb"]) is True          # 5 niveaux sous /Shared : exporté
    assert covered(["/Shared/a/b/c/d/e/nb"]) is False       # 6 niveaux : au-delà de max_depth
    assert covered(["/Shared/etl/x", "/Users/u@x.ch/y"]) is False
    assert covered([]) is False


# ── Verdict ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("kwargs,expected", [
    (dict(def_cov="SCRIPT", data_cov="SCRIPT"), "OK"),
    (dict(def_cov="SCRIPT", data_cov="NA"), "OK"),
    (dict(def_cov="IAC", data_cov="NA"), "AS_CODE"),
    (dict(def_cov="SCRIPT", data_cov="NONE"), "PARTIEL"),
    (dict(def_cov="SCRIPT", data_cov="OTHER"), "AUTRE"),
    (dict(def_cov="OTHER", data_cov="NA"), "AUTRE"),
    (dict(def_cov="NONE", data_cov="OTHER"), "NON_COUVERT"),
    (dict(def_cov="SCRIPT", data_cov="SCRIPT", accessible=False), "INACCESSIBLE"),
    (dict(def_cov="IAC", data_cov="NA", accessible=False), "AS_CODE"),
    (dict(def_cov="SCRIPT", data_cov="SCRIPT", last_status="error"), "ECHEC"),
    (dict(def_cov="SCRIPT", data_cov="SCRIPT", excluded=True), "EXCLU"),
])
def test_verdict(kwargs, expected):
    assert R["verdict"](**kwargs) == expected


# ── Cas relevés à l'exécution simulée ────────────────────────────────────

def test_browse_is_enough_for_volume_and_function_definitions():
    assert R["can_access"]("VOLUME", {"USE_CATALOG", "BROWSE"}, {"USE_SCHEMA"}, set()) is True
    assert R["can_access"]("FUNCTION", {"USE_CATALOG"}, {"USE_SCHEMA"}, {"BROWSE"}) is True
    # mais pas pour lire les données d'une table
    assert R["can_access"]("TABLE", {"USE_CATALOG", "BROWSE"}, {"USE_SCHEMA"}, set()) is False


def test_iac_rescues_inaccessible_object_without_data():
    # Fonction créée par Terraform mais illisible pour le backup : l'IaC la recrée
    assert R["verdict"]("SCRIPT", "NA", accessible=False, iac=True) == "AS_CODE"
    # Table créée par Terraform mais illisible : ses DONNÉES ne sont pas sauvegardées
    assert R["verdict"]("SCRIPT", "SCRIPT", accessible=False, iac=True) == "INACCESSIBLE"


def test_pipeline_files_are_not_covered_only_notebooks():
    covered = R["pipeline_code_covered"]
    assert covered(["/Shared/etl/nb"], has_files=True) is False
    assert covered(["/Shared/etl/nb"], has_files=False) is True
