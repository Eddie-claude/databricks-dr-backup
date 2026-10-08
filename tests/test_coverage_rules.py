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
    # v4.1 : jobs sauvegardés sans leurs tâches (expand_tasks ignoré) → recréés vides
    ("JOB",     "",         "", "v4.1", ("NONE", "NA")),
    ("PIPELINE", "",        "", "v4.2", ("SCRIPT", "OTHER")),
    ("PIPELINE", "",        "", "v4.1", ("NONE", "OTHER")),
    # metric views : SHOW CREATE TABLE refusé sur le runtime des jobs en v4.1, DDL reconstruite en v4.2
    ("TABLE",   "METRIC_VIEW", "", "v4.2", ("SCRIPT", "NA")),
    ("TABLE",   "METRIC_VIEW", "", "v4.1", ("NONE", "NA")),
])
def test_coverage_rules(kind, sub, fmt, version, expected):
    d, data, _note = R["coverage"](kind, sub, fmt, version)
    assert (d, data) == expected


def test_table_with_row_filter_or_column_mask_has_no_data_coverage():
    # DEEP CLONE refuse les tables protégées : leurs données ne sont jamais sauvegardées
    d, data, note = R["coverage"]("TABLE", "MANAGED", "DELTA", "v4.2", protected=True)
    assert (d, data) == ("SCRIPT", "NONE")
    assert "filtre" in note.lower()


def test_system_catalog_use_is_required_for_information_schema():
    assert R["missing_system_access"](set()) is True
    assert R["missing_system_access"]({"USE_CATALOG"}) is False
    assert R["missing_system_access"]({"ALL_PRIVILEGES"}) is False


def test_iac_replaces_missing_definition_but_not_script():
    assert R["apply_iac"]("NONE", "créateur") == "IAC"
    assert R["apply_iac"]("SCRIPT", "créateur") == "SCRIPT"
    assert R["apply_iac"]("NONE", None) == "NONE"


# ── Espace de travail ────────────────────────────────────────────────────

@pytest.mark.parametrize("path,version,expected", [
    ("/Shared",   "v4.1", ("SCRIPT", "NONE")),
    ("/Users",    "v4.1", ("NONE", "NONE")),
    ("/Repos",    "v4.1", ("OTHER", "NA")),
    ("/Projets",  "v4.1", ("NONE", "NONE")),
    # v4.2 : tout le workspace (notebooks, fichiers, dashboards), toute profondeur, hors /Repos
    ("/Shared",   "v4.2", ("SCRIPT", "NA")),
    ("/Users",    "v4.2", ("SCRIPT", "NA")),
    ("/Projets",  "v4.2", ("SCRIPT", "NA")),
    ("/Repos",    "v4.2", ("OTHER", "NA")),
])
def test_workspace_root_coverage(path, version, expected):
    d, data, _ = R["workspace_root_coverage"](path, version)
    assert (d, data) == expected


def test_pipeline_code_is_covered_only_under_shared_within_depth_in_v41():
    covered = R["pipeline_code_covered"]
    assert covered(["/Workspace/Shared/etl/ingest", "/Shared/etl/clean"], version="v4.1") is True
    assert covered(["/Shared/a/b/c/d/nb"], version="v4.1") is True          # 5 niveaux : exporté
    assert covered(["/Shared/a/b/c/d/e/nb"], version="v4.1") is False       # 6 niveaux : au-delà
    assert covered(["/Shared/etl/x", "/Users/u@x.ch/y"], version="v4.1") is False
    assert covered([], version="v4.1") is False


def test_pipeline_code_is_covered_anywhere_but_repos_in_v42():
    covered = R["pipeline_code_covered"]
    assert covered(["/Users/u@x.ch/a/b/c/d/e/f/nb"], version="v4.2") is True
    assert covered(["/Shared/etl/nb"], has_files=True, version="v4.2") is True   # fichiers exportés
    assert covered(["/Repos/u/projet/nb"], version="v4.2") is False              # contenu dans Git
    assert covered([], version="v4.2") is False


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
    # table ignorée au clone (non clonable) : ses données manquent au backup
    (dict(def_cov="SCRIPT", data_cov="SCRIPT", last_status="skipped"), "ECHEC"),
    # vue en erreur ou ignorée au clone (cas client v4.1) : sa DDL est exportée à part, pas un échec
    (dict(def_cov="SCRIPT", data_cov="NA", last_status="error"), "OK"),
    (dict(def_cov="SCRIPT", data_cov="NA", last_status="skipped"), "OK"),
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


def test_pipeline_files_are_not_covered_only_notebooks_in_v41():
    covered = R["pipeline_code_covered"]
    assert covered(["/Shared/etl/nb"], has_files=True, version="v4.1") is False
    assert covered(["/Shared/etl/nb"], has_files=False, version="v4.1") is True
