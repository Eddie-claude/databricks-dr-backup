# tests/test_workspace_export.py
from lib.workspace_export import (
    explicit_acl, import_request, notebook_manifest_path, pipeline_create_payload, remap_path,
)


# ── Import d'un objet sauvegardé ─────────────────────────────────────────

def test_notebook_is_imported_with_its_extension_in_auto_format():
    # L'export AUTO renvoie file_type = 'py' / 'sql' / 'ipynb' ; l'import AUTO reconnaît le
    # notebook à l'extension + en-tête, et retire l'extension du nom final.
    req = import_request({"object_type": "NOTEBOOK", "file_type": "ipynb"}, "Y29udGVudA==", "/Users/u/analyse")
    assert req == {"path": "/Users/u/analyse.ipynb", "format": "AUTO", "content": "Y29udGVudA==", "overwrite": True}


def test_file_is_imported_raw_at_its_exact_path():
    # RAW : un fichier .py commençant par « # Databricks notebook source » reste un fichier
    req = import_request({"object_type": "FILE", "file_type": ""}, "eA==", "/Shared/scripts/run.sh")
    assert req["path"] == "/Shared/scripts/run.sh" and req["format"] == "RAW"


def test_dashboard_is_imported_auto_at_its_path():
    req = import_request({"object_type": "DASHBOARD", "file_type": ""}, "e30=", "/Shared/kpi.lvdash.json")
    assert req["path"] == "/Shared/kpi.lvdash.json" and req["format"] == "AUTO"


def test_remap_path_replaces_the_root_folder():
    assert remap_path("/Shared/etl/job", "") == "/Shared/etl/job"
    assert remap_path("/Shared/etl/job", "/Users/u/restore") == "/Users/u/restore/etl/job"
    assert remap_path("/Projets", "/Users/u/restore") == "/Users/u/restore/Projets"


# ── Manifest du différentiel : même format de chemin qu'avant ────────────

def test_manifest_path_keeps_previous_relative_format():
    assert notebook_manifest_path({"path": "/Shared/AdminScript/ControleTags", "language": "PYTHON"}) \
        == "Shared/AdminScript/ControleTags.py"
    assert notebook_manifest_path({"path": "/Users/u@x.ch/q", "language": "SQL"}) == "Users/u@x.ch/q.sql"
    assert notebook_manifest_path({"path": "/Shared/x", "language": None}) == "Shared/x.py"


# ── Permissions ──────────────────────────────────────────────────────────

def test_explicit_acl_keeps_direct_permissions_only():
    acl = [
        {"user_name": "owner@x.ch", "all_permissions": [{"permission_level": "IS_OWNER", "inherited": False}]},
        {"group_name": "admins", "all_permissions": [
            {"permission_level": "CAN_MANAGE", "inherited": True, "inherited_from_object": ["/jobs/"]}]},
        {"group_name": "data-team", "all_permissions": [{"permission_level": "CAN_MANAGE_RUN", "inherited": False}]},
        {"service_principal_name": "abc-123", "all_permissions": [
            {"permission_level": "CAN_VIEW", "inherited": False},
            {"permission_level": "CAN_MANAGE", "inherited": True}]},
    ]
    # IS_OWNER exclu : la ressource recréée appartient à l'identité qui restaure (transfert manuel)
    assert explicit_acl(acl) == [
        {"group_name": "data-team", "permission_level": "CAN_MANAGE_RUN"},
        {"service_principal_name": "abc-123", "permission_level": "CAN_VIEW"},
    ]


def test_explicit_acl_of_empty_or_missing():
    assert explicit_acl([]) == []
    assert explicit_acl(None) == []


# ── Pipelines ────────────────────────────────────────────────────────────

def test_pipeline_payload_drops_identifiers():
    spec = {"id": "abc", "name": "ingest", "catalog": "prod", "schema": "bronze",
            "libraries": [{"notebook": {"path": "/Shared/etl/ingest"}}], "continuous": False}
    payload = pipeline_create_payload(spec)
    assert "id" not in payload
    assert payload["name"] == "ingest" and payload["libraries"] == spec["libraries"]
    assert spec["id"] == "abc"   # l'original n'est pas modifié
