# tests/test_workspace_export.py
import pytest
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


# ── Liste complète des jobs (05_workspace_config) ────────────────────────

from types import SimpleNamespace

from lib.workspace_export import call_with_retry, list_all_jobs


class FakeJobsApi:
    """GET /api/2.1/jobs/list et /jobs/get ; enregistre les paramètres reçus."""

    def __init__(self, pages, details=None):
        self.pages, self.details, self.calls = pages, details or {}, []

    def __call__(self, path, params):
        self.calls.append((path, dict(params)))
        if path.endswith("/jobs/list"):
            return self.pages[params.get("page_token") or ""]
        pages = self.details[params["job_id"]]
        return pages[params.get("page_token") or ""]


def test_list_all_jobs_asks_expanded_tasks_as_lowercase_string():
    # requests transmet le booléen Python True sous la forme « True », que l'API ignore :
    # les jobs étaient sauvegardés sans leurs tâches.
    api = FakeJobsApi({"": {"jobs": []}})
    list_all_jobs(api)
    assert api.calls[0][1]["expand_tasks"] == "true"


def test_list_all_jobs_follows_pagination():
    api = FakeJobsApi({
        "": {"jobs": [{"job_id": 1, "settings": {"tasks": [{"task_key": "a"}]}}],
             "has_more": True, "next_page_token": "p2"},
        "p2": {"jobs": [{"job_id": 2, "settings": {"tasks": [{"task_key": "b"}]}}]},
    })
    assert [j["job_id"] for j in list_all_jobs(api)] == [1, 2]


def test_list_all_jobs_completes_truncated_tasks_with_jobs_get():
    # jobs/list renvoie au plus 100 tâches par job (has_more) : la définition complète vient de jobs/get
    api = FakeJobsApi(
        {"": {"jobs": [{"job_id": 7, "has_more": True, "settings": {"tasks": [{"task_key": "t1"}]}}]}},
        {7: {"": {"job_id": 7, "has_more": True, "next_page_token": "x",
                  "settings": {"name": "gros", "tasks": [{"task_key": "t1"}]}},
             "x": {"job_id": 7, "settings": {"tasks": [{"task_key": "t2"}]}}}},
    )
    job = list_all_jobs(api)[0]
    assert [t["task_key"] for t in job["settings"]["tasks"]] == ["t1", "t2"]
    assert job["settings"]["name"] == "gros" and "has_more" not in job


# ── Réessais sur limitation de débit (HTTP 429) ──────────────────────────

def _resp(code, retry_after=None):
    return SimpleNamespace(status_code=code, headers={"Retry-After": retry_after} if retry_after else {})


def test_retry_on_429_until_success_honouring_retry_after():
    responses, sleeps = [_resp(429, "3"), _resp(429), _resp(200)], []
    r = call_with_retry(lambda: responses.pop(0), sleep=sleeps.append)
    assert r.status_code == 200
    assert sleeps[0] == 3 and len(sleeps) == 2


def test_retry_gives_up_and_returns_last_response():
    sleeps = []
    r = call_with_retry(lambda: _resp(429), retries=3, sleep=sleeps.append)
    assert r.status_code == 429 and len(sleeps) == 3


def test_no_retry_on_client_error():
    calls = []
    r = call_with_retry(lambda: calls.append(1) or _resp(404), sleep=lambda s: None)
    assert r.status_code == 404 and len(calls) == 1


# ── Objet déjà présent à la restauration (jobs, pipelines) ───────────────

from lib.workspace_export import conflict_action, job_reset_payload, pipeline_edit_payload


def test_absent_object_is_created_whatever_the_mode():
    assert conflict_action("skip", exists=False) == "create"
    assert conflict_action("replace", exists=False) == "create"


def test_existing_object_is_skipped_or_replaced_in_place():
    assert conflict_action("skip", exists=True) == "skip"
    assert conflict_action("replace", exists=True) == "replace"


def test_recreate_is_the_old_name_of_replace():
    # « recreate » créait un second job à côté de l'existant (deux exécutions planifiées) et
    # supprimait le pipeline existant avec ses tables : il remplace désormais en place
    assert conflict_action("recreate", exists=True) == "replace"


def test_unknown_mode_is_refused():
    with pytest.raises(ValueError):
        conflict_action("overwrite", exists=True)


def test_job_reset_keeps_the_existing_job_id():
    assert job_reset_payload(42, {"name": "j", "tasks": []}) == {
        "job_id": 42, "new_settings": {"name": "j", "tasks": []}}


def test_pipeline_edit_targets_the_existing_pipeline():
    spec = {"id": "ancien", "pipeline_id": "ancien", "name": "p", "catalog": "c"}
    assert pipeline_edit_payload("actuel", spec) == {"id": "actuel", "name": "p", "catalog": "c"}
