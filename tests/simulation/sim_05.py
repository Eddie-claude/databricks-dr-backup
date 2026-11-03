"""Simulation de bout en bout de 05_workspace_config : .db_internal / .ide exclus, repos paginés,
avertissements explicites quand le compte n'est pas admin du workspace (cas client datalab)."""
import base64
import contextlib
import io
import json
import os
import sys

HERE = os.path.dirname(__file__)
sys.path.insert(0, HERE)
from fakes import FakeSpark, install

ROOT = "abfss://c@s.dfs.core.windows.net/backup"
TREE = {"/": [("/Users", "DIRECTORY"), ("/Shared", "DIRECTORY"), ("/Repos", "DIRECTORY")],
        "/Users": [("/Users/favrel@gm.ch", "DIRECTORY")],
        "/Users/favrel@gm.ch": [("/Users/favrel@gm.ch/nb", "NOTEBOOK"), ("/Users/favrel@gm.ch/.db_internal", "DIRECTORY"),
                                ("/Users/favrel@gm.ch/.ide", "DIRECTORY")],
        "/Users/favrel@gm.ch/.db_internal": [("/Users/favrel@gm.ch/.db_internal/x", "FILE")],
        "/Shared": [("/Shared/f.sh", "FILE")]}


def run(admin: bool):
    listed = []

    def api(path, params):
        if path == "/api/2.0/preview/scim/v2/Me":
            return 200, {"groups": [{"display": "users"}] + ([{"display": "admins"}] if admin else [])}
        if path == "/api/2.0/workspace/list":
            listed.append(params["path"])
            return 200, {"objects": [{"path": p, "object_type": t, "object_id": i}
                                     for i, (p, t) in enumerate(TREE.get(params["path"], []))]}
        if path == "/api/2.0/workspace/export":
            return 200, {"content": base64.b64encode(b"print(1)").decode(), "file_type": "py"}
        if path.startswith("/api/2.0/permissions/"):
            if ".db_internal" in path:
                return 403, {"error_code": "PERMISSION_DENIED"}
            return 200, {"access_control_list": []}
        if path == "/api/2.0/repos":
            if not admin:
                return 200, {}                              # cas client : {} sans erreur
            if params.get("next_page_token") == "p2":
                return 200, {"repos": [{"id": 2, "path": "/Repos/b/r2"}]}
            return 200, {"repos": [{"id": 1, "path": "/Repos/a/r1"}], "next_page_token": "p2"}
        if path == "/api/2.1/jobs/list":
            return 200, {"jobs": [{"job_id": 7, "settings": {"name": "j"}}]} if admin else {}
        if path == "/api/2.1/jobs/get":
            return 200, {"job_id": 7, "settings": {"name": "j", "tasks": []}}
        if path == "/api/2.0/pipelines":
            return 200, {"statuses": [{"pipeline_id": "p1", "name": "etl"}]} if admin else {}
        if path == "/api/2.0/pipelines/p1":
            return 200, {"pipeline_id": "p1", "spec": {"name": "etl"}}
        return 404, {}

    files, widgets = {}, {"backup_root": ROOT, "backup_date": "2026-11-03",
                          "lib_path": os.path.join(HERE, "../../lib")}
    spark = FakeSpark(lambda q: [], files)
    g, out, req = install(spark, api, files, widgets)
    src = open(os.path.join(HERE, "../../notebooks/05_workspace_config.py"), encoding="utf-8").read()
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        try:
            exec(compile(src, "05_workspace_config", "exec"), g)
        except SystemExit as ex:
            summary = json.loads(str(ex))
    return summary, buf.getvalue(), listed, files


summary, log, listed, files = run(admin=False)
assert not any(".db_internal" in p or ".ide" in p for p in listed), listed      # jamais parcourus
assert "pas admin du workspace" in log
assert "Aucun repo listé" in log and "Aucun pipeline listé" in log and "Aucun job trouvé" in log
assert summary["workspace_admin"] is False and summary["repos_count"] == 0
print("[SIM OK] 05 non admin : dossiers techniques exclus, avertissements explicites (repos, jobs, pipelines)")

summary, log, listed, files = run(admin=True)
assert "[OK] Compte du job admin du workspace" in log and "Aucun repo listé" not in log
assert summary["repos_count"] == 2, summary                       # deux pages de repos
assert summary["pipelines_ok"] == 1 and summary["jobs_ok"] == 1 and summary["notebooks_ok"] == 1
manifest = json.loads(files[f"{ROOT}/2026-11-03/workspace/manifest.json"])
assert manifest["exclude_names"] == [".db_internal", ".ide"]
assert {o["path"] for o in manifest["objects"]} == {"/Users/favrel@gm.ch/nb", "/Shared/f.sh"}
print("[SIM OK] 05 admin : repos paginés (2), job et pipeline exportés, manifest sans dossiers techniques")
