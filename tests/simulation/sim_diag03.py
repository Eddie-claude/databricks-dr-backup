"""Simulation de bout en bout de diag_03_backup_coverage sur les cas remontés par le client (datalab).
Compte de backup sans AUCUN droit visible (groupes illisibles) : seule la présence dans le backup doit
faire passer les objets en vert."""
import json
import os
import sys
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(__file__))
from fakes import FakeSpark, install

ROOT = "abfss://c@s.dfs.core.windows.net/backup"
D = "2026-11-02"
OLD = datetime(2026, 1, 1)
AFTER = datetime(2026, 11, 2, 9, 0)                       # créé après l'écriture du manifest (03:00)
MANIFEST_MTIME = int(datetime(2026, 11, 2, 3, 0).timestamp() * 1000) + 3600 * 1000 * 0

files = {
    f"{ROOT}/latest.json": json.dumps({"date": D, "status": "degraded"}),
    f"{ROOT}/{D}/uc_metadata/01_catalogs.sql": "CREATE CATALOG IF NOT EXISTS `lab`;",
    f"{ROOT}/{D}/uc_metadata/02_schemas.sql": "CREATE SCHEMA IF NOT EXISTS `lab`.`edp`;\nCREATE SCHEMA IF NOT EXISTS `lab`.`default`;",
    f"{ROOT}/{D}/uc_metadata/03_tables.sql": ("CREATE TABLE `lab`.`edp`.`ok` (id INT) USING delta;\n\n"
                                               "CREATE TABLE `lab`.`edp`.`edp_partenaire` (id INT) USING delta;\n\n"
                                               "CREATE VIEW `lab`.`edp`.`v_view` (id) AS SELECT 1;"),
    f"{ROOT}/{D}/uc_metadata/05_volumes.sql": "CREATE VOLUME IF NOT EXISTS `lab`.`edp`.`vol`;",
    f"{ROOT}/{D}/uc_metadata/06_functions.sql": "",
    f"{ROOT}/incremental/_manifests/{D}.json": json.dumps([
        {"table": "lab.edp.ok", "status": "success"},
        {"table": "lab.edp.edp_partenaire", "status": "error", "reason": "droits insuffisants", "error": "[PERMISSION_DENIED]"}]),
    f"{ROOT}/{D}/jobs/jobs_all.json": json.dumps([{"job_id": 1}]),
    f"{ROOT}/{D}/pipelines/pipelines_all.json": "[]",
    f"{ROOT}/{D}/workspace/manifest.json": json.dumps({"objects": [{"path": "/Users/a/nb1"}, {"path": "/Users/a/nb2"}]}),
}
mtimes = {f"{ROOT}/incremental/_manifests/{D}.json": MANIFEST_MTIME}


def T(name, kind, fmt="DELTA", created=OLD, schema="edp"):
    return {"table_catalog": "lab", "table_schema": schema, "table_name": name, "table_type": kind,
            "data_source_format": fmt, "table_owner": "someone", "created_by": "someone", "created": created}


TABLES = [T("ok", "MANAGED"), T("edp_partenaire", "MANAGED"), T("v_view", "VIEW", fmt=None),
          T("knowledge_base", "FOREIGN", fmt=None, schema="default"), T("newt", "MANAGED", created=AFTER),
          T("missing", "MANAGED")]


def sql(q):
    if "current_user()" in q:
        return [{"u": "admin@gm.ch"}]
    if ".schemata" in q:
        return [{"catalog_name": "lab", "schema_name": s, "schema_owner": "x", "created_by": "x", "created": OLD}
                for s in ("edp", "default")]
    if ".tables" in q:
        return TABLES
    if ".volumes" in q:
        return [{"volume_catalog": "lab", "volume_schema": "edp", "volume_name": "vol", "volume_type": "MANAGED",
                 "volume_owner": "x", "created_by": "x", "created": OLD}]
    if ".routines" in q:
        return [{"routine_catalog": "lab", "routine_schema": "edp", "routine_name": "f", "routine_owner": "x",
                 "created": OLD, "created_by": "x"}]
    if "SHOW SCHEMAS IN hive_metastore" in q:
        raise RuntimeError("no hive")
    return []                                            # droits, filtres, locations… : rien


def api(path, params):
    if path == "/api/2.1/unity-catalog/catalogs":
        return 200, {"catalogs": [{"name": "lab", "catalog_type": "MANAGED_CATALOG", "owner": "x", "created_at": 1}]}
    if path.startswith("/api/2.1/unity-catalog/"):
        return 200, {}
    if "scim" in path:
        return 404, {"error": "not found"}               # groupes illisibles : droits directs seuls
    if path == "/api/2.1/jobs/list":
        return 200, {"jobs": [{"job_id": 1, "settings": {"name": "job_sauvegarde"}, "created_time": 1},
                              {"job_id": 2, "settings": {"name": "job_invisible_au_sp"}, "created_time": 1}]}
    if path == "/api/2.0/pipelines":
        return 200, {"statuses": [{"pipeline_id": "p1", "name": "etl"}]}
    if path == "/api/2.0/pipelines/p1":
        return 200, {"pipeline_id": "p1", "spec": {"name": "etl", "catalog": "lab"}, "creator_user_name": "x"}
    if path == "/api/2.0/workspace/list":
        p = params["path"]
        tree = {"/": [("/Users", "DIRECTORY"), ("/Repos", "DIRECTORY"), ("/Shared", "DIRECTORY")],
                "/Users": [("/Users/a", "DIRECTORY"), ("/Users/b", "DIRECTORY")],
                "/Users/a": [("/Users/a/nb1", "NOTEBOOK"), ("/Users/a/nb2", "NOTEBOOK")],
                "/Users/b": [("/Users/b/nb", "NOTEBOOK")], "/Shared": [], "/Repos": []}
        return 200, {"objects": [{"path": x, "object_type": t} for x, t in tree.get(p, [])]}
    return 404, {}


out_dir = tempfile.mkdtemp()
widgets = {"backup_principal": "e62e22db-b85b-46bd-a1d1-3ce711b64726", "backup_root": ROOT, "output_dir": out_dir}
spark = FakeSpark(sql, files)
g, out, req = install(spark, api, files, widgets, mtimes)
src = open(os.path.join(os.path.dirname(__file__), "../../notebooks/diag_03_backup_coverage.py"), encoding="utf-8").read()
exec(compile(src, "diag_03", "exec"), g)

expected = {
    "lab": "OK", "lab.edp": "OK", "lab.default": "OK",
    "lab.edp.ok": "OK",                       # cloné, aucun droit visible
    "lab.edp.edp_partenaire": "ECHEC",        # en erreur au clone
    "lab.edp.v_view": "OK",                   # vue : définition dans 03_tables.sql
    "lab.default.knowledge_base": "NON_COUVERT",  # index AI Search (FOREIGN)
    "lab.edp.newt": "INACCESSIBLE",           # créée après le backup, mais aucun droit visible
    "lab.edp.missing": "INACCESSIBLE",        # absente, et aucun droit visible : cause probable = droits
    "lab.edp.vol": "OK", "lab.edp.f": "INACCESSIBLE",   # fonction absente de 06 (vide), sans droits
    "job_sauvegarde": "OK", "job_invisible_au_sp": "ABSENT",
    "etl": "ABSENT",                          # pipeline invisible au SP (cas client)
    "/Users/a": "OK", "/Users/b": "ABSENT", "/Repos": "AUTRE", "/Shared": "OK",
}
got = {r["Objet"]: r["_code"] for r in g["rows"]}
types = {r["Objet"]: r["Type"] for r in g["rows"]}
bad = {k: (got.get(k), v) for k, v in expected.items() if got.get(k) != v}
for k in expected:
    print(f"  {k:30} {types.get(k, '?'):24} {got.get(k)}")
assert not bad, f"verdicts inattendus (obtenu, attendu) : {bad}"
labels = {r["Objet"]: r["Dernier backup"] for r in g["rows"]}
assert labels["lab.edp.newt"] == "nouvelle" and labels["lab.edp.missing"] == "absent"
assert labels["lab.edp.v_view"] == "dans le backup" and labels["job_invisible_au_sp"] == "absent"
html = out["html"]
assert "Vue" in types.values() and "Table / vue" not in html
assert "prime sur le calcul des droits" in html and "admin du workspace" in html
assert os.path.exists(g["xlsx_path"]) and os.path.exists(g["html_path"])
print(f"[SIM OK] {len(got)} objets, verdicts conformes ; rapport {g['html_path']}")
