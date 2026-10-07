# Databricks notebook source
# notebooks/demo/test_v42/00_commun.py

# COMMAND ----------
# MAGIC %md
# MAGIC # Jeu de test v4.2 — constantes et capture d'état
# MAGIC
# MAGIC Inclus par `%run ./00_commun` dans les autres notebooks du kit. Tous les objets de test portent
# MAGIC le préfixe `dr_test_v42` : les étapes destructives refusent tout autre nom.
# MAGIC
# MAGIC `etat()` photographie l'état des objets de test (données, définitions, droits, contenus) sous une
# MAGIC forme comparable : le même appel avant le sinistre et après la restauration doit donner le même
# MAGIC résultat.

# COMMAND ----------
import base64
import hashlib
import json
import os

import requests

PREFIX        = "dr_test_v42"
CATALOG       = PREFIX
ACCOUNT       = "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net"
STORAGE       = f"{ACCOUNT}/backup_datasets/{PREFIX}"
EXT_ROOT      = f"{ACCOUNT}/backup_datasets/{PREFIX}_ext"
WS_DIR        = f"/Shared/{PREFIX}"
JOB_NAME      = f"{PREFIX}_job"
PIPELINE_NAME = f"{PREFIX}_pipeline"
GRANTEE       = "account users"   # principal des droits Unity Catalog
WS_GROUP      = "users"           # groupe des droits workspace (dossier, job, pipeline)
WAREHOUSE_ID  = "29da4e6bc485379b"
CONFIRM       = f"DETRUIRE {PREFIX}"

_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST = _ctx.apiUrl().get()
HDRS = {"Authorization": f"Bearer {_ctx.apiToken().get()}"}
ME   = spark.sql("SELECT current_user()").first()[0]
RESULTS_DIR = f"/Workspace/Users/{ME}/{PREFIX}_resultats"


def api(method, path, **kw):
    r = requests.request(method, f"{HOST}{path}", headers=HDRS, timeout=120, **kw)
    if r.status_code >= 400:
        raise RuntimeError(f"{method} {path} → {r.status_code} {r.text[:400]}")
    return r.json() if r.text.strip() else {}


def short(e) -> str:
    return " ".join(str(e).split("JVM stacktrace:")[0].split())[:300]


def save_json(name, data):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(f"{RESULTS_DIR}/{name}", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True, ensure_ascii=False, default=str)
    print(f"[OK] {RESULTS_DIR}/{name}")


def load_json(name):
    with open(f"{RESULTS_DIR}/{name}", encoding="utf-8") as f:
        return json.load(f)


def md5(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


def guard(name: str) -> str:
    """Garde-fou des étapes destructives : seul un objet de test peut être visé."""
    if PREFIX not in name:
        raise ValueError(f"Refus : '{name}' n'est pas un objet de test {PREFIX}")
    return name

# COMMAND ----------
# MAGIC %md ## Capture d'état

# COMMAND ----------
def _rows(sql):
    return [r.asDict() for r in spark.sql(sql).collect()]


def catalog_exists() -> bool:
    return any(r.catalog == CATALOG for r in spark.sql("SHOW CATALOGS").collect())


def etat_tables():
    out = {}
    for t in _rows(f"""SELECT table_schema s, table_name n, table_type ty
                       FROM {CATALOG}.information_schema.tables
                       WHERE table_schema <> 'information_schema'"""):
        fqn = f"{CATALOG}.{t['s']}.{t['n']}"
        e = {"type": t["ty"]}
        if t["ty"] == "VIEW":
            v = _rows(f"""SELECT view_definition d FROM {CATALOG}.information_schema.views
                          WHERE table_schema = '{t['s']}' AND table_name = '{t['n']}'""")
            e["definition"] = " ".join((v[0]["d"] if v else "").split())
        else:
            try:
                r = spark.sql(f"SELECT count(*) c, coalesce(sum(hash(*)), 0) h FROM {fqn}").first()
                e.update(rows=r.c, checksum=int(r.h))
            except Exception as ex:
                e["erreur"] = short(ex)
        out[fqn] = e
    return out


def etat_volumes():
    return {f"{CATALOG}.{v['s']}.{v['n']}": {"type": v["ty"], "comment": v["c"],
                                             "location": v["loc"] if v["ty"] == "EXTERNAL" else None}
            for v in _rows(f"""SELECT volume_schema s, volume_name n, volume_type ty, comment c,
                                      storage_location loc
                               FROM {CATALOG}.information_schema.volumes""")}


def volume_files(fqn: str) -> dict:
    """{chemin relatif: md5} de tous les fichiers du volume."""
    root = "/Volumes/" + fqn.replace(".", "/")
    out = {}
    for d, _, files in os.walk(root):
        for f in files:
            p = os.path.join(d, f)
            with open(p, "rb") as fh:
                out[os.path.relpath(p, root).replace(os.sep, "/")] = md5(fh.read())
    return out


def etat_fonctions():
    params = {}
    for p in _rows(f"""SELECT specific_schema s, specific_name n, parameter_name pn, data_type dt,
                              ordinal_position o
                       FROM {CATALOG}.information_schema.parameters ORDER BY o"""):
        params.setdefault(f"{CATALOG}.{p['s']}.{p['n']}", []).append(f"{p['pn']} {p['dt']}")
    return {f"{CATALOG}.{f['s']}.{f['n']}": {
                "returns": f["dt"], "definition": f["d"], "deterministic": f["det"],
                "comment": f["c"], "language": f["lang"], "body": f["body"],
                "params": params.get(f"{CATALOG}.{f['s']}.{f['n']}", [])}
            for f in _rows(f"""SELECT routine_schema s, routine_name n, data_type dt,
                                      routine_definition d, is_deterministic det, comment c,
                                      external_language lang, routine_body body
                               FROM {CATALOG}.information_schema.routines""")}


def resultats_fonctions():
    out = {}
    for name, sql in {
        "tva(100)":             f"SELECT {CATALOG}.finance.tva(100.0)",
        "slug('Été 2026 !')":   f"SELECT {CATALOG}.finance.slug('Été 2026 !')",
        "clients_region(Vaud)": f"SELECT count(*) FROM {CATALOG}.finance.clients_region('Vaud')",
    }.items():
        try:
            out[name] = str(spark.sql(sql).first()[0])
        except Exception as ex:
            out[name] = f"ERREUR {short(ex)}"
    return out


def _securables():
    yield "CATALOG", CATALOG
    for s in _rows(f"SELECT schema_name n FROM {CATALOG}.information_schema.schemata WHERE schema_name <> 'information_schema'"):
        yield "SCHEMA", f"{CATALOG}.{s['n']}"
    for t in _rows(f"SELECT table_schema s, table_name n FROM {CATALOG}.information_schema.tables WHERE table_schema <> 'information_schema'"):
        yield "TABLE", f"{CATALOG}.{t['s']}.{t['n']}"
    for v in _rows(f"SELECT volume_schema s, volume_name n FROM {CATALOG}.information_schema.volumes"):
        yield "VOLUME", f"{CATALOG}.{v['s']}.{v['n']}"
    for f in _rows(f"SELECT routine_schema s, routine_name n FROM {CATALOG}.information_schema.routines"):
        yield "FUNCTION", f"{CATALOG}.{f['s']}.{f['n']}"


def etat_grants():
    """Droits posés directement sur chaque objet (les droits hérités du parent sont écartés)."""
    out = set()
    for kind, fqn in _securables():
        try:
            for g in spark.sql(f"SHOW GRANTS ON {kind} {fqn}").collect():
                key = g["ObjectKey"].replace("`", "")
                if key.lower() == fqn.lower():
                    out.add(f"{g['Principal']} | {g['ActionType']} | {kind} {fqn}")
        except Exception as ex:
            out.add(f"ERREUR SHOW GRANTS {kind} {fqn} : {short(ex)}")
    return sorted(out)


def _direct_acl(object_type, object_id):
    acl = api("GET", f"/api/2.0/permissions/{object_type}/{object_id}").get("access_control_list", [])
    out = []
    for a in acl:
        who = a.get("group_name") or a.get("user_name") or a.get("service_principal_name")
        for p in a.get("all_permissions", []):
            if not p.get("inherited") and p.get("permission_level") != "IS_OWNER" and a.get("group_name"):
                out.append(f"{who} | {p['permission_level']}")
    return sorted(out)


def etat_workspace():
    st = api("GET", "/api/2.0/workspace/get-status", params={"path": WS_DIR}) if _ws_exists() else None
    if not st:
        return {}
    objets, stack = {}, [WS_DIR]
    while stack:
        for o in api("GET", "/api/2.0/workspace/list", params={"path": stack.pop()}).get("objects", []):
            if o["object_type"] == "DIRECTORY":
                stack.append(o["path"])
                objets[o["path"]] = {"type": "DIRECTORY"}
                continue
            exp = api("GET", "/api/2.0/workspace/export", params={"path": o["path"], "format": "AUTO"})
            objets[o["path"]] = {"type": o["object_type"], "language": o.get("language"),
                                 "md5": md5(base64.b64decode(exp.get("content", "")))}
    return {"objets": objets, "acl_dossier": _direct_acl("directories", st["object_id"])}


def _ws_exists():
    r = requests.get(f"{HOST}/api/2.0/workspace/get-status", headers=HDRS, params={"path": WS_DIR}, timeout=60)
    return r.status_code == 200


def find_job_id():
    jobs = api("GET", "/api/2.1/jobs/list", params={"name": JOB_NAME}).get("jobs", [])
    return jobs[0]["job_id"] if jobs else None


def etat_job():
    job_id = find_job_id()
    if not job_id:
        return {}
    s = api("GET", "/api/2.1/jobs/get", params={"job_id": job_id})["settings"]
    return {"name": s["name"], "tags": s.get("tags"), "max_concurrent_runs": s.get("max_concurrent_runs"),
            "tasks": sorted(f"{t['task_key']} → {t.get('notebook_task', {}).get('notebook_path')}"
                            for t in s.get("tasks", [])),
            "acl": _direct_acl("jobs", job_id)}


def find_pipeline_id():
    for p in api("GET", "/api/2.0/pipelines", params={"filter": f"name LIKE '{PIPELINE_NAME}'"}).get("statuses", []):
        if p["name"] == PIPELINE_NAME:
            return p["pipeline_id"]
    return None


def etat_pipeline():
    pid = find_pipeline_id()
    if not pid:
        return {}
    s = api("GET", f"/api/2.0/pipelines/{pid}")["spec"]
    return {"name": s["name"], "catalog": s.get("catalog"), "schema": s.get("schema") or s.get("target"),
            "development": s.get("development"), "continuous": s.get("continuous"),
            "libraries": sorted(json.dumps(l, sort_keys=True) for l in s.get("libraries", [])),
            "acl": _direct_acl("pipelines", pid)}


def destroy(include_storage: bool = False):
    """Supprime les objets de test (étapes destructives 04 et 05 ; confirmation vérifiée par l'appelant)."""
    done = []
    if catalog_exists():
        spark.sql(f"DROP CATALOG {guard(CATALOG)} CASCADE")
        done.append(f"catalogue {CATALOG}")
    if _ws_exists():
        api("POST", "/api/2.0/workspace/delete", json={"path": guard(WS_DIR), "recursive": True})
        done.append(f"dossier {WS_DIR}")
    job_id = find_job_id()
    if job_id:
        api("POST", "/api/2.1/jobs/delete", json={"job_id": job_id})
        done.append(f"job {guard(JOB_NAME)}")
    pid = find_pipeline_id()
    if pid:
        api("DELETE", f"/api/2.0/pipelines/{pid}")
        done.append(f"pipeline {guard(PIPELINE_NAME)}")
    if include_storage:
        for path in (STORAGE, EXT_ROOT):
            try:
                dbutils.fs.rm(guard(path), recurse=True)
                done.append(f"stockage {path}")
            except Exception as e:
                print(f"[WARN] {path} : {short(e)}")
    for d in done:
        print(f"[OK] supprimé : {d}")
    return done


def etat():
    uc = catalog_exists()
    return {
        "tables":          etat_tables() if uc else {},
        "volumes":         etat_volumes() if uc else {},
        "fichiers_docs":   volume_files(f"{CATALOG}.finance.docs") if uc else {},
        "fonctions":       etat_fonctions() if uc else {},
        "resultats_fonctions": resultats_fonctions() if uc else {},
        "grants":          etat_grants() if uc else [],
        "workspace":       etat_workspace(),
        "job":             etat_job(),
        "pipeline":        etat_pipeline(),
    }
