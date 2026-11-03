"""Simulation de bout en bout de 01_uc_metadata : index AI Search (FOREIGN) sauté, compteur tables / vues."""
import json
import os
import sys

HERE = os.path.dirname(__file__)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(HERE, "../../lib"))
from fakes import FakeSpark, Row, install

ROOT = "abfss://c@s.dfs.core.windows.net/backup"
TYPES = {"edp": {"t1": "MANAGED", "t2": "EXTERNAL", "v1": "VIEW", "mv": "METRIC_VIEW"},
         "default": {"knowledge_base": "FOREIGN", "t3": "MANAGED"}}


def sql(q):
    if q.startswith("SHOW CATALOGS"):
        return [{"catalog": c} for c in ("lab", "system")]
    if q.startswith("SHOW SCHEMAS IN"):
        return [{"databaseName": s} for s in ("edp", "default", "information_schema")]
    if "information_schema.tables" in q:
        schema = q.split("table_schema = '")[1].split("'")[0]
        return [{"table_name": n, "table_type": t} for n, t in TYPES[schema].items()]
    if q.startswith("SHOW TABLES IN"):
        schema = q.split("`.`")[1].rstrip("`")
        return [{"tableName": n} for n in TYPES[schema]]
    if q.startswith("SHOW CREATE TABLE"):
        name = q.split("`.`")[-1].rstrip("`")
        if name == "knowledge_base":
            raise RuntimeError("[DATA_SOURCE_NOT_FOUND] Failed to find the data source: unsupported.")
        if name == "mv":
            raise RuntimeError("[UNSUPPORTED_SHOW_CREATE_TABLE.ON_METRIC_VIEW]")
        kind = "VIEW" if TYPES["edp"].get(name) == "VIEW" else "TABLE"
        return [{"createtab_stmt": f"CREATE {kind} x.{name} AS SELECT 1"}]
    if q.startswith("DESCRIBE TABLE EXTENDED"):
        return [{"json": json.dumps({"view_text": "version: 0.1"})}]
    return []                                            # volumes, fonctions, grants : rien


def api(path, params):
    if path == "/api/2.1/unity-catalog/catalogs":
        return 200, {"catalogs": [{"name": "lab", "catalog_type": "MANAGED_CATALOG"}]}
    return 404, {}


files, widgets = {}, {"backup_root": ROOT, "backup_date": "2026-11-03",
                      "lib_path": os.path.join(HERE, "../../lib")}
spark = FakeSpark(sql, files)
g, out, req = install(spark, api, files, widgets)
src = open(os.path.join(HERE, "../../notebooks/01_uc_metadata.py"), encoding="utf-8").read()
try:
    exec(compile(src, "01_uc_metadata", "exec"), g)
except SystemExit as ex:
    result = json.loads(str(ex))

assert result["table_names"] == ["lab.edp.t1", "lab.edp.t2", "lab.default.t3"], result["table_names"]
assert result["view_count"] == 2, result["view_count"]
assert result["ddl_skipped"] == {"lab.default.knowledge_base": "table étrangère ou index AI Search : définition à recréer en IaC"}
ddl = files[f"{ROOT}/2026-11-03/uc_metadata/03_tables.sql"]
assert "knowledge_base" not in ddl and "`lab`.`edp`.`mv`" in ddl and "`lab`.`edp`.`v1`" in ddl
assert not any("SHOW CREATE TABLE `lab`.`default`.`knowledge_base`" in q for q in spark.queries)
print("[SIM OK] 01 : FOREIGN sauté sans SHOW CREATE TABLE, 3 tables + 2 vues, metric view reconstruite")

# Bout en bout : ce que 01 écrit, diag_03 le relit (même nom qualifié)
cov_src = open(os.path.join(HERE, "../../notebooks/diag_03_backup_coverage.py"), encoding="utf-8").read()
R = {}
exec(cov_src.split("# >>> REGLES")[1].split("# <<< REGLES")[0], R)
base = f"{ROOT}/2026-11-03/uc_metadata"
assert R["exported_names"](files[f"{base}/03_tables.sql"]) == {"lab.edp.t1", "lab.edp.t2", "lab.edp.v1", "lab.edp.mv", "lab.default.t3"}
assert R["exported_names"](files[f"{base}/02_schemas.sql"]) == {"lab.edp", "lab.default"}
assert R["exported_names"](files[f"{base}/01_catalogs.sql"]) == {"lab"}
print("[SIM OK] diag_03 relit les noms exportés par 01")
