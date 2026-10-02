# Databricks notebook source
# notebooks/admin_sync_backup_grants.py

# COMMAND ----------
# MAGIC %md # Synchronisation des droits du compte de backup
# MAGIC
# MAGIC Le backup s'exécute avec un service principal aux droits limités. Les droits accordés au
# MAGIC niveau d'un **catalog** s'héritent sur tout son contenu, y compris les objets créés plus tard :
# MAGIC le seul risque d'oubli est donc un **nouveau catalog**. Ce notebook compare, catalog par
# MAGIC catalog, les droits requis à ceux du compte de backup et, en mode `apply`, accorde ceux qui manquent.
# MAGIC
# MAGIC **À exécuter avec une identité admin du metastore**, et non avec le compte de backup : un
# MAGIC compte sans droit sur un catalog peut ne pas le voir du tout, il ne peut donc pas détecter seul
# MAGIC ce qui lui échappe. À planifier chaque jour **avant** le backup (job `dr-backup-grants-sync`).
# MAGIC
# MAGIC | Paramètre | Rôle |
# MAGIC |-----------|------|
# MAGIC | `backup_principal` | Groupe (recommandé) ou service principal (Application ID) du backup |
# MAGIC | `privileges` | Droits requis sur chaque catalog |
# MAGIC | `excluded_catalogs` | Catalogs exclus volontairement (tests, bacs à sable) |
# MAGIC | `backup_location` | External location du stockage de backup (READ FILES + WRITE FILES) |
# MAGIC | `mode` | `report` : constat seul (défaut) · `apply` : accorde les droits manquants |
# MAGIC
# MAGIC Les catalogs fédérés et Delta Sharing sont ignorés : leurs données ne sont pas sauvegardées.

# COMMAND ----------
import json

import requests

# COMMAND ----------
dbutils.widgets.text("backup_principal",  "", "Groupe / SP du backup")
dbutils.widgets.text("privileges",        "USE_CATALOG,USE_SCHEMA,BROWSE,SELECT,READ_VOLUME,EXECUTE",
                     "Droits requis par catalog")
dbutils.widgets.text("excluded_catalogs", "", "Catalogs exclus (virgules)")
dbutils.widgets.text("backup_location",   "", "External location du backup")
dbutils.widgets.dropdown("mode",          "report", ["report", "apply"], "report / apply")

# >>> REGLES
def parse_privileges(text: str) -> set:
    return {p.strip().upper().replace(" ", "_") for p in text.split(",") if p.strip()}


def missing_privileges(required: set, current: set) -> list:
    """Droits requis absents ; ALL_PRIVILEGES les couvre tous."""
    if "ALL_PRIVILEGES" in current:
        return []
    return sorted(required - current)


def grant_statement(securable: str, name: str, privileges: list, principal: str) -> str:
    """GRANT SQL (noms de privilèges avec espaces, comme le veut la syntaxe)."""
    privs = ", ".join(p.replace("_", " ") for p in privileges)
    return f"GRANT {privs} ON {securable} `{name}` TO `{principal}`"
# <<< REGLES

principal  = dbutils.widgets.get("backup_principal").strip()
required   = parse_privileges(dbutils.widgets.get("privileges"))
excluded   = {c.strip().lower() for c in dbutils.widgets.get("excluded_catalogs").split(",") if c.strip()}
location   = dbutils.widgets.get("backup_location").strip()
apply_mode = dbutils.widgets.get("mode") == "apply"
if not principal:
    raise ValueError("backup_principal est vide")

_ctx  = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
HOST  = _ctx.apiUrl().get()
_HDRS = {"Authorization": f"Bearer {_ctx.apiToken().get()}"}
print(f"[OK] principal={principal} | mode={'apply' if apply_mode else 'report'} | droits requis={sorted(required)}")

# COMMAND ----------
# MAGIC %md ## 1 — Écarts par catalog

# COMMAND ----------
def _catalogs() -> list:
    items, params = [], {"max_results": 1000}
    while True:
        r = requests.get(f"{HOST}/api/2.1/unity-catalog/catalogs", headers=_HDRS, params=params, timeout=60)
        r.raise_for_status()
        d = r.json()
        items += d.get("catalogs", [])
        if not d.get("next_page_token"):
            return items
        params["page_token"] = d["next_page_token"]

SKIP_TYPES = {"FOREIGN_CATALOG", "DELTASHARING_CATALOG", "SYSTEM_CATALOG", "INTERNAL_CATALOG"}
quoted = principal.replace("'", "''")
current = {}
for r in spark.sql(f"""SELECT catalog_name, privilege_type FROM system.information_schema.catalog_privileges
                       WHERE grantee = '{quoted}'""").collect():
    current.setdefault(r.catalog_name.lower(), set()).add(r.privilege_type.upper().replace(" ", "_"))

rows, statements = [], []
for c in sorted(_catalogs(), key=lambda c: c["name"]):
    name, ctype = c["name"], c.get("catalog_type", "")
    if ctype in SKIP_TYPES or name.lower() == "hive_metastore":
        continue
    if name.lower() in excluded:
        rows.append((name, "exclu", ""))
        continue
    missing = missing_privileges(required, current.get(name.lower(), set()))
    if missing:
        statements.append(grant_statement("CATALOG", name, missing, principal))
    rows.append((name, "OK" if not missing else "manquant", ", ".join(missing)))

# External location du stockage de backup
if location:
    loc_privs = {r.privilege_type.upper().replace(" ", "_") for r in spark.sql(f"""
        SELECT privilege_type FROM system.information_schema.external_location_privileges
        WHERE grantee = '{quoted}' AND external_location_name = '{location.replace("'", "''")}'""").collect()}
    missing = missing_privileges({"READ_FILES", "WRITE_FILES"}, loc_privs)
    if missing:
        statements.append(grant_statement("EXTERNAL LOCATION", location, missing, principal))
    rows.append((f"[external location] {location}", "OK" if not missing else "manquant", ", ".join(missing)))

display(spark.createDataFrame(rows, "objet STRING, statut STRING, droits_manquants STRING"))
n_missing = sum(1 for r in rows if r[1] == "manquant")
print(f"[{'WARN' if n_missing else 'OK'}] {n_missing} objet(s) avec des droits manquants sur {len(rows)}")

# COMMAND ----------
# MAGIC %md ## 2 — Application (mode apply)

# COMMAND ----------
applied, errors = 0, []
for stmt in statements:
    if not apply_mode:
        print(f"  [À ACCORDER] {stmt}")
        continue
    try:
        spark.sql(stmt)
        applied += 1
        print(f"  [OK] {stmt}")
    except Exception as e:
        msg = " ".join(str(e).split("JVM stacktrace:")[0].split())[:200]
        errors.append({"statement": stmt, "error": msg})
        print(f"  [ERROR] {stmt} → {msg}")

result = {"principal": principal, "mode": "apply" if apply_mode else "report",
          "objects": len(rows), "missing": n_missing, "applied": applied, "errors": len(errors)}
print(json.dumps(result, ensure_ascii=False))
# Mode report avec des écarts : échec explicite, pour qu'une notification parte
if not apply_mode and n_missing:
    raise RuntimeError(f"{n_missing} objet(s) sans les droits requis pour le compte de backup {principal} "
                       "— relancer en mode apply ou accorder les droits (détail ci-dessus)")
if errors:
    raise RuntimeError(f"{len(errors)} GRANT en échec (détail ci-dessus)")
dbutils.notebook.exit(json.dumps(result))
