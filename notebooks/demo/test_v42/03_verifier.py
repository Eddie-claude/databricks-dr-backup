# Databricks notebook source
# notebooks/demo/test_v42/03_verifier.py

# COMMAND ----------
# MAGIC %md
# MAGIC # Jeu de test v4.2 — E4 / E7 : vérification
# MAGIC
# MAGIC - `mode = backup` (E4) : contrôle le contenu du backup ADLS pour les deux dates, lance le rapport de
# MAGIC   couverture (`diag_03`) et la synchronisation des droits en mode `report`.
# MAGIC - `mode = restore` (E7) : compare l'état réel des objets restaurés à l'état attendu v2, et le volume
# MAGIC   de contrôle restauré à J-1 aux fichiers v1.
# MAGIC
# MAGIC Résultat : un tableau OK / KO / OBS (observation : comportement à connaître, pas une régression),
# MAGIC enregistré dans `resultats_<mode>.json` et `resultats_<mode>.html`.

# COMMAND ----------
# MAGIC %run ./00_commun

# COMMAND ----------
dbutils.widgets.dropdown("mode", "backup", ["backup", "restore"], "Vérification")
dbutils.widgets.text("backup_root", f"{ACCOUNT}/backup-dev", "Backup root")
dbutils.widgets.text("date_v1", "", "Date du backup J-1 (fichiers v1)")
dbutils.widgets.text("date_v2", "", "Date du backup J (fichiers v2)")

mode        = dbutils.widgets.get("mode")
backup_root = dbutils.widgets.get("backup_root").rstrip("/")
date_v1     = dbutils.widgets.get("date_v1").strip()
date_v2     = dbutils.widgets.get("date_v2").strip()

results = []


def check(ligne, controle, ok, detail="", obs=False):
    statut = "OBS" if obs else ("OK" if ok else "KO")
    results.append({"ligne": ligne, "controle": controle, "statut": statut, "detail": str(detail)[:1500]})
    print(f"[{statut}] {ligne} — {controle}" + (f" : {str(detail)[:300]}" if detail else ""))


def head(path):
    try:
        return dbutils.fs.head(path, 10_000_000)
    except Exception as e:
        return None


def norm(sql_text):
    return " ".join((sql_text or "").replace("`", "").lower().split())


def diff_dict(expected: dict, actual: dict, ignore=()):
    out = []
    for k in sorted(set(expected) | set(actual)):
        if k in ignore:
            continue
        if k not in actual:
            out.append(f"absent : {k}")
        elif k not in expected:
            out.append(f"en trop : {k}")
        elif expected[k] != actual[k]:
            out.append(f"différent : {k} → attendu {expected[k]} / obtenu {actual[k]}")
    return out

# COMMAND ----------
# MAGIC %md ## Mode backup (E4)

# COMMAND ----------
if mode == "backup":
    v1, v2 = load_json("attendu_v1.json"), load_json("attendu_v2.json")
    meta = f"{backup_root}/{date_v2}/uc_metadata"

    # 1 — correctif lib_path : sans lib/ complet, 01 n'écrit pas ces deux fichiers
    vols_sql, funcs_sql = head(f"{meta}/05_volumes.sql"), head(f"{meta}/06_functions.sql")
    check("1 lib_path", "05_volumes.sql et 06_functions.sql produits", vols_sql is not None and funcs_sql is not None,
          "" if vols_sql and funcs_sql else "fichier(s) absent(s) : lib/uc_ddl.py non trouvé ?")

    # 2 — tables clonées sous incremental/
    for fqn in [f"{CATALOG}.finance.clients", f"{CATALOG}.finance.transactions", f"{CATALOG}.rh.employes"]:
        c, s, t = fqn.split(".")
        present = head(f"{backup_root}/incremental/{c}/{s}/{t}/_delta_log/00000000000000000000.json") is not None
        check("2 tables", f"{fqn} présente dans incremental/", present)
    tables_sql = norm(head(f"{meta}/03_tables.sql"))
    check("2 tables", "DDL de la vue v_ca dans 03_tables.sql", "finance.v_ca" in tables_sql)
    check("2 tables", "DDL de la vue qualifiée par son catalogue (rejouable hors du catalogue courant)",
          f"{CATALOG}.finance.v_ca" in tables_sql,
          next((l for l in (head(f"{meta}/03_tables.sql") or "").splitlines() if "v_ca" in l), ""))

    # 3 — définitions des volumes
    vs = norm(vols_sql)
    check("3 volumes", "volume managé finance.docs exporté", f"volume if not exists {CATALOG}.finance.docs" in vs, vs[:300])
    check("3 volumes", "volume externe", True,
          "non testé : le compte n'a pas CREATE EXTERNAL VOLUME sur la location", obs=True)

    # 4 — fichiers des volumes, état par date dans l'index
    try:
        idx = spark.read.format("delta").load(f"{backup_root}/volumes/_index") \
            .where(f"catalog = '{CATALOG}' AND volume = 'docs'").collect()
        by_date = {}
        for r in idx:
            by_date.setdefault(str(r.snapshot_date), set()).add(r.rel_path)
        for d, att in [(date_v1, v1["fichiers_docs"]), (date_v2, v2["fichiers_docs"])]:
            got = by_date.get(d, set())
            check("4 fichiers volumes", f"index du {d} = fichiers attendus", got == set(att),
                  f"attendu {sorted(att)} / index {sorted(got)}")
    except Exception as e:
        check("4 fichiers volumes", "lecture de l'index volumes/_index", False, short(e))

    # 5 — fonctions
    fs = norm(funcs_sql)
    for f in ["finance.tva", "finance.clients_region", "finance.slug", "rh.filtre_rh"]:
        check("5 fonctions", f"{f} exportée", f"function if not exists {CATALOG}.{f}" in fs)
    check("5 fonctions", "corps Python de slug présent", "def to_slug" in (funcs_sql or ""))

    # 6 — droits : directs exportés, hérités non
    gs = norm(head(f"{meta}/04_grants.sql"))
    for on in [f"on catalog {CATALOG} ", f"on schema {CATALOG}.finance ", f"on table {CATALOG}.rh.employes ",
               f"on volume {CATALOG}.finance.docs ", f"on function {CATALOG}.finance.tva "]:
        check("6 droits", f"droit direct exporté : {on.strip()}", on in gs + " ")
    check("6 droits", "SELECT hérité du schéma NON exporté sur finance.clients",
          f"on table {CATALOG}.finance.clients " not in gs + " ")

    # 7 — workspace
    manifest = json.loads(head(f"{backup_root}/{date_v2}/workspace/manifest.json") or "{}")
    saved = {o["path"] for o in manifest.get("objects", []) if o["path"].startswith(WS_DIR + "/")}
    expected_ws = {p for p, o in v2["workspace"].get("objets", {}).items() if o["type"] != "DIRECTORY"}
    check("7 workspace", "tous les objets du dossier de test exportés", expected_ws <= saved,
          f"manquants : {sorted(expected_ws - saved)}")
    acls = json.loads(head(f"{backup_root}/{date_v2}/workspace_config/workspace_acls.json") or "[]")
    check("7 workspace", "droits du dossier de test exportés", any(a.get("path") == WS_DIR for a in acls))

    # 8, 9 — job et pipeline, avec leurs droits
    jobs_all = head(f"{backup_root}/{date_v2}/jobs/jobs_all.json") or ""
    check("8 jobs", f"{JOB_NAME} exporté", JOB_NAME in jobs_all)
    job_perms = head(f"{backup_root}/{date_v2}/jobs/jobs_permissions.json") or ""
    check("8 jobs", "droits du job exportés", str(find_job_id()) in job_perms and "CAN_VIEW" in job_perms)
    pipes = head(f"{backup_root}/{date_v2}/pipelines/pipelines_all.json") or ""
    check("9 pipelines", f"{PIPELINE_NAME} exporté", PIPELINE_NAME in pipes)
    check("9 pipelines", "droits du pipeline exportés", PIPELINE_NAME in pipes and "CAN_VIEW" in pipes)

    # 12 — tables non copiables : présentes dans le décompte, absentes du backup, visibles dans le rapport ?
    clone = json.loads(head(f"{backup_root}/incremental/_manifests/{date_v2}.json") or "[]")
    st = {r["table"]: r for r in clone if r["table"].startswith(CATALOG + ".")}
    r = st.get(f"{CATALOG}.rh.salaires", {})
    check("12 non copiables", f"{CATALOG}.rh.salaires (filtre de lignes) : statut du clone",
          r.get("status") in ("skipped", "error"),
          f"{r.get('status')} — {r.get('reason') or r.get('error', '')}", obs=True)
    present = head(f"{backup_root}/incremental/{CATALOG}/rh/salaires/_delta_log/00000000000000000000.json") is not None
    check("12 non copiables", "rh.salaires absente de incremental/ (donc de la restauration)", not present,
          "présente" if present else "absente", obs=True)
    report = head(f"{backup_root}/{date_v2}/report/dr_report_{date_v2}.html") or ""
    check("12 non copiables", "le rapport signale les tables non copiées", "salaires" in report,
          "le rapport n'affiche que le nombre de tables découvertes" if "salaires" not in report else "")

    # Observation : recréation d'un catalogue sur un metastore sans stockage racine
    cats = head(f"{meta}/01_catalogs.sql") or ""
    line = next((l for l in cats.splitlines() if CATALOG in l), "")
    check("OBS catalogues", "01_catalogs.sql recrée le catalogue avec son emplacement",
          "MANAGED LOCATION" in line.upper(), line, obs="MANAGED LOCATION" not in line.upper())

    # 10, 11 — rapport de couverture et synchronisation des droits
    try:
        out = dbutils.notebook.run("../../diag_03_backup_coverage", 3600, {
            "backup_principal": ME, "backup_root": backup_root, "output_dir": RESULTS_DIR})
        check("10 couverture", "diag_03_backup_coverage s'exécute", True, out)
    except Exception as e:
        check("10 couverture", "diag_03_backup_coverage s'exécute", False, short(e))
    try:
        out = dbutils.notebook.run("../../admin_sync_backup_grants", 1800, {
            "backup_principal": ME, "backup_location": "drp-dev-chn-location", "mode": "report"})
        check("11 sync droits", "admin_sync_backup_grants (report) s'exécute", True, out)
    except Exception as e:
        # En mode report, un écart de droits fait échouer le notebook volontairement (notification) :
        # c'est le comportement attendu. dbutils.notebook.run ne remonte pas le message du notebook
        # enfant, d'où un contrôle direct des droits manquants.
        check("11 sync droits", "admin_sync_backup_grants (report) signale les écarts en échouant",
              "WorkflowException" in str(e) or "sans les droits requis" in str(e), short(e))

# COMMAND ----------
# MAGIC %md ## Mode restore (E7)

# COMMAND ----------
if mode == "restore":
    v1, v2 = load_json("attendu_v1.json"), load_json("attendu_v2.json")
    now = etat()
    NOT_BY_NOTEBOOKS = {f"{CATALOG}.finance.v_ca": "vue : DDL restaurée par scripts/restore_uc.py, pas par les notebooks",
                        f"{CATALOG}.rh.salaires": "table à filtre de lignes : jamais copiée (clone refusé)"}

    for fqn, e in v2["tables"].items():
        if e["type"] == "VIEW" and fqn in now["tables"]:
            # DDL rejouée depuis 03_tables.sql (comme scripts/restore_uc.py)
            check("2 tables", f"vue {fqn} recréée dans son catalogue, même définition",
                  now["tables"][fqn] == e, f"attendu {e} / obtenu {now['tables'][fqn]}")
        elif fqn in NOT_BY_NOTEBOOKS:
            check("2 tables", f"{fqn}", fqn not in now["tables"], NOT_BY_NOTEBOOKS[fqn], obs=True)
        else:
            check("2 tables", f"{fqn} restaurée à l'identique (lignes + checksum)",
                  now["tables"].get(fqn) == e, f"attendu {e} / obtenu {now['tables'].get(fqn)}")

    for fqn, e in v2["volumes"].items():
        check("3 volumes", f"{fqn} recréé (type, commentaire, emplacement)", now["volumes"].get(fqn) == e,
              f"attendu {e} / obtenu {now['volumes'].get(fqn)}")

    d = diff_dict(v2["fichiers_docs"], now["fichiers_docs"])
    check("4 fichiers volumes", "volume docs restauré à J = fichiers v2", not d, d)
    d = diff_dict(v1["fichiers_docs"], volume_files(f"{CATALOG}.finance.controle_pit"))
    check("4 fichiers volumes", "volume de contrôle restauré à J-1 = fichiers v1", not d, d)

    for fqn, e in v2["fonctions"].items():
        got = now["fonctions"].get(fqn) or {}
        same_but_edges = (got != e and {**got, "definition": None} == {**e, "definition": None}
                          and (got.get("definition") or "").strip("\r\n") == (e.get("definition") or "").strip("\r\n"))
        # uc_ddl normalise les sauts de ligne autour du corps Python (une seule ligne de chaque côté)
        check("5 fonctions", f"{fqn} recréée à l'identique", got == e,
              "seuls les sauts de ligne en début/fin de corps diffèrent" if same_but_edges else diff_dict(e, got),
              obs=same_but_edges)
    d = diff_dict(v2["resultats_fonctions"], now["resultats_fonctions"])
    check("5 fonctions", "mêmes résultats d'appel", not d, d)

    exp_g = {g for g in v2["grants"] if "controle_pit" not in g}
    now_g = {g for g in now["grants"] if "controle_pit" not in g}
    check("6 droits", "droits directs identiques", exp_g == now_g,
          f"manquants : {sorted(exp_g - now_g)} / en trop : {sorted(now_g - exp_g)}")

    d = diff_dict(v2["workspace"].get("objets", {}), now["workspace"].get("objets", {}))
    check("7 workspace", "objets du dossier identiques (contenu)", not d, d)
    check("7 workspace", "droits du dossier identiques",
          v2["workspace"].get("acl_dossier") == now["workspace"].get("acl_dossier"),
          f"attendu {v2['workspace'].get('acl_dossier')} / obtenu {now['workspace'].get('acl_dossier')}")

    d = diff_dict(v2["job"], now["job"])
    check("8 jobs", f"{JOB_NAME} recréé avec ses droits", not d, d)
    d = diff_dict(v2["pipeline"], now["pipeline"])
    check("9 pipelines", f"{PIPELINE_NAME} recréé avec ses droits", not d, d)
    save_json("etat_apres_restauration.json", now)

# COMMAND ----------
# MAGIC %md ## Résultat

# COMMAND ----------
summary = {s: sum(1 for r in results if r["statut"] == s) for s in ("OK", "KO", "OBS")}
save_json(f"resultats_{mode}.json", {"mode": mode, "resume": summary, "controles": results})
rows = "".join(
    f"<tr class='{r['statut']}'><td>{r['statut']}</td><td>{r['ligne']}</td><td>{r['controle']}</td>"
    f"<td>{r['detail'].replace('<', '&lt;')}</td></tr>" for r in results)
html = f"""<html><head><meta charset='utf-8'><style>
body{{font-family:sans-serif}} td{{border:1px solid #ccc;padding:4px;vertical-align:top}}
.OK td:first-child{{background:#cfc}} .KO td:first-child{{background:#fcc}} .OBS td:first-child{{background:#ffd}}
</style></head><body><h2>Jeu de test v4.2 — vérification {mode}</h2>
<p>OK {summary['OK']} · KO {summary['KO']} · OBS {summary['OBS']}</p>
<table><tr><th>Statut</th><th>Ligne</th><th>Contrôle</th><th>Détail</th></tr>{rows}</table></body></html>"""
with open(f"{RESULTS_DIR}/resultats_{mode}.html", "w", encoding="utf-8") as f:
    f.write(html)
displayHTML(html)
dbutils.notebook.exit(json.dumps({"resume": summary, "controles": results}, ensure_ascii=False)[:900000])
