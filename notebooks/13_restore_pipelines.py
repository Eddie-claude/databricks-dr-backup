# Databricks notebook source
# notebooks/13_restore_pipelines.py

# COMMAND ----------
# MAGIC %md # 13 — Restauration des pipelines (Lakeflow / DLT)
# MAGIC
# MAGIC Recrée les pipelines sauvegardés par `05_workspace_config` dans `pipelines/pipelines_all.json`
# MAGIC (définition complète + permissions).
# MAGIC
# MAGIC **Remarques :**
# MAGIC - Le **code source** du pipeline (notebooks / fichiers) doit exister : le restaurer d'abord
# MAGIC   (scope `notebooks` de l'orchestrateur), sinon le pipeline est créé mais échouera à son lancement.
# MAGIC - Les **tables produites** (vues matérialisées, streaming tables) ne sont pas clonées : elles sont
# MAGIC   recalculées par le pipeline. Une streaming table relit ses sources depuis le début — il faut que
# MAGIC   celles-ci aient conservé assez d'historique.
# MAGIC - Un pipeline **continu** démarre dès sa création.
# MAGIC - Les pipelines déployés par un **bundle** sont ignorés par défaut : les redéployer avec le bundle.
# MAGIC - **dry_run = true** : affiche ce qui serait créé, sans rien créer.

# COMMAND ----------
import json
import sys
from datetime import date

import requests

token   = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
host    = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
headers = {"Authorization": f"Bearer {token}"}

def _uc_head(path: str) -> str:
    return dbutils.fs.head(path, 50_000_000)

# COMMAND ----------
dbutils.widgets.text(     "backup_root",     "", "Backup root (abfss://...)")
dbutils.widgets.text(     "backup_date",     str(date.today()), "Date du backup (YYYY-MM-DD)")
dbutils.widgets.text(     "pipeline_filter", "", "Filtre nom de pipeline (sous-chaîne, vide = tous)")
dbutils.widgets.dropdown( "conflict_mode",   "skip", ["skip", "recreate"], "Si le pipeline existe : skip / recreate")
dbutils.widgets.dropdown( "include_bundle",  "false", ["false", "true"], "Restaurer aussi les pipelines déployés par bundle")
dbutils.widgets.dropdown( "dry_run",         "true", ["true", "false"], "Dry-run (true = simulation)")
# lib/ est déployé par le bundle à côté de notebooks/ : …/files/notebooks/x → …/files/lib
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_nb_path = _nb_path if _nb_path.startswith("/Workspace") else "/Workspace" + _nb_path
dbutils.widgets.text("lib_path", _nb_path.rsplit("/notebooks/", 1)[0] + "/lib", "Chemin vers lib/")

backup_root     = dbutils.widgets.get("backup_root").strip().rstrip("/")
backup_date     = dbutils.widgets.get("backup_date").strip()
pipeline_filter = dbutils.widgets.get("pipeline_filter").strip().lower()
conflict_mode   = dbutils.widgets.get("conflict_mode")
include_bundle  = dbutils.widgets.get("include_bundle") == "true"
dry_run         = dbutils.widgets.get("dry_run").lower() == "true"

sys.path.insert(0, dbutils.widgets.get("lib_path"))
from workspace_export import explicit_acl, pipeline_create_payload

if not backup_root:
    raise ValueError("backup_root est vide — renseignez le chemin abfss://...")
prefix = "[DRY-RUN] " if dry_run else ""

# COMMAND ----------
# MAGIC %md ## Étape 1 — Pipelines sauvegardés et existants

# COMMAND ----------
try:
    saved = json.loads(_uc_head(f"{backup_root}/{backup_date}/pipelines/pipelines_all.json"))
except Exception as e:
    print(f"[WARN] Aucun pipeline sauvegardé pour {backup_date} (backup antérieur à la v4.2 ?) : {str(e)[:150]}")
    dbutils.notebook.exit(json.dumps({"backup_date": backup_date, "selected": 0, "ok": 0, "skipped": 0, "errors": 0}))

selected = [p for p in saved if not pipeline_filter or pipeline_filter in (p.get("name") or "").lower()]
print(f"[OK] {len(saved)} pipeline(s) dans le backup, {len(selected)} sélectionné(s)")

existing, params = {}, {"max_results": 100}
while True:
    r = requests.get(f"{host}/api/2.0/pipelines", headers=headers, params=params, timeout=30)
    r.raise_for_status()
    d = r.json()
    existing.update({s["name"]: s["pipeline_id"] for s in d.get("statuses", []) or []})
    if not d.get("next_page_token"):
        break
    params["page_token"] = d["next_page_token"]

# COMMAND ----------
# MAGIC %md ## Étape 2 — Création

# COMMAND ----------
results = []

for p in selected:
    name, spec = p.get("name"), p.get("spec", {})
    bundle = (spec.get("deployment") or {}).get("kind") == "BUNDLE"
    acl = explicit_acl(p.get("acl"))
    print(f"{'── ' if dry_run else '▶  '}{name}  (catalog={spec.get('catalog', '—')}, "
          f"{'continu' if spec.get('continuous') else 'déclenché'}, {len(acl)} permission(s))")

    if bundle and not include_bundle:
        print("   [SKIP] Déployé par un bundle — à redéployer avec `databricks bundle deploy`\n")
        results.append({"name": name, "status": "skipped", "reason": "bundle"})
        continue
    if name in existing:
        if conflict_mode == "skip":
            print(f"   [SKIP] Existe déjà (id={existing[name]})\n")
            results.append({"name": name, "status": "skipped", "reason": "existe"})
            continue
        if not dry_run:
            requests.delete(f"{host}/api/2.0/pipelines/{existing[name]}", headers=headers, timeout=30)
        print(f"   [INFO] Existant {'supprimé' if not dry_run else 'serait supprimé'} (conflict_mode=recreate)")
    if dry_run:
        print("   → Créerait le pipeline via POST /api/2.0/pipelines\n")
        results.append({"name": name, "status": "dry_run"})
        continue

    r = requests.post(f"{host}/api/2.0/pipelines", headers=headers, json=pipeline_create_payload(spec), timeout=60)
    if not r.ok:
        print(f"   [ERROR] {r.status_code} — {r.text[:200]}\n")
        results.append({"name": name, "status": "error", "error": r.text[:300]})
        continue
    new_id = r.json().get("pipeline_id")
    perm = "aucune"
    if acl:
        pr = requests.patch(f"{host}/api/2.0/permissions/pipelines/{new_id}", headers=headers,
                            json={"access_control_list": acl}, timeout=30)
        perm = f"{len(acl)} appliquée(s)" if pr.ok else f"ÉCHEC {pr.status_code} {pr.text[:120]}"
    print(f"   [OK] Créé — id={new_id} — permissions : {perm}\n")
    results.append({"name": name, "status": "created", "new_id": new_id, "old_id": p.get("pipeline_id")})

# COMMAND ----------
def count(status):
    return sum(1 for r in results if r["status"] == status)

ok = count("dry_run" if dry_run else "created")
print(f"""
╔══════════════════════════════════════════════════════╗
║    {"DRY-RUN — " if dry_run else ""}RESTAURATION PIPELINES — RÉSUMÉ
╠══════════════════════════════════════════════════════╣
║  Date backup      : {backup_date:<33} ║
║  Sélectionnés     : {len(selected):<33} ║
║  Créés            : {ok:<33} ║
║  Ignorés (skip)   : {count("skipped"):<33} ║
║  Erreurs          : {count("error"):<33} ║
╚══════════════════════════════════════════════════════╝
""")

dbutils.notebook.exit(json.dumps({
    "backup_date": backup_date, "dry_run": dry_run, "selected": len(selected),
    "ok": ok, "skipped": count("skipped"), "errors": count("error"), "results": results,
}))
