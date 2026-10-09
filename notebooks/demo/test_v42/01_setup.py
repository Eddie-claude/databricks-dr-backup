# Databricks notebook source
# notebooks/demo/test_v42/01_setup.py

# COMMAND ----------
# MAGIC %md
# MAGIC # Jeu de test v4.2 — E0 : création des objets
# MAGIC
# MAGIC Crée dans le catalogue `dr_test_v42` et le dossier `/Shared/dr_test_v42` un exemplaire de chaque
# MAGIC objet que la v4.2 sauvegarde et restaure, puis enregistre l'état attendu (`attendu_v1.json`).
# MAGIC Refuse de s'exécuter si le catalogue existe déjà (lancer `05_nettoyer` d'abord).

# COMMAND ----------
# MAGIC %run ./00_commun

# COMMAND ----------
if catalog_exists():
    raise RuntimeError(f"Le catalogue {CATALOG} existe déjà — lancer 05_nettoyer avant un nouveau setup")


def sql(stmt):
    spark.sql(stmt)
    print(f"[OK] {' '.join(stmt.split())[:110]}")

# COMMAND ----------
# MAGIC %md ## Catalogue, schémas, tables, vue

# COMMAND ----------
sql(f"CREATE CATALOG {CATALOG} MANAGED LOCATION '{STORAGE}' COMMENT 'Jeu de test DR v4.2 — à supprimer'")
sql(f"CREATE SCHEMA {CATALOG}.finance COMMENT 'Données finance de test'")
sql(f"CREATE SCHEMA {CATALOG}.rh COMMENT 'Données RH de test'")

sql(f"""CREATE TABLE {CATALOG}.finance.clients (client_id INT, nom STRING, region STRING)
        COMMENT 'Clients de test'""")
sql(f"""INSERT INTO {CATALOG}.finance.clients VALUES
        (1, 'Alpha SA', 'Vaud'), (2, 'Beta Sàrl', 'Genève'), (3, 'Gamma AG', 'Vaud'),
        (4, 'Delta GmbH', 'Zurich'), (5, 'Epsilon SA', 'Valais')""")
sql(f"""CREATE TABLE {CATALOG}.finance.transactions (txn_id INT, client_id INT, montant DOUBLE, txn_date DATE)""")
sql(f"""INSERT INTO {CATALOG}.finance.transactions VALUES
        (1, 1, 1500.0, '2026-09-01'), (2, 2, 820.5, '2026-09-03'), (3, 3, 99.9, '2026-09-10'),
        (4, 1, 4300.0, '2026-09-15'), (5, 4, 12.0, '2026-09-20'), (6, 5, 670.0, '2026-09-28')""")
sql(f"""CREATE TABLE {CATALOG}.rh.employes (emp_id INT, nom STRING, departement STRING)""")
sql(f"""INSERT INTO {CATALOG}.rh.employes VALUES
        (1, 'Alice', 'IT'), (2, 'Bruno', 'Finance'), (3, 'Chloé', 'IT'), (4, 'David', 'RH')""")
sql(f"""CREATE VIEW {CATALOG}.finance.v_ca COMMENT 'Chiffre d''affaires par région' AS
        SELECT c.region, sum(t.montant) AS ca
        FROM {CATALOG}.finance.transactions t JOIN {CATALOG}.finance.clients c USING (client_id)
        GROUP BY c.region""")
# Metric view : SHOW CREATE TABLE la refuse sur le runtime des jobs, 01 reconstruit sa DDL depuis le YAML
spark.sql(f"""CREATE VIEW {CATALOG}.finance.mv_ca WITH METRICS LANGUAGE YAML
COMMENT 'Metric view de test'
AS $$
version: 0.1
source: {CATALOG}.finance.transactions
dimensions:
  - name: client
    expr: client_id
measures:
  - name: total
    expr: SUM(montant)
$$""")
print(f"[OK] CREATE VIEW {CATALOG}.finance.mv_ca WITH METRICS")

# COMMAND ----------
# MAGIC %md ## Tables que le backup ne peut pas copier (écart rapport / restauration)

# COMMAND ----------
# Pas de table externe CSV ni de volume externe : le compte de test n'a ni CREATE EXTERNAL TABLE ni
# CREATE EXTERNAL VOLUME sur l'external location de KeyIT dev.

# Table avec filtre de lignes : DEEP CLONE refuse les tables protégées par un filtre ou un masque
sql(f"""CREATE FUNCTION {CATALOG}.rh.filtre_rh(dept STRING) RETURNS BOOLEAN
        COMMENT 'Filtre de lignes de test'
        RETURN is_account_group_member('admins') OR dept = 'IT'""")
sql(f"""CREATE TABLE {CATALOG}.rh.salaires (emp_id INT, departement STRING, salaire DOUBLE)""")
sql(f"""INSERT INTO {CATALOG}.rh.salaires VALUES (1, 'IT', 9000.0), (2, 'Finance', 8500.0), (3, 'IT', 9500.0)""")
sql(f"ALTER TABLE {CATALOG}.rh.salaires SET ROW FILTER {CATALOG}.rh.filtre_rh ON (departement)")

# COMMAND ----------
# MAGIC %md ## Volumes et fichiers (v1)

# COMMAND ----------
sql(f"CREATE VOLUME {CATALOG}.finance.docs COMMENT 'Documents de test (managé)'")

FILES_V1 = {
    "a.csv":                "id,valeur\n1,alpha\n2,beta\n",
    "b.json":               '{"version": 1, "objet": "à supprimer en v2"}\n',
    "rapports/2026/c.txt":  "Rapport annuel — version 1\n",
    "notes été.txt":        "Nom avec espace et accent\n",
}
for rel, content in FILES_V1.items():
    path = f"/Volumes/{CATALOG}/finance/docs/{rel}"
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"[OK] fichier {path}")

# COMMAND ----------
# MAGIC %md ## Fonctions

# COMMAND ----------
sql(f"""CREATE FUNCTION {CATALOG}.finance.tva(montant DOUBLE) RETURNS DOUBLE
        COMMENT 'Montant TTC (TVA suisse 8,1 %)'
        RETURN round(montant * 1.081, 2)""")
sql(f"""CREATE FUNCTION {CATALOG}.finance.clients_region(r STRING)
        RETURNS TABLE (client_id INT, nom STRING)
        COMMENT 'Fonction de table de test'
        RETURN SELECT client_id, nom FROM {CATALOG}.finance.clients WHERE region = r""")
# Corps Python avec lignes vides en tête, au milieu et en fin : la v4.2 corrige leur multiplication
spark.sql(f"""CREATE FUNCTION {CATALOG}.finance.slug(s STRING) RETURNS STRING
LANGUAGE PYTHON
DETERMINISTIC
COMMENT 'Fonction Python de test'
AS $$

import re
import unicodedata


def to_slug(x):
    x = unicodedata.normalize('NFKD', x).encode('ascii', 'ignore').decode()
    return re.sub(r'[^a-z0-9]+', '-', x.lower()).strip('-')


return to_slug(s)

$$""")
print(f"[OK] CREATE FUNCTION {CATALOG}.finance.slug (Python)")

# COMMAND ----------
# MAGIC %md ## Droits Unity Catalog (directs et hérités)

# COMMAND ----------
# Hérités : SELECT posé sur le schéma finance, donc hérité par ses tables (ne doit PAS être exporté
# comme droit direct sur chaque table). Directs : posés sur l'objet lui-même.
for stmt in [
    f"GRANT USE CATALOG ON CATALOG {CATALOG} TO `{GRANTEE}`",
    f"GRANT USE SCHEMA, SELECT ON SCHEMA {CATALOG}.finance TO `{GRANTEE}`",
    f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.rh TO `{GRANTEE}`",
    f"GRANT SELECT ON TABLE {CATALOG}.rh.employes TO `{GRANTEE}`",
    f"GRANT READ VOLUME ON VOLUME {CATALOG}.finance.docs TO `{GRANTEE}`",
    f"GRANT EXECUTE ON FUNCTION {CATALOG}.finance.tva TO `{GRANTEE}`",
]:
    sql(stmt)

# COMMAND ----------
# MAGIC %md ## Workspace : dossier, notebooks, fichiers, tableau de bord, droits

# COMMAND ----------
def ws_import(path, content: str, fmt: str, language=None):
    body = {"path": path, "format": fmt, "overwrite": True,
            "content": base64.b64encode(content.encode("utf-8")).decode()}
    if language:
        body["language"] = language
    api("POST", "/api/2.0/workspace/import", json=body)
    print(f"[OK] workspace {path}")


api("POST", "/api/2.0/workspace/mkdirs", json={"path": f"{WS_DIR}/config"})
ws_import(f"{WS_DIR}/nb_python", "# Databricks notebook source\nprint('notebook Python de test')\n",
          "SOURCE", "PYTHON")
ws_import(f"{WS_DIR}/nb_sql", "-- Databricks notebook source\nSELECT 'notebook SQL de test' AS msg\n",
          "SOURCE", "SQL")
ws_import(f"{WS_DIR}/nb_pipeline",
          f"-- Databricks notebook source\nCREATE OR REFRESH MATERIALIZED VIEW mv_clients AS "
          f"SELECT * FROM {CATALOG}.finance.clients\n", "SOURCE", "SQL")
ws_import(f"{WS_DIR}/analyse.ipynb", json.dumps({
    "cells": [{"cell_type": "code", "execution_count": None, "metadata": {}, "outputs": [],
               "source": ["print('notebook Jupyter de test')"]}],
    "metadata": {"language_info": {"name": "python"}}, "nbformat": 4, "nbformat_minor": 5}), "JUPYTER")
ws_import(f"{WS_DIR}/script.sh", "#!/bin/bash\necho 'script shell de test'\n", "AUTO")
ws_import(f"{WS_DIR}/config/params.yml", "environnement: test\nseuil: 42\n", "AUTO")

dash = api("POST", "/api/2.0/lakeview/dashboards", json={
    "display_name": "tableau_test", "parent_path": WS_DIR, "warehouse_id": WAREHOUSE_ID,
    "serialized_dashboard": json.dumps({"pages": [{"name": "p1", "displayName": "Page de test"}]})})
print(f"[OK] tableau de bord {dash.get('path')}")

ws_dir_id = api("GET", "/api/2.0/workspace/get-status", params={"path": WS_DIR})["object_id"]
api("PATCH", f"/api/2.0/permissions/directories/{ws_dir_id}",
    json={"access_control_list": [{"group_name": WS_GROUP, "permission_level": "CAN_RUN"}]})
print(f"[OK] droit CAN_RUN de {WS_GROUP} sur {WS_DIR}")

# COMMAND ----------
# MAGIC %md ## Job et pipeline (jamais exécutés), avec leurs droits

# COMMAND ----------
job_id = api("POST", "/api/2.1/jobs/create", json={
    "name": JOB_NAME, "max_concurrent_runs": 1, "tags": {"projet": PREFIX},
    "tasks": [{"task_key": "t1", "notebook_task": {"notebook_path": f"{WS_DIR}/nb_python"},
               "new_cluster": {"spark_version": "17.3.x-scala2.13", "node_type_id": "Standard_DS3_v2",
                               "num_workers": 0, "data_security_mode": "SINGLE_USER",
                               "spark_conf": {"spark.databricks.cluster.profile": "singleNode",
                                              "spark.master": "local[*]"},
                               "custom_tags": {"ResourceClass": "SingleNode"}}}]})["job_id"]
api("PATCH", f"/api/2.0/permissions/jobs/{job_id}",
    json={"access_control_list": [{"group_name": WS_GROUP, "permission_level": "CAN_VIEW"}]})
print(f"[OK] job {JOB_NAME} ({job_id}) + CAN_VIEW {WS_GROUP}")

pipeline_id = api("POST", "/api/2.0/pipelines", json={
    "name": PIPELINE_NAME, "catalog": CATALOG, "schema": "finance", "development": True,
    "continuous": False, "libraries": [{"notebook": {"path": f"{WS_DIR}/nb_pipeline"}}]})["pipeline_id"]
api("PATCH", f"/api/2.0/permissions/pipelines/{pipeline_id}",
    json={"access_control_list": [{"group_name": WS_GROUP, "permission_level": "CAN_VIEW"}]})
print(f"[OK] pipeline {PIPELINE_NAME} ({pipeline_id}) + CAN_VIEW {WS_GROUP}")

# COMMAND ----------
# MAGIC %md ## État attendu v1

# COMMAND ----------
attendu = etat()
attendu["fichiers_docs_contenu"] = FILES_V1
save_json("attendu_v1.json", attendu)
dbutils.notebook.exit(json.dumps({"status": "ok", "tables": len(attendu["tables"]),
                                  "fonctions": len(attendu["fonctions"]), "grants": len(attendu["grants"]),
                                  "workspace": len(attendu["workspace"].get("objets", {}))}))
