# Databricks notebook source
# notebooks/demo/test_v42/04_sinistre.py

# COMMAND ----------
# MAGIC %md
# MAGIC # Jeu de test v4.2 — E5 : sinistre simulé (DESTRUCTIF)
# MAGIC
# MAGIC Supprime le catalogue `dr_test_v42` (CASCADE), le dossier `/Shared/dr_test_v42`, le job et le
# MAGIC pipeline de test. Les fichiers du volume externe restent sur leur stockage, comme lors d'un vrai
# MAGIC sinistre. Exige `confirm = DETRUIRE dr_test_v42` ; ne touche à aucun objet sans ce préfixe.

# COMMAND ----------
# MAGIC %run ./00_commun

# COMMAND ----------
dbutils.widgets.text("confirm", "", f"Taper : {CONFIRM}")
if dbutils.widgets.get("confirm") != CONFIRM:
    raise ValueError(f"Confirmation absente : renseigner confirm = '{CONFIRM}'")

done = destroy(include_storage=False)
dbutils.notebook.exit(json.dumps({"status": "ok", "supprime": done}, ensure_ascii=False))
