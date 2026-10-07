# Databricks notebook source
# notebooks/demo/test_v42/05_nettoyer.py

# COMMAND ----------
# MAGIC %md
# MAGIC # Jeu de test v4.2 — E8 : nettoyage (DESTRUCTIF)
# MAGIC
# MAGIC Supprime tous les objets de test, y compris les fichiers du stockage externe et l'emplacement du
# MAGIC catalogue. Les résultats (`…/dr_test_v42_resultats`) et les backups (`backup-dev`) sont conservés.
# MAGIC Exige `confirm = DETRUIRE dr_test_v42`.

# COMMAND ----------
# MAGIC %run ./00_commun

# COMMAND ----------
dbutils.widgets.text("confirm", "", f"Taper : {CONFIRM}")
if dbutils.widgets.get("confirm") != CONFIRM:
    raise ValueError(f"Confirmation absente : renseigner confirm = '{CONFIRM}'")

done = destroy(include_storage=True)
dbutils.notebook.exit(json.dumps({"status": "ok", "supprime": done}, ensure_ascii=False))
