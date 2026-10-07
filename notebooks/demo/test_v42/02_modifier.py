# Databricks notebook source
# notebooks/demo/test_v42/02_modifier.py

# COMMAND ----------
# MAGIC %md
# MAGIC # Jeu de test v4.2 — E2 : modifications entre les deux backups
# MAGIC
# MAGIC À lancer **après** le backup J-1 et **avant** le backup J. Modifie les fichiers du volume managé
# MAGIC (un modifié, un supprimé, un ajouté) et ajoute des lignes dans une table, puis enregistre l'état
# MAGIC attendu v2 (`attendu_v2.json`). La restauration à J-1 doit redonner les fichiers v1, à J les v2.

# COMMAND ----------
# MAGIC %run ./00_commun

# COMMAND ----------
DOCS = f"/Volumes/{CATALOG}/finance/docs"

with open(f"{DOCS}/a.csv", "w", encoding="utf-8") as f:
    f.write("id,valeur\n1,alpha\n2,beta\n3,gamma ajouté en v2\n")
os.remove(f"{DOCS}/b.json")
os.makedirs(f"{DOCS}/nouveau", exist_ok=True)
with open(f"{DOCS}/nouveau/d.txt", "w", encoding="utf-8") as f:
    f.write("Fichier ajouté en v2\n")
print("[OK] a.csv modifié, b.json supprimé, nouveau/d.txt ajouté")

spark.sql(f"""INSERT INTO {CATALOG}.finance.transactions VALUES
              (7, 2, 310.0, '2026-10-01'), (8, 3, 45.5, '2026-10-02')""")
print("[OK] 2 lignes ajoutées dans finance.transactions")

# COMMAND ----------
attendu = etat()
save_json("attendu_v2.json", attendu)
dbutils.notebook.exit(json.dumps({"status": "ok", "fichiers_docs": sorted(attendu["fichiers_docs"])}))
