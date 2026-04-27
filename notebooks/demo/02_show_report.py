# Databricks notebook source
# notebooks/demo/02_show_report.py

# COMMAND ----------
# MAGIC %md
# MAGIC # 📊 Acte 3 — Diff J/J-1 et rapport HTML
# MAGIC
# MAGIC Ce notebook affiche :
# MAGIC 1. Le diff JSON entre deux backups consécutifs
# MAGIC 2. Le rapport HTML généré automatiquement
# MAGIC
# MAGIC **Durée estimée : ~2 minutes**

# COMMAND ----------
# MAGIC %md ## 3.1 — Paramètres

# COMMAND ----------
dbutils.widgets.text("backup_root", "abfss://uc-data@st10keyitdpdrpdevchn00.dfs.core.windows.net/backup", "Backup root")
dbutils.widgets.text("backup_date", "", "Date du rapport (vide = dernier)")

backup_root = dbutils.widgets.get("backup_root")
backup_date = dbutils.widgets.get("backup_date")

# Si pas de date fournie, lire latest.json
if not backup_date:
    import json
    latest = json.loads(dbutils.fs.head(f"{backup_root}/latest.json"))
    backup_date = latest["date"]
    print(f"[AUTO] Date détectée depuis latest.json : {backup_date}")
else:
    print(f"[OK] Date backup : {backup_date}")

# COMMAND ----------
# MAGIC %md ## 3.2 — Statut du dernier backup

# COMMAND ----------
import json

latest = json.loads(dbutils.fs.head(f"{backup_root}/latest.json"))

status_icon = "✅" if latest["status"] == "success" else ("⚠️" if latest["status"] == "degraded" else "❌")
print(f"{status_icon} Dernier backup : {latest['date']} — statut : {latest['status'].upper()}")
print("\nDétail des étapes :")
for step, result in latest.get("steps_summary", {}).items():
    icon = "✅" if result == "success" else "❌"
    print(f"  {icon} {step}: {result}")

# COMMAND ----------
# MAGIC %md ## 3.3 — Diff J vs J-1

# COMMAND ----------
diff_path = f"{backup_root}/{backup_date}/diff/diff_{backup_date}.json"

try:
    diff = json.loads(dbutils.fs.head(diff_path, 10_000_000))

    print(f"Comparaison : {diff['date_prev']} → {diff['date_curr']}\n")

    tables_added   = diff["tables"]["added"]
    tables_removed = diff["tables"]["removed"]
    tables_unchanged = diff["tables"]["unchanged"]

    print(f"📊 Tables Delta :")
    print(f"  [+] Ajoutées   : {len(tables_added)}")
    for t in tables_added:   print(f"      ✅ {t}")
    print(f"  [-] Supprimées : {len(tables_removed)}")
    for t in tables_removed: print(f"      ❌ {t}")
    print(f"  [=] Inchangées : {len(tables_unchanged)}")

    jobs_added   = diff["jobs"]["added"]
    jobs_removed = diff["jobs"]["removed"]
    print(f"\n💼 Jobs :")
    print(f"  [+] Ajoutés    : {len(jobs_added)}")
    for j in jobs_added:   print(f"      ✅ {j}")
    print(f"  [-] Supprimés  : {len(jobs_removed)}")
    for j in jobs_removed: print(f"      ❌ {j}")

except Exception as e:
    print(f"[WARN] Pas de diff disponible pour {backup_date}: {e}")
    print("       Assurez-vous que deux backups ont été effectués (J et J+1)")

# COMMAND ----------
# MAGIC %md ## 3.4 — Rapport HTML complet

# COMMAND ----------
report_path = f"{backup_root}/{backup_date}/report/dr_report_{backup_date}.html"

try:
    html = dbutils.fs.head(report_path, 10_000_000)
    print(f"[OK] Rapport chargé depuis : {report_path}")
    displayHTML(html)
except Exception as e:
    print(f"[ERROR] Rapport non disponible : {e}")
    print(f"        Chemin attendu : {report_path}")

# COMMAND ----------
# MAGIC %md ## 3.5 — Fichiers UC Metadata sauvegardés

# COMMAND ----------
uc_path = f"{backup_root}/{backup_date}/uc_metadata"
print(f"Fichiers UC Metadata dans {uc_path} :\n")

files = dbutils.fs.ls(uc_path)
for f in files:
    size_kb = round(f.size / 1024, 1)
    print(f"  📄 {f.name} ({size_kb} KB)")

print("\n[NEXT] Acte 4 : Scénario DR — simulation d'un sinistre et restauration")
