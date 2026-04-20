# Databricks notebook source
# notebooks/demo/07_volume_booster.py
#
# OBJECTIF : Atteindre ~1 To de données de test en utilisant DEEP CLONE.
#
# POURQUOI DEEP CLONE et pas de génération Spark ?
#   DEEP CLONE copie les fichiers Parquet côté serveur Azure (server-side copy)
#   sans les lire ni les écrire via le cluster. Pour 36 Go : ~2 min au lieu
#   de ~70 min avec une approche Spark write. Gain : ×35 sur le temps.
#
# STRATÉGIE :
#   Cloner les 3 grandes tables (payments 36.8 Go, sensor_readings 26.2 Go,
#   refunds 8.8 Go) N fois chacune, en round-robin, jusqu'à la cible.
#   Les clones sont créés dans les mêmes schémas avec un suffixe _c01, _c02...

# COMMAND ----------
# MAGIC %md # 07 — Volume Booster via DEEP CLONE (→ 1 To)
# MAGIC
# MAGIC **Méthode** : `CREATE TABLE target DEEP CLONE source`
# MAGIC
# MAGIC La copie est serveur-side (Azure Blob copy) — pas de lecture/écriture Spark.
# MAGIC
# MAGIC | Table source | Taille | ×13 clones | Volume total |
# MAGIC |---|---|---|---|
# MAGIC | `payments` | 36.8 Go | +478 Go | 515 Go |
# MAGIC | `sensor_readings` | 26.2 Go | +341 Go | 367 Go |
# MAGIC | `refunds` | 8.8 Go | +114 Go | 123 Go |
# MAGIC | Autres (existant) | 5.4 Go | — | 5.4 Go |
# MAGIC | **Total estimé** | | | **~1 010 Go** |

# COMMAND ----------
from datetime import datetime

dbutils.widgets.text("target_gb",   "1024", "Cible totale en Go")
dbutils.widgets.text("dry_run",     "false", "true = affiche les commandes sans les exécuter")
dbutils.widgets.text("max_clones",  "15",   "Nombre max de clones par table source (garde-fou)")

TARGET_GB  = int(dbutils.widgets.get("target_gb"))
DRY_RUN    = dbutils.widgets.get("dry_run").lower() == "true"
MAX_CLONES = int(dbutils.widgets.get("max_clones"))

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def size_gb(table_fqn):
    try:
        row = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
        return row["sizeInBytes"] / (1024**3)
    except Exception:
        return 0.0

ALL_TABLES = [
    "backup_test_finance.transactions.payments",
    "backup_test_finance.transactions.refunds",
    "backup_test_finance.reporting.monthly_summary",
    "backup_test_iot.raw.sensor_readings",
    "backup_test_iot.processed.aggregates",
    "backup_test_hr.employees.contracts",
    "backup_test_hr.employees.payroll",
    "backup_test_hr.audit.access_logs",
    "backup_test_ref.geo.postal_codes",
] + [f"backup_test_ref.config.{n}" for n in [
    "feature_flags", "rate_limits", "thresholds", "mappings",
    "schedules", "templates", "rules", "parameters"
]]

def total_gb_all():
    return sum(size_gb(t) for t in ALL_TABLES)

# COMMAND ----------
# MAGIC %md ## État initial

# COMMAND ----------
start_gb = total_gb_all()
needed   = TARGET_GB - start_gb

log(f"Volume actuel : {start_gb:.1f} Go")
log(f"Cible         : {TARGET_GB} Go")
log(f"À ajouter     : {needed:.1f} Go")
log(f"Dry-run       : {DRY_RUN}")

if needed <= 0:
    print(f"\n[OK] Cible déjà atteinte ({start_gb:.1f} Go >= {TARGET_GB} Go)")
    dbutils.notebook.exit("already_at_target")

# COMMAND ----------
# MAGIC %md ## Boucle DEEP CLONE
# MAGIC
# MAGIC Round-robin sur les 3 grandes tables, stop quand la cible est atteinte.

# COMMAND ----------

# Sources à cloner, triées par taille décroissante pour maximiser le volume par clone
CLONE_SOURCES = [
    {
        "src":          "backup_test_finance.transactions.payments",
        "tgt_pattern":  "backup_test_finance.transactions.payments_c{n:02d}",
        "est_gb":        36.8,
    },
    {
        "src":          "backup_test_iot.raw.sensor_readings",
        "tgt_pattern":  "backup_test_iot.raw.sensor_readings_c{n:02d}",
        "est_gb":        26.2,
    },
    {
        "src":          "backup_test_finance.transactions.refunds",
        "tgt_pattern":  "backup_test_finance.transactions.refunds_c{n:02d}",
        "est_gb":        8.8,
    },
    {
        "src":          "backup_test_iot.processed.aggregates",
        "tgt_pattern":  "backup_test_iot.processed.aggregates_c{n:02d}",
        "est_gb":        2.9,
    },
]

# Compteur de clones par source
clone_counters = {cfg["src"]: 0 for cfg in CLONE_SOURCES}
round_idx      = 0
total_cloned   = 0.0

while True:
    current_gb = start_gb + total_cloned   # estimation rapide (évite DESCRIBE DETAIL à chaque tour)
    remaining  = TARGET_GB - current_gb

    if remaining <= 0:
        log(f"[DONE] Cible {TARGET_GB} Go atteinte (estimé : {current_gb:.1f} Go)")
        break

    cfg = CLONE_SOURCES[round_idx % len(CLONE_SOURCES)]
    round_idx += 1

    n   = clone_counters[cfg["src"]] + 1
    tgt = cfg["tgt_pattern"].format(n=n)

    if n > MAX_CLONES:
        log(f"[SKIP] {cfg['src']} : limite {MAX_CLONES} clones atteinte")
        # Passer à la source suivante
        if all(clone_counters[c["src"]] >= MAX_CLONES for c in CLONE_SOURCES):
            log("[WARN] Toutes les sources ont atteint la limite MAX_CLONES. Arrêt.")
            break
        continue

    log(f"CLONE {n:02d} : {cfg['src']} → {tgt}  (+{cfg['est_gb']:.1f} Go estimés | restant : {remaining:.1f} Go)")

    if DRY_RUN:
        print(f"  [DRY-RUN] CREATE TABLE {tgt} DEEP CLONE {cfg['src']}")
    else:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {tgt} DEEP CLONE {cfg['src']}")

    clone_counters[cfg["src"]] = n
    total_cloned += cfg["est_gb"]

# COMMAND ----------
# MAGIC %md ## Rapport final

# COMMAND ----------
# Scan réel pour le rapport (DESCRIBE DETAIL sur toutes les tables + clones)
real_total = 0.0
clone_rows = []

for cfg in CLONE_SOURCES:
    src_gb = size_gb(cfg["src"])
    real_total += src_gb
    n_clones = clone_counters[cfg["src"]]
    for i in range(1, n_clones + 1):
        tgt = cfg["tgt_pattern"].format(n=i)
        gb  = size_gb(tgt)
        real_total += gb
        clone_rows.append((tgt, gb))

# Tables non-clonées
other_tables = [t for t in ALL_TABLES
                if not any(t == cfg["src"] for cfg in CLONE_SOURCES)]
for t in other_tables:
    gb = size_gb(t)
    real_total += gb

print(f"\n{'Table':<60} {'Go':>8}")
print("─" * 72)
for cfg in CLONE_SOURCES:
    gb = size_gb(cfg["src"])
    print(f"  {cfg['src']:<58} {gb:>7.1f}")
    n_clones = clone_counters[cfg["src"]]
    for i in range(1, n_clones + 1):
        tgt = cfg["tgt_pattern"].format(n=i)
        gb  = size_gb(tgt)
        print(f"  {tgt:<58} {gb:>7.1f}  (clone)")

print("─" * 72)
print(f"  {'TOTAL':<58} {real_total:>7.1f} Go")
print(f"\n{'[OK]' if real_total >= TARGET_GB else '[WARN]'} Cible {TARGET_GB} Go : {real_total:.1f} Go réels")
