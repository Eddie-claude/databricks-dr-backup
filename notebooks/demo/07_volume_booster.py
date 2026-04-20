# Databricks notebook source
# notebooks/demo/07_volume_booster.py
#
# OBJECTIF : Compléter les tables de test jusqu'à ~1 To en ajoutant des batches
#            de données denses (valeurs réellement aléatoires par élément d'array).
#
# POURQUOI le script initial n'a produit que ~78 Go au lieu de ~1 To ?
#   array_repeat(hex(rand()), 18) évalue rand() UNE SEULE FOIS et répète
#   18× la même valeur → compression Parquet quasi-parfaite → ×7 à ×13 de ratio.
#   Ce notebook utilise transform(sequence(1, N), i -> hex(rand())) qui génère
#   N valeurs DISTINCTES → données incompressibles → densité réelle.

# COMMAND ----------
# MAGIC %md # 07 — Volume Booster (→ 1 To)
# MAGIC
# MAGIC Ce notebook complète les tables de test backup jusqu'à ~1 To.
# MAGIC
# MAGIC **Stratégie** : append de batches denses sur les 2 tables volumineuses
# MAGIC (`payments` et `sensor_readings`), puis complétion des autres.
# MAGIC
# MAGIC **Durée estimée** : ~3-5h selon le cluster

# COMMAND ----------
from pyspark.sql import functions as F
from datetime import datetime

dbutils.widgets.text("target_gb",    "1024", "Cible totale en Go")
dbutils.widgets.text("batch_rows",   "10000000", "Lignes par batch (défaut 10M)")
dbutils.widgets.text("dry_run",      "false", "true = calcul uniquement, pas d'écriture")

TARGET_GB  = int(dbutils.widgets.get("target_gb"))
BATCH_ROWS = int(dbutils.widgets.get("batch_rows"))
DRY_RUN    = dbutils.widgets.get("dry_run").lower() == "true"

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def size_gb(table_fqn):
    row = spark.sql(f"DESCRIBE DETAIL {table_fqn}").collect()[0]
    return row["sizeInBytes"] / (1024**3), row["numFiles"]

def total_gb():
    tables = [
        "backup_test_finance.transactions.payments",
        "backup_test_finance.transactions.refunds",
        "backup_test_finance.reporting.monthly_summary",
        "backup_test_iot.raw.sensor_readings",
        "backup_test_iot.processed.aggregates",
        "backup_test_hr.employees.contracts",
        "backup_test_hr.employees.payroll",
        "backup_test_hr.audit.access_logs",
        "backup_test_ref.geo.postal_codes",
    ]
    return sum(size_gb(t)[0] for t in tables)

# COMMAND ----------
# MAGIC %md ## État actuel

# COMMAND ----------
current_gb = total_gb()
needed_gb  = TARGET_GB - current_gb
log(f"Volume actuel : {current_gb:.1f} Go")
log(f"Cible         : {TARGET_GB} Go")
log(f"À générer     : {needed_gb:.1f} Go")
log(f"Dry-run       : {DRY_RUN}")

if needed_gb <= 0:
    print(f"\n[OK] Cible déjà atteinte ({current_gb:.1f} Go >= {TARGET_GB} Go)")
    dbutils.notebook.exit("already_at_target")

# COMMAND ----------
# MAGIC %md ## Fonctions de génération dense
# MAGIC
# MAGIC Différence clé : `transform(sequence(1, N), i -> expr)` évalue `expr`
# MAGIC indépendamment pour chaque élément → N valeurs distinctes → pas de compression.

# COMMAND ----------

def gen_payments_dense(n_rows=BATCH_ROWS):
    """
    Payload de ~350 bytes/row non compressible.
    array_repeat remplacé par transform(sequence()) → valeurs distinctes par élément.
    """
    return spark.range(n_rows).select(
        F.expr("uuid()").alias("payment_id"),
        F.expr("uuid()").alias("reference"),
        F.expr("date_add('2019-01-01', cast(rand() * 2190 as int))").alias("payment_date"),
        F.expr("element_at(array('FR','DE','CH','ES','IT','US','GB','NL','BE','PL'), cast(rand()*10+1 as int))").alias("country"),
        F.expr("element_at(array('CARD','WIRE','SEPA','PAYPAL','CRYPTO'), cast(rand()*5+1 as int))").alias("method"),
        F.expr("cast(rand() * 50000 as decimal(18,2))").alias("amount"),
        F.expr("cast(rand() * 500000 as int)").alias("customer_id"),
        F.expr("cast(rand() * 10000 as int)").alias("merchant_id"),
        F.expr("element_at(array('SUCCESS','FAILED','PENDING','REFUNDED'), cast(rand()*4+1 as int))").alias("status"),
        # Valeurs DISTINCTES par élément → incompressible
        F.expr("transform(sequence(1, 20), i -> hex(rand()))").alias("payload"),
        F.expr("transform(sequence(1, 10), i -> rand())").alias("amounts_history"),
        F.expr("transform(sequence(1, 8),  i -> uuid())").alias("audit_trail"),
    )

def gen_sensor_dense(n_rows=BATCH_ROWS):
    """Payload de ~280 bytes/row non compressible."""
    return spark.range(n_rows).select(
        F.expr("uuid()").alias("reading_id"),
        F.expr("cast(rand() * 100000 as int)").alias("device_id"),
        F.expr("element_at(array('TEMP','PRESSURE','HUMIDITY','FLOW','VOLTAGE','CURRENT'), cast(rand()*6+1 as int))").alias("device_type"),
        F.expr("date_add('2021-01-01', cast(rand() * 1095 as int))").alias("reading_date"),
        F.expr("to_timestamp(date_add('2021-01-01', cast(rand()*1095 as int)))").alias("reading_ts"),
        F.expr("cast(rand() * 1000 as decimal(12,4))").alias("value"),
        F.expr("cast(rand() * 100 as decimal(5,2))").alias("battery_pct"),
        F.expr("element_at(array('OK','WARN','ERROR','OFFLINE'), cast(rand()*4+1 as int))").alias("status"),
        F.expr("transform(sequence(1, 18), i -> hex(rand()))").alias("raw_payload"),
        F.expr("transform(sequence(1, 6),  i -> rand())").alias("calibration_offsets"),
        F.expr("struct(rand(), rand(), rand())").alias("geo_coords"),
    )

def gen_refunds_dense(n_rows=BATCH_ROWS):
    """Payload ~200 bytes/row."""
    return spark.range(n_rows).select(
        F.expr("uuid()").alias("refund_id"),
        F.expr("uuid()").alias("original_payment_ref"),
        F.expr("date_add('2020-01-01', cast(rand() * 1825 as int))").alias("refund_date"),
        F.expr("element_at(array('FR','DE','CH','ES','IT','US','GB'), cast(rand()*7+1 as int))").alias("country"),
        F.expr("cast(rand() * 5000 as decimal(18,2))").alias("refund_amount"),
        F.expr("element_at(array('CUSTOMER_REQUEST','FRAUD','TECHNICAL','DUPLICATE'), cast(rand()*4+1 as int))").alias("reason"),
        F.expr("transform(sequence(1, 12), i -> hex(rand()))").alias("notes"),
        F.expr("transform(sequence(1, 5),  i -> uuid())").alias("approvers"),
    )

# COMMAND ----------
# MAGIC %md ## Boucle de remplissage
# MAGIC
# MAGIC Le notebook s'arrête automatiquement quand la cible est atteinte.

# COMMAND ----------

# Estimation de la taille par batch (mesurée empiriquement sur les fonctions denses)
# ~0.35 Go/batch pour payments, ~0.28 Go pour sensor_readings, ~0.20 Go pour refunds
BATCH_ESTIMATES = {
    "payments":        0.35,   # Go par batch de 10M lignes
    "sensor_readings": 0.28,
    "refunds":         0.20,
}

# Ordre de priorité : les plus grandes tables d'abord (meilleur ratio volume/temps)
TABLES_CONFIG = [
    {
        "name":       "payments",
        "fqn":        "backup_test_finance.transactions.payments",
        "gen_fn":     gen_payments_dense,
        "partition":  ["payment_date", "country"],
    },
    {
        "name":       "sensor_readings",
        "fqn":        "backup_test_iot.raw.sensor_readings",
        "gen_fn":     gen_sensor_dense,
        "partition":  ["reading_date", "device_type"],
    },
    {
        "name":       "refunds",
        "fqn":        "backup_test_finance.transactions.refunds",
        "gen_fn":     gen_refunds_dense,
        "partition":  ["refund_date", "country"],
    },
]

batch_global = 0

while True:
    current_gb = total_gb()
    remaining  = TARGET_GB - current_gb

    log(f"─── Volume courant : {current_gb:.1f} Go | Restant : {remaining:.1f} Go ───")

    if remaining <= 0:
        log(f"[DONE] Cible {TARGET_GB} Go atteinte !")
        break

    # Choisir la table à remplir (round-robin sur les 3)
    cfg = TABLES_CONFIG[batch_global % len(TABLES_CONFIG)]
    batch_global += 1

    estimated_add = BATCH_ESTIMATES[cfg["name"]]
    log(f"  Batch {batch_global} → {cfg['name']} (+~{estimated_add:.2f} Go estimés)")

    if DRY_RUN:
        log(f"  [DRY-RUN] Écriture simulée dans {cfg['fqn']}")
        # En dry-run : simuler 3 batches puis sortir
        if batch_global >= 3:
            log("[DRY-RUN] Simulation terminée (3 batches)")
            break
        continue

    df = cfg["gen_fn"](BATCH_ROWS)
    writer = df.write.format("delta").mode("append")
    if cfg["partition"]:
        writer = writer.partitionBy(*cfg["partition"])
    writer.saveAsTable(cfg["fqn"])

    # Vérification réelle toutes les 5 batches (DESCRIBE DETAIL est lent)
    if batch_global % 5 == 0:
        gb_after, nf = size_gb(cfg["fqn"])
        log(f"  → {cfg['name']} : {gb_after:.1f} Go | {nf:,} fichiers")

# COMMAND ----------
# MAGIC %md ## Rapport final

# COMMAND ----------
all_tables = [
    ("backup_test_finance", "transactions", "payments"),
    ("backup_test_finance", "transactions", "refunds"),
    ("backup_test_finance", "reporting",    "monthly_summary"),
    ("backup_test_iot",     "raw",          "sensor_readings"),
    ("backup_test_iot",     "processed",    "aggregates"),
    ("backup_test_hr",      "employees",    "contracts"),
    ("backup_test_hr",      "employees",    "payroll"),
    ("backup_test_hr",      "audit",        "access_logs"),
    ("backup_test_ref",     "geo",          "postal_codes"),
] + [("backup_test_ref", "config", n) for n in [
    "feature_flags", "rate_limits", "thresholds", "mappings",
    "schedules", "templates", "rules", "parameters"
]]

total = 0
print(f"\n{'Catalog':<25} {'Schema':<15} {'Table':<25} {'Go':>8} {'Fichiers':>10}")
print("─" * 90)
for cat, sch, tbl in all_tables:
    gb, nf = size_gb(f"{cat}.{sch}.{tbl}")
    total += gb
    print(f"{cat:<25} {sch:<15} {tbl:<25} {gb:>7.1f} {nf:>10,}")
print("─" * 90)
print(f"{'TOTAL':<65} {total:>7.1f} Go")
print(f"\n{'[OK]' if total >= TARGET_GB else '[WARN]'} Cible {TARGET_GB} Go : {total:.1f} Go générés")
