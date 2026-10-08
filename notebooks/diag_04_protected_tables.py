# Databricks notebook source
# notebooks/diag_04_protected_tables.py

# COMMAND ----------
# MAGIC %md # Visibilité des tables protégées pour le compte de backup
# MAGIC
# MAGIC **Lecture seule.** DEEP CLONE refuse les tables portant un **filtre de lignes** ou un **masque de
# MAGIC colonnes**, quel que soit le compte. La seule façon de sauvegarder leurs données serait une copie
# MAGIC par lecture (`CREATE TABLE … AS SELECT`), qui n'est complète que si le compte qui lit voit
# MAGIC **toutes les lignes** et **les valeurs réelles**.
# MAGIC
# MAGIC Ce notebook vérifie ce point table par table, en appelant directement chaque fonction de filtre et
# MAGIC de masque avec des valeurs de test (dont NULL). Ces fonctions décident selon l'identité de
# MAGIC l'appelant : **le résultat vaut pour le compte qui exécute le notebook**. Le lancer donc sous
# MAGIC l'identité du job de backup (job ponctuel dont le « Run as » est le service principal du backup).
# MAGIC
# MAGIC | Verdict | Signification |
# MAGIC |---------|---------------|
# MAGIC | COMPLETE | Toutes les lignes et les valeurs réelles : une copie par lecture serait complète |
# MAGIC | LIGNES_FILTREES | Le filtre masque des lignes à ce compte : copie incomplète |
# MAGIC | VALEURS_MASQUEES | Des colonnes sont masquées pour ce compte : copie inutilisable pour ces colonnes |
# MAGIC | INDETERMINE | Fonction non appelable (droit EXECUTE manquant, type non testable…) : à vérifier à la main |
# MAGIC
# MAGIC Prérequis : `USE CATALOG` sur le catalog `system` (lecture d'information_schema) et `EXECUTE` sur
# MAGIC les fonctions de filtre et de masque.

# COMMAND ----------
import json
from itertools import zip_longest

dbutils.widgets.text("catalog_filter", "", "Catalogs à contrôler (virgules, vide = tous)")
dbutils.widgets.text("output_dir", "", "Dossier workspace du rapport HTML (vide = pas de fichier)")
catalog_filter = {c.strip().lower() for c in dbutils.widgets.get("catalog_filter").split(",") if c.strip()}
output_dir     = dbutils.widgets.get("output_dir").strip().rstrip("/")

me = spark.sql("SELECT current_user()").first()[0]
print(f"[OK] Identité contrôlée : {me} — les verdicts valent pour CE compte")

# COMMAND ----------
# MAGIC %md ## Règles
# MAGIC Bloc pur (aucune I/O), testé hors Databricks par `tests/test_protected_rules.py`.

# COMMAND ----------
# >>> REGLES
def test_literals(data_type: str) -> list:
    """Littéraux SQL de test pour un type de colonne (variés, pour qu'un filtre métier en écarte)."""
    t = (data_type or "").strip()
    low = t.lower()
    if low.startswith(("string", "varchar", "char")):
        return ["'dr_test_a'", "'dr_test_b'", "''"]
    if low in ("int", "integer", "bigint", "smallint", "tinyint", "long", "short", "byte"):
        return ["0", "-1", "123456"]
    if low.startswith(("decimal", "double", "float", "real", "numeric")):
        return [f"CAST({v} AS {t})" for v in ("0", "-1.5", "12345.25")]
    if low == "boolean":
        return ["true", "false"]
    if low == "date":
        return ["DATE'1970-01-01'", "DATE'2999-12-31'"]
    if low.startswith("timestamp"):
        return ["TIMESTAMP'1970-01-01 00:00:00'", "TIMESTAMP'2999-12-31 23:59:59'"]
    return ["NULL"]


def test_calls(types: list) -> list:
    """Jeux d'arguments : chaque valeur de test au moins une fois, plus une ligne tout NULL."""
    lists = [test_literals(t) for t in types]
    width = max((len(l) for l in lists), default=0)
    calls = [[l[i % len(l)] for l in lists] for i in range(width)]
    return calls + [["NULL"] * len(types)]


def filter_verdict(results: list) -> str:
    """results : True / False / None (NULL renvoyé) / 'ERR' pour chaque appel du filtre."""
    if not results or "ERR" in results:
        return "INDETERMINE"
    return "TOUTES" if all(r is True for r in results) else "PARTIELLE"


def mask_verdict(results: list) -> str:
    """results : True si le masque rend la valeur intacte (<=>), False sinon, 'ERR' si erreur."""
    if not results or "ERR" in results:
        return "INDETERMINE"
    return "INTACTES" if all(r is True for r in results) else "MASQUEES"


def table_verdict(filter_v, mask_vs: list) -> str:
    verdicts = ([filter_v] if filter_v else []) + list(mask_vs)
    if "INDETERMINE" in verdicts:
        return "INDETERMINE"
    if filter_v == "PARTIELLE":
        return "LIGNES_FILTREES"
    if "MASQUEES" in mask_vs:
        return "VALEURS_MASQUEES"
    return "COMPLETE"
# <<< REGLES

# COMMAND ----------
# MAGIC %md ## Tables protégées et types de leurs colonnes

# COMMAND ----------
IS = "system.information_schema"


def _rows(sql: str) -> list:
    return [r.asDict() for r in spark.sql(sql).collect()]


def _short(e) -> str:
    return " ".join(str(e).split("JVM stacktrace:")[0].split())[:300]


try:
    filters = _rows(f"SELECT * FROM {IS}.row_filters")
    masks   = _rows(f"SELECT * FROM {IS}.column_masks")
except Exception as e:
    raise RuntimeError(f"information_schema illisible pour {me} ({_short(e)}) — accorder "
                       "USE CATALOG sur le catalog system à ce compte") from e


def _key(r):
    return (r["table_catalog"], r["table_schema"], r["table_name"])


def _in_scope(r):
    return r["table_catalog"] != "system" and (not catalog_filter or r["table_catalog"].lower() in catalog_filter)


filters = [r for r in filters if _in_scope(r)]
masks   = [r for r in masks if _in_scope(r)]
tables  = sorted({_key(r) for r in filters + masks})
print(f"[OK] {len(tables)} table(s) protégée(s) : {len(filters)} filtre(s), {len(masks)} masque(s)")

col_types = {}
for cat in sorted({t[0] for t in tables}):
    for r in _rows(f"SELECT table_schema, table_name, column_name, full_data_type FROM `{cat}`.information_schema.columns"):
        col_types[(cat, r["table_schema"], r["table_name"], r["column_name"].lower())] = r["full_data_type"]


def _cols(key, csv: str) -> list:
    return [c.strip().strip("`") for c in (csv or "").split(",") if c.strip()]


def _call(sql: str):
    try:
        return spark.sql(sql).first()[0]
    except Exception as e:
        print(f"    [ERR] {sql[:140]} → {_short(e)[:200]}")
        return "ERR"

# COMMAND ----------
# MAGIC %md ## Contrôle

# COMMAND ----------
results = []
for key in tables:
    fqn = ".".join(f"`{p}`" for p in key)
    print(f"\n▶ {'.'.join(key)}")
    try:
        spark.sql(f"SELECT 1 FROM {fqn} LIMIT 1").collect()
        readable = True
    except Exception as e:
        readable = False
        print(f"    [WARN] table illisible : {_short(e)[:200]}")

    filter_v, detail = None, []
    for f in [f for f in filters if _key(f) == key]:
        cols = _cols(key, f["target_columns"])
        types = [col_types.get((*key, c.lower()), "") for c in cols]
        calls = [_call(f"SELECT {f['filter_name']}({', '.join(args)})") for args in test_calls(types)]
        filter_v = filter_verdict(calls)
        detail.append(f"filtre {f['filter_name']}({', '.join(cols)}) → {filter_v}")

    mask_vs = []
    for m in [m for m in masks if _key(m) == key]:
        col = m["column_name"]
        types = [col_types.get((*key, c.lower()), "") for c in [col] + _cols(key, m["using_columns"])]
        checks = []
        for args in test_calls(types):
            # La valeur rendue par le masque est-elle identique à la valeur d'entrée ?
            checks.append(_call(f"SELECT {m['mask_name']}({', '.join(args)}) <=> {args[0]}"))
        mv = mask_verdict(checks)
        mask_vs.append(mv)
        detail.append(f"masque {m['mask_name']} sur {col} → {mv}")

    verdict = table_verdict(filter_v, mask_vs) if readable else "INDETERMINE"
    print(f"    {verdict} — {' ; '.join(detail)}")
    results.append({"table": ".".join(key), "lisible": readable, "verdict": verdict, "detail": " ; ".join(detail)})

# COMMAND ----------
# MAGIC %md ## Résultat

# COMMAND ----------
summary = {v: sum(1 for r in results if r["verdict"] == v)
           for v in ("COMPLETE", "LIGNES_FILTREES", "VALEURS_MASQUEES", "INDETERMINE")}
print(f"\n[OK] Identité {me} — {len(results)} table(s) : {summary}")
if results:
    display(spark.createDataFrame(results, "table STRING, lisible BOOLEAN, verdict STRING, detail STRING"))

if output_dir:
    from datetime import datetime
    from html import escape
    rows_html = "".join(f"<tr><td>{escape(r['table'])}</td><td>{escape(r['verdict'])}</td>"
                        f"<td>{escape(r['detail'])}</td></tr>" for r in results)
    path = f"{output_dir}/tables_protegees_{datetime.now():%Y%m%d_%H%M}.html"
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(f"<html><head><meta charset='utf-8'><title>Tables protégées</title></head><body>"
                 f"<h2>Visibilité des tables protégées pour {escape(me)}</h2><p>{escape(json.dumps(summary))}</p>"
                 f"<table border=1 cellpadding=4><tr><th>Table</th><th>Verdict</th><th>Détail</th></tr>"
                 f"{rows_html}</table></body></html>")
    print(f"[OK] Rapport : {path}")

dbutils.notebook.exit(json.dumps({"identity": me, "summary": summary, "tables": results}, ensure_ascii=False))
