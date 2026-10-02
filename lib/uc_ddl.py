# lib/uc_ddl.py
"""
Reconstruction des DDL Unity Catalog qui n'ont pas d'équivalent à SHOW CREATE TABLE
(volumes, fonctions) à partir des lignes d'information_schema, et découpage des
fichiers SQL de backup en statements.

Fonctions pures (dicts en entrée, str en sortie) : testables hors Databricks.
"""
import re
from typing import Optional


def sql_string(value: str) -> str:
    """Littéral chaîne Databricks SQL (échappement par backslash)."""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def _fqn(catalog: str, schema: str, name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{name}`"


def _by_position(rows: list) -> list:
    return sorted(rows, key=lambda r: r["ordinal_position"])


# ── Volumes ──────────────────────────────────────────────────────────────

def build_volume_ddl(volume: dict) -> str:
    """volume = ligne de <catalog>.information_schema.volumes."""
    fqn = _fqn(volume["volume_catalog"], volume["volume_schema"], volume["volume_name"])
    if (volume["volume_type"] or "").upper() == "EXTERNAL":
        ddl = (f"CREATE EXTERNAL VOLUME IF NOT EXISTS {fqn}\n"
               f"  LOCATION {sql_string(volume['storage_location'])}")
    else:
        # Volume managé : l'emplacement est attribué par UC, on ne le rejoue pas.
        ddl = f"CREATE VOLUME IF NOT EXISTS {fqn}"
    if volume.get("comment"):
        ddl += f"\n  COMMENT {sql_string(volume['comment'])}"
    return ddl


# ── Fonctions ────────────────────────────────────────────────────────────

def _typed(name: str, row: dict) -> str:
    out = f"{name} {row.get('full_data_type') or row.get('data_type')}"
    if row.get("parameter_default") is not None:
        out += f" DEFAULT {row['parameter_default']}"
    if row.get("comment"):
        out += f" COMMENT {sql_string(row['comment'])}"
    return out


def build_function_ddl(routine: dict, params: list, return_columns: Optional[list] = None) -> Optional[str]:
    """
    routine        = ligne de <catalog>.information_schema.routines
    params         = lignes de information_schema.parameters de cette fonction
    return_columns = lignes de information_schema.routine_columns (fonctions table)

    Retourne None si le corps n'est pas lisible (routine_definition NULL).
    """
    body = routine.get("routine_definition")
    if body is None:
        return None

    fqn = _fqn(routine["routine_catalog"], routine["routine_schema"], routine["routine_name"])
    args = ", ".join(_typed(p["parameter_name"], p) for p in _by_position(params))

    if return_columns or routine.get("data_type") == "TABLE_TYPE":
        cols = ", ".join(_typed(c["column_name"], c) for c in _by_position(return_columns or []))
        returns = f"TABLE ({cols})"
    else:
        returns = routine.get("full_data_type") or routine["data_type"]

    is_python = (routine.get("routine_body") or "").upper() == "EXTERNAL"
    lines = [f"CREATE FUNCTION IF NOT EXISTS {fqn}({args})", f"  RETURNS {returns}"]
    if is_python:
        lines.append(f"  LANGUAGE {(routine.get('external_language') or 'PYTHON').upper()}")
    # Databricks renvoie 'true'/'false' et 'CONTAINS_SQL' ; on accepte aussi la forme
    # ANSI ('YES', 'CONTAINS SQL').
    deterministic = str(routine.get("is_deterministic") or "").upper() in ("TRUE", "YES")
    lines.append("  DETERMINISTIC" if deterministic else "  NOT DETERMINISTIC")
    if routine.get("comment"):
        lines.append(f"  COMMENT {sql_string(routine['comment'])}")
    access = (routine.get("sql_data_access") or "").upper().replace("_", " ")
    if not is_python and access in ("CONTAINS SQL", "READS SQL DATA"):
        lines.append(f"  {access}")

    if is_python:
        # Databricks conserve les sauts de ligne qui entouraient $$…$$ : sans ce nettoyage, chaque
        # cycle backup → restore ajouterait une ligne vide au début et à la fin du corps.
        # strip("\r\n") seulement : l'indentation de la première ligne est significative.
        body = body.strip("\r\n")
        tag = "$$" if "$$" not in body else "$py$"
        lines.append(f"  AS {tag}\n{body}\n{tag}")
    else:
        lines.append(f"  RETURN {body}")
    return "\n".join(lines)


# ── Découpage ────────────────────────────────────────────────────────────

_DOLLAR_TAG = re.compile(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$")


def _strip_leading_comments(stmt: str) -> str:
    lines = stmt.strip().split("\n")
    while lines and (not lines[0].strip() or lines[0].strip().startswith("--")):
        lines.pop(0)
    return "\n".join(lines).strip()


def split_sql_statements(content: str) -> list:
    """
    Découpe un fichier SQL sur les ';' de premier niveau, en ignorant ceux qui sont
    dans une chaîne ('...', "..."), un identifiant `...`, un commentaire (-- ou /* */)
    ou un bloc $tag$...$tag$ (corps de fonction Python).
    """
    statements, start, i, n = [], 0, 0, len(content)
    while i < n:
        c = content[i]
        if c in ("'", '"', "`"):
            i += 1
            while i < n and content[i] != c:
                i += 2 if content[i] == "\\" and c != "`" else 1
        elif content.startswith("--", i):
            nl = content.find("\n", i)
            i = n if nl == -1 else nl
        elif content.startswith("/*", i):
            end = content.find("*/", i + 2)
            i = n if end == -1 else end + 1
        elif c == "$" and (m := _DOLLAR_TAG.match(content, i)):
            end = content.find(m.group(0), m.end())
            i = n if end == -1 else end + len(m.group(0)) - 1
        elif c == ";":
            statements.append(content[start:i])
            start = i + 1
        i += 1
    statements.append(content[start:])
    return [s for s in (_strip_leading_comments(s) for s in statements) if s]
