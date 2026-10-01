# scripts/restore_uc.py
"""
Restauration du Unity Catalog depuis les dumps SQL.
Usage: python scripts/restore_uc.py --backup-date 2026-04-02 --backup-root abfss://...
Requiert: databricks CLI configuré + droits metastore admin
"""
import argparse
import os
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from lib.uc_ddl import split_sql_statements  # noqa: E402


_ALLOWED_SQL_VERBS = {"CREATE", "GRANT", "USE", "ALTER"}

# Les grants passent en dernier : ils visent aussi les volumes et fonctions (05, 06).
SQL_FILES_ORDER = [
    "01_catalogs.sql",
    "02_schemas.sql",
    "03_tables.sql",
    "05_volumes.sql",
    "06_functions.sql",
    "04_grants.sql",
]

# Fichiers dont les statements en échec sont rejoués une fois en fin de restauration :
# une vue (03) peut appeler une fonction (06) créée après elle, une fonction peut en
# appeler une autre.
RETRY_FILES = {"03_tables.sql", "06_functions.sql"}


def download_sql_files(backup_root: str, backup_date: str, local_dir: str) -> None:
    """Télécharge les fichiers SQL depuis ADLS vers un dossier local."""
    src = f"{backup_root}/{backup_date}/uc_metadata"
    subprocess.run(
        ["azcopy", "sync", src, local_dir, "--recursive"],
        check=True
    )


def run_sql_file(sql_path: str, host: str, token: str) -> list:
    """Exécute un fichier SQL statement par statement via le Databricks CLI.
    Retourne la liste des statements en échec (hors already exists).
    """
    with open(sql_path, "r", encoding="utf-8") as f:
        content = f.read()
    # Découpage qui ignore les ';' des chaînes et des corps de fonctions ($$ ... $$)
    return run_statements(split_sql_statements(content), host, token)


def run_statements(statements: list, host: str, token: str) -> list:
    """Valide que chaque statement est un CREATE/GRANT/USE/ALTER avant exécution."""
    failed = []
    for stmt in statements:
        first_word = stmt.split()[0].upper() if stmt.split() else ""
        if first_word not in _ALLOWED_SQL_VERBS:
            print(f"[BLOCKED] Statement non autorisé ignoré: {stmt[:80]}...")
            failed.append(stmt)
            continue
        result = subprocess.run(
            ["databricks", "sql", "execute", "--statement", stmt],
            capture_output=True, text=True,
            env={**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
        )
        if result.returncode != 0:
            if "already exists" in result.stderr.lower():
                print(f"[SKIP already exists] {stmt[:80]}...")
            else:
                failed.append(stmt)
                print(f"[ERROR] {stmt[:80]}... → {result.stderr.strip()}")
        else:
            print(f"[OK] {stmt[:80]}...")
    return failed


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore Unity Catalog from SQL dump")
    parser.add_argument("--backup-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--backup-root", required=True, help="abfss://...")
    args = parser.parse_args()

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]

    # Fichiers critiques : leur échec doit bloquer la suite
    critical_files = {"01_catalogs.sql", "02_schemas.sql"}
    to_retry = []

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"[Restore UC] Téléchargement des dumps SQL depuis {args.backup_root}...")
        download_sql_files(args.backup_root, args.backup_date, tmpdir)

        for sql_file in SQL_FILES_ORDER:
            # Rejeu avant les grants, pour que ceux-ci trouvent les objets rattrapés
            if sql_file == "04_grants.sql" and to_retry:
                print(f"\n[Restore UC] Nouvelle tentative pour {len(to_retry)} statement(s) en échec (dépendances)...")
                still_failed = run_statements(to_retry, host, token)
                print(f"[Restore UC] {len(to_retry) - len(still_failed)} rattrapé(s), {len(still_failed)} toujours en échec")

            path = os.path.join(tmpdir, sql_file)
            if not os.path.exists(path):
                print(f"[SKIP] {sql_file} introuvable")
                continue
            print(f"\n[Restore UC] Exécution {sql_file}...")
            failed = run_sql_file(path, host, token)
            if failed and sql_file in critical_files:
                raise RuntimeError(f"{sql_file}: {len(failed)} erreur(s) critique(s) — restauration interrompue")
            if sql_file in RETRY_FILES:
                to_retry.extend(failed)

    print("\n[Restore UC] Terminé.")


if __name__ == "__main__":
    main()
