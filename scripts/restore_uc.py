# scripts/restore_uc.py
"""
Restauration du Unity Catalog depuis les dumps SQL.
Usage: python scripts/restore_uc.py --backup-date 2026-04-02 --backup-root abfss://...
Requiert: databricks CLI configuré + droits metastore admin
"""
import argparse
import os
import subprocess
import tempfile


SQL_FILES_ORDER = [
    "01_catalogs.sql",
    "02_schemas.sql",
    "03_tables.sql",
    "04_grants.sql",
]


def download_sql_files(backup_root: str, backup_date: str, local_dir: str) -> None:
    """Télécharge les fichiers SQL depuis ADLS vers un dossier local."""
    src = f"{backup_root}/{backup_date}/uc_metadata"
    subprocess.run(
        ["azcopy", "sync", src, local_dir, "--recursive"],
        check=True
    )


def run_sql_file(sql_path: str, host: str, token: str) -> None:
    """Exécute un fichier SQL statement par statement via le Databricks CLI."""
    with open(sql_path, "r", encoding="utf-8") as f:
        content = f.read()

    statements = [s.strip() for s in content.split(";") if s.strip()]
    for stmt in statements:
        result = subprocess.run(
            ["databricks", "sql", "execute", "--statement", stmt],
            capture_output=True, text=True,
            env={**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
        )
        if result.returncode != 0 and "already exists" not in result.stderr.lower():
            print(f"[WARN] {stmt[:80]}... → {result.stderr.strip()}")
        else:
            print(f"[OK] {stmt[:80]}...")


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore Unity Catalog from SQL dump")
    parser.add_argument("--backup-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--backup-root", required=True, help="abfss://...")
    args = parser.parse_args()

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]

    with tempfile.TemporaryDirectory() as tmpdir:
        print(f"[Restore UC] Téléchargement des dumps SQL depuis {args.backup_root}...")
        download_sql_files(args.backup_root, args.backup_date, tmpdir)

        for sql_file in SQL_FILES_ORDER:
            path = os.path.join(tmpdir, sql_file)
            if not os.path.exists(path):
                print(f"[SKIP] {sql_file} introuvable")
                continue
            print(f"\n[Restore UC] Exécution {sql_file}...")
            run_sql_file(path, host, token)

    print("\n[Restore UC] Terminé.")


if __name__ == "__main__":
    main()
