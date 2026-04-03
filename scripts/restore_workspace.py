# scripts/restore_workspace.py
"""
Restauration des assets workspace (notebooks, jobs, clusters) depuis le backup.
Usage: python scripts/restore_workspace.py --backup-date 2026-04-02 --local-dir /tmp/backup
"""
import argparse
import json
import os
import subprocess

import requests


def restore_notebooks(notebooks_dir: str, host: str, token: str) -> None:
    """Re-importe les notebooks .dbc dans le workspace."""
    result = subprocess.run(
        ["databricks", "workspace", "import-dir", notebooks_dir, "/", "--overwrite"],
        capture_output=True, text=True,
        env={**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
    )
    if result.returncode != 0:
        print(f"[ERROR] Import notebooks: {result.stderr}")
    else:
        print(f"[OK] Notebooks importés depuis {notebooks_dir}")


def restore_jobs(jobs_json_path: str, host: str, token: str) -> None:
    """Recrée les jobs depuis jobs.json."""
    headers = {"Authorization": f"Bearer {token}"}
    with open(jobs_json_path, "r") as f:
        jobs = json.load(f)
    for job in jobs:
        settings = job.get("settings", job)
        resp = requests.post(f"{host}/api/2.1/jobs/create", headers=headers, json=settings)
        if resp.status_code == 200:
            print(f"[OK] Job créé: {settings.get('name', 'unknown')}")
        else:
            print(f"[WARN] Job {settings.get('name')}: {resp.text}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore workspace assets")
    parser.add_argument("--backup-date", required=True)
    parser.add_argument("--local-dir", required=True, help="Dossier local contenant le backup workspace/")
    args = parser.parse_args()

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    workspace_dir = os.path.join(args.local_dir, "workspace")

    print(f"[Restore Workspace] Date: {args.backup_date}")

    notebooks_dir = os.path.join(workspace_dir, "notebooks")
    if os.path.exists(notebooks_dir):
        restore_notebooks(notebooks_dir, host, token)
    else:
        print(f"[SKIP] Dossier notebooks introuvable: {notebooks_dir}")

    jobs_path = os.path.join(workspace_dir, "jobs.json")
    if os.path.exists(jobs_path):
        restore_jobs(jobs_path, host, token)
    else:
        print(f"[SKIP] jobs.json introuvable: {jobs_path}")

    print("[Restore Workspace] Terminé.")


if __name__ == "__main__":
    main()
