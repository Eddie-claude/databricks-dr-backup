# scripts/export_workspace.py
"""
Export des assets workspace Databricks via CLI + REST API.
Utilisé par le CI/CD pipeline avant de déclencher le Databricks Job.
"""
import json
import os
import subprocess
from datetime import date
from typing import Any, List

import requests


def build_export_commands(backup_path: str) -> List[List[str]]:
    """Retourne la liste des commandes CLI Databricks pour l'export workspace.
    L'hôte est lu depuis la variable d'env DATABRICKS_HOST par le CLI.
    """
    return [
        ["databricks", "workspace", "export-dir", "/", f"{backup_path}/notebooks", "--overwrite"],
    ]


def write_json_asset(data: Any, output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def export_jobs(host: str, token: str, output_path: str) -> List[str]:
    headers = {"Authorization": f"Bearer {token}"}
    jobs = []
    params: dict = {"limit": 100}
    while True:
        resp = requests.get(f"{host}/api/2.1/jobs/list", headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        jobs.extend(body.get("jobs", []))
        if not body.get("has_more"):
            break
        params["page_token"] = body["next_page_token"]
    write_json_asset(jobs, output_path)
    return [str(j["job_id"]) for j in jobs]


def trigger_databricks_job(host: str, token: str, job_name: str, backup_date: str) -> int:
    headers = {"Authorization": f"Bearer {token}"}
    jobs = []
    params: dict = {"limit": 100}
    while True:
        resp = requests.get(f"{host}/api/2.1/jobs/list", headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        jobs.extend(body.get("jobs", []))
        if not body.get("has_more"):
            break
        params["page_token"] = body["next_page_token"]
    job = next((j for j in jobs if j["settings"]["name"] == job_name), None)
    if not job:
        raise ValueError(f"Job '{job_name}' introuvable dans le workspace")
    job_id = job["job_id"]
    run_resp = requests.post(
        f"{host}/api/2.1/jobs/run-now",
        headers=headers,
        json={"job_id": job_id, "notebook_params": {"backup_date": backup_date}},
        timeout=30,
    )
    run_resp.raise_for_status()
    run_id = run_resp.json()["run_id"]
    print(f"[OK] Job '{job_name}' déclenché — run_id={run_id}")
    return run_id


def main() -> None:
    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_TOKEN"]
    backup_date = os.environ.get("BACKUP_DATE", str(date.today()))
    local_backup_dir = os.environ.get("LOCAL_BACKUP_DIR", f"/tmp/dr-backup/{backup_date}")
    workspace_dir = f"{local_backup_dir}/workspace"

    os.makedirs(workspace_dir, exist_ok=True)
    print(f"[CI/CD] Export workspace — date={backup_date}")

    cmds = build_export_commands(workspace_dir)
    for cmd in cmds:
        print(f"  → {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

    export_jobs(host, token, f"{workspace_dir}/jobs.json")

    print(f"[CI/CD] Export terminé → {workspace_dir}")

    backup_root = os.environ.get("BACKUP_ROOT", "")
    if backup_root:
        subprocess.run(
            ["azcopy", "sync", workspace_dir, f"{backup_root}/{backup_date}/workspace", "--recursive"],
            check=True
        )
        print(f"[CI/CD] Upload ADLS terminé → {backup_root}/{backup_date}/workspace")
    else:
        print("[WARN] BACKUP_ROOT non défini — upload ADLS ignoré")

    trigger_databricks_job(host, token, "dr-backup-daily", backup_date)


if __name__ == "__main__":
    main()
