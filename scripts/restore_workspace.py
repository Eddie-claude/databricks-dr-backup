#!/usr/bin/env python3
"""
scripts/restore_workspace.py
Restaure les Jobs Databricks et les Notebooks workspace depuis un backup ADLS.

Usage:
    python restore_workspace.py \
        --backup-path /local/path/to/backup/2026-04-05 \
        --host https://adb-xxx.azuredatabricks.net \
        --token dapiXXXX \
        [--restore-jobs] \
        [--restore-notebooks] \
        [--target-dir /Shared/dr-restore] \
        [--skip-dab-jobs] \
        [--dry-run]

Notes:
    - --backup-path : chemin LOCAL vers le répertoire de backup (ex: ./backup/2026-04-05)
      Pour télécharger depuis ADLS : azcopy sync abfss://... ./backup/2026-04-05 --recursive
    - --target-dir  : dossier de destination dans le workspace pour les notebooks
                      (défaut: /Shared/dr-restore — évite d'écraser /Shared/dr-backup)
    - --skip-dab-jobs : ignore les jobs gérés par Databricks Asset Bundles (tag bundle)
"""

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import requests


# ── Helpers HTTP ──────────────────────────────────────────────────────────────

def api_get(host, token, path, params=None):
    r = requests.get(
        f"{host}{path}",
        headers={"Authorization": f"Bearer {token}"},
        params=params,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def api_post(host, token, path, payload):
    r = requests.post(
        f"{host}{path}",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=30,
    )
    return r


def load_backup_file(backup_path, *parts):
    """Charge un fichier JSON depuis le répertoire de backup local."""
    filepath = Path(backup_path).joinpath(*parts)
    if not filepath.exists():
        raise FileNotFoundError(f"Fichier backup non trouvé : {filepath}")
    with open(filepath, encoding="utf-8") as f:
        return json.load(f)


def list_all_jobs(host, token):
    """Récupère tous les jobs existants dans le workspace (pagination)."""
    jobs = []
    params = {"limit": 100, "expand_tasks": False}
    while True:
        body = api_get(host, token, "/api/2.1/jobs/list", params)
        jobs.extend(body.get("jobs", []))
        if not body.get("has_more"):
            break
        params["page_token"] = body["next_page_token"]
    return jobs


# ── Restore Jobs ──────────────────────────────────────────────────────────────

def is_dab_job(job_settings):
    """Détecte si un job est géré par Databricks Asset Bundles."""
    tags = job_settings.get("tags", {})
    return "bundle" in tags or any(k.startswith("databricks.bundle") for k in tags)


def clean_job_settings(settings):
    """Nettoie les settings d'un job pour la recréation via API."""
    readonly_fields = {
        "job_id", "creator_user_name", "created_time",
        "run_as_user_name", "effective_budget_policy_id",
        "run_as", "settings_schema_version",
    }
    return {k: v for k, v in settings.items() if k not in readonly_fields}


def restore_jobs(host, token, jobs_backup, dry_run=False, skip_dab=True):
    print("\n=== RESTAURATION JOBS ===")

    existing = {j["settings"]["name"]: j["job_id"] for j in list_all_jobs(host, token)}

    ok = skip = err = 0
    for job_entry in jobs_backup:
        settings = job_entry.get("settings", job_entry)
        name = settings.get("name", f"job_{job_entry.get('job_id', '?')}")

        if skip_dab and is_dab_job(settings):
            print(f"  ⏭ [SKIP-DAB] Job géré par bundle : {name}")
            skip += 1
            continue

        if name in existing:
            print(f"  ⏭ [SKIP] Job déjà existant : {name} (id={existing[name]})")
            skip += 1
            continue

        if dry_run:
            print(f"  [DRY-RUN] Créerait job : {name}")
            ok += 1
            continue

        payload = clean_job_settings(settings)
        resp = api_post(host, token, "/api/2.1/jobs/create", payload)

        if resp.ok:
            new_id = resp.json().get("job_id")
            print(f"  ✅ Job créé : {name} (new_id={new_id})")
            ok += 1
        else:
            try:
                detail = resp.json().get("message", resp.text[:200])
            except Exception:
                detail = resp.text[:200]
            print(f"  ❌ Erreur job {name}: {detail}")
            err += 1

    print(f"\n  Résultat : {ok} créés | {skip} ignorés | {err} erreurs")
    return err


# ── Restore Notebooks ─────────────────────────────────────────────────────────

def restore_notebooks(backup_path, host, token, target_dir="/Shared/dr-restore", dry_run=False):
    """Importe les notebooks depuis le backup local vers le workspace via le Databricks CLI."""
    print("\n=== RESTAURATION NOTEBOOKS ===")

    notebooks_dir = Path(backup_path) / "workspace" / "notebooks"
    if not notebooks_dir.exists():
        print(f"  [SKIP] Dossier notebooks introuvable : {notebooks_dir}")
        return 0

    nb_files = list(notebooks_dir.rglob("*"))
    nb_count = sum(1 for f in nb_files if f.is_file())
    print(f"  Notebooks à importer : {nb_count} fichiers depuis {notebooks_dir}")
    print(f"  Destination workspace : {target_dir}")

    if dry_run:
        print(f"  [DRY-RUN] Importerait {nb_count} fichiers vers {target_dir}")
        return 0

    env = {**os.environ, "DATABRICKS_HOST": host, "DATABRICKS_TOKEN": token}
    cmd = ["databricks", "workspace", "import-dir", str(notebooks_dir), target_dir, "--overwrite"]
    print(f"  Commande : {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True, env=env)

    if result.returncode != 0:
        print(f"  ❌ Erreur import notebooks:\n{result.stderr[:500]}")
        return 1
    else:
        print(f"  ✅ Notebooks importés vers {target_dir}")
        if result.stdout:
            print(f"  {result.stdout[:300]}")
        return 0


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Restore Databricks Jobs and Notebooks from DR backup"
    )
    parser.add_argument("--backup-path", required=True,
                        help="Chemin local vers le répertoire de backup (ex: ./backup/2026-04-05)")
    parser.add_argument("--host",  required=True, help="Databricks workspace URL")
    parser.add_argument("--token", required=True, help="Databricks PAT token")
    parser.add_argument("--restore-jobs",      action="store_true", help="Restaurer les jobs")
    parser.add_argument("--restore-notebooks", action="store_true", help="Restaurer les notebooks")
    parser.add_argument("--target-dir", default="/Shared/dr-restore",
                        help="Dossier destination dans le workspace pour les notebooks (défaut: /Shared/dr-restore)")
    parser.add_argument("--skip-dab-jobs", action="store_true", default=True,
                        help="Ignorer les jobs gérés par Databricks Asset Bundles (défaut: activé)")
    parser.add_argument("--no-skip-dab-jobs", dest="skip_dab_jobs", action="store_false",
                        help="Inclure les jobs DAB dans la restauration")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simuler sans appliquer les changements")
    args = parser.parse_args()

    if args.dry_run:
        print("[DRY-RUN] Mode simulation — aucun changement ne sera appliqué\n")

    if not any([args.restore_jobs, args.restore_notebooks]):
        print("Aucune option de restauration spécifiée.")
        print("Utilisez --restore-jobs et/ou --restore-notebooks")
        sys.exit(1)

    backup_path = Path(args.backup_path)
    if not backup_path.exists():
        print(f"[ERROR] Chemin backup introuvable : {backup_path}")
        print("        Téléchargez d'abord depuis ADLS :")
        print(f"        azcopy sync <backup_root>/YYYY-MM-DD {backup_path} --recursive")
        sys.exit(1)

    total_errors = 0

    if args.restore_jobs:
        jobs_path = backup_path / "jobs" / "jobs_all.json"
        if jobs_path.exists():
            jobs_backup = json.loads(jobs_path.read_text(encoding="utf-8"))
            total_errors += restore_jobs(
                args.host, args.token, jobs_backup,
                dry_run=args.dry_run, skip_dab=args.skip_dab_jobs
            )
        else:
            print(f"\n[SKIP] jobs_all.json introuvable : {jobs_path}")

    if args.restore_notebooks:
        err = restore_notebooks(
            args.backup_path, args.host, args.token,
            target_dir=args.target_dir, dry_run=args.dry_run
        )
        total_errors += err

    print(f"\n{'='*55}")
    if total_errors == 0:
        print("✅ Restauration terminée sans erreur")
    else:
        print(f"⚠️  Restauration terminée avec {total_errors} erreur(s)")
        sys.exit(1)


if __name__ == "__main__":
    main()
