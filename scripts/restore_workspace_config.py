#!/usr/bin/env python3
"""
scripts/restore_workspace_config.py
Restaure les ACLs workspace, cluster policies et configurations clusters
depuis un backup ADLS.

Usage:
    python restore_workspace_config.py \
        --backup-path abfss://uc-data@st10keyitdpdrpdevwe00.dfs.core.windows.net/dr-backup/2026-04-05 \
        --host https://adb-xxx.azuredatabricks.net \
        --token dapiXXXX \
        [--restore-acls] \
        [--restore-policies] \
        [--restore-clusters] \
        [--dry-run]
"""

import argparse
import json
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


def api_put(host, token, path, payload):
    r = requests.put(
        f"{host}{path}",
        headers={"Authorization": f"Bearer {token}"},
        json=payload,
        timeout=30,
    )
    return r


def load_backup_file(backup_path, filename):
    """Charge un fichier JSON depuis le répertoire de backup local."""
    filepath = Path(backup_path) / "workspace_config" / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Fichier backup non trouvé : {filepath}")
    with open(filepath) as f:
        return json.load(f)


# ── Restore Cluster Policies ──────────────────────────────────────────────────

def restore_cluster_policies(host, token, policies, dry_run=False):
    print("\n=== RESTAURATION CLUSTER POLICIES ===")

    # Récupérer les policies existantes
    existing = {p["name"]: p["policy_id"]
                for p in api_get(host, token, "/api/2.0/policies/clusters/list").get("policies", [])}

    ok = skip = err = 0
    for policy in policies:
        name = policy.get("name", "")
        definition = policy.get("definition", "{}")

        if name in existing:
            print(f"  ⏭ [SKIP] Policy déjà existante : {name}")
            skip += 1
            continue

        if dry_run:
            print(f"  [DRY-RUN] Créerait policy : {name}")
            ok += 1
            continue

        payload = {"name": name, "definition": definition}
        if policy.get("description"):
            payload["description"] = policy["description"]
        if policy.get("max_clusters_per_user"):
            payload["max_clusters_per_user"] = policy["max_clusters_per_user"]

        resp = api_post(host, token, "/api/2.0/policies/clusters/create", payload)
        if resp.ok:
            print(f"  ✅ Policy créée : {name}")
            ok += 1
        else:
            print(f"  ❌ Erreur policy {name}: {resp.text[:150]}")
            err += 1

    print(f"\n  Résultat : {ok} créées | {skip} déjà existantes | {err} erreurs")
    return err


# ── Restore Cluster Configs ───────────────────────────────────────────────────

def restore_clusters(host, token, clusters, dry_run=False):
    print("\n=== RESTAURATION CLUSTERS ===")

    existing_names = {c["cluster_name"]
                      for c in api_get(host, token, "/api/2.0/clusters/list").get("clusters", [])}

    ok = skip = err = 0
    for cluster in clusters:
        name   = cluster.get("cluster_name", "")
        source = cluster.get("cluster_source", "")

        # Ne pas recréer les clusters gérés par Databricks (UI, Jobs...)
        if source in ("JOB", "PIPELINE", "MODELS"):
            print(f"  ⏭ [SKIP] Cluster géré automatiquement : {name} (source={source})")
            skip += 1
            continue

        if name in existing_names:
            print(f"  ⏭ [SKIP] Cluster déjà existant : {name}")
            skip += 1
            continue

        if dry_run:
            print(f"  [DRY-RUN] Créerait cluster : {name}")
            ok += 1
            continue

        # Construire le payload de création
        payload = {k: v for k, v in cluster.items()
                   if k not in ("cluster_id", "state", "cluster_source")
                   and v is not None}

        resp = api_post(host, token, "/api/2.0/clusters/create", payload)
        if resp.ok:
            new_id = resp.json().get("cluster_id")
            print(f"  ✅ Cluster créé : {name} (new_id={new_id})")
            ok += 1
        else:
            print(f"  ❌ Erreur cluster {name}: {resp.text[:150]}")
            err += 1

    print(f"\n  Résultat : {ok} créés | {skip} ignorés | {err} erreurs")
    return err


# ── Restore Workspace ACLs ────────────────────────────────────────────────────

def restore_workspace_acls(host, token, acls_backup, dry_run=False):
    print("\n=== RESTAURATION ACLs WORKSPACE ===")

    type_map = {
        "NOTEBOOK":  "notebooks",
        "DIRECTORY": "directories",
        "REPO":      "repos",
        "FILE":      "files",
    }

    ok = skip = err = 0
    for entry in acls_backup:
        path      = entry["path"]
        obj_type  = entry["object_type"]
        acl       = entry["acl"]
        perm_type = type_map.get(obj_type)

        if not perm_type:
            skip += 1
            continue

        # Résoudre le path → nouvel object_id sur ce workspace
        try:
            status = api_get(host, token, "/api/2.0/workspace/get-status", {"path": path})
            new_id = status.get("object_id")
        except requests.HTTPError:
            print(f"  ⏭ [SKIP] Chemin introuvable : {path}")
            skip += 1
            continue

        if dry_run:
            print(f"  [DRY-RUN] Appliquerait ACL sur : {path}")
            ok += 1
            continue

        # Construire le payload ACL (uniquement les permissions non héritées)
        acl_list = []
        for entry_acl in acl:
            principal = (entry_acl.get("user_name")
                         or entry_acl.get("group_name")
                         or entry_acl.get("service_principal_name"))
            if not principal:
                continue
            perms = [p["permission_level"]
                     for p in entry_acl.get("all_permissions", [])
                     if not p.get("inherited")]
            if perms:
                key = "user_name" if entry_acl.get("user_name") else (
                      "service_principal_name" if entry_acl.get("service_principal_name")
                      else "group_name")
                acl_list.append({key: principal, "permission_level": perms[0]})

        if not acl_list:
            skip += 1
            continue

        resp = api_put(
            host, token,
            f"/api/2.0/permissions/{perm_type}/{new_id}",
            {"access_control_list": acl_list},
        )
        if resp.ok:
            print(f"  ✅ ACL restaurée : {path}")
            ok += 1
        else:
            print(f"  ❌ Erreur ACL {path}: {resp.text[:150]}")
            err += 1

    print(f"\n  Résultat : {ok} ACLs restaurées | {skip} ignorées | {err} erreurs")
    return err


# ── Restore Repos ACLs ────────────────────────────────────────────────────────

def restore_repos_acls(host, token, repos_acls, dry_run=False):
    print("\n=== RESTAURATION ACLs REPOS ===")

    # Récupérer les repos existants sur le workspace cible
    existing_repos = {r["path"]: r["id"]
                      for r in api_get(host, token, "/api/2.0/repos").get("repos", [])}

    ok = skip = err = 0
    for entry in repos_acls:
        path = entry.get("path", "")
        acl  = entry.get("acl", [])

        if not acl:
            skip += 1
            continue

        new_id = existing_repos.get(path)
        if not new_id:
            print(f"  ⏭ [SKIP] Repo introuvable : {path}")
            skip += 1
            continue

        if dry_run:
            print(f"  [DRY-RUN] Appliquerait ACL repo : {path}")
            ok += 1
            continue

        acl_list = []
        for entry_acl in acl:
            principal = (entry_acl.get("user_name")
                         or entry_acl.get("group_name")
                         or entry_acl.get("service_principal_name"))
            perms = [p["permission_level"]
                     for p in entry_acl.get("all_permissions", [])
                     if not p.get("inherited")]
            if principal and perms:
                key = "user_name" if entry_acl.get("user_name") else (
                      "service_principal_name" if entry_acl.get("service_principal_name")
                      else "group_name")
                acl_list.append({key: principal, "permission_level": perms[0]})

        resp = api_put(
            host, token,
            f"/api/2.0/permissions/repos/{new_id}",
            {"access_control_list": acl_list},
        )
        if resp.ok:
            print(f"  ✅ ACL repo restaurée : {path}")
            ok += 1
        else:
            print(f"  ❌ Erreur ACL repo {path}: {resp.text[:150]}")
            err += 1

    print(f"\n  Résultat : {ok} ACLs repos restaurées | {skip} ignorées | {err} erreurs")
    return err


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Restore workspace config from DR backup")
    parser.add_argument("--backup-path", required=True,
                        help="Chemin local vers le répertoire de backup (ex: ./backup/2026-04-05)")
    parser.add_argument("--host",  required=True, help="Databricks workspace URL")
    parser.add_argument("--token", required=True, help="Databricks PAT token")
    parser.add_argument("--restore-acls",     action="store_true", help="Restaurer les ACLs workspace")
    parser.add_argument("--restore-policies", action="store_true", help="Restaurer les cluster policies")
    parser.add_argument("--restore-clusters", action="store_true", help="Restaurer les clusters")
    parser.add_argument("--dry-run", action="store_true",
                        help="Simuler sans appliquer les changements")
    args = parser.parse_args()

    if args.dry_run:
        print("[DRY-RUN] Mode simulation — aucun changement ne sera appliqué\n")

    if not any([args.restore_acls, args.restore_policies, args.restore_clusters]):
        print("Aucune option de restauration spécifiée. Utilisez --restore-acls, "
              "--restore-policies ou --restore-clusters")
        sys.exit(1)

    total_errors = 0

    if args.restore_policies:
        policies = load_backup_file(args.backup_path, "cluster_policies.json")
        total_errors += restore_cluster_policies(args.host, args.token, policies, args.dry_run)

    if args.restore_clusters:
        clusters = load_backup_file(args.backup_path, "clusters.json")
        total_errors += restore_clusters(args.host, args.token, clusters, args.dry_run)

    if args.restore_acls:
        acls = load_backup_file(args.backup_path, "workspace_acls.json")
        total_errors += restore_workspace_acls(args.host, args.token, acls, args.dry_run)

        repos_acls = load_backup_file(args.backup_path, "repos_acls.json")
        total_errors += restore_repos_acls(args.host, args.token, repos_acls, args.dry_run)

    print(f"\n{'='*50}")
    if total_errors == 0:
        print("✅ Restauration terminée sans erreur")
    else:
        print(f"⚠️  Restauration terminée avec {total_errors} erreur(s)")
        sys.exit(1)


if __name__ == "__main__":
    main()
