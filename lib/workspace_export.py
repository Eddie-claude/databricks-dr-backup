# lib/workspace_export.py
"""
Règles de restauration des objets du workspace (notebooks, fichiers, dashboards), des
permissions et des pipelines sauvegardés par 05_workspace_config.

Fonctions pures (dicts en entrée, dicts en sortie) : testables hors Databricks.
"""

# Extension utilisée historiquement dans le manifest du différentiel (par langage du notebook)
LANG_EXT = {"PYTHON": ".py", "SQL": ".sql", "SCALA": ".scala", "R": ".r"}

PRINCIPAL_KEYS = ("user_name", "group_name", "service_principal_name")


def import_request(obj: dict, content_b64: str, target_path: str) -> dict:
    """Corps de POST /api/2.0/workspace/import pour un objet exporté en format AUTO."""
    kind = obj.get("object_type")
    if kind == "NOTEBOOK":
        # L'import AUTO reconnaît un notebook à son extension (+ en-tête) et retire l'extension
        ext = f".{obj['file_type']}" if obj.get("file_type") else ".py"
        return {"path": target_path + ext, "format": "AUTO", "content": content_b64, "overwrite": True}
    if kind == "FILE":
        # RAW : le fichier reste un fichier, même s'il ressemble à un notebook
        return {"path": target_path, "format": "RAW", "content": content_b64, "overwrite": True}
    return {"path": target_path, "format": "AUTO", "content": content_b64, "overwrite": True}


def remap_path(path: str, target_root: str) -> str:
    """Remplace le dossier racine d'origine par target_root (vide = chemin d'origine)."""
    if not target_root:
        return path
    parts = path.strip("/").split("/", 1)
    rest = parts[1] if len(parts) > 1 else parts[0]
    return f"{target_root.rstrip('/')}/{rest}"


def notebook_manifest_path(entry: dict) -> str:
    """Chemin relatif d'un notebook dans le manifest du différentiel, au format historique
    (« Shared/dossier/notebook.py ») : un changement de format ferait apparaître tous les
    notebooks ajoutés ET supprimés au premier run."""
    return entry["path"].lstrip("/") + LANG_EXT.get(entry.get("language") or "PYTHON", ".py")


def explicit_acl(acl) -> list:
    """Permissions posées directement sur l'objet, au format de PUT /api/2.0/permissions.
    Exclut les droits hérités (recréés par le parent) et IS_OWNER (la ressource recréée
    appartient à l'identité qui restaure ; un transfert de propriété reste manuel)."""
    out = []
    for entry in acl or []:
        principal = next(((k, entry[k]) for k in PRINCIPAL_KEYS if entry.get(k)), None)
        if not principal:
            continue
        for perm in entry.get("all_permissions", []):
            if perm.get("inherited") or perm.get("permission_level") == "IS_OWNER":
                continue
            out.append({principal[0]: principal[1], "permission_level": perm["permission_level"]})
    return out


def pipeline_create_payload(spec: dict) -> dict:
    """Corps de POST /api/2.0/pipelines à partir de la spec sauvegardée (sans identifiant)."""
    return {k: v for k, v in spec.items() if k not in ("id", "pipeline_id")}
