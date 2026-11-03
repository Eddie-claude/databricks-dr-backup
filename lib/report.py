# lib/report.py
import os
from typing import Any, Dict, List, Optional
from jinja2 import Environment, FileSystemLoader


_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")

# Au-delà, le rapport HTML devient illisible : le détail complet reste dans le manifest de clone
MAX_LISTED = 500


def non_saved_tables(clone_manifest: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Tables du manifest de clone sans copie réussie : erreurs d'abord, puis non clonables."""
    order = {"error": 0, "skipped": 1}
    return sorted((r for r in clone_manifest if r.get("status") != "success"),
                  key=lambda r: (order.get(r.get("status"), 2), r.get("table", "")))


def top_copied(clone_manifest: List[Dict[str, Any]], n: int = 20) -> List[Dict[str, Any]]:
    """Tables au plus gros volume copié ce jour, avec la part de la table recopiée. Une part proche
    de 100 % chaque jour signale une table réécrite entièrement (INSERT OVERWRITE, MERGE large,
    OPTIMIZE) : c'est elle, pas le nombre de tables, qui fait le volume d'un backup incrémental."""
    rows = []
    for r in clone_manifest:
        copied = r.get("size_gb") or 0
        if r.get("status") != "success" or copied <= 0:
            continue
        total = r.get("table_size_gb")
        rows.append({"table": r.get("table", ""), "copied_gb": round(copied, 2),
                     "table_gb": round(total, 2) if total else None,
                     "ratio_pct": round(100 * copied / total) if total else None})
    return sorted(rows, key=lambda x: -x["copied_gb"])[:n]


# Étapes de l'orchestrateur postérieures à la génération du rapport : sans elles, le tableau des
# étapes s'arrêtait à « diff » et laissait croire que le rapport et la rétention n'avaient pas tourné
STEPS_AFTER_REPORT = (("report", "en cours"), ("retention", "à venir"))


def with_later_steps(steps: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Ajoute au tableau les étapes que le rapport ne peut pas encore connaître (lui-même, puis la
    rétention), marquées comme telles ; leur statut final est dans latest.json. Sans étape transmise
    (04_report lancé seul), rien n'est ajouté."""
    if not steps:
        return steps
    known = {s.get("name") for s in steps}
    return steps + [{"name": name, "status": status, "pending": True,
                     "error": "statut final dans latest.json"}
                    for name, status in STEPS_AFTER_REPORT if name not in known]


def generate_report(
    diff: Dict[str, Any],
    stats: Dict[str, Any],
    steps: List[Dict[str, Any]],
    non_saved: Optional[List[Dict[str, Any]]] = None,
    top: Optional[List[Dict[str, Any]]] = None,
) -> str:
    env = Environment(loader=FileSystemLoader(_TEMPLATES_DIR), autoescape=True)
    template = env.get_template("dr_report.html.j2")
    non_saved = non_saved or []
    return template.render(diff=diff, stats=stats, steps=with_later_steps(steps),
                           non_saved=non_saved[:MAX_LISTED], non_saved_total=len(non_saved),
                           top=top or [])
