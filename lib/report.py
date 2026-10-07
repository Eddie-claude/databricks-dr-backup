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


def generate_report(
    diff: Dict[str, Any],
    stats: Dict[str, Any],
    steps: List[Dict[str, Any]],
    non_saved: Optional[List[Dict[str, Any]]] = None,
) -> str:
    env = Environment(loader=FileSystemLoader(_TEMPLATES_DIR), autoescape=True)
    template = env.get_template("dr_report.html.j2")
    non_saved = non_saved or []
    return template.render(diff=diff, stats=stats, steps=steps,
                           non_saved=non_saved[:MAX_LISTED], non_saved_total=len(non_saved))
