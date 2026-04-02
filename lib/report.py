# lib/report.py
import os
from typing import Any, Dict, List
from jinja2 import Environment, FileSystemLoader


_TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "templates")


def generate_report(
    diff: Dict[str, Any],
    stats: Dict[str, Any],
    steps: List[Dict[str, Any]],
) -> str:
    env = Environment(loader=FileSystemLoader(_TEMPLATES_DIR), autoescape=True)
    template = env.get_template("dr_report.html.j2")
    return template.render(diff=diff, stats=stats, steps=steps)
