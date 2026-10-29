"""Construit le dossier et le ZIP de livraison client : ../DR_Backup_Livraison_AAAAMMJJ(.zip).

Périmètre livré : databricks.yml (valeurs propres à l'environnement de développement masquées, job
Game Day retiré), docs/*.docx livrés, lib/, notebooks/ (hors demo/), scripts/, requirements.txt.
Fichiers texte écrits en LF. Usage : python build_delivery.py [AAAAMMJJ]
"""
import re
import shutil
import sys
import zipfile
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TAG = sys.argv[1] if len(sys.argv) > 1 else date.today().strftime("%Y%m%d")
OUT = ROOT.parent / f"DR_Backup_Livraison_{TAG}"
DEST = OUT / "DR_Backup"

DOCS = ["DR_Backup_Guide_Deploiement_v4.2.docx", "DR_Backup_Notes_de_Version_v4.2.docx",
        "DR_Backup_Runbook_Restauration_v4.2.docx"]
EXCLUDED_NAMES = {"__pycache__", "demo", "gameday.py"}

# Valeurs de l'environnement de développement → valeurs génériques
MASKS = [
    (re.compile(r"abfss://[^@\s\"]+@[^.\s\"]+\.dfs\.core\.windows\.net"), "abfss://<container>@<compte>.dfs.core.windows.net"),
    (re.compile(r"https://adb-[0-9]+\.[0-9]+\.azuredatabricks\.net"), "https://<workspace>.azuredatabricks.net"),
    (re.compile(r'(default:\s*")[\w.+-]+@[\w.-]+\.[a-z]{2,}(")'), r"\1<adresse-alertes>\2"),
]


def delivered_yml(text: str) -> str:
    for rx, repl in MASKS:
        text = rx.sub(repl, text)
    # Job Game Day (démo) retiré : du commentaire « Job N : Game Day » au prochain bloc « Job »
    m = re.search(r"    # ── Job (\d+) : Game Day.*?(?=    # ── Job )", text, flags=re.S)
    if m:
        removed = int(m.group(1))
        text = text[:m.start()] + text[m.end():]
        text = re.sub(r"# ── Job (\d+) :", lambda j: f"# ── Job {int(j.group(1)) - 1 if int(j.group(1)) > removed else j.group(1)} :", text)
    for forbidden in ("keyit", "adb-", "@keyit"):
        assert forbidden not in text.lower(), f"valeur d'environnement encore présente : {forbidden}"
    return text


def copy_lf(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if src.suffix in (".py", ".yml", ".txt", ".j2", ".md"):
        dst.write_bytes(src.read_bytes().replace(b"\r\n", b"\n"))
    else:
        shutil.copy2(src, dst)


def copy_tree(rel: str) -> None:
    for p in sorted((ROOT / rel).rglob("*")):
        if p.is_file() and not (EXCLUDED_NAMES & set(p.relative_to(ROOT).parts)):
            copy_lf(p, DEST / p.relative_to(ROOT))


if OUT.exists():
    shutil.rmtree(OUT)
DEST.mkdir(parents=True)
(DEST / "databricks.yml").write_bytes(delivered_yml((ROOT / "databricks.yml").read_text(encoding="utf-8")).replace("\r\n", "\n").encode("utf-8"))
copy_lf(ROOT / "requirements.txt", DEST / "requirements.txt")
for d in DOCS:
    copy_lf(ROOT / "docs" / d, DEST / "docs" / d)
for rel in ("lib", "notebooks", "scripts"):
    copy_tree(rel)

files = sorted(p for p in DEST.rglob("*") if p.is_file())
with zipfile.ZipFile(OUT.with_suffix(".zip"), "w", zipfile.ZIP_DEFLATED) as z:
    for p in files:
        z.write(p, p.relative_to(OUT).as_posix())
print(f"[OK] {len(files)} fichiers → {OUT.with_suffix('.zip')}")
