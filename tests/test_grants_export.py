# tests/test_grants_export.py
"""
Filtre des grants hérités dans 01_uc_metadata : SHOW GRANTS ON <objet> renvoie aussi les droits
hérités du catalog / schéma (ObjectKey du parent). Les réécrire « ON <objet> » les transformerait
en grants explicites à la restauration. Le filtre est extrait du notebook.
"""
from pathlib import Path
from types import SimpleNamespace as Row

SRC = (Path(__file__).resolve().parent.parent / "notebooks" / "01_uc_metadata.py").read_text(encoding="utf-8")
NS = {}
exec(SRC.split("# >>> GRANTS DIRECTS")[1].split("# <<< GRANTS DIRECTS")[0], NS)
is_direct = NS["_is_direct_grant"]


def test_direct_grant_is_kept():
    g = Row(Principal="account users", ActionType="EXECUTE", ObjectType="FUNCTION",
            ObjectKey="backup_test_iot.dr_v42_test.f_double")
    assert is_direct(g, "`backup_test_iot`.`dr_v42_test`.`f_double`")


def test_inherited_grants_are_dropped():
    fqn = "`backup_test_iot`.`dr_v42_test`.`f_double`"
    assert not is_direct(Row(ObjectType="SCHEMA", ObjectKey="backup_test_iot.dr_v42_test"), fqn)
    assert not is_direct(Row(ObjectType="CATALOG", ObjectKey="backup_test_iot"), fqn)


def test_comparison_ignores_case_and_backticks():
    assert is_direct(Row(ObjectType="CATALOG", ObjectKey="Prod"), "`prod`")


def test_row_without_object_key_is_kept():
    # Ancien format de SHOW GRANTS sans ObjectKey : comportement précédent conservé
    assert is_direct(Row(Principal="x", ActionType="SELECT"), "`a`.`b`.`c`")
