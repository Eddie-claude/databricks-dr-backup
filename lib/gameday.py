# lib/gameday.py
from typing import Dict, List


def compare_row_counts(expected: Dict[str, int], actual: Dict[str, int]) -> List[str]:
    """Compare les row counts attendus (baseline pré-sinistre) aux row counts
    réels (post-restauration). Retourne la liste des écarts ; liste vide = conforme.
    Les tables présentes dans `actual` mais absentes de `expected` sont ignorées.
    """
    mismatches: List[str] = []
    for table in sorted(expected):
        expected_count = expected[table]
        if table not in actual:
            mismatches.append(
                f"{table}: table manquante après restauration (attendu {expected_count} lignes)"
            )
            continue
        actual_count = actual[table]
        if actual_count != expected_count:
            mismatches.append(
                f"{table}: {actual_count} lignes après restauration, {expected_count} attendues"
            )
    return mismatches
