# lib/diff.py
from dataclasses import dataclass, field
from typing import Set, Dict, List, Any


@dataclass
class BackupManifest:
    date: str
    tables: Set[str] = field(default_factory=set)
    jobs: Set[str] = field(default_factory=set)
    notebooks: Set[str] = field(default_factory=set)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "BackupManifest":
        return cls(
            date=d.get("date", ""),
            tables=set(d.get("tables", [])),
            jobs=set(d.get("jobs", [])),
            notebooks=set(d.get("notebooks", [])),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "date": self.date,
            "tables": sorted(self.tables),
            "jobs": sorted(self.jobs),
            "notebooks": sorted(self.notebooks),
        }


def _set_diff(prev: Set[str], curr: Set[str]) -> Dict[str, List[str]]:
    return {
        "added": sorted(curr - prev),
        "removed": sorted(prev - curr),
        "unchanged": sorted(prev & curr),
    }


def compute_diff(prev: BackupManifest, curr: BackupManifest) -> Dict[str, Any]:
    return {
        "date_prev": prev.date,
        "date_curr": curr.date,
        "tables": _set_diff(prev.tables, curr.tables),
        "jobs": _set_diff(prev.jobs, curr.jobs),
        "notebooks": _set_diff(prev.notebooks, curr.notebooks),
    }
