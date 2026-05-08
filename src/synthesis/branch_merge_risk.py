"""Branch merge risk analyzer."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Sequence


@dataclass(frozen=True)
class BranchTouchRecord:
    branch_name: str
    changed_files: tuple[str, ...]
    test_files: tuple[str, ...] = ()


@dataclass(frozen=True)
class BranchMergeRiskReport:
    total_branches: int
    shared_file_hotspots: tuple[tuple[str, int], ...]
    impact_hint_branches: int
    source_files_touched: int
    source_files_with_tests: int
    test_coverage_ratio: float
    risk_label: str
    insights: tuple[str, ...]


def analyze_branch_merge_risk(records: Sequence[BranchTouchRecord]) -> BranchMergeRiskReport:
    _validate_records(records)
    file_counts: dict[str, int] = {}
    impact_branches = 0
    touched_sources: set[str] = set()
    covered_sources: set[str] = set()

    for record in records:
        if any(_is_impact_hint(path) for path in record.changed_files):
            impact_branches += 1
        for path in record.changed_files:
            file_counts[path] = file_counts.get(path, 0) + 1
            if _is_source(path):
                touched_sources.add(path)
                if _has_companion_test(path, record.test_files):
                    covered_sources.add(path)

    hotspots = tuple(sorted(((path, count) for path, count in file_counts.items() if count > 1), key=lambda item: (-item[1], item[0])))
    ratio = round(len(covered_sources) / len(touched_sources), 3) if touched_sources else 1.0
    risk = _risk_label(hotspots, impact_branches, ratio)
    return BranchMergeRiskReport(
        total_branches=len(records),
        shared_file_hotspots=hotspots,
        impact_hint_branches=impact_branches,
        source_files_touched=len(touched_sources),
        source_files_with_tests=len(covered_sources),
        test_coverage_ratio=ratio,
        risk_label=risk,
        insights=_risk_insights(hotspots, impact_branches, ratio),
    )


def _validate_records(records: Sequence[BranchTouchRecord]) -> None:
    if not isinstance(records, (list, tuple)):
        raise ValueError("records must be a list or tuple")
    seen_branch_names: set[str] = set()
    for index, record in enumerate(records):
        if not isinstance(record, BranchTouchRecord):
            raise ValueError(f"records[{index}] must be a BranchTouchRecord")
        if not isinstance(record.branch_name, str) or not record.branch_name.strip():
            raise ValueError("branch_name must be a non-empty string")
        if record.branch_name in seen_branch_names:
            raise ValueError(f"duplicate branch_name values are not supported: {record.branch_name}")
        seen_branch_names.add(record.branch_name)
        for attr in ("changed_files", "test_files"):
            value = getattr(record, attr)
            if not isinstance(value, tuple) or any(not isinstance(path, str) or not path.strip() for path in value):
                raise ValueError(f"{attr} must be a tuple of non-empty strings")


def _is_source(path: str) -> bool:
    return path.startswith("src/") and path.endswith((".py", ".ts", ".tsx", ".js", ".jsx"))


def _has_companion_test(source_path: str, test_files: tuple[str, ...]) -> bool:
    stem = PurePosixPath(source_path).stem
    return any(stem in PurePosixPath(test).stem for test in test_files)


def _is_impact_hint(path: str) -> bool:
    lowered = path.lower()
    return any(token in lowered for token in ("generated", "coverage", "schema", "migration", "database", ".lock"))


def _risk_label(hotspots: tuple[tuple[str, int], ...], impact_branches: int, ratio: float) -> str:
    if len(hotspots) >= 2 or (hotspots and impact_branches) or ratio < 0.5:
        return "high"
    if hotspots or impact_branches or ratio < 1.0:
        return "medium"
    return "low"


def _risk_insights(hotspots: tuple[tuple[str, int], ...], impact_branches: int, ratio: float) -> tuple[str, ...]:
    insights: list[str] = []
    if hotspots:
        insights.append("Shared file hotspots: " + ", ".join(path for path, _ in hotspots) + ".")
    if impact_branches:
        insights.append(f"{impact_branches} branch(es) touched generated, database, coverage, or lock files.")
    if ratio < 1.0:
        insights.append(f"Companion test coverage ratio is {ratio:.1%}.")
    return tuple(insights or ["Branches look independent with companion tests."])
