"""Branch naming hygiene analyzer for autonomous agent work."""

from __future__ import annotations

import re
from collections import Counter
from typing import Any, Mapping


VALID_PREFIXES = ("relay/codex", "relay/claude-code")


def analyze_branch_naming_hygiene(records: object) -> dict[str, Any]:
    """Classify branch names for traceability and naming issues."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of branch record dictionaries")

    prefix_counts: Counter[str] = Counter({prefix: 0 for prefix in VALID_PREFIXES})
    issue_counts: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    valid_count = 0

    for index, record in enumerate(records):
        branch = _branch(record)
        issues = _issues(branch)
        prefix = _prefix(branch)
        if prefix:
            prefix_counts[prefix] += 1
        if issues:
            for issue in issues:
                issue_counts[issue] += 1
            if len(examples) < 5:
                examples.append({"index": index, "branch": branch, "issues": issues})
        else:
            valid_count += 1

    return {
        "total_branches": len(records),
        "valid_branches": valid_count,
        "invalid_branches": len(records) - valid_count,
        "valid_percentage": _percentage(valid_count, len(records)),
        "agent_prefix_counts": dict(prefix_counts),
        "issue_counts": dict(issue_counts),
        "examples": examples,
    }


def _branch(record: object) -> str:
    if not isinstance(record, Mapping):
        return ""
    for key in ("branch", "branch_name", "ref"):
        value = record.get(key)
        if isinstance(value, str):
            return value.strip()
    return ""


def _prefix(branch: str) -> str:
    for prefix in VALID_PREFIXES:
        if branch.startswith(prefix + "/"):
            return prefix
    return ""


def _issues(branch: str) -> list[str]:
    issues: list[str] = []
    if not branch:
        return ["missing_branch"]
    if branch != branch.lower():
        issues.append("uppercase")
    if re.search(r"\s", branch):
        issues.append("whitespace")
    if len(branch) > 96:
        issues.append("too_long")
    prefix = _prefix(branch)
    if not prefix:
        issues.append("missing_agent_prefix")
        return issues
    suffix = branch[len(prefix) + 1 :]
    segments = [segment for segment in suffix.split("/") if segment]
    if not segments or not re.search(r"[a-z0-9]+-[a-z0-9-]+", segments[0]):
        issues.append("missing_task_slug")
    suffix_match = re.search(r"(?:^|[-/])([0-9a-z]{6,})$", suffix)
    if not suffix_match:
        issues.append("missing_unique_suffix")
    else:
        unique_suffix = suffix_match.group(1)
        looks_unique = any(char.isdigit() for char in unique_suffix) or bool(
            re.fullmatch(r"[a-f]{6,}", unique_suffix)
        )
        if not looks_unique:
            issues.append("missing_unique_suffix")
    return issues


def _percentage(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)
