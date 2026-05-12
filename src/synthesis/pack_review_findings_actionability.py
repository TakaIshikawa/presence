"""Pack review findings actionability analyzer."""

from __future__ import annotations

import re
from typing import Any, Mapping


COMPONENTS = ("file_reference", "line_reference", "failure_mode", "severity", "suggested_fix")
FINDING_KEYS = ("findings", "review_findings", "comments", "issues")
SEVERITIES = ("critical", "high", "medium", "low", "major", "minor", "blocker", "nit")


def analyze_pack_review_findings_actionability(records: object) -> dict[str, Any]:
    """Score whether review findings contain actionable debugging context."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack review dictionaries")

    total_findings = 0
    actionable_findings = 0
    low_actionability_findings = 0
    missing_component_counts = {component: 0 for component in COMPONENTS}
    examples: list[dict[str, Any]] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        for finding in _extract_findings(record):
            total_findings += 1
            text = _finding_text(finding)
            components = _detect_components(finding, text)
            missing = [component for component in COMPONENTS if component not in components]

            for component in missing:
                missing_component_counts[component] += 1

            if not missing:
                actionable_findings += 1
            else:
                low_actionability_findings += 1
                if len(examples) < 5:
                    examples.append(
                        {
                            "pack_id": _string_or_none(record.get("pack_id")),
                            "finding": text[:160],
                            "missing_components": missing,
                        }
                    )

    return {
        "total_findings": total_findings,
        "actionable_findings": actionable_findings,
        "low_actionability_findings": low_actionability_findings,
        "actionability_rate_percent": _percent(actionable_findings, total_findings),
        "missing_component_counts": missing_component_counts,
        "examples": examples,
    }


def _extract_findings(record: Mapping[str, Any]) -> list[object]:
    findings: list[object] = []
    for key in FINDING_KEYS:
        value = record.get(key)
        if isinstance(value, list):
            findings.extend(value)
        elif isinstance(value, (str, Mapping)):
            findings.append(value)
    return findings


def _finding_text(finding: object) -> str:
    if isinstance(finding, str):
        return finding.strip()
    if isinstance(finding, Mapping):
        parts: list[str] = []
        for key in ("title", "message", "body", "comment", "description", "text", "suggestion", "fix"):
            value = finding.get(key)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())
        return " ".join(parts)
    return ""


def _detect_components(finding: object, text: str) -> set[str]:
    combined = text.lower()
    components: set[str] = set()

    if isinstance(finding, Mapping):
        if _string_or_none(finding.get("file")) or _string_or_none(finding.get("path")):
            components.add("file_reference")
        if _intish(finding.get("line")) or _intish(finding.get("line_number")):
            components.add("line_reference")
        if _string_or_none(finding.get("severity")):
            components.add("severity")
        if _string_or_none(finding.get("suggested_fix")) or _string_or_none(finding.get("fix")):
            components.add("suggested_fix")

    if re.search(r"\b[\w./-]+\.(?:py|js|ts|tsx|jsx|go|rs|rb|java|md|yml|yaml|json|toml|css|html)(?::\d+)?\b", text):
        components.add("file_reference")
    if re.search(r"(?:line|lines|:)\s*\d+", text, re.IGNORECASE):
        components.add("line_reference")
    if any(severity in combined for severity in SEVERITIES):
        components.add("severity")
    if re.search(r"\b(?:fails?|breaks?|crashes?|regress(?:es|ion)?|incorrect|missing|raises?|throws?|leaks?|overwrites?|skips?)\b", combined):
        components.add("failure_mode")
    if re.search(r"\b(?:fix|change|update|replace|guard|add|remove|use|return|handle|suggest)\b", combined):
        components.add("suggested_fix")

    return components


def _string_or_none(value: object) -> str | None:
    return value.strip() if isinstance(value, str) and value.strip() else None


def _intish(value: object) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    return isinstance(value, str) and value.strip().isdigit()


def _percent(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator * 100, 2)
