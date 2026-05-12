"""Analyze prompt template placeholder coverage for explicit files."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import json
from pathlib import Path
import re
from string import Formatter
from typing import Any, Mapping, Sequence


PLACEHOLDER_RE = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")


@dataclass(frozen=True)
class PromptTemplateVariableCoverageRow:
    """Placeholder inventory and coverage issues for one prompt template."""

    template_path: str
    content_type: str
    placeholders: tuple[str, ...]
    duplicate_placeholders: tuple[str, ...]
    required_placeholders: tuple[str, ...]
    missing_required_placeholders: tuple[str, ...]
    unknown_placeholders: tuple[str, ...]
    has_companion_test: bool
    missing_companion_test: bool

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in (
            "duplicate_placeholders",
            "missing_required_placeholders",
            "placeholders",
            "required_placeholders",
            "unknown_placeholders",
        ):
            payload[key] = list(payload[key])
        return payload


@dataclass(frozen=True)
class PromptTemplateVariableCoverageReport:
    """Prompt template variable coverage report."""

    artifact_type: str
    totals: dict[str, Any]
    rows: tuple[PromptTemplateVariableCoverageRow, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_prompt_template_variable_coverage_report(
    template_paths: Sequence[str | Path],
    *,
    required_placeholders_by_content_type: Mapping[str, Sequence[str]] | None = None,
    known_placeholders_by_content_type: Mapping[str, Sequence[str]] | None = None,
    companion_test_paths: Sequence[str | Path] | None = None,
) -> PromptTemplateVariableCoverageReport:
    """Return one coverage row per explicit prompt template path."""
    required = {
        key: set(values)
        for key, values in (required_placeholders_by_content_type or {}).items()
    }
    known = {
        key: set(values)
        for key, values in (known_placeholders_by_content_type or {}).items()
    }
    companion_tests = {Path(path).name for path in (companion_test_paths or ())}
    rows = [
        _analyze_template(
            Path(path),
            required_placeholders_by_content_type=required,
            known_placeholders_by_content_type=known,
            companion_test_names=companion_tests,
        )
        for path in template_paths
    ]
    rows.sort(key=lambda row: row.template_path)
    return PromptTemplateVariableCoverageReport(
        artifact_type="prompt_template_variable_coverage",
        totals={
            "missing_companion_test_count": sum(row.missing_companion_test for row in rows),
            "missing_required_placeholder_count": sum(
                len(row.missing_required_placeholders) for row in rows
            ),
            "template_count": len(rows),
            "unknown_placeholder_count": sum(len(row.unknown_placeholders) for row in rows),
        },
        rows=tuple(rows),
    )


def format_prompt_template_variable_coverage_json(
    report: PromptTemplateVariableCoverageReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_prompt_template_variable_coverage_text(
    report: PromptTemplateVariableCoverageReport,
) -> str:
    """Render a stable text report."""
    lines = [
        "Prompt Template Variable Coverage",
        f"Templates: {report.totals['template_count']}",
        (
            f"Missing required: {report.totals['missing_required_placeholder_count']} "
            f"unknown: {report.totals['unknown_placeholder_count']} "
            f"missing_tests: {report.totals['missing_companion_test_count']}"
        ),
    ]
    if not report.rows:
        lines.append("No prompt templates supplied.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            f"- {row.template_path} type={row.content_type} "
            f"placeholders={','.join(row.placeholders) or '-'} "
            f"missing={','.join(row.missing_required_placeholders) or '-'} "
            f"unknown={','.join(row.unknown_placeholders) or '-'} "
            f"duplicates={','.join(row.duplicate_placeholders) or '-'} "
            f"test={row.has_companion_test}"
        )
    return "\n".join(lines)


def _analyze_template(
    path: Path,
    *,
    required_placeholders_by_content_type: Mapping[str, set[str]],
    known_placeholders_by_content_type: Mapping[str, set[str]],
    companion_test_names: set[str],
) -> PromptTemplateVariableCoverageRow:
    text = path.read_text(encoding="utf-8")
    names = _placeholder_names(text)
    counts = Counter(names)
    content_type = _content_type(path)
    required = set(required_placeholders_by_content_type.get(content_type, ()))
    known = set(known_placeholders_by_content_type.get(content_type, ())) | required
    unknown = set(names) - known if known else set()
    has_test = _has_companion_test(path, companion_test_names)
    return PromptTemplateVariableCoverageRow(
        template_path=str(path),
        content_type=content_type,
        placeholders=tuple(sorted(set(names))),
        duplicate_placeholders=tuple(sorted(name for name, count in counts.items() if count > 1)),
        required_placeholders=tuple(sorted(required)),
        missing_required_placeholders=tuple(sorted(required - set(names))),
        unknown_placeholders=tuple(sorted(unknown)),
        has_companion_test=has_test,
        missing_companion_test=not has_test,
    )


def _placeholder_names(text: str) -> list[str]:
    names: list[str] = []
    for _, field_name, _, _ in Formatter().parse(text):
        if field_name:
            root = field_name.split(".", 1)[0].split("[", 1)[0]
            if PLACEHOLDER_RE.fullmatch("{" + root + "}"):
                names.append(root)
    return names


def _content_type(path: Path) -> str:
    stem = path.stem
    for suffix in ("_v1", "_v2", "_v3", "_v4", "_enhanced", "_batched"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
    return stem


def _has_companion_test(path: Path, companion_test_names: set[str]) -> bool:
    if not companion_test_names:
        return False
    stem = path.stem
    candidates = {
        f"test_{stem}.py",
        f"test_prompt_{stem}.py",
        f"test_{_content_type(path)}.py",
        "test_synthesis_templates.py",
    }
    return bool(candidates & companion_test_names)
