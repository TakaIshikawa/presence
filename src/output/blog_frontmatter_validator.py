"""Validate static-site blog draft frontmatter."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FrontmatterIssue:
    """Structured warning or error emitted during frontmatter validation."""

    level: str
    code: str
    message: str
    field: str | None = None
    path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "level": self.level,
            "code": self.code,
            "message": self.message,
            "field": self.field,
            "path": self.path,
        }


@dataclass
class FrontmatterValidationResult:
    """Validation result for a single markdown draft."""

    ok: bool
    frontmatter: dict[str, Any] = field(default_factory=dict)
    body: str = ""
    errors: list[FrontmatterIssue] = field(default_factory=list)
    warnings: list[FrontmatterIssue] = field(default_factory=list)
    path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "path": self.path,
            "frontmatter": self.frontmatter,
            "errors": [issue.to_dict() for issue in self.errors],
            "warnings": [issue.to_dict() for issue in self.warnings],
        }


_REQUIRED_FIELDS = ("title", "date", "description", "source_content_ids")


def _issue(
    level: str,
    code: str,
    message: str,
    *,
    field: str | None = None,
    path: str | None = None,
) -> FrontmatterIssue:
    return FrontmatterIssue(
        level=level,
        code=code,
        message=message,
        field=field,
        path=path,
    )


def _parse_scalar(value: str) -> Any:
    text = value.strip()
    if text == "":
        return ""

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    lowered = text.lower()
    if lowered == "null":
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if re.fullmatch(r"-?\d+\.\d+", text):
        return float(text)
    return text


def parse_markdown_frontmatter(
    markdown: str,
    *,
    path: str | None = None,
) -> tuple[dict[str, Any], str, list[FrontmatterIssue]]:
    """Parse simple YAML-like frontmatter from a markdown document.

    The static site emits JSON-compatible scalars inside frontmatter. This parser
    intentionally supports only the subset used by generated drafts.
    """
    normalized = markdown.replace("\r\n", "\n").replace("\r", "\n")
    if not normalized.startswith("---\n"):
        return {}, normalized, [
            _issue(
                "error",
                "missing_frontmatter",
                "Markdown draft must start with frontmatter delimited by ---.",
                path=path,
            )
        ]

    end = normalized.find("\n---", 4)
    if end == -1:
        return {}, normalized, [
            _issue(
                "error",
                "unterminated_frontmatter",
                "Frontmatter must end with a closing --- delimiter.",
                path=path,
            )
        ]

    raw_frontmatter = normalized[4:end]
    body_start = end + len("\n---")
    if normalized[body_start:].startswith("\n"):
        body_start += 1
    body = normalized[body_start:]
    fields: dict[str, Any] = {}
    issues: list[FrontmatterIssue] = []

    for line_number, line in enumerate(raw_frontmatter.split("\n"), start=2):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if ":" not in line:
            issues.append(
                _issue(
                    "error",
                    "invalid_frontmatter_line",
                    f"Frontmatter line {line_number} must use key: value syntax.",
                    path=path,
                )
            )
            continue
        key, raw_value = line.split(":", 1)
        key = key.strip()
        if not key:
            issues.append(
                _issue(
                    "error",
                    "empty_frontmatter_key",
                    f"Frontmatter line {line_number} has an empty key.",
                    path=path,
                )
            )
            continue
        fields[key] = _parse_scalar(raw_value)

    return fields, body, issues


def _valid_iso_date(value: str) -> bool:
    try:
        date.fromisoformat(value)
        return True
    except ValueError:
        pass

    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def validate_blog_draft_frontmatter(
    markdown: str,
    *,
    path: str | None = None,
) -> FrontmatterValidationResult:
    """Validate required generated blog draft frontmatter fields."""
    frontmatter, body, parse_issues = parse_markdown_frontmatter(markdown, path=path)
    errors = [issue for issue in parse_issues if issue.level == "error"]
    warnings = [issue for issue in parse_issues if issue.level == "warning"]
    if any(
        issue.code in {"missing_frontmatter", "unterminated_frontmatter"}
        for issue in errors
    ):
        return FrontmatterValidationResult(
            ok=False,
            frontmatter=frontmatter,
            body=body,
            errors=errors,
            warnings=warnings,
            path=path,
        )

    for field_name in _REQUIRED_FIELDS:
        value = frontmatter.get(field_name)
        if value is None or value == "" or value == []:
            errors.append(
                _issue(
                    "error",
                    "missing_required_field",
                    f"Missing required frontmatter field: {field_name}.",
                    field=field_name,
                    path=path,
                )
            )

    title = frontmatter.get("title")
    if title is not None and (not isinstance(title, str) or not title.strip()):
        errors.append(
            _issue(
                "error",
                "invalid_title",
                "Frontmatter title must be a non-empty string.",
                field="title",
                path=path,
            )
        )

    description = frontmatter.get("description")
    if description is not None and (
        not isinstance(description, str) or not description.strip()
    ):
        errors.append(
            _issue(
                "error",
                "invalid_description",
                "Frontmatter description must be a non-empty string.",
                field="description",
                path=path,
            )
        )

    date_value = frontmatter.get("date")
    if date_value is not None:
        if not isinstance(date_value, str) or not _valid_iso_date(date_value):
            errors.append(
                _issue(
                    "error",
                    "invalid_date",
                    "Frontmatter date must be an ISO 8601 date or datetime string.",
                    field="date",
                    path=path,
                )
            )

    source_ids = frontmatter.get("source_content_ids")
    if source_ids is not None:
        if not isinstance(source_ids, list):
            errors.append(
                _issue(
                    "error",
                    "invalid_source_content_ids",
                    "Frontmatter source_content_ids must be a non-empty list of positive integers.",
                    field="source_content_ids",
                    path=path,
                )
            )
        elif not source_ids or any(
            not isinstance(item, int) or isinstance(item, bool) or item <= 0
            for item in source_ids
        ):
            errors.append(
                _issue(
                    "error",
                    "invalid_source_content_ids",
                    "Frontmatter source_content_ids must be a non-empty list of positive integers.",
                    field="source_content_ids",
                    path=path,
                )
            )

    if not body.strip():
        warnings.append(
            _issue(
                "warning",
                "empty_body",
                "Markdown draft body is empty.",
                path=path,
            )
        )

    return FrontmatterValidationResult(
        ok=not errors,
        frontmatter=frontmatter,
        body=body,
        errors=errors,
        warnings=warnings,
        path=path,
    )


def validate_blog_draft_file(path: str | Path) -> FrontmatterValidationResult:
    """Read and validate one markdown blog draft file."""
    draft_path = Path(path)
    return validate_blog_draft_frontmatter(
        draft_path.read_text(),
        path=str(draft_path),
    )
