"""Safety linting for generated image prompts before visual publishing."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
SEVERITY_WARN = "warn"
SEVERITY_ERROR = "error"
FAIL_ON_WARN = "warn"
FAIL_ON_ERROR = "error"

RULE_MISSING_ALT_TEXT = "missing_alt_text"
RULE_REAL_PERSON_LIKENESS = "real_person_likeness"
RULE_PRIVATE_DATA = "private_data"
RULE_BRAND_LOGO_IMPERSONATION = "brand_logo_impersonation"
RULE_DENSE_RENDERED_TEXT = "dense_rendered_text"

_VISUAL_CONTENT_TYPES = {"visual", "x_visual", "image", "social_preview_card"}
_TOKEN_RE = re.compile(r"[a-z0-9']+")
_PERSON_NAME_RE = re.compile(
    r"\b(?:portrait|headshot|photo|photorealistic|realistic|likeness|face)\s+of\s+"
    r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\b"
)

_REAL_PERSON_PATTERNS = (
    re.compile(r"\b(real[- ]?person|real human|actual person|celebrity|public figure)\b", re.I),
    re.compile(r"\b(exact|accurate|recognizable)\s+(?:face|likeness|portrait)\b", re.I),
    re.compile(r"\b(?:look|looks|looking)\s+like\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}\b"),
    re.compile(r"\b(?:as|of)\s+(?:a\s+)?(?:famous|real)\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}\b"),
)

_PRIVATE_DATA_PATTERNS = (
    re.compile(r"\b(?:ssn|social security number|passport|driver'?s license)\b", re.I),
    re.compile(r"\b(?:credit card|bank account|routing number|medical record|patient record)\b", re.I),
    re.compile(r"\b(?:api key|secret token|access token|password|private key)\b", re.I),
    re.compile(r"\b(?:home address|personal address|phone number|email address|private dm|private message)\b", re.I),
    re.compile(r"\b\d{3}-\d{2}-\d{4}\b"),
    re.compile(r"\b(?:\d[ -]*?){13,16}\b"),
)

_LOGO_PATTERNS = (
    re.compile(r"\b(?:logo|logomark|brand mark|trademark|wordmark)\b", re.I),
    re.compile(r"\bofficial\s+(?:brand|company|product)\b", re.I),
    re.compile(r"\b(?:apple|google|microsoft|openai|meta|x|twitter|github|linkedin)\s+logo\b", re.I),
    re.compile(r"\b(?:impersonate|knockoff|fake)\s+(?:brand|company|product|logo)\b", re.I),
)

_DENSE_TEXT_PATTERNS = (
    re.compile(r"\b(?:lots of|a lot of|dense|tiny|small|fine print|paragraphs? of)\s+text\b", re.I),
    re.compile(r"\b(?:render|include|write|show)\s+(?:the\s+)?(?:full|entire)\s+(?:article|thread|post|essay|document)\b", re.I),
    re.compile(r"\b(?:wall of text|text-heavy|unreadable text|microcopy)\b", re.I),
    re.compile(r"\b(?:more than|over)\s+\d+\s+(?:words|lines)\b", re.I),
)

_NEGATION_WORDS = {"avoid", "exclude", "no", "not", "without", "remove", "omit"}


@dataclass(frozen=True)
class ImagePromptSafetyFinding:
    """One image prompt safety finding."""

    severity: str
    rule_id: str
    content_id: int
    remediation: str
    message: str
    field: str
    created_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def lint_image_prompt_row(row: dict[str, Any]) -> list[ImagePromptSafetyFinding]:
    """Lint one generated_content-like row."""
    content_id = _int_value(row.get("id"))
    prompt = str(row.get("image_prompt") or "")
    alt_text = str(row.get("image_alt_text") or "")
    image_path = str(row.get("image_path") or "")
    content_type = str(row.get("content_type") or "")
    created_at = row.get("created_at")

    if not _is_visual_content(
        image_path=image_path,
        image_prompt=prompt,
        content_type=content_type,
    ):
        return []

    findings: list[ImagePromptSafetyFinding] = []
    if not alt_text.strip():
        findings.append(
            ImagePromptSafetyFinding(
                severity=SEVERITY_ERROR,
                rule_id=RULE_MISSING_ALT_TEXT,
                content_id=content_id,
                field="image_alt_text",
                message="Visual content is missing image alt text.",
                remediation="Add concise alt text that describes the generated visual before publishing.",
                created_at=created_at,
            )
        )

    if not prompt.strip():
        return findings

    for rule_id, severity, field, message, remediation in _prompt_findings(prompt):
        findings.append(
            ImagePromptSafetyFinding(
                severity=severity,
                rule_id=rule_id,
                content_id=content_id,
                field=field,
                message=message,
                remediation=remediation,
                created_at=created_at,
            )
        )

    return findings


def lint_image_prompts(
    db: Any,
    *,
    content_id: int | None = None,
    days: int | None = DEFAULT_DAYS,
    now: datetime | None = None,
) -> list[ImagePromptSafetyFinding]:
    """Return image prompt safety findings for generated content rows."""
    if content_id is not None and content_id <= 0:
        raise ValueError("content_id must be positive")
    if days is not None and days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db)
    columns = _table_columns(conn, "generated_content")
    if not columns:
        return []

    rows = _fetch_generated_content_rows(
        conn,
        columns,
        content_id=content_id,
        days=days if content_id is None else None,
        now=_as_utc(now or datetime.now(timezone.utc)),
    )
    findings = [finding for row in rows for finding in lint_image_prompt_row(row)]
    return sorted(findings, key=lambda item: (item.content_id, _severity_rank(item.severity), item.rule_id))


def build_image_prompt_safety_report(
    db: Any,
    *,
    content_id: int | None = None,
    days: int | None = DEFAULT_DAYS,
    fail_on: str = FAIL_ON_ERROR,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a structured report for the image prompt linter."""
    if fail_on not in {FAIL_ON_WARN, FAIL_ON_ERROR}:
        raise ValueError("fail_on must be 'warn' or 'error'")

    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    findings = lint_image_prompts(db, content_id=content_id, days=days, now=now)
    counts = {
        "findings": len(findings),
        "warnings": sum(1 for finding in findings if finding.severity == SEVERITY_WARN),
        "errors": sum(1 for finding in findings if finding.severity == SEVERITY_ERROR),
    }
    failed = should_fail(findings, fail_on=fail_on)
    return {
        "artifact_type": "image_prompt_safety_lint",
        "generated_at": generated_at,
        "filters": {
            "content_id": content_id,
            "days": days,
            "fail_on": fail_on,
        },
        "status": "failed" if failed else "passed",
        "counts": counts,
        "findings": [finding.to_dict() for finding in findings],
    }


def should_fail(
    findings: list[ImagePromptSafetyFinding] | list[dict[str, Any]],
    *,
    fail_on: str,
) -> bool:
    """Return whether findings should produce a non-zero CLI exit code."""
    if fail_on not in {FAIL_ON_WARN, FAIL_ON_ERROR}:
        raise ValueError("fail_on must be 'warn' or 'error'")
    severities = [
        item.severity if isinstance(item, ImagePromptSafetyFinding) else str(item.get("severity") or "")
        for item in findings
    ]
    if fail_on == FAIL_ON_WARN:
        return any(severity in {SEVERITY_WARN, SEVERITY_ERROR} for severity in severities)
    return any(severity == SEVERITY_ERROR for severity in severities)


def format_image_prompt_safety_json(report: dict[str, Any]) -> str:
    """Format a safety lint report as JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_image_prompt_safety_text(report: dict[str, Any]) -> str:
    """Format a safety lint report for humans."""
    if not report["findings"]:
        return "No image prompt safety findings."

    lines = [
        "Image Prompt Safety Lint",
        (
            f"Counts: findings={report['counts']['findings']} "
            f"warnings={report['counts']['warnings']} errors={report['counts']['errors']}"
        ),
        "",
        "Findings",
    ]
    for item in report["findings"]:
        lines.append(
            f"  - {item['severity']} {item['rule_id']} content={item['content_id']} "
            f"field={item['field']}: {item['message']}"
        )
        lines.append(f"    remediation: {item['remediation']}")
    return "\n".join(lines)


def _prompt_findings(prompt: str) -> list[tuple[str, str, str, str, str]]:
    findings: list[tuple[str, str, str, str, str]] = []
    if _matches_unnegated(prompt, _REAL_PERSON_PATTERNS) or _PERSON_NAME_RE.search(prompt):
        findings.append(
            (
                RULE_REAL_PERSON_LIKENESS,
                SEVERITY_ERROR,
                "image_prompt",
                "Prompt appears to request a real person's likeness.",
                "Rewrite the prompt to use a fictional, non-identifiable person or an abstract scene.",
            )
        )
    if _matches_unnegated(prompt, _PRIVATE_DATA_PATTERNS):
        findings.append(
            (
                RULE_PRIVATE_DATA,
                SEVERITY_ERROR,
                "image_prompt",
                "Prompt appears to request private or sensitive information.",
                "Remove private data and represent the concept with generic placeholders.",
            )
        )
    if _matches_unnegated(prompt, _LOGO_PATTERNS):
        findings.append(
            (
                RULE_BRAND_LOGO_IMPERSONATION,
                SEVERITY_ERROR,
                "image_prompt",
                "Prompt appears to request logos, trademarks, or brand impersonation.",
                "Use generic product shapes or neutral branding instead of protected marks.",
            )
        )
    if _matches_unnegated(prompt, _DENSE_TEXT_PATTERNS):
        findings.append(
            (
                RULE_DENSE_RENDERED_TEXT,
                SEVERITY_WARN,
                "image_prompt",
                "Prompt asks the image model to render dense or likely unreadable text.",
                "Keep rendered text to a few large words, or add the text during layout instead.",
            )
        )
    return findings


def _fetch_generated_content_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    content_id: int | None,
    days: int | None,
    now: datetime,
) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id", "NULL"),
        _column_expr(columns, "content_type", "NULL"),
        _column_expr(columns, "created_at", "NULL"),
        _column_expr(columns, "image_path", "NULL"),
        _column_expr(columns, "image_prompt", "NULL"),
        _column_expr(columns, "image_alt_text", "NULL"),
    ]
    filters: list[str] = []
    params: list[Any] = []

    if content_id is not None and "id" in columns:
        filters.append("id = ?")
        params.append(content_id)
    if days is not None and "created_at" in columns:
        cutoff = now - timedelta(days=days)
        filters.append("(created_at IS NULL OR datetime(created_at) >= datetime(?))")
        params.append(cutoff.isoformat())

    query = f"SELECT {', '.join(selected)} FROM generated_content"
    if filters:
        query += " WHERE " + " AND ".join(filters)
    query += " ORDER BY " + ("datetime(created_at) DESC, id DESC" if {"created_at", "id"}.issubset(columns) else "rowid DESC")
    return [dict(row) for row in conn.execute(query, tuple(params)).fetchall()]


def _column_expr(columns: set[str], column: str, fallback: str) -> str:
    if column in columns:
        return column
    return f"{fallback} AS {column}"


def _matches_unnegated(text: str, patterns: tuple[re.Pattern[str], ...]) -> bool:
    for pattern in patterns:
        for match in pattern.finditer(text):
            if not _is_negated(text, match.start()):
                return True
    return False


def _is_negated(text: str, start: int) -> bool:
    prefix = text[max(0, start - 32) : start].lower()
    tokens = _TOKEN_RE.findall(prefix)
    return bool(tokens and any(token in _NEGATION_WORDS for token in tokens[-4:]))


def _is_visual_content(
    *,
    image_path: str | None,
    image_prompt: str | None,
    content_type: str | None,
) -> bool:
    return bool((image_path or "").strip()) or bool((image_prompt or "").strip()) or (content_type or "") in _VISUAL_CONTENT_TYPES


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _severity_rank(severity: str) -> int:
    return {SEVERITY_ERROR: 0, SEVERITY_WARN: 1}.get(severity, 2)


def _int_value(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
