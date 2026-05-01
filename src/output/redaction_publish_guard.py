"""Read-only redaction guard for queued generated content."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from typing import Any, Iterable

from ingestion.redaction import DEFAULT_REDACTION_PATTERNS, RedactionPattern, Redactor
from ingestion.redaction_audit import build_redacted_preview


VALID_PLATFORMS = ("all", "x", "bluesky")
DEFAULT_PREVIEW_CHARS = 160
DEFAULT_CONTEXT_CHARS = 56
PUBLISH_GUARD_EXTRA_PATTERNS: tuple[dict[str, str], ...] = (
    {
        "name": "token_like",
        "pattern": r"(?<![/\\])\b(?!gh[opsu]_)(?!github_pat_)(?!sk-)(?!sk-ant-)(?!xox[baprs]-)(?=[A-Za-z0-9._~+=-]{24,}\b)(?=.*[A-Za-z])(?=.*\d)[A-Za-z0-9._~+=-]+\b",
        "placeholder": "[REDACTED_SECRET]",
    },
)


@dataclass(frozen=True)
class RedactionPublishMatch:
    """One sanitized redaction match in queued content."""

    rule_code: str
    severity: str
    sanitized_snippet: str


@dataclass(frozen=True)
class RedactionPublishItem:
    """Publish-guard result for one queued item."""

    queue_id: int
    content_id: int
    platform: str
    status: str
    matched_rule_codes: list[str]
    severity: str
    sanitized_snippets: list[str]
    matches: list[RedactionPublishMatch]


@dataclass(frozen=True)
class RedactionPublishGuardReport:
    """Stable report for queued-content redaction guard checks."""

    artifact_type: str
    queue_id: int | None
    platform: str | None
    include_warnings: bool
    scanned_count: int
    blocked_count: int
    warning_count: int
    passed_count: int
    items: list[RedactionPublishItem]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _Rule:
    code: str
    severity: str
    pattern: RedactionPattern


def build_redaction_publish_guard_report(
    db_or_conn: Any,
    *,
    queue_id: int | None = None,
    platform: str | None = None,
    include_warnings: bool = False,
    patterns: Iterable[str | dict[str, Any]] | None = None,
    preview_chars: int = DEFAULT_PREVIEW_CHARS,
) -> RedactionPublishGuardReport:
    """Scan queued generated content for redaction-rule matches.

    The guard only reads ``publish_queue`` and ``generated_content`` rows. It
    returns sanitized snippets and never exposes raw matched values.
    """

    if queue_id is not None and queue_id <= 0:
        raise ValueError("queue_id must be positive")
    if platform is not None and platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")
    if preview_chars <= 0:
        raise ValueError("preview_chars must be positive")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    schema = _schema(conn)
    if "publish_queue" not in schema or "generated_content" not in schema:
        return _report(queue_id, platform, include_warnings, [])

    rows = _queued_rows(conn, queue_id=queue_id, platform=platform)
    raw_patterns = _raw_patterns(patterns)
    rules = _rules(raw_patterns, include_warnings=include_warnings)
    redactor = Redactor(raw_patterns)
    items = [
        _scan_row(row, rules=rules, redactor=redactor, preview_chars=preview_chars)
        for row in rows
    ]
    return _report(queue_id, platform, include_warnings, items)


def export_to_json(report: RedactionPublishGuardReport) -> str:
    """Serialize the report as stable JSON."""

    return json.dumps(report.as_dict(), indent=2, sort_keys=True)


def format_text_report(report: RedactionPublishGuardReport) -> str:
    """Render a deterministic operator-facing redaction guard report."""

    lines = [
        "Redaction Publish Guard",
        (
            "Filters: "
            f"queue_id={report.queue_id if report.queue_id is not None else '-'} "
            f"platform={report.platform or '-'} "
            f"include_warnings={str(report.include_warnings).lower()}"
        ),
        (
            f"Scanned: {report.scanned_count}  "
            f"Blocked: {report.blocked_count}  "
            f"Warnings: {report.warning_count}  "
            f"Passed: {report.passed_count}"
        ),
    ]
    if not report.items:
        lines.append("")
        lines.append("No queued publish items matched the filters.")
        return "\n".join(lines)

    lines.append("")
    for item in report.items:
        codes = ", ".join(item.matched_rule_codes) if item.matched_rule_codes else "-"
        lines.append(
            f"queue #{item.queue_id} content #{item.content_id} "
            f"{item.platform}: {item.status} severity={item.severity} rules={codes}"
        )
        for snippet in item.sanitized_snippets:
            lines.append(f"  - {snippet}")
    return "\n".join(lines)


def _report(
    queue_id: int | None,
    platform: str | None,
    include_warnings: bool,
    items: list[RedactionPublishItem],
) -> RedactionPublishGuardReport:
    blocked_count = sum(1 for item in items if item.status == "blocked")
    warning_count = sum(1 for item in items if item.status == "warning")
    return RedactionPublishGuardReport(
        artifact_type="redaction_publish_guard",
        queue_id=queue_id,
        platform=platform,
        include_warnings=include_warnings,
        scanned_count=len(items),
        blocked_count=blocked_count,
        warning_count=warning_count,
        passed_count=sum(1 for item in items if item.status == "passed"),
        items=items,
    )


def _queued_rows(
    conn: sqlite3.Connection,
    *,
    queue_id: int | None,
    platform: str | None,
) -> list[dict[str, Any]]:
    filters = ["pq.status = 'queued'"]
    params: list[Any] = []
    if queue_id is not None:
        filters.append("pq.id = ?")
        params.append(queue_id)
    if platform is not None:
        filters.append("pq.platform = ?")
        params.append(platform)

    rows = conn.execute(
        f"""SELECT pq.id AS queue_id, pq.content_id, pq.platform, gc.content
            FROM publish_queue pq
            INNER JOIN generated_content gc ON gc.id = pq.content_id
            WHERE {' AND '.join(filters)}
            ORDER BY pq.scheduled_at ASC, pq.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _scan_row(
    row: dict[str, Any],
    *,
    rules: list[_Rule],
    redactor: Redactor,
    preview_chars: int,
) -> RedactionPublishItem:
    text = str(row.get("content") or "")
    matches: list[RedactionPublishMatch] = []
    seen: set[tuple[str, str]] = set()
    for rule in rules:
        if not rule.pattern.regex.search(text):
            continue
        snippet = build_redacted_preview(
            text,
            rule.pattern,
            redactor=redactor,
            max_chars=preview_chars,
            context_chars=DEFAULT_CONTEXT_CHARS,
        )
        key = (rule.code, snippet)
        if key in seen:
            continue
        seen.add(key)
        matches.append(
            RedactionPublishMatch(
                rule_code=rule.code,
                severity=rule.severity,
                sanitized_snippet=snippet,
            )
        )

    matched_codes = sorted({match.rule_code for match in matches})
    severity = _max_severity(match.severity for match in matches)
    if severity == "block":
        status = "blocked"
    elif severity == "warning":
        status = "warning"
    else:
        status = "passed"
    return RedactionPublishItem(
        queue_id=int(row["queue_id"]),
        content_id=int(row["content_id"]),
        platform=str(row.get("platform") or "all"),
        status=status,
        matched_rule_codes=matched_codes,
        severity=severity,
        sanitized_snippets=[match.sanitized_snippet for match in matches],
        matches=matches,
    )


def _raw_patterns(patterns: Iterable[str | dict[str, Any]] | None) -> tuple[str | dict[str, Any], ...]:
    raw_patterns = DEFAULT_REDACTION_PATTERNS if patterns is None else tuple(patterns)
    existing_names = {pattern.get("name") for pattern in raw_patterns if isinstance(pattern, dict)}
    extra = tuple(
        pattern
        for pattern in PUBLISH_GUARD_EXTRA_PATTERNS
        if pattern["name"] not in existing_names
    )
    return (*raw_patterns, *extra)


def _rules(
    raw_patterns: Iterable[str | dict[str, Any]],
    *,
    include_warnings: bool,
) -> list[_Rule]:
    rules: list[_Rule] = []
    for index, raw in enumerate(raw_patterns):
        pattern = RedactionPattern.from_config(raw, index=index)
        severity = _pattern_severity(raw)
        if severity == "warning" and not include_warnings:
            continue
        rules.append(_Rule(code=pattern.name, severity=severity, pattern=pattern))
    return rules


def _pattern_severity(raw: str | dict[str, Any]) -> str:
    if isinstance(raw, dict):
        severity = str(raw.get("severity", "block")).lower()
        if severity in {"warning", "warn"}:
            return "warning"
    return "block"


def _max_severity(severities: Iterable[str]) -> str:
    current = "none"
    for severity in severities:
        if severity == "block":
            return "block"
        if severity == "warning":
            current = "warning"
    return current


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row["name"] if isinstance(row, sqlite3.Row) else row[0]: {
            column["name"] if isinstance(column, sqlite3.Row) else column[1]
            for column in conn.execute(
                f"PRAGMA table_info({_quote_identifier(row['name'] if isinstance(row, sqlite3.Row) else row[0])})"
            )
        }
        for row in rows
    }


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
