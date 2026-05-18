"""Audit generated content to knowledge link integrity."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
ISSUE_TYPES = (
    "missing_content",
    "missing_knowledge",
    "invalid_relevance_score",
    "restricted_knowledge",
    "unapproved_knowledge",
)


def build_content_knowledge_link_orphans_report(
    link_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for row in link_rows:
        issues = _issues_for_row(row)
        if not issues:
            continue
        for issue_type in issues:
            counts[issue_type] += 1
        findings.append(
            {
                "link_id": _value(row, "link_id", "id"),
                "content_id": row.get("content_id"),
                "knowledge_id": row.get("knowledge_id"),
                "content_exists": row.get("resolved_content_id") is not None,
                "knowledge_exists": row.get("resolved_knowledge_id") is not None,
                "relevance_score": row.get("relevance_score"),
                "knowledge_license": row.get("knowledge_license") or row.get("license"),
                "knowledge_approved": row.get("knowledge_approved") if "knowledge_approved" in row else row.get("approved"),
                "issue_type": issues[0],
                "issue_types": issues,
            }
        )

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "content_knowledge_link_orphans",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "summary": {
            "link_count": len(link_rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
        },
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No content knowledge link orphan issues found." if not findings else None,
        },
    }


def build_content_knowledge_link_orphans_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if not {"content_knowledge_links", "generated_content", "knowledge"}.issubset(schema):
        return build_content_knowledge_link_orphans_report([], **kwargs)
    return build_content_knowledge_link_orphans_report(_load_links(conn, schema), **kwargs)


def format_content_knowledge_link_orphans_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_knowledge_link_orphans_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Content Knowledge Link Orphans",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: links={summary['link_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
        "Issue counts: "
        + ", ".join(f"{issue_type}={summary['by_issue_type'].get(issue_type, 0)}" for issue_type in ISSUE_TYPES),
    ]
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "link_id | content_id | knowledge_id | issue_types | relevance_score | license | approved"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['link_id'] or '-'} | {finding['content_id'] or '-'} | {finding['knowledge_id'] or '-'} | "
            f"{','.join(finding['issue_types'])} | {_display(finding['relevance_score'])} | "
            f"{finding['knowledge_license'] or '-'} | {_display(finding['knowledge_approved'])}"
        )
    return "\n".join(lines)


def _issues_for_row(row: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    content_exists = row.get("resolved_content_id") is not None
    knowledge_exists = row.get("resolved_knowledge_id") is not None
    if not content_exists:
        issues.append("missing_content")
    if not knowledge_exists:
        issues.append("missing_knowledge")
    if not _valid_score(row.get("relevance_score")):
        issues.append("invalid_relevance_score")
    if content_exists and knowledge_exists:
        if _clean(row.get("knowledge_license") or row.get("license")).lower() == "restricted":
            issues.append("restricted_knowledge")
        if not _approved(row.get("knowledge_approved") if "knowledge_approved" in row else row.get("approved")):
            issues.append("unapproved_knowledge")
    return issues


def _load_links(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    link_cols = schema["content_knowledge_links"]
    knowledge_cols = schema["knowledge"]
    relevance = _column_expr(link_cols, "relevance_score", "NULL", alias="ckl")
    knowledge_license = _column_expr(knowledge_cols, "license", "NULL", alias="k")
    approved = _column_expr(knowledge_cols, "approved", "1", alias="k")
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   ckl.rowid AS link_id,
                   ckl.content_id AS content_id,
                   gc.id AS resolved_content_id,
                   ckl.knowledge_id AS knowledge_id,
                   k.id AS resolved_knowledge_id,
                   {relevance} AS relevance_score,
                   {knowledge_license} AS knowledge_license,
                   {approved} AS knowledge_approved
               FROM content_knowledge_links ckl
               LEFT JOIN generated_content gc ON gc.id = ckl.content_id
               LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
               ORDER BY ckl.rowid ASC"""
        ).fetchall()
    ]


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        ISSUE_TYPES.index(finding["issue_type"]) if finding["issue_type"] in ISSUE_TYPES else len(ISSUE_TYPES),
        _int_or_text(finding.get("content_id")),
        _int_or_text(finding.get("knowledge_id")),
        _int_or_text(finding.get("link_id")),
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], column: str, fallback: str, *, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _valid_score(value: Any) -> bool:
    if value in (None, ""):
        return False
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return False
    return 0 <= parsed <= 1


def _approved(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    return _clean(value).lower() not in {"0", "false", "no", "n", "unapproved"}


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _display(value: Any) -> str:
    text = _clean(value)
    return text if text else "-"


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
