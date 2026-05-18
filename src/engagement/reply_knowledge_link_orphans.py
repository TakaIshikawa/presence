"""Audit reply to knowledge link integrity."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_LIMIT = 100
DEFAULT_STATUS = "all"
ISSUE_TYPES = (
    "missing_reply",
    "missing_knowledge",
    "invalid_relevance_score",
    "dismissed_reply_attached",
)


def build_reply_knowledge_link_orphans_report(
    link_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    status: str | Iterable[str] = DEFAULT_STATUS,
    now: datetime | None = None,
    missing_tables: Iterable[str] = (),
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")

    statuses = _normalize_status_filter(status)
    generated_at = _utc(now or datetime.now(timezone.utc))
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    scanned = 0

    for row in link_rows:
        reply_status = _clean(row.get("reply_status")).lower()
        if statuses and "all" not in statuses and (reply_status or "pending") not in statuses:
            continue
        scanned += 1
        issues = _issues_for_row(row)
        if not issues:
            continue
        for issue_type in issues:
            counts[issue_type] += 1
        findings.append(
            {
                "link_id": _value(row, "link_id", "id"),
                "reply_queue_id": row.get("reply_queue_id"),
                "knowledge_id": row.get("knowledge_id"),
                "reply_exists": row.get("resolved_reply_queue_id") is not None,
                "knowledge_exists": row.get("resolved_knowledge_id") is not None,
                "reply_status": row.get("reply_status"),
                "relevance_score": row.get("relevance_score"),
                "issue_type": issues[0],
                "issue_types": issues,
            }
        )

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "reply_knowledge_link_orphans",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit, "status": list(statuses)},
        "summary": {
            "link_count": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
        },
        "missing_tables": sorted(str(table) for table in missing_tables),
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No reply knowledge link orphan issues found." if not findings else None,
        },
    }


def build_reply_knowledge_link_orphans_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"reply_knowledge_links", "reply_queue", "knowledge"}
    missing = sorted(required - set(schema))
    if missing:
        return build_reply_knowledge_link_orphans_report([], missing_tables=missing, **kwargs)
    return build_reply_knowledge_link_orphans_report(_load_links(conn, schema), **kwargs)


def format_reply_knowledge_link_orphans_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_knowledge_link_orphans_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Reply Knowledge Link Orphans",
        f"Generated: {report['generated_at']}",
        f"Status: {', '.join(report['filters']['status'])}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: links={summary['link_count']} findings={summary['finding_count']} shown={summary['shown_count']}",
        "Issue counts: "
        + ", ".join(f"{issue_type}={summary['by_issue_type'].get(issue_type, 0)}" for issue_type in ISSUE_TYPES),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "link_id | reply_queue_id | knowledge_id | reply_status | issue_types | relevance_score"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['link_id'] or '-'} | {finding['reply_queue_id'] or '-'} | "
            f"{finding['knowledge_id'] or '-'} | {finding['reply_status'] or '-'} | "
            f"{','.join(finding['issue_types'])} | {_display(finding['relevance_score'])}"
        )
    return "\n".join(lines)


def _issues_for_row(row: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    reply_exists = row.get("resolved_reply_queue_id") is not None
    knowledge_exists = row.get("resolved_knowledge_id") is not None
    if not reply_exists:
        issues.append("missing_reply")
    if not knowledge_exists:
        issues.append("missing_knowledge")
    if not _valid_score(row.get("relevance_score")):
        issues.append("invalid_relevance_score")
    if reply_exists and _clean(row.get("reply_status")).lower() == "dismissed":
        issues.append("dismissed_reply_attached")
    return issues


def _load_links(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    link_cols = schema["reply_knowledge_links"]
    reply_cols = schema["reply_queue"]
    score = _column_expr(link_cols, "relevance_score", "NULL", alias="rkl")
    status = _column_expr(reply_cols, "status", "'pending'", alias="rq")
    reply_fk = "reply_queue_id" if "reply_queue_id" in link_cols else "reply_id"
    if reply_fk not in link_cols or "knowledge_id" not in link_cols:
        return []
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   rkl.rowid AS link_id,
                   rkl.{reply_fk} AS reply_queue_id,
                   rq.id AS resolved_reply_queue_id,
                   rkl.knowledge_id AS knowledge_id,
                   k.id AS resolved_knowledge_id,
                   {status} AS reply_status,
                   {score} AS relevance_score
               FROM reply_knowledge_links rkl
               LEFT JOIN reply_queue rq ON rq.id = rkl.{reply_fk}
               LEFT JOIN knowledge k ON k.id = rkl.knowledge_id
               ORDER BY rkl.rowid ASC"""
        ).fetchall()
    ]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], column: str, fallback: str, *, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _normalize_status_filter(status: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(status, str):
        values = status.split(",")
    else:
        values = list(status)
    normalized = tuple(value for value in (_clean(item).lower() for item in values) if value)
    return normalized or (DEFAULT_STATUS,)


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        ISSUE_TYPES.index(finding["issue_type"]) if finding["issue_type"] in ISSUE_TYPES else len(ISSUE_TYPES),
        _int_or_text(finding.get("reply_queue_id")),
        _int_or_text(finding.get("knowledge_id")),
        _int_or_text(finding.get("link_id")),
    )


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
