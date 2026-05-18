"""Validate newsletter source_content_ids against generated content."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
DEFAULT_STATUS = "sent"
ISSUE_TYPES = (
    "malformed_source_content_ids",
    "missing_generated_content",
    "duplicate_source_content_id",
    "abandoned_source_content",
    "unpublished_source_content",
)


def build_newsletter_source_content_link_gaps_report(
    newsletter_send_rows: list[dict[str, Any]],
    generated_content_rows: list[dict[str, Any]] | None = None,
    *,
    days: int = DEFAULT_DAYS,
    status: str = DEFAULT_STATUS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    normalized_status = _clean(status).lower() or DEFAULT_STATUS
    generated_at = _utc(now or datetime.now(timezone.utc))
    content_by_id = {_int(row.get("id")): row for row in generated_content_rows or [] if _int(row.get("id")) is not None}
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    referenced_count = 0

    for send in newsletter_send_rows:
        send_status = _clean(_first(send, "status", "send_status")).lower() or "sent"
        if normalized_status != "all" and send_status != normalized_status:
            continue
        send_id = _first(send, "send_id", "id")
        parsed, parse_findings = _parse_source_ids(send)
        findings.extend(parse_findings)
        for finding in parse_findings:
            counts[finding["issue_type"]] += 1
        referenced_count += len(parsed)

        seen: set[int] = set()
        duplicate_reported: set[int] = set()
        for position, content_id in enumerate(parsed):
            if content_id in seen and content_id not in duplicate_reported:
                duplicate_reported.add(content_id)
                finding = _finding(
                    send,
                    "duplicate_source_content_id",
                    f"source_content_id {content_id} appears more than once in this send",
                    source_content_id=content_id,
                    position=position,
                )
                findings.append(finding)
                counts[finding["issue_type"]] += 1
            seen.add(content_id)

            content = content_by_id.get(content_id)
            if content is None:
                finding = _finding(
                    send,
                    "missing_generated_content",
                    f"source_content_id {content_id} does not exist in generated_content",
                    source_content_id=content_id,
                    position=position,
                )
                findings.append(finding)
                counts[finding["issue_type"]] += 1
                continue
            if send_status != "sent":
                continue
            published = _int(content.get("published"))
            content_status = _clean(content.get("status")).lower()
            if published == -1 or content_status in {"abandoned", "rejected"}:
                finding = _finding(
                    send,
                    "abandoned_source_content",
                    f"source_content_id {content_id} is abandoned while newsletter send is sent",
                    source_content_id=content_id,
                    position=position,
                )
                findings.append(finding)
                counts[finding["issue_type"]] += 1
            elif published in (None, 0) or content_status in {"draft", "unpublished", "pending"}:
                finding = _finding(
                    send,
                    "unpublished_source_content",
                    f"source_content_id {content_id} is unpublished while newsletter send is sent",
                    source_content_id=content_id,
                    position=position,
                )
                findings.append(finding)
                counts[finding["issue_type"]] += 1

    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": "newsletter_source_content_link_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "status": normalized_status, "limit": limit},
        "summary": {
            "send_count": len([row for row in newsletter_send_rows if normalized_status == "all" or (_clean(_first(row, "status", "send_status")).lower() or "sent") == normalized_status]),
            "referenced_source_count": referenced_count,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
        },
        "findings": shown,
        "empty_state": {
            "is_empty": not findings,
            "message": "No newsletter source content link gaps found." if not findings else None,
        },
    }


def build_newsletter_source_content_link_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if not {"newsletter_sends", "generated_content"}.issubset(schema):
        return build_newsletter_source_content_link_gaps_report([], [], **kwargs)
    days = int(kwargs.get("days", DEFAULT_DAYS))
    now = kwargs.get("now")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    return build_newsletter_source_content_link_gaps_report(
        _load_sends(conn, schema, cutoff=cutoff, status=kwargs.get("status", DEFAULT_STATUS)),
        _load_generated_content(conn, schema),
        **kwargs,
    )


def format_newsletter_source_content_link_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_source_content_link_gaps_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Newsletter Source Content Link Gaps",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days",
        f"Status: {report['filters']['status']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: sends={summary['send_count']} sources={summary['referenced_source_count']} findings={summary['finding_count']}",
        "Issue counts: "
        + ", ".join(f"{issue_type}={summary['by_issue_type'].get(issue_type, 0)}" for issue_type in ISSUE_TYPES),
    ]
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "send_id | issue_id | status | source_content_id | position | issue_type | message"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['send_id']} | {finding['issue_id'] or '-'} | {finding['status'] or '-'} | "
            f"{_display(finding['source_content_id'])} | {_display(finding['position'])} | "
            f"{finding['issue_type']} | {finding['message']}"
        )
    return "\n".join(lines)


def _parse_source_ids(send: dict[str, Any]) -> tuple[list[int], list[dict[str, Any]]]:
    raw_value = _first(send, "source_content_ids", "raw_source_content_ids")
    try:
        parsed = json.loads(raw_value) if isinstance(raw_value, str) else raw_value
    except (TypeError, json.JSONDecodeError) as exc:
        return [], [
            _finding(
                send,
                "malformed_source_content_ids",
                f"source_content_ids is not valid JSON: {exc}",
                raw_value=raw_value,
            )
        ]
    if not isinstance(parsed, list):
        return [], [
            _finding(
                send,
                "malformed_source_content_ids",
                f"source_content_ids must be a JSON array, got {type(parsed).__name__}",
                raw_value=raw_value,
            )
        ]

    source_ids: list[int] = []
    findings: list[dict[str, Any]] = []
    for position, item in enumerate(parsed):
        content_id = _int(item)
        if content_id is None or content_id <= 0:
            findings.append(
                _finding(
                    send,
                    "malformed_source_content_ids",
                    "source_content_ids must contain only positive integers",
                    position=position,
                    raw_value=item,
                )
            )
            continue
        source_ids.append(content_id)
    return source_ids, findings


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    status: str,
) -> list[dict[str, Any]]:
    columns = schema["newsletter_sends"]
    filters = []
    params: list[Any] = []
    sent_at = _column_expr(columns, "sent_at", "created_at", "published_at", fallback="NULL", alias="ns")
    if sent_at != "NULL":
        filters.append(f"{sent_at} >= ?")
        params.append(cutoff.isoformat())
    normalized_status = _clean(status).lower() or DEFAULT_STATUS
    status_expr = _column_expr(columns, "status", fallback="'sent'", alias="ns")
    if normalized_status != "all" and "status" in columns:
        filters.append("lower(ns.status) = ?")
        params.append(normalized_status)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    source_ids = _column_expr(columns, "source_content_ids", fallback="NULL", alias="ns")
    rows = conn.execute(
        f"""SELECT
               ns.id AS send_id,
               {_column_expr(columns, "issue_id", "newsletter_issue_id", fallback="''", alias="ns")} AS issue_id,
               {status_expr} AS status,
               {sent_at} AS sent_at,
               {source_ids} AS source_content_ids
           FROM newsletter_sends ns
           {where}
           ORDER BY {sent_at} DESC, ns.id DESC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _load_generated_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema["generated_content"]
    rows = conn.execute(
        f"""SELECT
               gc.id AS id,
               {_column_expr(columns, "published", fallback="NULL", alias="gc")} AS published,
               {_column_expr(columns, "status", fallback="NULL", alias="gc")} AS status
           FROM generated_content gc"""
    ).fetchall()
    return [dict(row) for row in rows]


def _finding(
    send: dict[str, Any],
    issue_type: str,
    message: str,
    *,
    source_content_id: int | None = None,
    position: int | None = None,
    raw_value: Any | None = None,
) -> dict[str, Any]:
    return {
        "send_id": _first(send, "send_id", "id"),
        "issue_id": _first(send, "issue_id", "newsletter_issue_id"),
        "status": _clean(_first(send, "status", "send_status")) or "sent",
        "sent_at": _first(send, "sent_at", "created_at", "published_at"),
        "source_content_id": source_content_id,
        "position": position,
        "raw_value": raw_value,
        "issue_type": issue_type,
        "message": message,
    }


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (
        finding.get("send_id") or 0,
        ISSUE_TYPES.index(finding["issue_type"]) if finding["issue_type"] in ISSUE_TYPES else len(ISSUE_TYPES),
        -1 if finding.get("position") is None else finding["position"],
        finding.get("source_content_id") or 0,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], *names: str, fallback: str, alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{alias}.{name}"
    return fallback


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _display(value: Any) -> str:
    text = _clean(value)
    return text if text else "-"


def _int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
