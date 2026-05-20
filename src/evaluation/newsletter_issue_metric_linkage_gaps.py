"""Report newsletter send and engagement metric linkage gaps."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
REQUIRED_COLUMNS = {
    "newsletter_sends": {"id", "issue_id", "sent_at"},
    "newsletter_engagement": {"id", "newsletter_send_id", "issue_id", "fetched_at"},
}


def build_newsletter_issue_metric_linkage_gaps_report(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a report of newsletter engagement metric linkage gaps."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _report(generated_at, limit, [], 0, missing_tables, missing_columns)

    sends = [dict(row) for row in conn.execute("SELECT id AS send_id, issue_id, sent_at FROM newsletter_sends ORDER BY sent_at ASC, id ASC")]
    metrics = [
        dict(row)
        for row in conn.execute(
            """SELECT id AS metric_id,
                      newsletter_send_id AS send_id,
                      issue_id,
                      fetched_at
               FROM newsletter_engagement
               ORDER BY fetched_at ASC, id ASC"""
        )
    ]
    items = _find_gaps(sends, metrics)
    items.sort(key=lambda item: (_gap_rank(item["gap_type"]), _clean(item["sent_at"]), _clean(item["fetched_at"]), _int_or_text(item["send_id"]), _int_or_text(item["metric_id"])))
    return _report(generated_at, limit, items, len(sends), [], {})


def format_newsletter_issue_metric_linkage_gaps_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_issue_metric_linkage_gaps_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    totals = report["summary"]
    lines = [
        "Newsletter Issue Metric Linkage Gaps",
        f"Generated: {report['generated_at']}",
        f"Limit: {report['filters']['limit']}",
        f"Totals: sends={totals['send_count']} gaps={totals['gap_count']} shown={totals['shown_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "send_id | metric_id | issue_id | sent_at | fetched_at | gap_type"])
    for item in report["items"]:
        lines.append(
            f"{item['send_id'] if item['send_id'] is not None else '-'} | "
            f"{item['metric_id'] if item['metric_id'] is not None else '-'} | "
            f"{item['issue_id'] or '-'} | {item['sent_at'] or '-'} | "
            f"{item['fetched_at'] or '-'} | {item['gap_type']}"
        )
    return "\n".join(lines)


def _find_gaps(sends: list[dict[str, Any]], metrics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    send_by_id = {send["send_id"]: send for send in sends}
    sends_by_issue: dict[str, list[dict[str, Any]]] = {}
    for send in sends:
        issue_id = _clean(send.get("issue_id"))
        if issue_id:
            sends_by_issue.setdefault(issue_id, []).append(send)

    metrics_by_send: dict[Any, list[dict[str, Any]]] = {}
    metrics_by_issue: dict[str, list[dict[str, Any]]] = {}
    snapshot_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for metric in metrics:
        if metric.get("send_id") is not None:
            metrics_by_send.setdefault(metric["send_id"], []).append(metric)
        issue_id = _clean(metric.get("issue_id"))
        if issue_id:
            metrics_by_issue.setdefault(issue_id, []).append(metric)
            snapshot_groups.setdefault((issue_id, _clean(metric.get("fetched_at"))), []).append(metric)

    items: list[dict[str, Any]] = []
    for send in sends:
        issue_id = _clean(send.get("issue_id"))
        if not metrics_by_send.get(send["send_id"]) and not (issue_id and metrics_by_issue.get(issue_id)):
            items.append(_item(send, None, issue_id, "missing_metrics"))

    for metric in metrics:
        send = send_by_id.get(metric.get("send_id"))
        if metric.get("send_id") is None or send is None:
            issue_send = _single_send_for_issue(sends_by_issue, metric.get("issue_id"))
            items.append(_item(issue_send, metric, metric.get("issue_id"), "orphan_metric"))
            continue
        metric_issue = _clean(metric.get("issue_id"))
        send_issue = _clean(send.get("issue_id"))
        if metric_issue and send_issue and metric_issue != send_issue:
            items.append(_item(send, metric, metric_issue, "issue_id_mismatch"))

    for (_issue_id, fetched_at), group in snapshot_groups.items():
        if fetched_at and len(group) > 1:
            for metric in group:
                send = send_by_id.get(metric.get("send_id")) or _single_send_for_issue(sends_by_issue, metric.get("issue_id"))
                items.append(_item(send, metric, metric.get("issue_id"), "duplicate_metric_snapshot"))
    return items


def _item(send: dict[str, Any] | None, metric: dict[str, Any] | None, issue_id: Any, gap_type: str) -> dict[str, Any]:
    return {
        "send_id": (send or {}).get("send_id") if send is not None else (metric or {}).get("send_id"),
        "metric_id": (metric or {}).get("metric_id"),
        "issue_id": issue_id,
        "sent_at": (send or {}).get("sent_at"),
        "fetched_at": (metric or {}).get("fetched_at"),
        "gap_type": gap_type,
    }


def _single_send_for_issue(sends_by_issue: dict[str, list[dict[str, Any]]], issue_id: Any) -> dict[str, Any] | None:
    matches = sends_by_issue.get(_clean(issue_id), [])
    return matches[0] if len(matches) == 1 else None


def _report(
    generated_at: datetime,
    limit: int,
    items: list[dict[str, Any]],
    send_count: int,
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    shown = items[:limit]
    return {
        "artifact_type": "newsletter_issue_metric_linkage_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "summary": {
            "send_count": send_count,
            "gap_count": len(items),
            "shown_count": len(shown),
            "by_gap_type": _counts(items, "gap_type"),
        },
        "items": shown,
        "missing_tables": sorted(missing_tables),
        "missing_columns": {table: sorted(columns) for table, columns in sorted(missing_columns.items())},
        "empty_state": {
            "is_empty": not items,
            "message": "No newsletter issue metric linkage gaps found." if not items else None,
        },
    }


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    missing_tables = [table for table in REQUIRED_COLUMNS if table not in schema]
    missing_columns = {
        table: sorted(columns - schema[table])
        for table, columns in REQUIRED_COLUMNS.items()
        if table in schema and columns - schema[table]
    }
    return missing_tables, missing_columns


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _counts(items: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in items:
        value = _clean(item.get(key))
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items()))


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _gap_rank(gap_type: str) -> int:
    return {
        "missing_metrics": 0,
        "orphan_metric": 1,
        "issue_id_mismatch": 2,
        "duplicate_metric_snapshot": 3,
    }.get(gap_type, 99)


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_text(value: Any) -> tuple[int, Any]:
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, "" if value is None else str(value))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
