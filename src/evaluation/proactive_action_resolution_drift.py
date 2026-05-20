"""Report proactive action resolution drift."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_SLA_HOURS = 24
DEFAULT_RELEVANCE_THRESHOLD = 0.8


def build_proactive_action_resolution_drift_report(
    rows: list[dict[str, Any]],
    *,
    sla_hours: int = DEFAULT_SLA_HOURS,
    relevance_threshold: float = DEFAULT_RELEVANCE_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if sla_hours <= 0:
        raise ValueError("sla_hours must be positive")
    if relevance_threshold < 0:
        raise ValueError("relevance_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(hours=sla_hours)
    items: list[dict[str, Any]] = []
    for row in rows:
        status = _status(row)
        if status == "posted":
            if not _clean(row.get("posted_tweet_id")):
                items.append(_item(row, "posted_missing_posted_tweet_id", generated_at))
            if not _parse_dt(row.get("posted_at")):
                items.append(_item(row, "posted_missing_posted_at", generated_at))
        if status == "approved":
            approved_at = _parse_dt(row.get("approved_at") or row.get("updated_at") or row.get("created_at"))
            if approved_at and approved_at < cutoff and not _has_posted_marker(row):
                items.append(_item(row, "approved_stale_without_posting", generated_at))
        if status == "pending" and _clean(row.get("draft_text")) and (_float_or_none(row.get("relevance_score")) or 0.0) >= relevance_threshold:
            if not _has_review(row):
                items.append(_item(row, "pending_strong_relevance_without_review", generated_at))
        if _clean(row.get("knowledge_ids")):
            try:
                parsed = json.loads(str(row.get("knowledge_ids")))
            except json.JSONDecodeError:
                items.append(_item(row, "malformed_knowledge_ids_json", generated_at))
            else:
                if not isinstance(parsed, list):
                    items.append(_item(row, "malformed_knowledge_ids_json", generated_at))

    items.extend(_duplicate_items(rows, generated_at))
    items.sort(key=_sort_key)
    shown = items[:limit]
    return {
        "artifact_type": "proactive_action_resolution_drift",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"sla_hours": sla_hours, "relevance_threshold": relevance_threshold, "limit": limit},
        "summary": {
            "rows_scanned": len(rows),
            "drift_count": len(items),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in items).items())),
        },
        "drift_items": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_proactive_action_resolution_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [] if "proactive_actions" in schema else ["proactive_actions"]
    missing_columns: dict[str, list[str]] = {}
    rows = _load_rows(conn, schema, missing_columns) if "proactive_actions" in schema else []
    return build_proactive_action_resolution_drift_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_proactive_action_resolution_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_action_resolution_drift_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Proactive Action Resolution Drift",
        f"Generated: {report['generated_at']}",
        f"SLA: {report['thresholds']['sla_hours']} hours",
        f"Relevance threshold: {report['thresholds']['relevance_threshold']}",
        f"Totals: scanned={summary['rows_scanned']} drift={summary['drift_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["drift_items"]:
        lines.append("No proactive action resolution drift found.")
        return "\n".join(lines)
    lines.extend(["", "proactive_action_id | action_type | target_author_handle | issue_type | age_hours"])
    for item in report["drift_items"]:
        lines.append(
            f"{item['proactive_action_id'] or '-'} | {item['action_type']} | {item['target_author_handle'] or '-'} | "
            f"{item['issue_type']} | {item['age_hours'] if item['age_hours'] is not None else '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]], missing_columns: dict[str, list[str]]) -> list[dict[str, Any]]:
    cols = schema["proactive_actions"]
    if "id" not in cols:
        missing_columns["proactive_actions"] = ["id"]
        return []
    wanted = [
        "id",
        "status",
        "action_type",
        "target_author_handle",
        "target_tweet_id",
        "posted_tweet_id",
        "posted_at",
        "approved_at",
        "updated_at",
        "created_at",
        "draft_text",
        "relevance_score",
        "reviewed_at",
        "review_status",
        "reviewer_id",
        "knowledge_ids",
    ]
    select = ["id AS proactive_action_id"] + [_expr(cols, col, col, "NULL") for col in wanted if col != "id"]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM proactive_actions ORDER BY rowid ASC")]


def _duplicate_items(rows: list[dict[str, Any]], now: datetime) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[(_clean(row.get("action_type")).lower(), _clean(row.get("target_author_handle")).lower())].append(row)
    items: list[dict[str, Any]] = []
    for grouped in groups.values():
        has_blank = any(not _clean(row.get("target_tweet_id")) for row in grouped)
        has_nonblank = any(_clean(row.get("target_tweet_id")) for row in grouped)
        if has_blank and has_nonblank:
            for row in grouped:
                if not _clean(row.get("target_tweet_id")):
                    items.append(_item(row, "duplicate_blank_target_tweet_id", now))
    return items


def _item(row: dict[str, Any], issue_type: str, now: datetime) -> dict[str, Any]:
    basis = _parse_dt(row.get("approved_at") or row.get("updated_at") or row.get("created_at"))
    return {
        "proactive_action_id": _int_or_none(row.get("proactive_action_id") or row.get("id")),
        "action_type": _clean(row.get("action_type"), "unknown").lower(),
        "target_author_handle": _clean(row.get("target_author_handle")) or None,
        "target_tweet_id": _clean(row.get("target_tweet_id")) or None,
        "issue_type": issue_type,
        "status": _status(row),
        "age_hours": round((now - basis).total_seconds() / 3600, 2) if basis else None,
    }


def _has_posted_marker(row: dict[str, Any]) -> bool:
    return bool(_clean(row.get("posted_tweet_id")) or _parse_dt(row.get("posted_at")))


def _has_review(row: dict[str, Any]) -> bool:
    return bool(_parse_dt(row.get("reviewed_at")) or _clean(row.get("review_status")) or _clean(row.get("reviewer_id")))


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["proactive_action_id"] or 0, item["issue_type"])


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _status(row: dict[str, Any]) -> str:
    return _clean(row.get("status"), "unknown").lower()


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
