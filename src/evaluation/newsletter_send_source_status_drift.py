"""Audit newsletter send source content status drift."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_WINDOW_DAYS = 30


def build_newsletter_send_source_status_drift_report(
    send_rows: list[dict[str, Any]],
    content_rows: list[dict[str, Any]],
    *,
    status: str = "all",
    window_days: int = DEFAULT_WINDOW_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    content_by_id = {
        content_id: row
        for row in content_rows
        if (content_id := _int_or_none(row.get("content_id") or row.get("id"))) is not None
    }
    drift_items: list[dict[str, Any]] = []

    for send in send_rows:
        send_id = _int_or_none(send.get("newsletter_send_id") or send.get("id"))
        parsed, malformed = _source_ids(send.get("source_content_ids"))
        if malformed is not None:
            drift_items.append(_item(send, "malformed_source_content_ids_json", issue_id=f"{send_id}:malformed", detail=malformed))
            continue
        sent_at = _parse_dt(send.get("sent_at"))
        if _status(send) == "sent":
            counts = Counter(parsed)
            for source_id, count in sorted(counts.items()):
                if count > 1:
                    drift_items.append(_item(send, "duplicate_source_id", source_content_id=source_id, issue_id=f"{send_id}:duplicate:{source_id}"))
        for source_id in parsed:
            content = content_by_id.get(source_id)
            if content is None:
                drift_items.append(_item(send, "missing_generated_content", source_content_id=source_id, issue_id=f"{send_id}:missing:{source_id}"))
                continue
            content_status = _clean(content.get("status")).lower()
            published_at = _parse_dt(content.get("published_at"))
            if content_status == "abandoned":
                drift_items.append(_item(send, "source_content_abandoned", source_content_id=source_id, issue_id=f"{send_id}:abandoned:{source_id}"))
            elif sent_at and not _published_by_send_time(content, sent_at):
                drift_items.append(_item(send, "source_content_unpublished_at_send_time", source_content_id=source_id, issue_id=f"{send_id}:unpublished:{source_id}"))
            elif not sent_at and content_status not in {"published", "sent"} and not published_at:
                drift_items.append(_item(send, "source_content_unpublished_at_send_time", source_content_id=source_id, issue_id=f"{send_id}:unpublished:{source_id}"))

    drift_items.sort(key=_sort_key)
    shown = drift_items[:limit]
    return {
        "artifact_type": "newsletter_send_source_status_drift",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"status": status, "window_days": window_days, "limit": limit},
        "summary": {
            "send_count": len(send_rows),
            "content_count": len(content_rows),
            "drift_count": len(drift_items),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(Counter(item["issue_type"] for item in drift_items).items())),
        },
        "drift_items": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(cols) for table, cols in sorted((missing_columns or {}).items())},
    }


def build_newsletter_send_source_status_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [table for table in ("generated_content", "newsletter_sends") if table not in schema]
    missing_columns: dict[str, list[str]] = {}
    send_rows: list[dict[str, Any]] = []
    content_rows: list[dict[str, Any]] = []
    if "newsletter_sends" in schema:
        send_rows = _load_sends(conn, schema, missing_columns, kwargs.get("status", "all"), kwargs.get("window_days", DEFAULT_WINDOW_DAYS), kwargs.get("now"))
    if "generated_content" in schema:
        content_rows = _load_content(conn, schema, missing_columns)
    return build_newsletter_send_source_status_drift_report(
        send_rows,
        content_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_newsletter_send_source_status_drift_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_send_source_status_drift_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Newsletter Send Source Status Drift",
        f"Generated: {report['generated_at']}",
        f"Status: {report['thresholds']['status']}",
        f"Window: {report['thresholds']['window_days']} days",
        f"Totals: sends={summary['send_count']} drift={summary['drift_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["drift_items"]:
        lines.append("No newsletter source status drift found.")
        return "\n".join(lines)
    lines.extend(["", "newsletter_send_id | issue_id | issue_type | source_content_id | sent_at"])
    for item in report["drift_items"]:
        lines.append(
            f"{item['newsletter_send_id'] or '-'} | {item['issue_id']} | {item['issue_type']} | "
            f"{item['source_content_id'] or '-'} | {item['sent_at'] or '-'}"
        )
    return "\n".join(lines)


def _load_sends(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_columns: dict[str, list[str]],
    status: str,
    window_days: int,
    now: datetime | None,
) -> list[dict[str, Any]]:
    cols = schema["newsletter_sends"]
    if "id" not in cols or "source_content_ids" not in cols:
        missing_columns["newsletter_sends"] = sorted({"id", "source_content_ids"} - cols)
        return []
    select = [
        "id AS newsletter_send_id",
        "source_content_ids AS source_content_ids",
        _expr(cols, "status", "status", "'unknown'"),
        _expr(cols, "sent_at", "sent_at", "NULL"),
    ]
    where: list[str] = []
    params: list[Any] = []
    if status and status.lower() != "all" and "status" in cols:
        where.append("lower(status) = ?")
        params.append(status.lower())
    if "sent_at" in cols:
        cutoff = _utc(now or datetime.now(timezone.utc)) - timedelta(days=window_days)
        where.append("sent_at >= ?")
        params.append(cutoff.isoformat())
    clause = " WHERE " + " AND ".join(where) if where else ""
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM newsletter_sends{clause} ORDER BY rowid ASC", params)]


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]], missing_columns: dict[str, list[str]]) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    if "id" not in cols:
        missing_columns["generated_content"] = ["id"]
        return []
    select = [
        "id AS content_id",
        _expr(cols, "status", "status", "'unknown'"),
        _expr(cols, "published", "published", "0"),
        _expr(cols, "published_at", "published_at", "NULL"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM generated_content ORDER BY id ASC")]


def _item(
    send: dict[str, Any],
    issue_type: str,
    *,
    source_content_id: int | None = None,
    issue_id: str,
    detail: str | None = None,
) -> dict[str, Any]:
    return {
        "newsletter_send_id": _int_or_none(send.get("newsletter_send_id") or send.get("id")),
        "issue_id": issue_id,
        "issue_type": issue_type,
        "source_content_id": source_content_id,
        "status": _status(send),
        "sent_at": _iso_or_none(send.get("sent_at")),
        "detail": detail,
    }


def _source_ids(value: Any) -> tuple[list[int], str | None]:
    if value in (None, ""):
        return [], None
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError as exc:
        return [], str(exc)
    if not isinstance(parsed, list):
        return [], "source_content_ids must be a JSON array"
    ids: list[int] = []
    for raw in parsed:
        source_id = _int_or_none(raw)
        if source_id is not None:
            ids.append(source_id)
    return ids, None


def _published_by_send_time(content: dict[str, Any], sent_at: datetime) -> bool:
    status = _clean(content.get("status")).lower()
    published_at = _parse_dt(content.get("published_at"))
    if published_at:
        return published_at <= sent_at
    return status in {"published", "sent"} or _truthy(content.get("published"))


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["newsletter_send_id"] or 0, item["issue_type"], item["source_content_id"] or 0)


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


def _iso_or_none(value: Any) -> str | None:
    parsed = _parse_dt(value)
    return parsed.isoformat() if parsed else (_clean(value) or None)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _truthy(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes"}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
