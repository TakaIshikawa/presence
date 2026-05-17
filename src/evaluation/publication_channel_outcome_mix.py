"""Summarize publication outcomes by channel over a recent window."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_FAILURE_THRESHOLD = 0.25
DEFAULT_PENDING_THRESHOLD = 0.4
STATUSES = ("success", "failure", "retry", "pending")


def build_publication_channel_outcome_mix_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    failure_threshold: float = DEFAULT_FAILURE_THRESHOLD,
    pending_threshold: float = DEFAULT_PENDING_THRESHOLD,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if not 0 <= failure_threshold <= 1 or not 0 <= pending_threshold <= 1:
        raise ValueError("thresholds must be between 0 and 1")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    grouped: dict[str, Counter[str]] = defaultdict(Counter)
    scanned = 0
    for row in rows:
        timestamp = _parse_ts(_first(row, "timestamp", "published_at", "updated_at", "created_at"))
        if timestamp and timestamp < cutoff:
            continue
        scanned += 1
        channel = _channel(_first(row, "channel", "platform", "content_type"))
        grouped[channel][_status(_first(row, "status", "outcome", "state"))] += 1

    channels = []
    for channel, counts in grouped.items():
        total = sum(counts.values())
        percentages = {status: round(counts.get(status, 0) / total, 4) for status in STATUSES}
        flags = []
        if percentages["failure"] >= failure_threshold:
            flags.append("failure_rate_exceeded")
        if percentages["pending"] >= pending_threshold:
            flags.append("pending_rate_exceeded")
        channels.append(
            {
                "channel": channel,
                "total_count": total,
                "counts": {status: counts.get(status, 0) for status in STATUSES},
                "percentages": percentages,
                "flags": flags,
                "is_flagged": bool(flags),
            }
        )
    channels.sort(key=lambda item: (-int(item["is_flagged"]), -item["percentages"]["failure"], -item["percentages"]["pending"], item["channel"]))
    return {
        "artifact_type": "publication_channel_outcome_mix",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "failure_threshold": failure_threshold, "pending_threshold": pending_threshold},
        "totals": {
            "rows_scanned": scanned,
            "channel_count": len(channels),
            "flagged_channel_count": sum(1 for channel in channels if channel["is_flagged"]),
        },
        "channels": channels,
        "empty_state": {"is_empty": not channels, "message": "No publication outcome rows found." if not channels else None},
    }


def build_publication_channel_outcome_mix_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_publication_channel_outcome_mix_report(_load_rows(conn, _schema(conn)), **kwargs)


def format_publication_channel_outcome_mix_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_channel_outcome_mix_table(report: dict[str, Any]) -> str:
    lines = [
        "Publication Channel Outcome Mix",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days failure_threshold={report['filters']['failure_threshold']} pending_threshold={report['filters']['pending_threshold']}",
        f"Totals: channels={report['totals']['channel_count']} flagged={report['totals']['flagged_channel_count']} rows={report['totals']['rows_scanned']}",
    ]
    if not report["channels"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "channel | total | success% | failure% | retry% | pending% | flags"])
    for channel in report["channels"]:
        pct = channel["percentages"]
        lines.append(
            f"{channel['channel']} | {channel['total_count']} | {pct['success']:.2f} | "
            f"{pct['failure']:.2f} | {pct['retry']:.2f} | {pct['pending']:.2f} | {', '.join(channel['flags']) or '-'}"
        )
    return "\n".join(lines)


format_publication_channel_outcome_mix_text = format_publication_channel_outcome_mix_table


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if "content_publications" in schema:
        cols = schema["content_publications"]
        selected = [
            _col(cols, "platform", "channel", "content_type", default="'unknown'") + " AS channel",
            _col(cols, "status", "outcome", "state", default="'success'") + " AS status",
            _col(cols, "published_at", "updated_at", "created_at", default="NULL") + " AS timestamp",
        ]
        rows.extend(dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM content_publications").fetchall())
    if "publish_queue" in schema:
        cols = schema["publish_queue"]
        selected = [
            _col(cols, "platform", "channel", "content_type", default="'unknown'") + " AS channel",
            _col(cols, "status", "outcome", "state", default="'pending'") + " AS status",
            _col(cols, "updated_at", "created_at", default="NULL") + " AS timestamp",
        ]
        rows.extend(dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM publish_queue").fetchall())
    return rows


def _status(value: Any) -> str:
    text = _text(value).lower()
    if text in {"published", "sent", "success", "succeeded", "ok", "complete", "completed"}:
        return "success"
    if text in {"failed", "failure", "error", "dead_letter", "abandoned"}:
        return "failure"
    if text in {"retry", "retrying", "queued_retry", "scheduled_retry"}:
        return "retry"
    return "pending"


def _channel(value: Any) -> str:
    text = _text(value).lower() or "unknown"
    return {"x": "x", "twitter": "x", "x_post": "x", "blog_post": "blog", "newsletter_send": "newsletter"}.get(text, text)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
