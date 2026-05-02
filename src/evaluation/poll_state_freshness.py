"""Freshness report for ingestion poll cursors."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any


DEFAULT_WARNING_HOURS = 6.0
DEFAULT_STALE_HOURS = 24.0
STATUSES = ("healthy", "warning", "stale")


def build_poll_state_freshness_report(
    db_or_conn: Any,
    *,
    warning_hours: float = DEFAULT_WARNING_HOURS,
    stale_hours: float = DEFAULT_STALE_HOURS,
    sources: list[str] | tuple[str, ...] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return poll_state rows classified by cursor freshness."""

    if warning_hours < 0:
        raise ValueError("warning_hours must be non-negative")
    if stale_hours < 0:
        raise ValueError("stale_hours must be non-negative")
    if warning_hours > stale_hours:
        raise ValueError("warning_hours must be less than or equal to stale_hours")

    conn = _connection(db_or_conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    source_filters = tuple(dict.fromkeys(_clean(source) for source in sources or () if _clean(source)))
    schema = _schema(conn)
    columns = schema.get("poll_state", set())
    missing_tables = [] if "poll_state" in schema else ["poll_state"]

    rows = _load_poll_rows(conn, columns) if columns else []
    if source_filters:
        wanted = set(source_filters)
        rows = [row for row in rows if row["source"] in wanted]

    classified = [
        _classify_row(
            row,
            warning_hours=warning_hours,
            stale_hours=stale_hours,
            now=generated_at,
        )
        for row in rows
    ]
    classified.sort(key=lambda row: (STATUSES.index(row["status"]), -(row["age_hours"] or -1), row["source"]))

    counts = {status: 0 for status in STATUSES}
    for row in classified:
        counts[row["status"]] += 1

    return {
        "artifact_type": "poll_state_freshness",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "warning_hours": warning_hours,
            "stale_hours": stale_hours,
            "sources": list(source_filters),
        },
        "counts": {
            "pollers_scanned": len(rows),
            "by_status": counts,
        },
        "missing_tables": missing_tables,
        "missing_columns": {},
        "rows": classified,
    }


def format_poll_state_freshness_json(report: dict[str, Any]) -> str:
    """Render the poll-state freshness report as deterministic JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_poll_state_freshness_text(report: dict[str, Any]) -> str:
    """Render the poll-state freshness report as concise terminal text."""

    filters = report["filters"]
    counts = report["counts"]
    by_status = counts["by_status"]
    sources = ", ".join(filters["sources"]) if filters["sources"] else "all"
    lines = [
        "Poll State Freshness",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: warning_hours={filters['warning_hours']:g} "
            f"stale_hours={filters['stale_hours']:g} sources={sources}"
        ),
        (
            f"Rows: scanned={counts['pollers_scanned']} "
            f"healthy={by_status.get('healthy', 0)} "
            f"warning={by_status.get('warning', 0)} "
            f"stale={by_status.get('stale', 0)}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))

    if not report["rows"]:
        lines.append("")
        lines.append("No poll_state rows found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Pollers:")
    for row in report["rows"]:
        age = "n/a" if row["age_hours"] is None else f"{row['age_hours']:g}h"
        lines.append(
            f"  - source={row['source']} status={row['status']} age={age} "
            f"cursor={row['cursor_summary']} action={row['recommended_action']}"
        )
        if row["reason"]:
            lines.append(f"    reason={row['reason']}")
    return "\n".join(lines)


def _load_poll_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    source_expr = _first_column_expr(
        columns,
        ("source", "source_name", "poller", "poller_name", "source_type"),
        "'poll_state'",
    )
    timestamp_expr = _first_column_expr(
        columns,
        ("last_success_at", "updated_at", "last_poll_time"),
        "NULL",
    )
    cursor_expr = _first_column_expr(
        columns,
        ("last_cursor", "cursor", "cursor_value", "last_seen_id", "last_poll_time"),
        "NULL",
    )
    select = [
        _column_expr(columns, "id", "rowid"),
        f"{source_expr} AS source",
        f"{timestamp_expr} AS freshness_timestamp",
        f"{cursor_expr} AS cursor_value",
    ]
    rows = conn.execute(
        f"""SELECT {", ".join(select)}
            FROM poll_state
            ORDER BY source ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _classify_row(
    row: dict[str, Any],
    *,
    warning_hours: float,
    stale_hours: float,
    now: datetime,
) -> dict[str, Any]:
    source = _clean(row.get("source")) or "poll_state"
    timestamp_raw = _clean(row.get("freshness_timestamp"))
    timestamp = _parse_datetime(timestamp_raw)
    cursor_summary = _cursor_summary(row.get("cursor_value"))

    if timestamp is None:
        return {
            "source": source,
            "status": "stale",
            "age_hours": None,
            "freshness_timestamp": timestamp_raw,
            "cursor_summary": cursor_summary,
            "reason": "missing freshness timestamp",
            "recommended_action": "Run or repair the poller; no freshness timestamp is recorded.",
        }

    age_hours = max(0.0, (now - timestamp).total_seconds() / 3600)
    if age_hours >= stale_hours:
        status = "stale"
        reason = f"age {age_hours:.2f}h >= stale threshold {stale_hours:g}h"
        action = "Run the poller and inspect ingestion logs if the cursor does not advance."
    elif age_hours >= warning_hours:
        status = "warning"
        reason = f"age {age_hours:.2f}h >= warning threshold {warning_hours:g}h"
        action = "Watch the next scheduled poll and verify the cursor advances."
    else:
        status = "healthy"
        reason = ""
        action = "No action needed."

    return {
        "source": source,
        "status": status,
        "age_hours": round(age_hours, 2),
        "freshness_timestamp": timestamp.isoformat(),
        "cursor_summary": cursor_summary,
        "reason": reason,
        "recommended_action": action,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info["name"] if isinstance(info, sqlite3.Row) else info[1])
            for info in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
    return schema


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    return column if column in columns else f"{fallback} AS {column}"


def _first_column_expr(columns: set[str], choices: tuple[str, ...], fallback: str) -> str:
    for column in choices:
        if column in columns:
            return column
    return fallback


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _ensure_utc(datetime.fromisoformat(text))
    except ValueError:
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _cursor_summary(value: Any) -> str:
    cleaned = _clean(value)
    if not cleaned:
        return "none"
    if len(cleaned) <= 48:
        return cleaned
    return cleaned[:45] + "..."


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
