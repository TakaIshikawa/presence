"""Report API rate-limit endpoints that remain starved across snapshots."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_LOW_REMAINING = 5
DEFAULT_LOOKBACK_MINUTES = 240
DEFAULT_MIN_SNAPSHOTS = 2
DEFAULT_RESET_GRACE_MINUTES = 5


def build_api_rate_limit_endpoint_starvation_report(
    rows: list[dict[str, Any]],
    *,
    low_remaining: int = DEFAULT_LOW_REMAINING,
    lookback_minutes: int = DEFAULT_LOOKBACK_MINUTES,
    min_snapshots: int = DEFAULT_MIN_SNAPSHOTS,
    reset_grace_minutes: int = DEFAULT_RESET_GRACE_MINUTES,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic endpoint starvation report from snapshot rows."""
    if low_remaining < 0:
        raise ValueError("low_remaining must be non-negative")
    if lookback_minutes <= 0:
        raise ValueError("lookback_minutes must be positive")
    if min_snapshots <= 0:
        raise ValueError("min_snapshots must be positive")
    if reset_grace_minutes <= 0:
        raise ValueError("reset_grace_minutes must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(minutes=lookback_minutes)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        fetched_at = _parse_timestamp(row.get("fetched_at"))
        if fetched_at is None or fetched_at < cutoff:
            continue
        grouped[(_text(row.get("provider"), "unknown"), _text(row.get("endpoint"), "default"))].append({**row, "fetched_at_dt": fetched_at})

    issue_items: list[dict[str, Any]] = []
    for (provider, endpoint), snapshots in grouped.items():
        snapshots.sort(key=lambda row: row["fetched_at_dt"])
        remaining_values = [_int(row.get("remaining"), default=0) for row in snapshots]
        latest = snapshots[-1]
        latest_remaining = remaining_values[-1]
        reset_at = _parse_timestamp(latest.get("reset_at"))
        base = {
            "provider": provider,
            "endpoint": endpoint,
            "first_seen_at": snapshots[0]["fetched_at_dt"].isoformat(),
            "last_seen_at": latest["fetched_at_dt"].isoformat(),
            "min_remaining": min(remaining_values),
            "latest_remaining": latest_remaining,
            "reset_at": reset_at.isoformat() if reset_at else None,
            "snapshot_count": len(snapshots),
        }
        if len(snapshots) >= min_snapshots and all(value <= low_remaining for value in remaining_values):
            issue_items.append({**base, "issue_type": "sustained_low_remaining"})
        if reset_at and reset_at <= generated_at - timedelta(minutes=reset_grace_minutes) and latest_remaining <= low_remaining:
            issue_items.append({**base, "issue_type": "reset_without_recovery"})

    issue_items.sort(key=lambda item: (_issue_order(item["issue_type"]), item["provider"], item["endpoint"]))
    shown = issue_items[:limit]
    counts = Counter(item["issue_type"] for item in issue_items)
    return {
        "artifact_type": "api_rate_limit_endpoint_starvation",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "limit": limit,
            "lookback_minutes": lookback_minutes,
            "low_remaining": low_remaining,
            "min_snapshots": min_snapshots,
            "reset_grace_minutes": reset_grace_minutes,
        },
        "summary": {
            "provider_endpoint_count": len(grouped),
            "snapshot_count": sum(len(values) for values in grouped.values()),
            "issue_count": len(issue_items),
            "shown_count": len(shown),
            "by_issue_type": dict(sorted(counts.items())),
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "issue_items": shown,
        "empty_state": {"is_empty": not issue_items, "message": "No API rate-limit endpoint starvation issues found." if not issue_items else None},
    }


def build_api_rate_limit_endpoint_starvation_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load API rate-limit snapshots from SQLite and build the report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "api_rate_limit_snapshots" not in schema:
        return build_api_rate_limit_endpoint_starvation_report([], missing_tables=["api_rate_limit_snapshots"], **kwargs)
    columns = schema["api_rate_limit_snapshots"]
    required = {"provider", "endpoint", "remaining", "fetched_at"}
    missing_required = sorted(required - columns)
    if missing_required:
        return build_api_rate_limit_endpoint_starvation_report([], missing_columns={"api_rate_limit_snapshots": missing_required}, **kwargs)
    return build_api_rate_limit_endpoint_starvation_report(_load_rows(conn, columns), **kwargs)


def format_api_rate_limit_endpoint_starvation_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_api_rate_limit_endpoint_starvation_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    thresholds = report["thresholds"]
    lines = [
        "API Rate-limit Endpoint Starvation",
        f"Generated: {report['generated_at']}",
        (
            "Thresholds: "
            f"low_remaining={thresholds['low_remaining']} "
            f"lookback_minutes={thresholds['lookback_minutes']} "
            f"min_snapshots={thresholds['min_snapshots']} "
            f"reset_grace_minutes={thresholds['reset_grace_minutes']} "
            f"limit={thresholds['limit']}"
        ),
        (
            "Totals: "
            f"provider_endpoints={summary['provider_endpoint_count']} "
            f"snapshots={summary['snapshot_count']} "
            f"issues={summary['issue_count']} "
            f"shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + ", ".join(f"{table}.{column}" for table, columns in report["missing_columns"].items() for column in columns))
    if not report["issue_items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Issues:")
    for item in report["issue_items"]:
        lines.append(
            "  - "
            f"{item['issue_type']} {item['provider']}:{item['endpoint']} "
            f"latest_remaining={item['latest_remaining']} "
            f"min_remaining={item['min_remaining']} "
            f"window={item['first_seen_at']}..{item['last_seen_at']} "
            f"reset_at={item['reset_at'] or '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        "provider",
        "endpoint",
        "remaining",
        _column_expr(columns, "limit_value", "NULL") + " AS limit_value",
        _column_expr(columns, "reset_at", "NULL") + " AS reset_at",
        "fetched_at",
    ]
    id_order = _column_expr(columns, "id", "rowid")
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM api_rate_limit_snapshots
            ORDER BY provider ASC, endpoint ASC, datetime(fetched_at) ASC, {id_order} ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _text(value: Any, default: str) -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _column_expr(columns: set[str], name: str, fallback: str) -> str:
    return name if name in columns else fallback


def _issue_order(issue_type: str) -> int:
    return {"sustained_low_remaining": 0, "reset_without_recovery": 1}.get(issue_type, 99)
