"""Detect short-window bursts of publication attempt failures."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any

from output.publish_errors import normalize_error_category


DEFAULT_HOURS = 1
DEFAULT_MIN_FAILURES = 3
DEFAULT_MIN_CONSECUTIVE = 2
PLATFORMS = ("x", "bluesky")
VALID_PLATFORMS = {"all", *PLATFORMS}


def build_publication_error_burst_report(
    db_or_conn: Any,
    *,
    hours: int = DEFAULT_HOURS,
    min_failures: int = DEFAULT_MIN_FAILURES,
    min_consecutive: int = DEFAULT_MIN_CONSECUTIVE,
    platform: str = "all",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return publication failure groups whose window or streak thresholds are met."""
    if hours <= 0:
        raise ValueError("hours must be positive")
    if min_failures <= 0:
        raise ValueError("min_failures must be positive")
    if min_consecutive <= 0:
        raise ValueError("min_consecutive must be positive")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")

    conn = _connection(db_or_conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff_dt = generated_at - timedelta(hours=hours)
    cutoff = cutoff_dt.isoformat()
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    filters = {
        "hours": hours,
        "cutoff": cutoff,
        "min_consecutive": min_consecutive,
        "min_failures": min_failures,
        "platform": platform,
    }
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at,
            filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    attempts = _load_attempts(conn, cutoff=cutoff, platform=platform)
    aggregate: dict[tuple[str, str], dict[str, Any]] = {}
    streaks: dict[tuple[str, str], dict[str, Any]] = {}
    active_keys_by_platform: dict[str, set[tuple[str, str]]] = {}

    for attempt in attempts:
        item_platform = str(attempt["platform"])
        if bool(attempt["success"]):
            active_keys_by_platform[item_platform] = set()
            continue

        category = normalize_error_category(attempt.get("error_category"))
        key = (item_platform, category)
        bucket = aggregate.setdefault(key, _empty_bucket(item_platform, category))
        bucket["count"] += 1
        bucket["first_seen"] = bucket["first_seen"] or attempt["attempted_at"]
        bucket["last_seen"] = attempt["attempted_at"]

        active = active_keys_by_platform.setdefault(item_platform, set())
        if key not in active:
            streaks[key] = {
                "count": 0,
                "first_seen": attempt["attempted_at"],
                "last_seen": None,
            }
            active.add(key)
        streak = streaks[key]
        streak["count"] += 1
        streak["last_seen"] = attempt["attempted_at"]
        if streak["count"] > bucket["max_consecutive_failures"]:
            bucket["max_consecutive_failures"] = streak["count"]
            bucket["consecutive_first_seen"] = streak["first_seen"]
            bucket["consecutive_last_seen"] = streak["last_seen"]

    bursts = [
        _finalize_bucket(bucket, min_failures, min_consecutive)
        for bucket in aggregate.values()
        if bucket["count"] >= min_failures
        or bucket["max_consecutive_failures"] >= min_consecutive
    ]
    bursts.sort(key=lambda item: (item["platform"], item["error_category"]))
    totals_by_platform: dict[str, int] = {}
    totals_by_category: dict[str, int] = {}
    for bucket in aggregate.values():
        totals_by_platform[bucket["platform"]] = totals_by_platform.get(bucket["platform"], 0) + bucket["count"]
        totals_by_category[bucket["error_category"]] = (
            totals_by_category.get(bucket["error_category"], 0) + bucket["count"]
        )

    return {
        "artifact_type": "publication_error_bursts",
        "bursts": bursts,
        "filters": filters,
        "generated_at": generated_at.isoformat(),
        "has_bursts": bool(bursts),
        "missing_columns": {},
        "missing_tables": [],
        "totals": {
            "attempts": len(attempts),
            "burst_count": len(bursts),
            "failed_attempts": sum(bucket["count"] for bucket in aggregate.values()),
            "by_error_category": dict(sorted(totals_by_category.items())),
            "by_platform": dict(sorted(totals_by_platform.items())),
        },
    }


def format_publication_error_burst_json(report: dict[str, Any]) -> str:
    """Render a publication error burst report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_error_burst_text(report: dict[str, Any]) -> str:
    """Render publication error bursts for terminal output."""
    lines = [
        "Publication Error Bursts",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['hours']} hours",
        f"Platform: {report['filters']['platform']}",
        (
            "Thresholds: "
            f"min_failures={report['filters']['min_failures']} "
            f"min_consecutive={report['filters']['min_consecutive']}"
        ),
        f"Bursts: {report['totals']['burst_count']}",
    ]
    if report["missing_tables"]:
        lines.append(f"Missing tables: {', '.join(report['missing_tables'])}")
    missing = [
        f"{table}({', '.join(columns)})"
        for table, columns in report["missing_columns"].items()
        if columns
    ]
    if missing:
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report["bursts"]:
        lines.append("No publication error bursts found.")
        return "\n".join(lines)

    lines.append("Burst groups:")
    for burst in report["bursts"]:
        lines.append(
            "- "
            f"{burst['platform']} / {burst['error_category']}: "
            f"count={burst['count']} "
            f"first_seen={burst['first_seen']} "
            f"last_seen={burst['last_seen']} "
            f"max_consecutive={burst['max_consecutive_failures']}"
        )
    return "\n".join(lines)


def _load_attempts(
    conn: sqlite3.Connection,
    *,
    cutoff: str,
    platform: str,
) -> list[dict[str, Any]]:
    filters = ["attempted_at >= ?"]
    params: list[Any] = [cutoff]
    if platform != "all":
        filters.append("platform = ?")
        params.append(platform)

    rows = conn.execute(
        f"""SELECT id, content_id, platform, attempted_at, success, error_category
            FROM publication_attempts
            WHERE {' AND '.join(filters)}
            ORDER BY attempted_at ASC, id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _empty_bucket(platform: str, category: str) -> dict[str, Any]:
    return {
        "platform": platform,
        "error_category": category,
        "count": 0,
        "first_seen": None,
        "last_seen": None,
        "max_consecutive_failures": 0,
        "consecutive_first_seen": None,
        "consecutive_last_seen": None,
    }


def _finalize_bucket(
    bucket: dict[str, Any],
    min_failures: int,
    min_consecutive: int,
) -> dict[str, Any]:
    reasons = []
    if bucket["count"] >= min_failures:
        reasons.append("failure_count")
    if bucket["max_consecutive_failures"] >= min_consecutive:
        reasons.append("consecutive_failures")
    return {
        "platform": bucket["platform"],
        "error_category": bucket["error_category"],
        "count": bucket["count"],
        "first_seen": bucket["first_seen"],
        "last_seen": bucket["last_seen"],
        "max_consecutive_failures": bucket["max_consecutive_failures"],
        "consecutive_first_seen": bucket["consecutive_first_seen"],
        "consecutive_last_seen": bucket["consecutive_last_seen"],
        "thresholds_exceeded": reasons,
    }


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[str, Any]:
    return {
        "artifact_type": "publication_error_bursts",
        "bursts": [],
        "filters": dict(filters),
        "generated_at": generated_at.isoformat(),
        "has_bursts": False,
        "missing_columns": {
            table: list(columns)
            for table, columns in sorted(missing_columns.items())
        },
        "missing_tables": list(missing_tables),
        "totals": {
            "attempts": 0,
            "burst_count": 0,
            "failed_attempts": 0,
            "by_error_category": {},
            "by_platform": {},
        },
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "publication_attempts": {
            "id",
            "content_id",
            "platform",
            "attempted_at",
            "success",
            "error_category",
        },
    }
    missing_tables = tuple(
        table for table in sorted(required) if table not in schema
    )
    missing_columns = {
        table: tuple(column for column in sorted(columns) if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema
        and any(column not in schema.get(table, set()) for column in columns)
    }
    return missing_tables, missing_columns


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
