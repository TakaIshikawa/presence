"""Report crowded publish queue schedule buckets by platform."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_BUCKET_SIZE_MINUTES = 60
DEFAULT_LOOKAHEAD_HOURS = 24
DEFAULT_MAX_POSTS_PER_BUCKET = 3
CONTENTION_STATUSES = ("queued", "held", "failed")


def build_publish_queue_slot_contention_report(
    rows: list[dict[str, Any]],
    *,
    bucket_size_minutes: int = DEFAULT_BUCKET_SIZE_MINUTES,
    lookahead_hours: int = DEFAULT_LOOKAHEAD_HOURS,
    max_posts_per_bucket: int = DEFAULT_MAX_POSTS_PER_BUCKET,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic report of scheduled publish_queue slot contention."""
    if bucket_size_minutes <= 0:
        raise ValueError("bucket_size_minutes must be positive")
    if lookahead_hours <= 0:
        raise ValueError("lookahead_hours must be positive")
    if max_posts_per_bucket <= 0:
        raise ValueError("max_posts_per_bucket must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    window_end = generated_at + timedelta(hours=lookahead_hours)
    grouped: dict[tuple[str, str], dict[str, Any]] = defaultdict(_empty_bucket)

    rows_scanned = 0
    for row in rows:
        scheduled_at = _parse_dt(row.get("scheduled_at"))
        status = _clean(row.get("status"), "unknown").lower()
        if status not in CONTENTION_STATUSES or scheduled_at is None:
            continue
        if scheduled_at < generated_at or scheduled_at > window_end:
            continue
        rows_scanned += 1
        platform = _clean(row.get("platform"), "unknown").lower()
        bucket_start = _bucket_start(scheduled_at, bucket_size_minutes).isoformat()
        bucket = grouped[(bucket_start, platform)]
        bucket["bucket_start"] = bucket_start
        bucket["platform"] = platform
        bucket[f"{status}_count"] += 1
        content_id = _clean(row.get("content_id"))
        if content_id:
            bucket["_content_ids"].add(content_id)

    buckets = []
    for bucket in grouped.values():
        total = bucket["queued_count"] + bucket["held_count"] + bucket["failed_count"]
        buckets.append(
            {
                "bucket_start": bucket["bucket_start"],
                "platform": bucket["platform"],
                "queued_count": bucket["queued_count"],
                "held_count": bucket["held_count"],
                "failed_count": bucket["failed_count"],
                "distinct_content_count": len(bucket["_content_ids"]),
                "severity": _severity(total, max_posts_per_bucket),
            }
        )
    buckets.sort(key=lambda item: (item["bucket_start"], item["platform"]))

    overloaded = [item for item in buckets if item["severity"] != "ok"]
    return {
        "artifact_type": "publish_queue_slot_contention",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "bucket_size_minutes": bucket_size_minutes,
            "lookahead_hours": lookahead_hours,
            "max_posts_per_bucket": max_posts_per_bucket,
            "window_start": generated_at.isoformat(),
            "window_end": window_end.isoformat(),
            "statuses": list(CONTENTION_STATUSES),
        },
        "summary": {
            "rows_scanned": rows_scanned,
            "bucket_count": len(buckets),
            "overloaded_bucket_count": len(overloaded),
            "queued_count": sum(item["queued_count"] for item in buckets),
            "held_count": sum(item["held_count"] for item in buckets),
            "failed_count": sum(item["failed_count"] for item in buckets),
        },
        "buckets": buckets,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
        },
    }


def build_publish_queue_slot_contention_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    rows: list[dict[str, Any]] = []
    if "publish_queue" not in schema:
        missing_tables.append("publish_queue")
    else:
        rows = _load_rows(conn, schema["publish_queue"], missing_columns)
    return build_publish_queue_slot_contention_report(
        rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_publish_queue_slot_contention_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_queue_slot_contention_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    filters = report["filters"]
    lines = [
        "Publish Queue Slot Contention",
        f"Generated: {report['generated_at']}",
        (
            f"Window: {filters['lookahead_hours']} hours "
            f"({filters['window_start']} to {filters['window_end']})"
        ),
        (
            f"Bucket: {filters['bucket_size_minutes']} minutes; "
            f"max_posts_per_bucket={filters['max_posts_per_bucket']}"
        ),
        (
            f"Totals: rows={summary['rows_scanned']} buckets={summary['bucket_count']} "
            f"overloaded={summary['overloaded_bucket_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in report["missing_columns"].items()
            )
        )
    if not report["buckets"]:
        lines.append("No publish queue slot contention found.")
        return "\n".join(lines)
    lines.extend(["", "bucket_start | platform | queued | held | failed | distinct_content | severity"])
    for bucket in report["buckets"]:
        lines.append(
            f"{bucket['bucket_start']} | {bucket['platform']} | "
            f"{bucket['queued_count']} | {bucket['held_count']} | "
            f"{bucket['failed_count']} | {bucket['distinct_content_count']} | "
            f"{bucket['severity']}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    missing_columns: dict[str, list[str]],
) -> list[dict[str, Any]]:
    required = {"scheduled_at", "status"}
    missing = sorted(required - columns)
    if missing:
        missing_columns["publish_queue"] = missing
        return []
    select = [
        _expr(columns, "id", "queue_id", "NULL"),
        _expr(columns, "content_id", "content_id", "NULL"),
        _expr(columns, "platform", "platform", "'unknown'"),
        "status AS status",
        "scheduled_at AS scheduled_at",
    ]
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT {', '.join(select)} FROM publish_queue ORDER BY scheduled_at ASC, rowid ASC"
        ).fetchall()
    ]


def _empty_bucket() -> dict[str, Any]:
    return {
        "bucket_start": None,
        "platform": None,
        "queued_count": 0,
        "held_count": 0,
        "failed_count": 0,
        "_content_ids": set(),
    }


def _severity(total: int, max_posts_per_bucket: int) -> str:
    if total >= max_posts_per_bucket * 2:
        return "critical"
    if total > max_posts_per_bucket:
        return "overloaded"
    return "ok"


def _bucket_start(value: datetime, bucket_size_minutes: int) -> datetime:
    seconds = int(value.timestamp())
    bucket_seconds = bucket_size_minutes * 60
    return datetime.fromtimestamp(seconds - (seconds % bucket_seconds), tz=timezone.utc)


def _expr(columns: set[str], column: str, output: str, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row[0]): {
            str(column[1])
            for column in conn.execute(f"PRAGMA table_info({row[0]})").fetchall()
        }
        for row in rows
    }


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
