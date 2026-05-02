"""Bucket publication attempts by retry timing and time to success."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 7
PLATFORMS = ("x", "bluesky")
VALID_PLATFORMS = {"all", *PLATFORMS}
NO_RETRY_BUCKET = "no_retry"
UNKNOWN_BUCKET = "unknown"
FAILED_ONLY_BUCKET = "failed_only"

LATENCY_BUCKETS: tuple[tuple[str, float | None], ...] = (
    ("<=15m", 15),
    ("15m-1h", 60),
    ("1h-6h", 360),
    ("6h-24h", 1440),
    (">24h", None),
)


def build_publication_attempt_latency_bucket_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    now: datetime | None = None,
) -> dict[str, Any]:
    """Summarize first attempts, retries, successes, and failed-only attempts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")

    conn = _connection(db_or_conn)
    now = _as_utc(now or datetime.now(timezone.utc))
    cutoff = (now - timedelta(days=days)).isoformat()
    schema = _schema(conn)
    selected = set(_selected_platforms(platform))

    groups = _attempt_groups(conn, schema, cutoff, selected)
    ledger_lookup = _publication_lookup(conn, schema, groups.keys())

    platform_reports = {
        name: _empty_platform_report() for name in _selected_platforms(platform)
    }
    items: list[dict[str, Any]] = []

    for key, attempts in sorted(groups.items()):
        content_id, item_platform = key
        if item_platform not in selected:
            continue
        ledger = ledger_lookup.get(key, {})
        item = _summarize_item(
            content_id=content_id,
            platform=item_platform,
            attempts=attempts,
            ledger=ledger,
            now=now,
        )
        items.append(item)
        _add_item(platform_reports[item_platform], item)

    finalized_platforms = {
        name: _finalize_platform_report(data)
        for name, data in sorted(platform_reports.items())
    }

    items.sort(
        key=lambda item: (
            item["platform"],
            item["outcome"] != "failed_only",
            -(item["failed_attempt_count"]),
            item["first_attempt_at"] or "",
            item["content_id"],
        )
    )

    return {
        "generated_at": now.isoformat(),
        "window_days": days,
        "platform": platform,
        "bucket_definitions_minutes": [label for label, _ in LATENCY_BUCKETS],
        "platforms": finalized_platforms,
        "failed_only_items": [
            item for item in items if item["outcome"] == "failed_only"
        ],
        "successful_items": [item for item in items if item["outcome"] == "success"],
    }


def format_publication_attempt_latency_buckets_json(report: dict[str, Any]) -> str:
    """Render a publication attempt latency bucket report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_latency_buckets_text(report: dict[str, Any]) -> str:
    """Render a concise terminal report for retry timing."""
    lines = [
        "Publication Attempt Latency Bucket Report",
        f"Generated: {report['generated_at']}",
        f"Window: {report['window_days']} days",
        f"Platform: {report['platform']}",
        "",
    ]
    if not any(data["attempted_content_count"] for data in report["platforms"].values()):
        lines.append("No publication attempts found.")
        return "\n".join(lines)

    columns = [
        ("platform", "PLATFORM", 10),
        ("attempted_content_count", "CONTENT", 7),
        ("attempt_count", "ATTEMPTS", 8),
        ("successful_content_count", "SUCCESS", 7),
        ("failed_only_content_count", "FAILED", 6),
        ("failed_attempt_count", "FAIL_ATT", 8),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for name, data in report["platforms"].items():
        rendered = {"platform": name, **data}
        lines.append(
            "  ".join(
                _format_cell(rendered.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )

    lines.extend(["", "Time to success buckets:"])
    for name, data in report["platforms"].items():
        lines.append(f"- {name}: {_format_buckets(data['time_to_success_buckets'])}")

    lines.extend(["", "Retry delay buckets:"])
    for name, data in report["platforms"].items():
        lines.append(f"- {name}: {_format_buckets(data['retry_delay_buckets'])}")

    if report["failed_only_items"]:
        lines.extend(["", "Failed-only content:"])
        for item in report["failed_only_items"][:10]:
            lines.append(
                "- "
                f"{item['platform']} content={item['content_id']} "
                f"failed_attempts={item['failed_attempt_count']} "
                f"first={item['first_attempt_at']} latest={item['latest_attempt_at']} "
                f"age={item['failed_age_bucket']}"
            )
    return "\n".join(lines)


def _attempt_groups(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: str,
    platforms: set[str],
) -> dict[tuple[int, str], list[dict[str, Any]]]:
    columns = schema.get("publication_attempts")
    required = {"content_id", "platform", "attempted_at", "success"}
    if not columns or not required.issubset(columns):
        return {}

    platform_placeholders = ", ".join("?" for _ in platforms)
    key_rows = conn.execute(
        f"""SELECT DISTINCT content_id, platform
            FROM publication_attempts
            WHERE attempted_at >= ?
              AND platform IN ({platform_placeholders})
            ORDER BY content_id ASC, platform ASC""",
        [cutoff, *sorted(platforms)],
    ).fetchall()
    groups: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for key_row in key_rows:
        content_id = int(key_row["content_id"])
        platform = str(key_row["platform"])
        rows = conn.execute(
            """SELECT
                   id,
                   content_id,
                   platform,
                   attempted_at,
                   success,
                   error,
                   error_category
                FROM publication_attempts
                WHERE content_id = ?
                  AND platform = ?
                ORDER BY attempted_at ASC, id ASC""",
            (content_id, platform),
        ).fetchall()
        groups[(content_id, platform)] = [dict(row) for row in rows]
    return groups


def _publication_lookup(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    keys: Any,
) -> dict[tuple[int, str], dict[str, Any]]:
    columns = schema.get("content_publications")
    required = {"content_id", "platform", "status"}
    key_list = list(keys)
    if not key_list or not columns or not required.issubset(columns):
        return {}

    lookup: dict[tuple[int, str], dict[str, Any]] = {}
    for content_id, platform in key_list:
        row = conn.execute(
            """SELECT content_id, platform, status, attempt_count, last_error_at,
                      published_at, updated_at
               FROM content_publications
               WHERE content_id = ? AND platform = ?""",
            (content_id, platform),
        ).fetchone()
        if row:
            lookup[(content_id, platform)] = dict(row)
    return lookup


def _summarize_item(
    *,
    content_id: int,
    platform: str,
    attempts: list[dict[str, Any]],
    ledger: dict[str, Any],
    now: datetime,
) -> dict[str, Any]:
    parsed_attempts = [
        (attempt, _parse_timestamp(attempt.get("attempted_at")))
        for attempt in attempts
        if _parse_timestamp(attempt.get("attempted_at")) is not None
    ]
    parsed_attempts.sort(key=lambda item: (item[1], item[0].get("id") or 0))

    first_attempt_at = parsed_attempts[0][1] if parsed_attempts else None
    latest_attempt_at = parsed_attempts[-1][1] if parsed_attempts else None
    success_at = _success_time(parsed_attempts, ledger)
    failed_before_success = [
        item
        for item, timestamp in parsed_attempts
        if not bool(item.get("success")) and (success_at is None or timestamp < success_at)
    ]
    latest_failure_at = None
    if failed_before_success:
        latest_failure_at = _parse_timestamp(failed_before_success[-1].get("attempted_at"))

    outcome = "success" if success_at is not None else "failed_only"
    time_to_success_minutes = _elapsed_minutes(first_attempt_at, success_at)
    retry_delay_minutes = _elapsed_minutes(latest_failure_at, success_at)
    failed_age_minutes = (
        _elapsed_minutes(first_attempt_at, now) if outcome == "failed_only" else None
    )

    return {
        "content_id": content_id,
        "platform": platform,
        "outcome": outcome,
        "attempt_count": len(attempts),
        "failed_attempt_count": len(failed_before_success),
        "first_attempt_at": _iso(first_attempt_at),
        "latest_attempt_at": _iso(latest_attempt_at),
        "success_at": _iso(success_at),
        "success_source": _success_source(parsed_attempts, ledger),
        "publication_status": ledger.get("status"),
        "ledger_attempt_count": ledger.get("attempt_count"),
        "time_to_success_minutes": _round(time_to_success_minutes),
        "time_to_success_bucket": _bucket(time_to_success_minutes)
        if outcome == "success"
        else FAILED_ONLY_BUCKET,
        "retry_delay_minutes": _round(retry_delay_minutes),
        "retry_delay_bucket": (
            _bucket(retry_delay_minutes)
            if latest_failure_at is not None
            else NO_RETRY_BUCKET
        )
        if outcome == "success"
        else FAILED_ONLY_BUCKET,
        "failed_age_minutes": _round(failed_age_minutes),
        "failed_age_bucket": _bucket(failed_age_minutes)
        if outcome == "failed_only"
        else None,
    }


def _success_time(
    parsed_attempts: list[tuple[dict[str, Any], datetime]],
    ledger: dict[str, Any],
) -> datetime | None:
    times = [
        timestamp
        for attempt, timestamp in parsed_attempts
        if bool(attempt.get("success"))
    ]
    ledger_published_at = _parse_timestamp(ledger.get("published_at"))
    if ledger.get("status") == "published" and ledger_published_at is not None:
        times.append(ledger_published_at)
    return min(times) if times else None


def _success_source(
    parsed_attempts: list[tuple[dict[str, Any], datetime]],
    ledger: dict[str, Any],
) -> str | None:
    success_at = _success_time(parsed_attempts, ledger)
    if success_at is None:
        return None
    attempt_successes = [
        timestamp for attempt, timestamp in parsed_attempts if bool(attempt.get("success"))
    ]
    if success_at in attempt_successes:
        return "publication_attempts"
    return "content_publications"


def _add_item(platform_report: dict[str, Any], item: dict[str, Any]) -> None:
    platform_report["attempted_content_count"] += 1
    platform_report["attempt_count"] += item["attempt_count"]
    platform_report["failed_attempt_count"] += item["failed_attempt_count"]
    if item["outcome"] == "success":
        platform_report["successful_content_count"] += 1
        platform_report["time_to_success_buckets"][item["time_to_success_bucket"]] += 1
        platform_report["retry_delay_buckets"][item["retry_delay_bucket"]] += 1
    else:
        platform_report["failed_only_content_count"] += 1
        platform_report["failed_age_buckets"][item["failed_age_bucket"]] += 1


def _empty_platform_report() -> dict[str, Any]:
    return {
        "attempted_content_count": 0,
        "attempt_count": 0,
        "successful_content_count": 0,
        "failed_only_content_count": 0,
        "failed_attempt_count": 0,
        "time_to_success_buckets": {label: 0 for label, _ in LATENCY_BUCKETS},
        "retry_delay_buckets": {
            NO_RETRY_BUCKET: 0,
            **{label: 0 for label, _ in LATENCY_BUCKETS},
        },
        "failed_age_buckets": {label: 0 for label, _ in LATENCY_BUCKETS},
    }


def _finalize_platform_report(data: dict[str, Any]) -> dict[str, Any]:
    return {
        **data,
        "time_to_success_buckets": dict(data["time_to_success_buckets"]),
        "retry_delay_buckets": dict(data["retry_delay_buckets"]),
        "failed_age_buckets": dict(data["failed_age_buckets"]),
    }


def _bucket(minutes: float | None) -> str:
    if minutes is None:
        return UNKNOWN_BUCKET
    for label, upper in LATENCY_BUCKETS:
        if upper is None or minutes <= upper:
            return label
    return UNKNOWN_BUCKET


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


def _selected_platforms(platform: str) -> tuple[str, ...]:
    return PLATFORMS if platform == "all" else (platform,)


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _elapsed_minutes(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return (end - start).total_seconds() / 60


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _round(value: float | None) -> float | None:
    return round(value, 2) if value is not None else None


def _format_buckets(buckets: dict[str, int]) -> str:
    populated = [f"{label}={count}" for label, count in buckets.items() if count]
    return ", ".join(populated) if populated else "none"


def _format_cell(value: Any, width: int) -> str:
    text = str(value) if value is not None else "-"
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."
