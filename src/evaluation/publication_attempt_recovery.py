"""Report whether publication failures recover after retry."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from statistics import median
from typing import Any

from output.publish_errors import normalize_error_category


DEFAULT_DAYS = 7
DEFAULT_REPRESENTATIVE_LIMIT = 3
PLATFORMS = ("x", "bluesky")
VALID_PLATFORMS = {"all", *PLATFORMS}
LOW_RECOVERY_THRESHOLD = 0.5


def build_publication_attempt_recovery_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    representative_limit: int = DEFAULT_REPRESENTATIVE_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Summarize failed publication attempts that did or did not later recover."""
    if days <= 0:
        raise ValueError("days must be positive")
    if platform not in VALID_PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")
    if representative_limit <= 0:
        raise ValueError("representative_limit must be positive")

    conn = _connection(db_or_conn)
    now = _as_utc(now or datetime.now(timezone.utc))
    cutoff = (now - timedelta(days=days)).isoformat()
    schema = _schema(conn)
    selected_platforms = set(_selected_platforms(platform))

    attempts_by_key = _attempts_by_key(conn, schema, cutoff, selected_platforms)
    ledger_successes = _ledger_successes(conn, schema, attempts_by_key.keys())
    bucket_map: dict[tuple[str, str], dict[str, Any]] = {}

    for key, attempts in sorted(attempts_by_key.items()):
        content_id, item_platform = key
        for index, attempt in enumerate(attempts):
            if bool(attempt.get("success")) or not attempt.get("in_window"):
                continue
            category = normalize_error_category(attempt.get("error_category"))
            bucket = bucket_map.setdefault(
                (item_platform, category),
                _empty_bucket(item_platform, category),
            )
            recovery = _recovery_for_attempt(
                attempt,
                attempts[index + 1 :],
                ledger_successes.get(key),
            )
            bucket["failed_attempts"] += 1
            bucket["_representative_candidates"].append(
                {
                    "content_id": content_id,
                    "first_failed_at": attempt["attempted_at"],
                    "recovered": recovery["recovered"],
                }
            )
            if recovery["recovered"]:
                bucket["later_successes"] += 1
                bucket["_attempts_to_recovery"].append(recovery["attempts_to_recovery"])

    buckets = [_finalize_bucket(bucket, representative_limit) for bucket in bucket_map.values()]
    buckets.sort(key=lambda item: (item["platform"], item["error_category"]))

    totals = {
        "failed_attempts": sum(bucket["failed_attempts"] for bucket in buckets),
        "later_successes": sum(bucket["later_successes"] for bucket in buckets),
        "unrecovered_count": sum(bucket["unrecovered_count"] for bucket in buckets),
    }
    totals["recovery_rate"] = _rate(totals["later_successes"], totals["failed_attempts"])

    return {
        "artifact_type": "publication_attempt_recovery",
        "generated_at": now.isoformat(),
        "window_days": days,
        "platform": platform,
        "representative_limit": representative_limit,
        "totals": totals,
        "buckets": buckets,
    }


def format_publication_attempt_recovery_json(report: dict[str, Any]) -> str:
    """Render a publication attempt recovery report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_recovery_text(report: dict[str, Any]) -> str:
    """Render a concise terminal report for publication attempt recovery."""
    lines = [
        "Publication Attempt Recovery Report",
        f"Generated: {report['generated_at']}",
        f"Window: {report['window_days']} days",
        f"Platform: {report['platform']}",
        (
            f"Failures: {report['totals']['failed_attempts']} "
            f"recovered={report['totals']['later_successes']} "
            f"unrecovered={report['totals']['unrecovered_count']} "
            f"recovery_rate={_format_percent(report['totals']['recovery_rate'])}"
        ),
        "",
    ]
    if not report["buckets"]:
        lines.append("No failed publication attempts found.")
        return "\n".join(lines)

    lines.append("Buckets:")
    for bucket in report["buckets"]:
        median_attempts = bucket["median_attempts_to_recovery"]
        lines.append(
            "- "
            f"{bucket['platform']} / {bucket['error_category']}: "
            f"failed={bucket['failed_attempts']} "
            f"recovered={bucket['later_successes']} "
            f"unrecovered={bucket['unrecovered_count']} "
            f"recovery_rate={_format_percent(bucket['recovery_rate'])} "
            f"median_attempts_to_recovery={median_attempts if median_attempts is not None else '-'}"
        )
        lines.append(f"  recommendation: {bucket['recommendation']}")
        if bucket["representative_content_ids"]:
            lines.append(
                "  representative_content_ids: "
                + ", ".join(str(item) for item in bucket["representative_content_ids"])
            )
    return "\n".join(lines)


def _attempts_by_key(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: str,
    platforms: set[str],
) -> dict[tuple[int, str], list[dict[str, Any]]]:
    columns = schema.get("publication_attempts")
    required = {"id", "content_id", "platform", "attempted_at", "success"}
    if not columns or not required.issubset(columns):
        return {}

    platform_placeholders = ", ".join("?" for _ in platforms)
    failure_rows = conn.execute(
        f"""SELECT DISTINCT content_id, platform
            FROM publication_attempts
            WHERE success = 0
              AND attempted_at >= ?
              AND platform IN ({platform_placeholders})
            ORDER BY content_id ASC, platform ASC""",
        [cutoff, *sorted(platforms)],
    ).fetchall()

    groups: dict[tuple[int, str], list[dict[str, Any]]] = {}
    for row in failure_rows:
        key = (int(row["content_id"]), str(row["platform"]))
        attempts = conn.execute(
            """SELECT id, content_id, platform, attempted_at, success, error_category
               FROM publication_attempts
               WHERE content_id = ?
                 AND platform = ?
               ORDER BY attempted_at ASC, id ASC""",
            key,
        ).fetchall()
        groups[key] = [
            {
                **dict(attempt),
                "attempted_at_dt": _parse_timestamp(attempt["attempted_at"]),
                "in_window": str(attempt["attempted_at"]) >= cutoff,
            }
            for attempt in attempts
            if _parse_timestamp(attempt["attempted_at"]) is not None
        ]
    return groups


def _ledger_successes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    keys: Any,
) -> dict[tuple[int, str], datetime]:
    columns = schema.get("content_publications")
    required = {"content_id", "platform", "status", "published_at"}
    key_list = list(keys)
    if not key_list or not columns or not required.issubset(columns):
        return {}

    successes: dict[tuple[int, str], datetime] = {}
    for content_id, platform in key_list:
        row = conn.execute(
            """SELECT published_at
               FROM content_publications
               WHERE content_id = ?
                 AND platform = ?
                 AND status = 'published'
                 AND published_at IS NOT NULL""",
            (content_id, platform),
        ).fetchone()
        if row:
            published_at = _parse_timestamp(row["published_at"])
            if published_at is not None:
                successes[(content_id, platform)] = published_at
    return successes


def _recovery_for_attempt(
    failure: dict[str, Any],
    later_attempts: list[dict[str, Any]],
    ledger_success_at: datetime | None,
) -> dict[str, Any]:
    failure_at = failure["attempted_at_dt"]
    for offset, attempt in enumerate(later_attempts, start=1):
        attempted_at = attempt["attempted_at_dt"]
        if attempted_at <= failure_at:
            continue
        if bool(attempt.get("success")):
            return {"recovered": True, "attempts_to_recovery": offset}
    if ledger_success_at is not None and ledger_success_at > failure_at:
        intervening = sum(
            1
            for attempt in later_attempts
            if failure_at < attempt["attempted_at_dt"] <= ledger_success_at
        )
        return {"recovered": True, "attempts_to_recovery": intervening + 1}
    return {"recovered": False, "attempts_to_recovery": None}


def _empty_bucket(platform: str, category: str) -> dict[str, Any]:
    return {
        "platform": platform,
        "error_category": category,
        "failed_attempts": 0,
        "later_successes": 0,
        "_attempts_to_recovery": [],
        "_representative_candidates": [],
    }


def _finalize_bucket(bucket: dict[str, Any], representative_limit: int) -> dict[str, Any]:
    failed_attempts = bucket["failed_attempts"]
    later_successes = bucket["later_successes"]
    unrecovered_count = failed_attempts - later_successes
    attempts_to_recovery = bucket["_attempts_to_recovery"]
    return {
        "platform": bucket["platform"],
        "error_category": bucket["error_category"],
        "failed_attempts": failed_attempts,
        "later_successes": later_successes,
        "unrecovered_count": unrecovered_count,
        "recovery_rate": _rate(later_successes, failed_attempts),
        "median_attempts_to_recovery": _median_attempts(attempts_to_recovery),
        "representative_content_ids": _representative_content_ids(
            bucket["_representative_candidates"],
            representative_limit,
        ),
        "recommendation": _recommendation(
            bucket["error_category"],
            later_successes,
            failed_attempts,
        ),
    }


def _representative_content_ids(candidates: list[dict[str, Any]], limit: int) -> list[int]:
    ordered = sorted(
        candidates,
        key=lambda item: (
            item["recovered"],
            item["first_failed_at"],
            item["content_id"],
        ),
    )
    seen: set[int] = set()
    representatives: list[int] = []
    for candidate in ordered:
        content_id = int(candidate["content_id"])
        if content_id in seen:
            continue
        seen.add(content_id)
        representatives.append(content_id)
        if len(representatives) >= limit:
            break
    return representatives


def _recommendation(category: str, later_successes: int, failed_attempts: int) -> str:
    if failed_attempts == 0:
        return "No action needed."
    if _rate(later_successes, failed_attempts) >= LOW_RECOVERY_THRESHOLD:
        return "Retries are recovering; keep monitoring this category."
    category_actions = {
        "auth": "Low recovery: refresh credentials before retrying.",
        "rate_limit": "Low recovery: extend backoff or reduce publish concurrency.",
        "duplicate": "Low recovery: revise duplicate copy before retrying.",
        "media": "Low recovery: inspect media assets and attachment metadata.",
        "network": "Low recovery: check platform availability and retry policy.",
        "validation": "Low recovery: fix validation errors before retrying.",
        "unknown": "Low recovery: inspect stored errors and publisher logs.",
    }
    return category_actions.get(category, category_actions["unknown"])


def _median_attempts(values: list[int]) -> float | int | None:
    if not values:
        return None
    result = median(values)
    return int(result) if float(result).is_integer() else result


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


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


def _format_percent(value: float) -> str:
    return f"{value * 100:.1f}%"
