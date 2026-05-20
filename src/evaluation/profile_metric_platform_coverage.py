"""Report expected profile metric platforms missing recent follower snapshots."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_EXPECTED_PLATFORMS = ("x", "linkedin", "bluesky", "mastodon")
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_STALE_DAYS = 2
DEFAULT_LIMIT = 100
ISSUE_TYPES = ("missing_platform", "stale_platform_metrics", "malformed_counts")


def build_profile_metric_platform_coverage_report(
    rows: list[dict[str, Any]],
    *,
    expected_platforms: list[str] | tuple[str, ...] = DEFAULT_EXPECTED_PLATFORMS,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
) -> dict[str, Any]:
    if lookback_days <= 0 or stale_days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=lookback_days)
    stale_before = generated_at - timedelta(days=stale_days)
    expected = tuple(dict.fromkeys(_clean(platform).lower() for platform in expected_platforms if _clean(platform)))
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    issues: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()

    for row in rows:
        platform = _clean(row.get("platform")).lower() or "unknown"
        grouped[platform].append(row)
        for column in ("follower_count", "listed_count"):
            if column in row and row.get(column) not in (None, "") and _to_int(row.get(column)) is None:
                issues.append({"issue_type": "malformed_counts", "platform": platform, "metric_id": row.get("metric_id"), "follower_count": row.get("follower_count"), "listed_count": row.get("listed_count")})
                counts["malformed_counts"] += 1
                break

    observed_recent: set[str] = set()
    stale_count = 0
    for platform in expected:
        platform_rows = grouped.get(platform, [])
        latest_at = max((_parse_time(row.get("fetched_at")) for row in platform_rows), default=None)
        recent_rows = [row for row in platform_rows if (_parse_time(row.get("fetched_at")) or datetime.min.replace(tzinfo=timezone.utc)) >= lookback_start]
        if recent_rows:
            observed_recent.add(platform)
        if not platform_rows or not recent_rows:
            issues.append({"issue_type": "missing_platform", "platform": platform, "latest_sample_at": latest_at.isoformat() if latest_at else None, "lookback_start": lookback_start.isoformat()})
            counts["missing_platform"] += 1
        elif latest_at is not None and latest_at < stale_before:
            stale_count += 1
            issues.append({"issue_type": "stale_platform_metrics", "platform": platform, "latest_sample_at": latest_at.isoformat(), "age_days": round((generated_at - latest_at).total_seconds() / 86400, 3)})
            counts["stale_platform_metrics"] += 1

    issues.sort(key=lambda item: (ISSUE_TYPES.index(item["issue_type"]), item.get("platform") or "", item.get("metric_id") or 0))
    shown = issues[:limit]
    return {
        "artifact_type": "profile_metric_platform_coverage",
        "generated_at": generated_at.isoformat(),
        "thresholds": {"expected_platforms": list(expected), "lookback_days": lookback_days, "stale_days": stale_days, "limit": limit},
        "summary": {
            "expected_platform_count": len(expected),
            "observed_platform_count": len(observed_recent),
            "stale_platform_count": stale_count,
            "row_count": len(rows),
            "issue_count": len(issues),
            "shown_count": len(shown),
            "by_issue_type": {issue_type: counts[issue_type] for issue_type in ISSUE_TYPES},
        },
        "missing_tables": sorted(missing_tables or []),
        "issue_items": shown,
        "empty_state": {"is_empty": not issues, "message": "No profile metric platform coverage issues found." if not issues else None},
    }


def build_profile_metric_platform_coverage_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "profile_metrics" not in schema:
        return build_profile_metric_platform_coverage_report([], missing_tables=["profile_metrics"], **kwargs)
    return build_profile_metric_platform_coverage_report(_load_rows(conn, schema["profile_metrics"]), **kwargs)


def format_profile_metric_platform_coverage_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_profile_metric_platform_coverage_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Profile Metric Platform Coverage",
        f"Generated: {report['generated_at']}",
        f"Thresholds: lookback_days={report['thresholds']['lookback_days']} stale_days={report['thresholds']['stale_days']} limit={report['thresholds']['limit']}",
        f"Totals: expected={summary['expected_platform_count']} observed={summary['observed_platform_count']} stale={summary['stale_platform_count']} issues={summary['issue_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["issue_items"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.append("Issues:")
    for item in report["issue_items"]:
        lines.append(f"  - {item['issue_type']} platform={item['platform']} latest={item.get('latest_sample_at') or '-'}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", fallback="rowid") + " AS metric_id",
        _expr(columns, "platform", fallback="'unknown'") + " AS platform",
        _expr(columns, "follower_count", "followers_count", fallback="NULL") + " AS follower_count",
        _expr(columns, "listed_count", fallback="NULL") + " AS listed_count",
        _expr(columns, "fetched_at", "created_at", fallback="NULL") + " AS fetched_at",
    ]
    order = _expr(columns, "fetched_at", "created_at", "id", fallback="rowid")
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM profile_metrics ORDER BY {order} ASC").fetchall()]


def _expr(columns: set[str], *names: str, fallback: str) -> str:
    for name in names:
        if name in columns:
            return name
    return fallback


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _to_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
