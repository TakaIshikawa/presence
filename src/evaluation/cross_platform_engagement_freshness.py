"""Report stale or missing engagement metrics for published content."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_MAX_AGE_HOURS = 24
SUPPORTED_PLATFORMS = ("x", "bluesky", "linkedin", "mastodon", "newsletter")
FRESHNESS_STATUSES = ("fresh", "stale", "missing_metrics")
_PLATFORM_TABLES = {
    "x": "post_engagement",
    "bluesky": "bluesky_engagement",
    "linkedin": "linkedin_engagement",
    "mastodon": "mastodon_engagement",
    "newsletter": "newsletter_link_clicks",
}


@dataclass(frozen=True)
class CrossPlatformEngagementFreshnessItem:
    """Freshness status for one published platform row."""

    content_id: int
    platform: str
    published_at: str
    latest_metric_at: str | None
    age_hours: float | None
    status: str
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CrossPlatformEngagementFreshnessReport:
    """Engagement metric freshness report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    items: tuple[CrossPlatformEngagementFreshnessItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None
    missing_optional_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "cross_platform_engagement_freshness",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "items": [item.to_dict() for item in self.items],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_optional_tables": list(self.missing_optional_tables),
            "missing_tables": list(self.missing_tables),
            "totals": self.totals,
        }


def build_cross_platform_engagement_freshness_report(
    db_or_conn: Any,
    *,
    platform: str = "all",
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> CrossPlatformEngagementFreshnessReport:
    """Return freshness status for recent published platform rows."""
    days = _positive_int(days, "days")
    max_age_hours = _positive_int(max_age_hours, "max_age_hours")
    selected = _selected_platforms(platform)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "max_age_hours": max_age_hours,
        "platform": platform,
        "window_start": window_start.isoformat(),
        "window_end": generated_at.isoformat(),
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _required_schema_gaps(schema)
    if missing_tables or missing_columns:
        return _report(
            generated_at=generated_at,
            filters=filters,
            items=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            missing_optional_tables=(),
        )

    missing_optional = tuple(
        table for table in _selected_metric_tables(selected) if table not in schema
    )
    publications = _load_publications(
        conn,
        selected=selected,
        window_start=window_start,
        now=generated_at,
    )
    latest_metrics = _latest_metrics(conn, schema, selected)
    items = tuple(
        _classify_publication(
            publication,
            latest_metrics.get((publication["content_id"], publication["platform"])),
            missing_optional_tables=set(missing_optional),
            now=generated_at,
            max_age_hours=max_age_hours,
        )
        for publication in publications
    )

    return _report(
        generated_at=generated_at,
        filters=filters,
        items=items,
        missing_tables=(),
        missing_columns={},
        missing_optional_tables=missing_optional,
    )


def format_cross_platform_engagement_freshness_json(
    report: CrossPlatformEngagementFreshnessReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_cross_platform_engagement_freshness_text(
    report: CrossPlatformEngagementFreshnessReport,
) -> str:
    """Render the report for terminal review."""
    filters = report.filters
    lines = [
        "Cross-Platform Engagement Freshness",
        f"Generated: {report.generated_at}",
        (
            f"Window: {filters['window_start']} to {filters['window_end']} "
            f"max_age_hours={filters['max_age_hours']}"
        ),
        f"Platform: {filters['platform']}",
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_optional_tables:
        lines.append("Missing optional metric tables: " + ", ".join(report.missing_optional_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    lines.append(
        "Totals: "
        f"publications={report.totals['publication_count']} "
        f"fresh={report.totals['by_status']['fresh']} "
        f"stale={report.totals['by_status']['stale']} "
        f"missing_metrics={report.totals['by_status']['missing_metrics']}"
    )
    lines.append("")

    if not report.items:
        if report.missing_tables or report.missing_columns:
            lines.append("No freshness findings available until schema gaps are resolved.")
        else:
            lines.append("No recent published content publications found.")
        return "\n".join(lines)

    lines.append("By platform:")
    for platform, counts in sorted(report.totals["by_platform"].items()):
        lines.append(
            f"- {platform}: fresh={counts['fresh']} stale={counts['stale']} "
            f"missing_metrics={counts['missing_metrics']}"
        )
    lines.append("")
    lines.append("Items:")
    for item in report.items:
        age = "-" if item.age_hours is None else f"{item.age_hours:.2f}h"
        metric_at = item.latest_metric_at or "-"
        lines.append(
            f"- content_id={item.content_id} platform={item.platform} "
            f"published_at={item.published_at} latest_metric_at={metric_at} "
            f"age={age} status={item.status} reason={item.reason}"
        )
    return "\n".join(lines)


def _load_publications(
    conn: sqlite3.Connection,
    *,
    selected: tuple[str, ...],
    window_start: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    placeholders = ", ".join("?" for _ in selected)
    cursor = conn.execute(
        f"""SELECT cp.content_id, LOWER(cp.platform) AS platform, cp.published_at
              FROM content_publications cp
              INNER JOIN generated_content gc ON gc.id = cp.content_id
             WHERE LOWER(COALESCE(cp.status, '')) = 'published'
               AND cp.published_at IS NOT NULL
               AND datetime(cp.published_at) >= datetime(?)
               AND datetime(cp.published_at) <= datetime(?)
               AND LOWER(cp.platform) IN ({placeholders})
             ORDER BY LOWER(cp.platform) ASC,
                      datetime(cp.published_at) ASC,
                      cp.content_id ASC""",
        (window_start.isoformat(), now.isoformat(), *selected),
    )
    return _dict_rows(cursor)


def _latest_metrics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    selected: tuple[str, ...],
) -> dict[tuple[int, str], str]:
    latest: dict[tuple[int, str], str] = {}
    for platform in selected:
        table = _PLATFORM_TABLES[platform]
        if table not in schema or not {"content_id", "fetched_at"}.issubset(schema[table]):
            continue
        cursor = conn.execute(
            f"""SELECT content_id, MAX(fetched_at) AS latest_metric_at
                  FROM {table}
                 WHERE content_id IS NOT NULL
                   AND fetched_at IS NOT NULL
                 GROUP BY content_id"""
        )
        for data in _dict_rows(cursor):
            content_id = _int_or_none(data.get("content_id"))
            latest_metric_at = data.get("latest_metric_at")
            if content_id is not None and latest_metric_at:
                latest[(content_id, platform)] = str(latest_metric_at)
    return latest


def _classify_publication(
    publication: dict[str, Any],
    latest_metric_at: str | None,
    *,
    missing_optional_tables: set[str],
    now: datetime,
    max_age_hours: int,
) -> CrossPlatformEngagementFreshnessItem:
    content_id = int(publication["content_id"])
    platform = str(publication["platform"])
    table = _PLATFORM_TABLES[platform]
    if table in missing_optional_tables:
        return CrossPlatformEngagementFreshnessItem(
            content_id=content_id,
            platform=platform,
            published_at=str(publication["published_at"] or ""),
            latest_metric_at=None,
            age_hours=None,
            status="missing_metrics",
            reason="missing_metric_table",
        )
    if not latest_metric_at:
        return CrossPlatformEngagementFreshnessItem(
            content_id=content_id,
            platform=platform,
            published_at=str(publication["published_at"] or ""),
            latest_metric_at=None,
            age_hours=None,
            status="missing_metrics",
            reason="no_matching_metric_rows",
        )

    metric_time = _parse_timestamp(latest_metric_at)
    if metric_time is None:
        return CrossPlatformEngagementFreshnessItem(
            content_id=content_id,
            platform=platform,
            published_at=str(publication["published_at"] or ""),
            latest_metric_at=latest_metric_at,
            age_hours=None,
            status="missing_metrics",
            reason="invalid_metric_timestamp",
        )

    age_hours = round(max(0.0, (now - metric_time).total_seconds() / 3600), 2)
    if age_hours <= max_age_hours:
        status = "fresh"
        reason = "metric_within_max_age"
    else:
        status = "stale"
        reason = "metric_older_than_max_age"
    return CrossPlatformEngagementFreshnessItem(
        content_id=content_id,
        platform=platform,
        published_at=str(publication["published_at"] or ""),
        latest_metric_at=latest_metric_at,
        age_hours=age_hours,
        status=status,
        reason=reason,
    )


def _report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    items: tuple[CrossPlatformEngagementFreshnessItem, ...],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
    missing_optional_tables: tuple[str, ...],
) -> CrossPlatformEngagementFreshnessReport:
    by_status = {status: 0 for status in FRESHNESS_STATUSES}
    by_status.update(Counter(item.status for item in items))
    by_platform: dict[str, dict[str, int]] = {}
    for item in items:
        counts = by_platform.setdefault(item.platform, {status: 0 for status in FRESHNESS_STATUSES})
        counts[item.status] += 1
    return CrossPlatformEngagementFreshnessReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "publication_count": len(items),
            "by_platform": by_platform,
            "by_status": by_status,
            "missing_optional_tables": len(missing_optional_tables),
        },
        items=items,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        missing_optional_tables=missing_optional_tables,
    )


def _selected_platforms(platform: str) -> tuple[str, ...]:
    normalized = platform.strip().lower()
    if normalized == "all":
        return SUPPORTED_PLATFORMS
    if normalized not in SUPPORTED_PLATFORMS:
        raise ValueError(
            "platform must be 'all' or one of: " + ", ".join(SUPPORTED_PLATFORMS)
        )
    return (normalized,)


def _selected_metric_tables(selected: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(_PLATFORM_TABLES[platform] for platform in selected))


def _required_schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "content_publications": {"content_id", "platform", "status", "published_at"},
        "generated_content": {"id"},
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns: dict[str, tuple[str, ...]] = {}
    for table, columns in required.items():
        if table not in schema:
            continue
        missing = tuple(sorted(columns - schema[table]))
        if missing:
            missing_columns[table] = missing
    return missing_tables, missing_columns


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {column[1] for column in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _dict_rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [description[0] for description in cursor.description or ()]
    return [
        {names[index]: value for index, value in enumerate(row)}
        for row in cursor.fetchall()
    ]


def _parse_timestamp(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None
    return _ensure_utc(parsed)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _positive_int(value: int, name: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be positive") from exc
    if parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
