"""Normalize engagement snapshots across social and newsletter platforms."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable

from evaluation.engagement_scorer import compute_newsletter_engagement_score


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 10
SUPPORTED_PLATFORMS = ("x", "bluesky", "linkedin", "mastodon", "newsletter")


@dataclass(frozen=True)
class CrossPlatformEngagementRow:
    """One latest engagement snapshot normalized within its platform."""

    platform: str
    content_id: int | None
    issue_id: str | None
    raw_metrics: dict[str, int | float | str | None]
    raw_total: float
    normalized_score: float
    freshness_adjusted_score: float
    fetched_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlatformEngagementSummary:
    """Aggregate normalized performance for one platform."""

    platform: str
    row_count: int
    average_raw_total: float
    average_normalized_score: float
    average_freshness_adjusted_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class CrossPlatformEngagementReport:
    """Read-only cross-platform engagement normalization report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    platform_averages: tuple[PlatformEngagementSummary, ...]
    rows: tuple[CrossPlatformEngagementRow, ...]
    top_rows: tuple[CrossPlatformEngagementRow, ...]
    bottom_rows: tuple[CrossPlatformEngagementRow, ...]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": dict(self.totals),
            "platform_averages": [
                summary.to_dict() for summary in self.platform_averages
            ],
            "rows": [row.to_dict() for row in self.rows],
            "top_rows": [row.to_dict() for row in self.top_rows],
            "bottom_rows": [row.to_dict() for row in self.bottom_rows],
            "missing_tables": list(self.missing_tables),
        }


def build_cross_platform_engagement_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> CrossPlatformEngagementReport:
    """Return latest engagement rows normalized per platform."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    selected = _selected_platforms(platform)
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)

    rows: list[CrossPlatformEngagementRow] = []
    missing_tables: list[str] = []
    for name in selected:
        platform_rows, missing = _load_platform_rows(
            conn,
            schema,
            name,
            cutoff=cutoff,
            now=generated_at,
            days=days,
        )
        rows.extend(platform_rows)
        missing_tables.extend(missing)

    rows = _normalize_rows(rows, now=generated_at, days=days)
    platform_averages = _platform_averages(rows)
    ranked = sorted(
        rows,
        key=lambda row: (
            -row.freshness_adjusted_score,
            row.platform,
            row.content_id if row.content_id is not None else -1,
            row.issue_id or "",
        ),
    )
    bottom = sorted(
        rows,
        key=lambda row: (
            row.freshness_adjusted_score,
            row.platform,
            row.content_id if row.content_id is not None else -1,
            row.issue_id or "",
        ),
    )
    sorted_rows = tuple(
        sorted(
            rows,
            key=lambda row: (
                row.platform,
                row.content_id if row.content_id is not None else -1,
                row.issue_id or "",
                row.fetched_at,
            ),
        )
    )

    return CrossPlatformEngagementReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "platform": platform,
            "limit": limit,
            "window_start": cutoff.isoformat(),
            "window_end": generated_at.isoformat(),
        },
        totals={
            "rows": len(sorted_rows),
            "platforms": len({row.platform for row in sorted_rows}),
            "missing_tables": len(set(missing_tables)),
        },
        platform_averages=tuple(platform_averages),
        rows=sorted_rows,
        top_rows=tuple(ranked[:limit]),
        bottom_rows=tuple(bottom[:limit]),
        missing_tables=tuple(sorted(set(missing_tables))),
    )


def format_cross_platform_engagement_json(
    report: CrossPlatformEngagementReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_cross_platform_engagement_text(
    report: CrossPlatformEngagementReport,
) -> str:
    """Format the report for terminal review."""
    lines = [
        "Cross-Platform Engagement Report",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['window_start']} to "
            f"{report.filters['window_end']}"
        ),
        f"Platform: {report.filters['platform']}",
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.rows:
        lines.append("No engagement snapshots found for this window.")
        return "\n".join(lines)

    lines.append(f"Rows: {report.totals['rows']}")
    lines.append("Platform averages:")
    for summary in report.platform_averages:
        lines.append(
            f"- {summary.platform}: rows={summary.row_count} "
            f"raw_avg={summary.average_raw_total:.2f} "
            f"normalized_avg={summary.average_normalized_score:.2f} "
            f"fresh_avg={summary.average_freshness_adjusted_score:.2f}"
        )

    lines.append("Top content:")
    for row in report.top_rows:
        lines.append(_format_ranked_row(row))

    lines.append("Bottom content:")
    for row in report.bottom_rows:
        lines.append(_format_ranked_row(row))
    return "\n".join(lines)


def _selected_platforms(platform: str) -> tuple[str, ...]:
    normalized = platform.strip().lower()
    if normalized == "all":
        return SUPPORTED_PLATFORMS
    if normalized not in SUPPORTED_PLATFORMS:
        raise ValueError(
            "platform must be 'all' or one of: " + ", ".join(SUPPORTED_PLATFORMS)
        )
    return (normalized,)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    names = [_value(row, "name", 0) for row in rows]
    return {
        str(name): {
            str(_value(column, "name", 1))
            for column in conn.execute(f"PRAGMA table_info({name})").fetchall()
        }
        for name in names
    }


def _load_platform_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    platform: str,
    *,
    cutoff: datetime,
    now: datetime,
    days: int,
) -> tuple[list[CrossPlatformEngagementRow], list[str]]:
    if platform == "x":
        return _load_content_platform_rows(
            conn,
            schema,
            table="post_engagement",
            platform="x",
            metrics=("like_count", "retweet_count", "reply_count", "quote_count"),
            cutoff=cutoff,
            now=now,
            days=days,
        )
    if platform == "bluesky":
        return _load_content_platform_rows(
            conn,
            schema,
            table="bluesky_engagement",
            platform="bluesky",
            metrics=("like_count", "repost_count", "reply_count", "quote_count"),
            cutoff=cutoff,
            now=now,
            days=days,
        )
    if platform == "linkedin":
        return _load_content_platform_rows(
            conn,
            schema,
            table="linkedin_engagement",
            platform="linkedin",
            metrics=("impression_count", "like_count", "comment_count", "share_count"),
            cutoff=cutoff,
            now=now,
            days=days,
        )
    if platform == "mastodon":
        return _load_content_platform_rows(
            conn,
            schema,
            table="mastodon_engagement",
            platform="mastodon",
            metrics=("favourite_count", "boost_count", "reply_count"),
            cutoff=cutoff,
            now=now,
            days=days,
        )
    return _load_newsletter_rows(conn, schema, cutoff=cutoff, now=now, days=days)


def _load_content_platform_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    table: str,
    platform: str,
    metrics: tuple[str, ...],
    cutoff: datetime,
    now: datetime,
    days: int,
) -> tuple[list[CrossPlatformEngagementRow], list[str]]:
    required = {"content_id", "fetched_at"}
    if table not in schema or not required.issubset(schema[table]):
        return [], [table]
    columns = schema[table]
    score_expr = _column_expr(columns, "engagement_score")
    metric_exprs = [_column_expr(columns, name) for name in metrics]
    rows = _dict_rows(
        conn.execute(
            f"""SELECT content_id, fetched_at, {score_expr},
                      {', '.join(metric_exprs)}
                 FROM (
                       SELECT *,
                              ROW_NUMBER() OVER (
                                  PARTITION BY content_id
                                  ORDER BY fetched_at DESC, id DESC
                              ) AS rn
                         FROM {table}
                        WHERE datetime(fetched_at) >= datetime(?)
                          AND datetime(fetched_at) <= datetime(?)
                      )
                WHERE rn = 1
                ORDER BY content_id ASC""",
            (cutoff.isoformat(), now.isoformat()),
        )
    )
    normalized: list[CrossPlatformEngagementRow] = []
    for row in rows:
        fetched_at = str(row.get("fetched_at") or "")
        if _parse_timestamp(fetched_at) is None:
            continue
        raw_metrics = {name: _number_or_zero(row.get(name)) for name in metrics}
        raw_total = _float_or_none(row.get("engagement_score"))
        if raw_total is None:
            raw_total = float(sum(float(value or 0) for value in raw_metrics.values()))
        normalized.append(
            CrossPlatformEngagementRow(
                platform=platform,
                content_id=_int_or_none(row.get("content_id")),
                issue_id=None,
                raw_metrics=raw_metrics,
                raw_total=round(raw_total, 4),
                normalized_score=0.0,
                freshness_adjusted_score=_freshness_multiplier(
                    fetched_at,
                    now=now,
                    days=days,
                ),
                fetched_at=fetched_at,
            )
        )
    return normalized, []


def _load_newsletter_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
    days: int,
) -> tuple[list[CrossPlatformEngagementRow], list[str]]:
    table = "newsletter_engagement"
    if table not in schema or not {"issue_id", "fetched_at"}.issubset(schema[table]):
        return [], [table]
    columns = schema[table]
    send_join = ""
    send_columns = "NULL AS subscriber_count"
    if "newsletter_sends" in schema and "newsletter_send_id" in columns:
        send_join = "LEFT JOIN newsletter_sends ns ON ns.id = ne.newsletter_send_id"
        send_columns = "ns.subscriber_count AS subscriber_count"
    rows = _dict_rows(
        conn.execute(
            f"""SELECT ne.issue_id, ne.fetched_at,
                      {_column_expr(columns, 'opens', 'ne')},
                      {_column_expr(columns, 'clicks', 'ne')},
                      {_column_expr(columns, 'unsubscribes', 'ne')},
                      {send_columns}
                 FROM (
                       SELECT *,
                              ROW_NUMBER() OVER (
                                  PARTITION BY issue_id
                                  ORDER BY fetched_at DESC, id DESC
                              ) AS rn
                         FROM newsletter_engagement
                        WHERE datetime(fetched_at) >= datetime(?)
                          AND datetime(fetched_at) <= datetime(?)
                      ) ne
                 {send_join}
                WHERE ne.rn = 1
                ORDER BY ne.issue_id ASC""",
            (cutoff.isoformat(), now.isoformat()),
        )
    )
    normalized: list[CrossPlatformEngagementRow] = []
    for row in rows:
        fetched_at = str(row.get("fetched_at") or "")
        if _parse_timestamp(fetched_at) is None:
            continue
        opens = int(_number_or_zero(row.get("opens")))
        clicks = int(_number_or_zero(row.get("clicks")))
        unsubscribes = int(_number_or_zero(row.get("unsubscribes")))
        subscriber_count = _int_or_none(row.get("subscriber_count"))
        normalized.append(
            CrossPlatformEngagementRow(
                platform="newsletter",
                content_id=None,
                issue_id=str(row.get("issue_id") or ""),
                raw_metrics={
                    "opens": opens,
                    "clicks": clicks,
                    "unsubscribes": unsubscribes,
                    "subscriber_count": subscriber_count,
                },
                raw_total=round(compute_newsletter_engagement_score(opens, clicks), 4),
                normalized_score=0.0,
                freshness_adjusted_score=_freshness_multiplier(
                    fetched_at,
                    now=now,
                    days=days,
                ),
                fetched_at=fetched_at,
            )
        )
    return normalized, []


def _normalize_rows(
    rows: Iterable[CrossPlatformEngagementRow],
    *,
    now: datetime,
    days: int,
) -> list[CrossPlatformEngagementRow]:
    by_platform: dict[str, list[CrossPlatformEngagementRow]] = defaultdict(list)
    for row in rows:
        by_platform[row.platform].append(row)

    normalized: list[CrossPlatformEngagementRow] = []
    for platform_rows in by_platform.values():
        max_total = max((row.raw_total for row in platform_rows), default=0.0)
        for row in platform_rows:
            score = (row.raw_total / max_total * 100.0) if max_total > 0 else 0.0
            multiplier = _freshness_multiplier(row.fetched_at, now=now, days=days)
            normalized.append(
                CrossPlatformEngagementRow(
                    platform=row.platform,
                    content_id=row.content_id,
                    issue_id=row.issue_id,
                    raw_metrics=row.raw_metrics,
                    raw_total=round(row.raw_total, 4),
                    normalized_score=round(score, 4),
                    freshness_adjusted_score=round(score * multiplier, 4),
                    fetched_at=row.fetched_at,
                )
            )
    return normalized


def _platform_averages(
    rows: Iterable[CrossPlatformEngagementRow],
) -> list[PlatformEngagementSummary]:
    by_platform: dict[str, list[CrossPlatformEngagementRow]] = defaultdict(list)
    for row in rows:
        by_platform[row.platform].append(row)
    summaries = []
    for platform in sorted(by_platform):
        platform_rows = by_platform[platform]
        count = len(platform_rows)
        summaries.append(
            PlatformEngagementSummary(
                platform=platform,
                row_count=count,
                average_raw_total=round(
                    sum(row.raw_total for row in platform_rows) / count,
                    4,
                ),
                average_normalized_score=round(
                    sum(row.normalized_score for row in platform_rows) / count,
                    4,
                ),
                average_freshness_adjusted_score=round(
                    sum(row.freshness_adjusted_score for row in platform_rows) / count,
                    4,
                ),
            )
        )
    return summaries


def _freshness_multiplier(fetched_at: str, *, now: datetime, days: int) -> float:
    fetched = _parse_timestamp(fetched_at)
    if fetched is None:
        return 0.0
    age_days = max((now - fetched).total_seconds() / 86400.0, 0.0)
    if days <= 0:
        return 1.0
    return round(max(0.0, 1.0 - (age_days / days * 0.5)), 4)


def _format_ranked_row(row: CrossPlatformEngagementRow) -> str:
    identifier = (
        f"content_id={row.content_id}"
        if row.content_id is not None
        else f"issue_id={row.issue_id or 'n/a'}"
    )
    return (
        f"- {row.platform} {identifier}: raw={row.raw_total:.2f} "
        f"normalized={row.normalized_score:.2f} "
        f"fresh={row.freshness_adjusted_score:.2f} fetched={row.fetched_at}"
    )


def _column_expr(columns: set[str], column: str, table_alias: str | None = None) -> str:
    prefix = f"{table_alias}." if table_alias else ""
    if column in columns:
        return f"{prefix}{column} AS {column}"
    return f"NULL AS {column}"


def _dict_rows(cursor: sqlite3.Cursor) -> list[dict[str, Any]]:
    names = [column[0] for column in cursor.description or []]
    return [
        {
            name: row[name] if hasattr(row, "keys") else row[index]
            for index, name in enumerate(names)
        }
        for row in cursor.fetchall()
    ]


def _value(row: Any, key: str, index: int) -> Any:
    return row[key] if hasattr(row, "keys") else row[index]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _number_or_zero(value: Any) -> int | float:
    if value is None:
        return 0
    try:
        number = float(value)
    except (TypeError, ValueError):
        return 0
    return int(number) if number.is_integer() else number


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
