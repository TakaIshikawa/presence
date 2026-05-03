"""Detect suspicious engagement metric changes between fetched snapshots."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 50
METRIC_NAMES = ("likes", "replies", "reposts", "impressions", "bookmarks")
DEFAULT_JUMP_THRESHOLDS = {
    "likes": 500,
    "replies": 100,
    "reposts": 100,
    "impressions": 50_000,
    "bookmarks": 200,
}
DEFAULT_RATE_THRESHOLDS_PER_HOUR = {
    "likes": 1_000,
    "replies": 300,
    "reposts": 300,
    "impressions": 100_000,
    "bookmarks": 500,
}


@dataclass(frozen=True)
class EngagementMetricSnapshot:
    """Normalized engagement metrics fetched for one published post."""

    post_id: str
    fetched_at: str
    likes: int = 0
    replies: int = 0
    reposts: int = 0
    impressions: int = 0
    bookmarks: int = 0
    content_id: int | None = None

    def metric_value(self, metric: str) -> int:
        return int(getattr(self, metric))


@dataclass(frozen=True)
class EngagementMetricAnomaly:
    """One suspicious metric delta between two chronological snapshots."""

    anomaly_type: str
    severity: str
    post_id: str
    content_id: int | None
    metric: str
    previous_fetched_at: str
    current_fetched_at: str
    previous_value: int
    current_value: int
    delta: int
    elapsed_hours: float | None
    threshold: float | None
    rate_per_hour: float | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class EngagementAnomalyReport:
    """Engagement metric anomaly report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    anomalies: tuple[EngagementMetricAnomaly, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    @property
    def has_issues(self) -> bool:
        return bool(self.anomalies)

    def to_dict(self) -> dict[str, Any]:
        return {
            "anomalies": [anomaly.to_dict() for anomaly in self.anomalies],
            "artifact_type": "engagement_anomaly_report",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": _sorted_totals(self.totals),
        }


def analyze_engagement_metric_snapshots(
    snapshots: Iterable[EngagementMetricSnapshot | Mapping[str, Any]],
    *,
    jump_thresholds: Mapping[str, int] | None = None,
    rate_thresholds_per_hour: Mapping[str, float] | None = None,
) -> tuple[EngagementMetricAnomaly, ...]:
    """Compare chronological snapshots per post and return suspicious deltas."""
    normalized = [_normalize_snapshot(snapshot) for snapshot in snapshots]
    thresholds = _thresholds(DEFAULT_JUMP_THRESHOLDS, jump_thresholds)
    rate_thresholds = _thresholds(DEFAULT_RATE_THRESHOLDS_PER_HOUR, rate_thresholds_per_hour)
    by_post: dict[str, list[EngagementMetricSnapshot]] = defaultdict(list)
    for snapshot in normalized:
        by_post[snapshot.post_id].append(snapshot)

    anomalies: list[EngagementMetricAnomaly] = []
    for post_id in sorted(by_post):
        ordered = sorted(
            by_post[post_id],
            key=lambda item: (_parse_timestamp(item.fetched_at) or datetime.min.replace(tzinfo=timezone.utc), item.fetched_at),
        )
        for previous, current in zip(ordered, ordered[1:]):
            elapsed_hours = _elapsed_hours(previous.fetched_at, current.fetched_at)
            for metric in METRIC_NAMES:
                previous_value = previous.metric_value(metric)
                current_value = current.metric_value(metric)
                delta = current_value - previous_value
                if delta < 0:
                    anomalies.append(
                        _anomaly(
                            "negative_delta",
                            "critical",
                            previous,
                            current,
                            metric,
                            delta,
                            elapsed_hours,
                            None,
                            None,
                            f"{metric} decreased by {abs(delta)}",
                        )
                    )
                    continue

                jump_threshold = thresholds[metric]
                if delta > jump_threshold:
                    anomalies.append(
                        _anomaly(
                            "large_jump",
                            "warning",
                            previous,
                            current,
                            metric,
                            delta,
                            elapsed_hours,
                            float(jump_threshold),
                            None,
                            f"{metric} increased by {delta}, above jump threshold {jump_threshold}",
                        )
                    )

                rate_threshold = rate_thresholds[metric]
                if elapsed_hours is not None and elapsed_hours > 0:
                    rate = delta / elapsed_hours
                    if rate > rate_threshold:
                        anomalies.append(
                            _anomaly(
                                "impossible_rate",
                                "high",
                                previous,
                                current,
                                metric,
                                delta,
                                elapsed_hours,
                                float(rate_threshold),
                                rate,
                                f"{metric} changed at {rate:.2f}/hour, above rate threshold {rate_threshold:g}/hour",
                            )
                        )

    anomalies.sort(key=_anomaly_sort_key)
    return tuple(anomalies)


def build_engagement_anomaly_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    jump_thresholds: Mapping[str, int] | None = None,
    rate_thresholds_per_hour: Mapping[str, float] | None = None,
    now: datetime | None = None,
) -> EngagementAnomalyReport:
    """Load recent engagement snapshots and report suspicious metric changes."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "jump_thresholds": dict(_thresholds(DEFAULT_JUMP_THRESHOLDS, jump_thresholds)),
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "rate_thresholds_per_hour": dict(
            _thresholds(DEFAULT_RATE_THRESHOLDS_PER_HOUR, rate_thresholds_per_hour)
        ),
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = () if "post_engagement" in schema else ("post_engagement",)
    missing_columns = _missing_columns(schema)
    snapshots = _load_post_engagement_snapshots(conn, schema, cutoff=cutoff) if not missing_tables else []
    anomalies = analyze_engagement_metric_snapshots(
        snapshots,
        jump_thresholds=filters["jump_thresholds"],
        rate_thresholds_per_hour=filters["rate_thresholds_per_hour"],
    )[:limit]

    return EngagementAnomalyReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(snapshots, anomalies),
        anomalies=tuple(anomalies),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_engagement_anomaly_report_json(report: EngagementAnomalyReport) -> str:
    """Serialize an engagement anomaly report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_engagement_anomaly_report_text(report: EngagementAnomalyReport) -> str:
    """Render a concise human-readable engagement anomaly report."""
    totals = report.totals
    filters = report.filters
    by_type = totals["by_anomaly_type"]
    lines = [
        "Engagement Metric Anomaly Report",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"lookback_start={filters['lookback_start']}"
        ),
        (
            "Totals: "
            f"snapshots={totals['snapshots_scanned']} "
            f"posts={totals['posts_scanned']} "
            f"anomalies={totals['anomalies']} "
            f"negative_delta={by_type.get('negative_delta', 0)} "
            f"impossible_rate={by_type.get('impossible_rate', 0)} "
            f"large_jump={by_type.get('large_jump', 0)}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        )
        if missing:
            lines.append("Missing columns: " + missing)

    if not report.anomalies:
        lines.extend(["", "No suspicious engagement metric changes found."])
        return "\n".join(lines)

    lines.extend(["", "Anomalies:"])
    for anomaly in report.anomalies:
        elapsed = "-" if anomaly.elapsed_hours is None else f"{anomaly.elapsed_hours:.2f}h"
        rate = "-" if anomaly.rate_per_hour is None else f"{anomaly.rate_per_hour:.2f}/h"
        lines.append(
            f"- severity={anomaly.severity} type={anomaly.anomaly_type} "
            f"post_id={anomaly.post_id} content_id={anomaly.content_id or '-'} "
            f"metric={anomaly.metric} previous_at={anomaly.previous_fetched_at} "
            f"current_at={anomaly.current_fetched_at} delta={anomaly.delta} "
            f"values={anomaly.previous_value}->{anomaly.current_value} "
            f"elapsed={elapsed} rate={rate}"
        )
        lines.append(f"  reason={anomaly.reason}")
    return "\n".join(lines)


def _load_post_engagement_snapshots(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[EngagementMetricSnapshot]:
    columns = schema["post_engagement"]
    if not {"tweet_id", "fetched_at"}.issubset(columns):
        return []
    select_columns = [
        _column_expr(columns, "content_id", "pe", "content_id", default="NULL"),
        "pe.tweet_id AS post_id",
        "pe.fetched_at AS fetched_at",
        _column_expr(columns, "like_count", "pe", "likes"),
        _column_expr(columns, "reply_count", "pe", "replies"),
        _column_expr(columns, "retweet_count", "pe", "reposts"),
        _column_expr(columns, "impression_count", "pe", "impressions"),
        _column_expr(columns, "bookmark_count", "pe", "bookmarks"),
    ]
    joins = ""
    where = ["datetime(pe.fetched_at) >= datetime(?)", "pe.tweet_id IS NOT NULL"]
    if "generated_content" in schema and "content_id" in columns and "id" in schema["generated_content"]:
        joins = "LEFT JOIN generated_content gc ON gc.id = pe.content_id"
        gc_columns = schema["generated_content"]
        published_clauses = []
        if "published" in gc_columns:
            published_clauses.append("gc.published = 1")
        if "published_at" in gc_columns:
            published_clauses.append("gc.published_at IS NOT NULL")
        if "published_url" in gc_columns:
            published_clauses.append("gc.published_url IS NOT NULL")
        if published_clauses:
            where.append(f"(gc.id IS NULL OR {' OR '.join(published_clauses)})")

    rows = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM post_engagement pe
            {joins}
            WHERE {' AND '.join(where)}
            ORDER BY pe.tweet_id ASC, datetime(pe.fetched_at) ASC{_id_order(columns, 'pe')}""",
        (cutoff.isoformat(),),
    ).fetchall()
    return [
        EngagementMetricSnapshot(
            post_id=str(row["post_id"]),
            content_id=_int_or_none(row["content_id"]),
            fetched_at=str(row["fetched_at"]),
            likes=_non_negative_int(row["likes"]),
            replies=_non_negative_int(row["replies"]),
            reposts=_non_negative_int(row["reposts"]),
            impressions=_non_negative_int(row["impressions"]),
            bookmarks=_non_negative_int(row["bookmarks"]),
        )
        for row in rows
        if _parse_timestamp(row["fetched_at"]) is not None
    ]


def _anomaly(
    anomaly_type: str,
    severity: str,
    previous: EngagementMetricSnapshot,
    current: EngagementMetricSnapshot,
    metric: str,
    delta: int,
    elapsed_hours: float | None,
    threshold: float | None,
    rate_per_hour: float | None,
    reason: str,
) -> EngagementMetricAnomaly:
    return EngagementMetricAnomaly(
        anomaly_type=anomaly_type,
        severity=severity,
        post_id=current.post_id,
        content_id=current.content_id or previous.content_id,
        metric=metric,
        previous_fetched_at=previous.fetched_at,
        current_fetched_at=current.fetched_at,
        previous_value=previous.metric_value(metric),
        current_value=current.metric_value(metric),
        delta=delta,
        elapsed_hours=elapsed_hours,
        threshold=threshold,
        rate_per_hour=rate_per_hour,
        reason=reason,
    )


def _normalize_snapshot(snapshot: EngagementMetricSnapshot | Mapping[str, Any]) -> EngagementMetricSnapshot:
    if isinstance(snapshot, EngagementMetricSnapshot):
        return snapshot
    post_id = snapshot.get("post_id") or snapshot.get("tweet_id") or snapshot.get("platform_post_id")
    if post_id is None:
        raise ValueError("snapshot post_id is required")
    fetched_at = snapshot.get("fetched_at")
    if fetched_at is None:
        raise ValueError("snapshot fetched_at is required")
    return EngagementMetricSnapshot(
        post_id=str(post_id),
        content_id=_int_or_none(snapshot.get("content_id")),
        fetched_at=str(fetched_at),
        likes=_non_negative_int(snapshot.get("likes", snapshot.get("like_count", 0))),
        replies=_non_negative_int(snapshot.get("replies", snapshot.get("reply_count", 0))),
        reposts=_non_negative_int(
            snapshot.get("reposts", snapshot.get("repost_count", snapshot.get("retweet_count", 0)))
        ),
        impressions=_non_negative_int(snapshot.get("impressions", snapshot.get("impression_count", 0))),
        bookmarks=_non_negative_int(snapshot.get("bookmarks", snapshot.get("bookmark_count", 0))),
    )


def _thresholds(defaults: Mapping[str, float], overrides: Mapping[str, float] | None) -> dict[str, float]:
    thresholds = {metric: float(defaults[metric]) for metric in METRIC_NAMES}
    for metric, value in (overrides or {}).items():
        if metric not in thresholds:
            raise ValueError(f"unknown engagement metric: {metric}")
        if value < 0:
            raise ValueError("thresholds must be non-negative")
        thresholds[metric] = float(value)
    return thresholds


def _totals(
    snapshots: list[EngagementMetricSnapshot],
    anomalies: Iterable[EngagementMetricAnomaly],
) -> dict[str, Any]:
    anomaly_list = list(anomalies)
    return {
        "anomalies": len(anomaly_list),
        "by_anomaly_type": dict(Counter(item.anomaly_type for item in anomaly_list)),
        "by_metric": dict(Counter(item.metric for item in anomaly_list)),
        "by_severity": dict(Counter(item.severity for item in anomaly_list)),
        "posts_scanned": len({snapshot.post_id for snapshot in snapshots}),
        "snapshots_scanned": len(snapshots),
    }


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    required = {"post_engagement": {"tweet_id", "fetched_at"}}
    return {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    schema: dict[str, set[str]] = {}
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {info[1] for info in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _column_expr(
    columns: set[str],
    column: str,
    alias: str,
    output: str,
    *,
    default: str = "0",
) -> str:
    if column in columns:
        return f"{alias}.{column} AS {output}"
    return f"{default} AS {output}"


def _id_order(columns: set[str], alias: str) -> str:
    return f", {alias}.id ASC" if "id" in columns else ""


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _elapsed_hours(previous: str, current: str) -> float | None:
    previous_at = _parse_timestamp(previous)
    current_at = _parse_timestamp(current)
    if previous_at is None or current_at is None:
        return None
    return (current_at - previous_at).total_seconds() / 3600


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _non_negative_int(value: Any) -> int:
    if value is None:
        return 0
    return max(int(value), 0)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _anomaly_sort_key(anomaly: EngagementMetricAnomaly) -> tuple[int, str, str, str]:
    severity_rank = {"critical": 0, "high": 1, "warning": 2}
    return (
        severity_rank.get(anomaly.severity, 9),
        anomaly.post_id,
        anomaly.current_fetched_at,
        anomaly.metric,
    )


def _sorted_totals(totals: dict[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key in sorted(totals):
        value = totals[key]
        result[key] = dict(sorted(value.items())) if isinstance(value, dict) else value
    return result
