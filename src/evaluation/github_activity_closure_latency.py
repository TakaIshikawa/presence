"""Summarize GitHub issue and pull request closure latency."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from statistics import median
from typing import Any, Mapping, Sequence


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 10
SUPPORTED_ACTIVITY_TYPES = ("issue", "pull_request")
ACTIVITY_TYPE_ALL = "all"


@dataclass(frozen=True)
class GitHubActivityLatencyMetrics:
    """Lifecycle latency metrics for one repo/activity type pair."""

    repo: str
    activity_type: str
    total_count: int
    open_count: int
    closed_count: int
    merged_count: int
    missing_created_at_count: int
    open_age_days_median: float | None
    open_age_days_p90: float | None
    close_latency_days_median: float | None
    close_latency_days_p90: float | None
    merge_latency_days_median: float | None
    merge_latency_days_p90: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "activity_type": self.activity_type,
            "close_latency_days_median": self.close_latency_days_median,
            "close_latency_days_p90": self.close_latency_days_p90,
            "closed_count": self.closed_count,
            "merge_latency_days_median": self.merge_latency_days_median,
            "merge_latency_days_p90": self.merge_latency_days_p90,
            "merged_count": self.merged_count,
            "missing_created_at_count": self.missing_created_at_count,
            "open_age_days_median": self.open_age_days_median,
            "open_age_days_p90": self.open_age_days_p90,
            "open_count": self.open_count,
            "repo": self.repo,
            "total_count": self.total_count,
        }


@dataclass(frozen=True)
class StaleGitHubActivityItem:
    """Representative stale open GitHub activity."""

    repo: str
    activity_type: str
    number: str
    title: str
    url: str | None
    age_days: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "activity_type": self.activity_type,
            "age_days": self.age_days,
            "number": self.number,
            "repo": self.repo,
            "title": self.title,
            "url": self.url,
        }


@dataclass(frozen=True)
class GitHubActivityClosureLatencyReport:
    """Read-only GitHub activity closure latency report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    metrics: tuple[GitHubActivityLatencyMetrics, ...]
    stale_open_items: tuple[StaleGitHubActivityItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "github_activity_closure_latency",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "metrics": [item.to_dict() for item in self.metrics],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "stale_open_items": [item.to_dict() for item in self.stale_open_items],
            "totals": dict(self.totals),
        }


def build_github_activity_closure_latency_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    repo: str | None = None,
    activity_type: str = ACTIVITY_TYPE_ALL,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> GitHubActivityClosureLatencyReport:
    """Build a closure latency report for GitHub issues and pull requests."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    repo_filter = _clean_optional(repo, "repo")
    if activity_type not in (ACTIVITY_TYPE_ALL, *SUPPORTED_ACTIVITY_TYPES):
        raise ValueError("activity_type must be one of all, issue, pull_request")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    window_start = generated_at - timedelta(days=days)
    selected_types = (
        SUPPORTED_ACTIVITY_TYPES
        if activity_type == ACTIVITY_TYPE_ALL
        else (activity_type,)
    )
    filters = {
        "activity_type": activity_type,
        "days": days,
        "limit": limit,
        "repo": repo_filter,
        "window_end": generated_at.isoformat(),
        "window_start": window_start.isoformat(),
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return GitHubActivityClosureLatencyReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_empty_totals(),
            metrics=(),
            stale_open_items=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_rows(
        conn,
        activity_types=selected_types,
        window_start=window_start,
        repo=repo_filter,
    )
    metrics = _build_metrics(rows, generated_at=generated_at)
    stale_items = _stale_open_items(rows, generated_at=generated_at, limit=limit)
    totals = {
        "activity_count": len(rows),
        "closed_count": sum(item.closed_count for item in metrics),
        "group_count": len(metrics),
        "merged_count": sum(item.merged_count for item in metrics),
        "missing_created_at_count": sum(item.missing_created_at_count for item in metrics),
        "open_count": sum(item.open_count for item in metrics),
        "stale_open_item_count": len(stale_items),
    }

    return GitHubActivityClosureLatencyReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=totals,
        metrics=tuple(metrics),
        stale_open_items=tuple(stale_items),
    )


def format_github_activity_closure_latency_json(
    report: GitHubActivityClosureLatencyReport,
) -> str:
    """Serialize a closure latency report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_github_activity_closure_latency_text(
    report: GitHubActivityClosureLatencyReport,
) -> str:
    """Format a closure latency report for terminal review."""
    lines = [
        "GitHub Activity Closure Latency",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['window_start']} to {report.filters['window_end']} "
            f"({report.filters['days']} days)"
        ),
        f"Activity type: {report.filters['activity_type']}",
        f"Limit: {report.filters['limit']}",
    ]
    if report.filters.get("repo"):
        lines.append(f"Repo: {report.filters['repo']}")
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
        return "\n".join(lines)
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
        return "\n".join(lines)

    totals = report.totals
    lines.extend(
        [
            (
                "Totals: "
                f"activity={totals['activity_count']} "
                f"open={totals['open_count']} "
                f"closed={totals['closed_count']} "
                f"merged={totals['merged_count']} "
                f"missing_created_at={totals['missing_created_at_count']}"
            ),
            "",
        ]
    )
    if not report.metrics:
        lines.append("No GitHub issue or pull request activity found.")
        return "\n".join(lines)

    lines.append("Metrics:")
    for item in report.metrics:
        lines.append(
            "  - "
            f"{item.repo} {item.activity_type}: "
            f"total={item.total_count} open={item.open_count} "
            f"closed={item.closed_count} merged={item.merged_count} "
            f"open_age_median={_format_days(item.open_age_days_median)} "
            f"open_age_p90={_format_days(item.open_age_days_p90)} "
            f"close_median={_format_days(item.close_latency_days_median)} "
            f"close_p90={_format_days(item.close_latency_days_p90)} "
            f"merge_median={_format_days(item.merge_latency_days_median)} "
            f"merge_p90={_format_days(item.merge_latency_days_p90)}"
        )

    lines.append("")
    lines.append("Stale open items:")
    if not report.stale_open_items:
        lines.append("  None.")
    for item in report.stale_open_items:
        lines.append(
            "  - "
            f"{item.repo} {item.activity_type} #{item.number} "
            f"age={_format_days(item.age_days)} {item.title} ({item.url or '-'})"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3.Connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        name = str(row["name"])
        schema[name] = {
            str(column["name"]) for column in conn.execute(f"PRAGMA table_info({name})")
        }
    return schema


def _schema_gaps(
    schema: Mapping[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "github_activity": (
            "repo_name",
            "activity_type",
            "number",
            "title",
            "state",
            "url",
            "updated_at",
            "created_at_github",
            "closed_at",
            "merged_at",
        )
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema
        and any(column not in schema.get(table, set()) for column in columns)
    }
    return missing_tables, missing_columns


def _load_rows(
    conn: sqlite3.Connection,
    *,
    activity_types: Sequence[str],
    window_start: datetime,
    repo: str | None,
) -> list[dict[str, Any]]:
    placeholders = ", ".join("?" for _ in activity_types)
    where = [
        f"activity_type IN ({placeholders})",
        "(updated_at >= ? OR closed_at >= ? OR merged_at >= ?)",
    ]
    params: list[Any] = [
        *activity_types,
        window_start.isoformat(),
        window_start.isoformat(),
        window_start.isoformat(),
    ]
    if repo:
        where.append("repo_name = ?")
        params.append(repo)
    cursor = conn.execute(
        f"""SELECT repo_name, activity_type, number, title, state, url,
                  updated_at, created_at_github, closed_at, merged_at
             FROM github_activity
             WHERE {' AND '.join(where)}
             ORDER BY repo_name ASC, activity_type ASC, updated_at DESC, number ASC""",
        tuple(params),
    )
    return [dict(row) for row in cursor.fetchall()]


def _build_metrics(
    rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
) -> list[GitHubActivityLatencyMetrics]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((str(row["repo_name"]), str(row["activity_type"])), []).append(row)

    metrics: list[GitHubActivityLatencyMetrics] = []
    for (repo, activity_type), group_rows in sorted(groups.items()):
        open_ages: list[float] = []
        close_latencies: list[float] = []
        merge_latencies: list[float] = []
        missing_created = 0
        open_count = 0
        closed_count = 0
        merged_count = 0

        for row in group_rows:
            created_at = _parse_datetime(row.get("created_at_github"))
            if created_at is None:
                missing_created += 1
            state = str(row.get("state") or "").lower()
            is_open = state == "open" and not row.get("closed_at") and not row.get("merged_at")
            if is_open:
                open_count += 1
                if created_at is not None:
                    open_ages.append(_days_between(created_at, generated_at))
            if activity_type == "issue" and (closed_at := _parse_datetime(row.get("closed_at"))):
                closed_count += 1
                if created_at is not None:
                    close_latencies.append(_days_between(created_at, closed_at))
            if activity_type == "pull_request" and (merged_at := _parse_datetime(row.get("merged_at"))):
                merged_count += 1
                if created_at is not None:
                    merge_latencies.append(_days_between(created_at, merged_at))

        metrics.append(
            GitHubActivityLatencyMetrics(
                repo=repo,
                activity_type=activity_type,
                total_count=len(group_rows),
                open_count=open_count,
                closed_count=closed_count,
                merged_count=merged_count,
                missing_created_at_count=missing_created,
                open_age_days_median=_percentile(open_ages, 50),
                open_age_days_p90=_percentile(open_ages, 90),
                close_latency_days_median=_percentile(close_latencies, 50),
                close_latency_days_p90=_percentile(close_latencies, 90),
                merge_latency_days_median=_percentile(merge_latencies, 50),
                merge_latency_days_p90=_percentile(merge_latencies, 90),
            )
        )
    return metrics


def _stale_open_items(
    rows: list[dict[str, Any]],
    *,
    generated_at: datetime,
    limit: int,
) -> list[StaleGitHubActivityItem]:
    items: list[StaleGitHubActivityItem] = []
    for row in rows:
        state = str(row.get("state") or "").lower()
        if state != "open" or row.get("closed_at") or row.get("merged_at"):
            continue
        created_at = _parse_datetime(row.get("created_at_github"))
        age_days = _days_between(created_at, generated_at) if created_at else None
        items.append(
            StaleGitHubActivityItem(
                repo=str(row["repo_name"]),
                activity_type=str(row["activity_type"]),
                number=str(row["number"]),
                title=str(row["title"] or ""),
                url=row.get("url"),
                age_days=age_days,
            )
        )
    return sorted(
        items,
        key=lambda item: (
            item.age_days is None,
            -(item.age_days or 0),
            item.repo,
            item.activity_type,
            item.number,
        ),
    )[:limit]


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _days_between(start: datetime, end: datetime) -> float:
    return round(max((end - start).total_seconds(), 0) / 86400, 2)


def _percentile(values: list[float], percentile: int) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if percentile == 50:
        return round(float(median(ordered)), 2)
    if len(ordered) < 2:
        return None
    rank = (len(ordered) - 1) * (percentile / 100)
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 2)


def _format_days(value: float | None) -> str:
    return "null" if value is None else f"{value:.2f}d"


def _clean_optional(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{name} must not be blank")
    return cleaned


def _empty_totals() -> dict[str, Any]:
    return {
        "activity_count": 0,
        "closed_count": 0,
        "group_count": 0,
        "merged_count": 0,
        "missing_created_at_count": 0,
        "open_count": 0,
        "stale_open_item_count": 0,
    }
