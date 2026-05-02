"""Report profile metrics ingestion coverage by platform."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_EXPECTED_INTERVAL_HOURS = 24.0
DEFAULT_MAX_STALE_HOURS = 48.0
REQUIRED_COLUMNS = {
    "platform",
    "follower_count",
    "tweet_count",
    "fetched_at",
}


@dataclass(frozen=True)
class ProfileMetricsCoverageRow:
    """Coverage summary for one profile metrics platform."""

    platform: str
    status: str
    sample_count: int
    first_sample_at: str | None
    latest_sample_at: str | None
    latest_sample_age_hours: float | None
    max_gap_hours: float | None
    follower_delta: int | None
    tweet_count_delta: int | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ProfileMetricsIngestionCoverageReport:
    """Profile metrics ingestion health report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ProfileMetricsCoverageRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    @property
    def has_issues(self) -> bool:
        return any(row.status != "fresh" for row in self.rows) or bool(
            self.missing_tables or self.missing_columns
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "profile_metrics_ingestion_coverage",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "has_issues": self.has_issues,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_profile_metrics_ingestion_coverage_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str | None = None,
    expected_interval_hours: float = DEFAULT_EXPECTED_INTERVAL_HOURS,
    max_stale_hours: float = DEFAULT_MAX_STALE_HOURS,
    now: datetime | None = None,
) -> ProfileMetricsIngestionCoverageReport:
    """Return profile metrics polling coverage grouped by platform."""
    if days <= 0:
        raise ValueError("days must be positive")
    if expected_interval_hours <= 0:
        raise ValueError("expected_interval_hours must be positive")
    if max_stale_hours <= 0:
        raise ValueError("max_stale_hours must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "cutoff": cutoff.isoformat(),
        "expected_interval_hours": _round_hours(expected_interval_hours),
        "max_stale_hours": _round_hours(max_stale_hours),
        "platform": platform,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_rows(conn, cutoff=cutoff, platform=platform)
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row["platform"]), []).append(row)

    coverage_rows = tuple(
        _coverage_row(
            platform_name,
            platform_rows,
            now=generated_at,
            expected_interval_hours=expected_interval_hours,
            max_stale_hours=max_stale_hours,
        )
        for platform_name, platform_rows in sorted(grouped.items())
    )
    status_counts = {
        status: sum(1 for row in coverage_rows if row.status == status)
        for status in ("fresh", "sparse", "stale")
    }
    return ProfileMetricsIngestionCoverageReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "platform_count": len(coverage_rows),
            "sample_count": len(rows),
            **status_counts,
        },
        rows=coverage_rows,
        missing_tables=(),
        missing_columns={},
    )


def format_profile_metrics_ingestion_coverage_json(
    report: ProfileMetricsIngestionCoverageReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_profile_metrics_ingestion_coverage_text(
    report: ProfileMetricsIngestionCoverageReport,
) -> str:
    """Render profile metrics ingestion coverage for operators."""
    totals = report.totals
    lines = [
        "Profile Metrics Ingestion Coverage",
        f"Generated: {report.generated_at}",
        (
            f"Window: {report.filters['days']} days "
            f"platform={report.filters['platform'] or 'all'} "
            f"expected_interval_hours={report.filters['expected_interval_hours']} "
            f"max_stale_hours={report.filters['max_stale_hours']}"
        ),
        (
            "Totals: "
            f"platforms={totals['platform_count']} "
            f"samples={totals['sample_count']} "
            f"fresh={totals['fresh']} "
            f"sparse={totals['sparse']} "
            f"stale={totals['stale']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in report.missing_columns.items()
        ]
        lines.append(f"Missing columns: {'; '.join(missing)}")
    lines.append("")

    if not report.rows:
        lines.append("No profile metrics samples found.")
        return "\n".join(lines)

    lines.append("Coverage rows:")
    for row in report.rows:
        lines.append(
            f"  - platform={row.platform} status={row.status} "
            f"samples={row.sample_count} "
            f"latest_age_hours={_display(row.latest_sample_age_hours)} "
            f"max_gap_hours={_display(row.max_gap_hours)} "
            f"follower_delta={_display(row.follower_delta)} "
            f"tweet_count_delta={_display(row.tweet_count_delta)} "
            f"latest={row.latest_sample_at or '-'}"
        )
    return "\n".join(lines)


def _coverage_row(
    platform: str,
    rows: list[dict[str, Any]],
    *,
    now: datetime,
    expected_interval_hours: float,
    max_stale_hours: float,
) -> ProfileMetricsCoverageRow:
    ordered = sorted(rows, key=lambda row: _parse_datetime(row["fetched_at"]))
    first = ordered[0]
    latest = ordered[-1]
    first_at = _parse_datetime(first["fetched_at"])
    latest_at = _parse_datetime(latest["fetched_at"])
    latest_age_hours = _hours_between(latest_at, now)
    gaps = [
        _hours_between(
            _parse_datetime(previous["fetched_at"]),
            _parse_datetime(current["fetched_at"]),
        )
        for previous, current in zip(ordered, ordered[1:])
    ]
    max_gap_hours = max(gaps) if gaps else None
    status = _status(
        sample_count=len(ordered),
        latest_sample_age_hours=latest_age_hours,
        max_gap_hours=max_gap_hours,
        expected_interval_hours=expected_interval_hours,
        max_stale_hours=max_stale_hours,
    )
    return ProfileMetricsCoverageRow(
        platform=platform,
        status=status,
        sample_count=len(ordered),
        first_sample_at=first_at.isoformat(),
        latest_sample_at=latest_at.isoformat(),
        latest_sample_age_hours=_round_hours(latest_age_hours),
        max_gap_hours=None if max_gap_hours is None else _round_hours(max_gap_hours),
        follower_delta=_int_delta(first.get("follower_count"), latest.get("follower_count")),
        tweet_count_delta=_int_delta(first.get("tweet_count"), latest.get("tweet_count")),
    )


def _status(
    *,
    sample_count: int,
    latest_sample_age_hours: float,
    max_gap_hours: float | None,
    expected_interval_hours: float,
    max_stale_hours: float,
) -> str:
    if latest_sample_age_hours > max_stale_hours:
        return "stale"
    if sample_count < 2:
        return "sparse"
    if max_gap_hours is not None and max_gap_hours > expected_interval_hours:
        return "sparse"
    return "fresh"


def _load_rows(
    conn: sqlite3.Connection,
    *,
    cutoff: datetime,
    platform: str | None,
) -> list[dict[str, Any]]:
    params: list[Any] = []
    where = ""
    if platform:
        where = " WHERE platform = ?"
        params.append(platform)
    sql = (
        "SELECT platform, follower_count, tweet_count, fetched_at "
        f"FROM profile_metrics{where} "
        "ORDER BY platform ASC, fetched_at ASC"
    )
    rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    return [row for row in rows if _parse_datetime(row["fetched_at"]) >= cutoff]


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    if "profile_metrics" not in schema:
        return ("profile_metrics",), {}
    missing = REQUIRED_COLUMNS - schema["profile_metrics"]
    missing_columns = (
        {"profile_metrics": tuple(sorted(missing))}
        if missing
        else {}
    )
    return (), missing_columns


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ProfileMetricsIngestionCoverageReport:
    return ProfileMetricsIngestionCoverageReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "platform_count": 0,
            "sample_count": 0,
            "fresh": 0,
            "sparse": 0,
            "stale": 0,
        },
        rows=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema


def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _hours_between(start: datetime, end: datetime) -> float:
    return (end - start).total_seconds() / 3600


def _round_hours(value: float) -> float:
    return round(float(value), 2)


def _int_delta(start: Any, end: Any) -> int | None:
    if start is None or end is None:
        return None
    return int(end) - int(start)


def _display(value: Any) -> str:
    return "n/a" if value is None else str(value)
