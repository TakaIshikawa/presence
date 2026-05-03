"""Compare recent engagement resonance against an earlier baseline window."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_RECENT_DAYS = 14
DEFAULT_BASELINE_DAYS = 28
DEFAULT_BUCKET_DAYS = 7

ENGAGEMENT_TABLES = {
    "x": "post_engagement",
    "bluesky": "bluesky_engagement",
    "linkedin": "linkedin_engagement",
    "mastodon": "mastodon_engagement",
}
RESONATED = "resonated"
LOW_RESONANCE = "low_resonance"


@dataclass(frozen=True)
class ResonanceWindowMetrics:
    """Aggregate engagement and resonance metrics for one time window."""

    name: str
    start: str
    end: str
    row_count: int
    content_count: int
    average_engagement_score: float | None
    labeled_count: int
    resonated_count: int
    low_resonance_count: int
    resonance_rate: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResonanceMetricDrift:
    """Recent-vs-baseline drift for one metric."""

    metric: str
    baseline: float | None
    recent: float | None
    absolute_drift: float | None
    percent_drift: float | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ResonanceDriftReport:
    """Read-only resonance drift report."""

    generated_at: str
    filters: dict[str, Any]
    status: str
    recent: ResonanceWindowMetrics
    baseline: ResonanceWindowMetrics
    drift: tuple[ResonanceMetricDrift, ...]
    buckets: tuple[ResonanceWindowMetrics, ...]
    totals: dict[str, Any]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "resonance_drift",
            "baseline": self.baseline.to_dict(),
            "buckets": [bucket.to_dict() for bucket in self.buckets],
            "drift": [item.to_dict() for item in self.drift],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "recent": self.recent.to_dict(),
            "status": self.status,
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _Outcome:
    content_id: int | None
    platform: str
    engagement_score: float
    fetched_at: datetime
    auto_quality: str | None


def build_resonance_drift_report(
    db_or_conn: Any,
    *,
    recent_days: int = DEFAULT_RECENT_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    bucket_days: int = DEFAULT_BUCKET_DAYS,
    now: datetime | None = None,
) -> ResonanceDriftReport:
    """Build a deterministic read-only report for engagement resonance drift."""
    if recent_days <= 0:
        raise ValueError("recent_days must be positive")
    if baseline_days <= 0:
        raise ValueError("baseline_days must be positive")
    if bucket_days <= 0:
        raise ValueError("bucket_days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    recent_start = generated_at - timedelta(days=recent_days)
    baseline_start = recent_start - timedelta(days=baseline_days)

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(
        sorted(table for table in ENGAGEMENT_TABLES.values() if table not in schema)
    )
    missing_columns = _missing_columns(schema)
    outcomes = _load_outcomes(conn, schema, start=baseline_start, end=generated_at)

    recent_rows = [
        row for row in outcomes if recent_start <= row.fetched_at < generated_at
    ]
    baseline_rows = [
        row for row in outcomes if baseline_start <= row.fetched_at < recent_start
    ]
    recent = _metrics("recent", recent_start, generated_at, recent_rows)
    baseline = _metrics("baseline", baseline_start, recent_start, baseline_rows)
    drift = (
        _metric_drift(
            "average_engagement_score",
            baseline.average_engagement_score,
            recent.average_engagement_score,
        ),
        _metric_drift("resonance_rate", baseline.resonance_rate, recent.resonance_rate),
    )
    buckets = tuple(
        _bucket_metrics(outcomes, start=baseline_start, end=generated_at, bucket_days=bucket_days)
    )
    status = _overall_status(recent, baseline, drift)

    return ResonanceDriftReport(
        generated_at=generated_at.isoformat(),
        filters={
            "baseline_days": baseline_days,
            "baseline_end": recent_start.isoformat(),
            "baseline_start": baseline_start.isoformat(),
            "bucket_days": bucket_days,
            "recent_days": recent_days,
            "recent_end": generated_at.isoformat(),
            "recent_start": recent_start.isoformat(),
        },
        status=status,
        recent=recent,
        baseline=baseline,
        drift=drift,
        buckets=buckets,
        totals={
            "baseline_rows": baseline.row_count,
            "bucket_count": len(buckets),
            "outcome_rows": len(outcomes),
            "recent_rows": recent.row_count,
            "unique_content": len(
                {row.content_id for row in outcomes if row.content_id is not None}
            ),
        },
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_resonance_drift_json(report: ResonanceDriftReport) -> str:
    """Serialize a resonance drift report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _load_outcomes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    start: datetime,
    end: datetime,
) -> list[_Outcome]:
    outcomes: list[_Outcome] = []
    for platform, table in ENGAGEMENT_TABLES.items():
        if table not in schema:
            continue
        columns = schema[table]
        required = {"content_id", "engagement_score", "fetched_at"}
        if not required.issubset(columns):
            continue
        outcomes.extend(
            _load_platform_outcomes(
                conn,
                schema,
                table=table,
                platform=platform,
                start=start,
                end=end,
            )
        )
    outcomes.sort(key=lambda row: (row.fetched_at, row.platform, row.content_id or -1))
    return outcomes


def _load_platform_outcomes(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    table: str,
    platform: str,
    start: datetime,
    end: datetime,
) -> list[_Outcome]:
    columns = schema[table]
    has_generated = "generated_content" in schema and "id" in schema["generated_content"]
    gc_columns = schema.get("generated_content", set())
    joins = ""
    content_columns = "NULL AS auto_quality"
    published_filter = ""
    if has_generated:
        joins = "LEFT JOIN generated_content gc ON gc.id = e.content_id"
        content_columns = _column_expr(gc_columns, "auto_quality", "gc", "auto_quality", default="NULL")
        predicate = _published_predicate(schema, platform)
        if predicate:
            published_filter = f" AND (gc.id IS NULL OR {predicate})"

    rows = conn.execute(
        f"""SELECT e.content_id AS content_id,
                   ? AS platform,
                   e.engagement_score AS engagement_score,
                   e.fetched_at AS fetched_at,
                   {content_columns}
            FROM {table} e
            {joins}
            WHERE e.engagement_score IS NOT NULL
              AND datetime(e.fetched_at) >= datetime(?)
              AND datetime(e.fetched_at) < datetime(?)
              {published_filter}
            ORDER BY platform ASC, e.content_id ASC, datetime(e.fetched_at) ASC{_id_order(columns, 'e')}""",
        (platform, start.isoformat(), end.isoformat()),
    ).fetchall()
    outcomes = []
    for row in rows:
        fetched_at = _parse_timestamp(row["fetched_at"])
        if fetched_at is None:
            continue
        outcomes.append(
            _Outcome(
                content_id=_int_or_none(row["content_id"]),
                platform=str(row["platform"]),
                engagement_score=float(row["engagement_score"]),
                fetched_at=fetched_at,
                auto_quality=_clean_label(row["auto_quality"]),
            )
        )
    return outcomes


def _published_predicate(schema: dict[str, set[str]], platform: str) -> str:
    gc = schema.get("generated_content", set())
    predicates: list[str] = []
    if "published" in gc:
        predicates.append("gc.published = 1")
    if "published_at" in gc:
        predicates.append("gc.published_at IS NOT NULL")
    if "published_url" in gc:
        predicates.append("gc.published_url IS NOT NULL")
    if "content_publications" in schema:
        cp = schema["content_publications"]
        if {"content_id", "platform", "status"}.issubset(cp):
            predicates.append(
                "EXISTS ("
                "SELECT 1 FROM content_publications cp "
                "WHERE cp.content_id = gc.id "
                f"AND cp.platform = '{platform}' "
                "AND cp.status = 'published'"
                ")"
            )
    return " OR ".join(predicates)


def _metrics(
    name: str,
    start: datetime,
    end: datetime,
    rows: Iterable[_Outcome],
) -> ResonanceWindowMetrics:
    row_list = _latest_per_content_platform(rows)
    labels = [row.auto_quality for row in row_list if row.auto_quality]
    resonated = sum(1 for label in labels if label == RESONATED)
    low_resonance = sum(1 for label in labels if label == LOW_RESONANCE)
    scores = [row.engagement_score for row in row_list]
    return ResonanceWindowMetrics(
        name=name,
        start=start.isoformat(),
        end=end.isoformat(),
        row_count=len(row_list),
        content_count=len({row.content_id for row in row_list if row.content_id is not None}),
        average_engagement_score=_average(scores),
        labeled_count=len(labels),
        resonated_count=resonated,
        low_resonance_count=low_resonance,
        resonance_rate=round(resonated / len(labels), 3) if labels else None,
    )


def _latest_per_content_platform(rows: Iterable[_Outcome]) -> list[_Outcome]:
    latest: dict[tuple[str, int | None], _Outcome] = {}
    for row in rows:
        key = (row.platform, row.content_id)
        previous = latest.get(key)
        if previous is None or row.fetched_at >= previous.fetched_at:
            latest[key] = row
    return list(latest.values())


def _bucket_metrics(
    rows: list[_Outcome],
    *,
    start: datetime,
    end: datetime,
    bucket_days: int,
) -> list[ResonanceWindowMetrics]:
    buckets: list[ResonanceWindowMetrics] = []
    cursor = start
    index = 1
    while cursor < end:
        bucket_end = min(cursor + timedelta(days=bucket_days), end)
        bucket_rows = [row for row in rows if cursor <= row.fetched_at < bucket_end]
        buckets.append(_metrics(f"bucket_{index}", cursor, bucket_end, bucket_rows))
        cursor = bucket_end
        index += 1
    return buckets


def _metric_drift(
    metric: str,
    baseline: float | None,
    recent: float | None,
) -> ResonanceMetricDrift:
    if baseline is None or recent is None:
        return ResonanceMetricDrift(
            metric=metric,
            baseline=baseline,
            recent=recent,
            absolute_drift=None,
            percent_drift=None,
            status="insufficient_data",
        )
    absolute = round(recent - baseline, 3)
    percent = round((absolute / baseline) * 100, 3) if baseline else None
    if absolute > 0:
        status = "improved"
    elif absolute < 0:
        status = "declined"
    else:
        status = "flat"
    return ResonanceMetricDrift(
        metric=metric,
        baseline=baseline,
        recent=recent,
        absolute_drift=absolute,
        percent_drift=percent,
        status=status,
    )


def _overall_status(
    recent: ResonanceWindowMetrics,
    baseline: ResonanceWindowMetrics,
    drift: tuple[ResonanceMetricDrift, ...],
) -> str:
    if recent.row_count == 0 and baseline.row_count == 0:
        return "no_data"
    if recent.row_count == 0:
        return "no_recent_data"
    if baseline.row_count == 0:
        return "no_baseline_data"
    if any(item.status == "declined" for item in drift):
        return "declined"
    if any(item.status == "improved" for item in drift):
        return "improved"
    return "flat"


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    required = {
        table: {"content_id", "engagement_score", "fetched_at"}
        for table in ENGAGEMENT_TABLES.values()
    }
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
    default: str = "NULL",
) -> str:
    if column in columns:
        return f"{alias}.{column} AS {output}"
    return f"{default} AS {output}"


def _id_order(columns: set[str], alias: str) -> str:
    return f", {alias}.id DESC" if "id" in columns else ""


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 3)


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


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean_label(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    return text or None
