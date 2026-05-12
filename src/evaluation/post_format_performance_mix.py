"""Analyze engagement performance mix by generated content format."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_MIN_SAMPLES = 3
DEFAULT_DELTA = 0.2


@dataclass(frozen=True)
class PostFormatPerformanceRow:
    content_format: str
    sample_count: int
    average_engagement_score: float
    classification: str
    content_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["content_ids"] = list(self.content_ids)
        return payload


@dataclass(frozen=True)
class PostFormatPerformanceMixReport:
    generated_at: str
    filters: dict[str, Any]
    overall_average_engagement_score: float
    scored_formats: tuple[PostFormatPerformanceRow, ...]
    underused_formats: tuple[PostFormatPerformanceRow, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "post_format_performance_mix",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "overall_average_engagement_score": self.overall_average_engagement_score,
            "scored_formats": [row.to_dict() for row in self.scored_formats],
            "totals": {
                "scored_format_count": len(self.scored_formats),
                "underused_format_count": len(self.underused_formats),
            },
            "underused_formats": [row.to_dict() for row in self.underused_formats],
        }


def build_post_format_performance_mix_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_samples: int = DEFAULT_MIN_SAMPLES,
    delta: float = DEFAULT_DELTA,
    now: datetime | None = None,
) -> PostFormatPerformanceMixReport:
    """Compare average engagement by content_format for published posts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_samples <= 0:
        raise ValueError("min_samples must be positive")
    if delta < 0:
        raise ValueError("delta must be non-negative")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {"days": days, "min_samples": min_samples, "delta": delta}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or _has_required_gaps(missing_columns):
        return PostFormatPerformanceMixReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            overall_average_engagement_score=0.0,
            scored_formats=(),
            underused_formats=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    records = _published_latest_engagement_records(conn, schema, days=days, now=generated_at)
    format_buckets: dict[str, list[dict[str, Any]]] = {}
    for record in records:
        content_format = _clean(record["content_format"]) or "unknown"
        format_buckets.setdefault(content_format, []).append(record)

    all_scores = [float(record["engagement_score"] or 0.0) for record in records]
    overall_average = _round(sum(all_scores) / len(all_scores)) if all_scores else 0.0
    scored: list[PostFormatPerformanceRow] = []
    underused: list[PostFormatPerformanceRow] = []
    for content_format, bucket in format_buckets.items():
        scores = [float(record["engagement_score"] or 0.0) for record in bucket]
        average = _round(sum(scores) / len(scores)) if scores else 0.0
        content_ids = tuple(sorted(int(record["content_id"]) for record in bucket))
        if len(bucket) < min_samples:
            underused.append(
                PostFormatPerformanceRow(
                    content_format=content_format,
                    sample_count=len(bucket),
                    average_engagement_score=average,
                    classification="underused",
                    content_ids=content_ids,
                )
            )
            continue
        scored.append(
            PostFormatPerformanceRow(
                content_format=content_format,
                sample_count=len(bucket),
                average_engagement_score=average,
                classification=_classification(average, overall_average, delta=delta),
                content_ids=content_ids,
            )
        )

    scored.sort(key=lambda row: (-row.average_engagement_score, row.content_format))
    underused.sort(key=lambda row: (row.sample_count, row.content_format))
    return PostFormatPerformanceMixReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        overall_average_engagement_score=overall_average,
        scored_formats=tuple(scored),
        underused_formats=tuple(underused),
        missing_columns=missing_columns,
    )


def format_post_format_performance_mix_json(report: PostFormatPerformanceMixReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_post_format_performance_mix_text(report: PostFormatPerformanceMixReport) -> str:
    lines = [
        "Post Format Performance Mix",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={report.filters['days']} "
            f"min_samples={report.filters['min_samples']} "
            f"delta={report.filters['delta']}"
        ),
        f"Overall average engagement: {report.overall_average_engagement_score:.3f}",
        "",
        "Scored formats:",
    ]
    if report.scored_formats:
        for row in report.scored_formats:
            lines.append(
                f"- {row.content_format}: samples={row.sample_count} "
                f"avg={row.average_engagement_score:.3f} class={row.classification}"
            )
    else:
        lines.append("- none")
    lines.append("")
    lines.append("Underused formats:")
    if report.underused_formats:
        for row in report.underused_formats:
            lines.append(
                f"- {row.content_format}: samples={row.sample_count} "
                f"avg={row.average_engagement_score:.3f}"
            )
    else:
        lines.append("- none")
    return "\n".join(lines)


def _published_latest_engagement_records(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
    gc_columns = schema["generated_content"]
    cp_columns = schema["content_publications"]
    cutoff = (now - timedelta(days=days)).isoformat()
    published_terms = ["cp.status = 'published'"]
    if "published" in gc_columns:
        published_terms.append("gc.published = 1")
    published_at_expr = "cp.published_at"
    if "published_at" in gc_columns:
        published_at_expr = "COALESCE(cp.published_at, gc.published_at)"
    elif "published_at" not in cp_columns:
        published_at_expr = "gc.created_at"

    rows = conn.execute(
        f"""WITH latest_engagement AS (
               SELECT content_id, engagement_score, fetched_at
               FROM (
                   SELECT
                       content_id,
                       engagement_score,
                       fetched_at,
                       ROW_NUMBER() OVER (
                           PARTITION BY content_id
                           ORDER BY fetched_at DESC, id DESC
                       ) AS rn
                   FROM post_engagement
                   WHERE engagement_score IS NOT NULL
               )
               WHERE rn = 1
           )
           SELECT
               gc.id AS content_id,
               gc.content_format AS content_format,
               le.engagement_score AS engagement_score,
               le.fetched_at AS fetched_at,
               {published_at_expr} AS published_at
           FROM generated_content gc
           INNER JOIN latest_engagement le ON le.content_id = gc.id
           LEFT JOIN content_publications cp
             ON cp.content_id = gc.id
            AND cp.status = 'published'
           WHERE gc.content_format IS NOT NULL
             AND trim(gc.content_format) != ''
             AND ({" OR ".join(published_terms)})
             AND ({published_at_expr} IS NULL OR {published_at_expr} >= ?)
           GROUP BY gc.id
           ORDER BY gc.id ASC""",
        (cutoff,),
    ).fetchall()
    return [dict(row) for row in rows]


def _classification(average: float, overall_average: float, *, delta: float) -> str:
    if average >= overall_average + delta:
        return "overperforming"
    if average <= overall_average - delta:
        return "underperforming"
    return "stable"


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "generated_content": ("id", "content_format"),
        "content_publications": ("content_id", "status"),
        "post_engagement": ("content_id", "engagement_score", "fetched_at", "id"),
    }
    missing_tables = tuple(table for table in required if table not in schema)
    missing_columns = {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema
    }
    return missing_tables, {table: cols for table, cols in missing_columns.items() if cols}


def _has_required_gaps(missing_columns: dict[str, tuple[str, ...]]) -> bool:
    return any(missing_columns.values())


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in tables:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _round(value: float) -> float:
    return round(value, 6)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
