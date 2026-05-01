"""Report downstream outcomes for durable content variants."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from statistics import mean
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_MIN_SAMPLE = 3
LOW_ENGAGEMENT_SCORE = 5.0

ENGAGEMENT_TABLES = {
    "x": "post_engagement",
    "twitter": "post_engagement",
    "bluesky": "bluesky_engagement",
    "linkedin": "linkedin_engagement",
    "mastodon": "mastodon_engagement",
}


@dataclass(frozen=True)
class ContentVariantOutcomeGroup:
    """Aggregate outcomes for one platform, variant type, and selection state."""

    platform: str
    variant_type: str
    selection_status: str
    variant_count: int
    published_count: int
    unpublished_count: int
    engagement_snapshot_count: int
    no_engagement_count: int
    low_engagement_count: int
    average_engagement_score: float | None
    publication_rate: float
    engagement_snapshot_rate: float
    low_engagement_rate: float
    sample_status: str
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContentVariantOutcomeReport:
    """Read-only A/B outcome report for content variants."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    groups: tuple[ContentVariantOutcomeGroup, ...]
    recommendations: tuple[str, ...]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "groups": [group.to_dict() for group in self.groups],
            "recommendations": list(self.recommendations),
            "availability": dict(sorted(self.availability.items())),
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_content_variant_outcome_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str | None = None,
    variant_type: str | None = None,
    min_sample: int = DEFAULT_MIN_SAMPLE,
    now: datetime | None = None,
) -> ContentVariantOutcomeReport:
    """Aggregate variant outcomes by platform/type and selected state."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_sample <= 0:
        raise ValueError("min_sample must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}

    variants = _load_variants(
        conn,
        schema,
        cutoff=cutoff,
        now=generated_at,
        platform=platform,
        variant_type=variant_type,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    publications, publications_available = _load_publications(
        conn,
        schema,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    engagement, engagement_availability = _load_latest_engagement(
        conn,
        schema,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )
    availability = {
        "content_variants": "content_variants" in schema,
        "content_publications": publications_available,
        **engagement_availability,
    }
    groups = _build_groups(
        variants=variants,
        publications=publications,
        engagement=engagement,
        min_sample=min_sample,
    )
    recommendations = _build_recommendations(groups, min_sample=min_sample)

    return ContentVariantOutcomeReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "platform": platform,
            "variant_type": variant_type,
            "min_sample": min_sample,
        },
        totals={
            "variant_count": len(variants),
            "selected_variant_count": sum(1 for row in variants if row["selected"]),
            "unselected_variant_count": sum(1 for row in variants if not row["selected"]),
            "published_variant_count": sum(
                1
                for row in variants
                if publications.get((row["content_id"], row["platform"]), {}).get("status")
                == "published"
            ),
            "engagement_snapshot_count": sum(
                1
                for row in variants
                if row["selected"]
                and (row["content_id"], row["platform"]) in engagement
            ),
            "group_count": len(groups),
            "weak_group_count": sum(
                1 for group in groups if group.recommendation == "review_variant_type"
            ),
        },
        groups=tuple(groups),
        recommendations=tuple(recommendations),
        availability=availability,
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_content_variant_outcome_json(report: ContentVariantOutcomeReport) -> str:
    """Serialize a variant outcome report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_variant_outcome_text(report: ContentVariantOutcomeReport) -> str:
    """Format a variant outcome report for terminal review."""
    lines = [
        "Content Variant Outcomes",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        f"Platform: {report.filters['platform'] or 'all'}",
        f"Variant type: {report.filters['variant_type'] or 'all'}",
        f"Minimum sample: {report.filters['min_sample']}",
        (
            f"Variants: {report.totals['variant_count']} "
            f"(selected={report.totals['selected_variant_count']}, "
            f"unselected={report.totals['unselected_variant_count']})"
        ),
        "Availability: "
        + ", ".join(
            f"{name}={'yes' if value else 'no'}"
            for name, value in sorted(report.availability.items())
        ),
    ]
    if report.missing_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if not report.groups:
        lines.append("No content variants found for the selected filters.")
        return "\n".join(lines)

    lines.append("Groups:")
    for group in report.groups:
        lines.append(
            f"- {group.platform}/{group.variant_type}/{group.selection_status}: "
            f"variants={group.variant_count} published={group.published_count} "
            f"snapshots={group.engagement_snapshot_count} "
            f"no_snapshots={group.no_engagement_count} "
            f"low={group.low_engagement_count} "
            f"avg={_format_score(group.average_engagement_score)} "
            f"recommendation={group.recommendation}"
        )
    if report.recommendations:
        lines.append("Recommendations:")
        lines.extend(f"- {recommendation}" for recommendation in report.recommendations)
    return "\n".join(lines)


def _load_variants(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    now: datetime,
    platform: str | None,
    variant_type: str | None,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if "content_variants" not in schema:
        missing_tables.add("content_variants")
        return []
    required = ("id", "content_id", "platform", "variant_type", "selected", "created_at")
    missing = tuple(column for column in required if column not in schema["content_variants"])
    if missing:
        missing_columns["content_variants"] = missing
        return []

    clauses = [
        "datetime(created_at) >= datetime(?)",
        "datetime(created_at) <= datetime(?)",
    ]
    params: list[Any] = [cutoff.isoformat(), now.isoformat()]
    if platform:
        clauses.append("platform = ?")
        params.append(platform)
    if variant_type:
        clauses.append("variant_type = ?")
        params.append(variant_type)

    rows = _fetch_dicts(
        conn,
        f"""SELECT id, content_id, platform, variant_type, selected, created_at
            FROM content_variants
            WHERE {' AND '.join(clauses)}
            ORDER BY platform ASC, variant_type ASC, selected DESC, content_id ASC, id ASC""",
        params,
    )
    for row in rows:
        row["content_id"] = int(row["content_id"])
        row["selected"] = bool(row.get("selected"))
        row["platform"] = _normalize_label(row.get("platform"))
        row["variant_type"] = _normalize_label(row.get("variant_type"))
    return rows


def _load_publications(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> tuple[dict[tuple[int, str], dict[str, Any]], bool]:
    if "content_publications" not in schema:
        missing_tables.add("content_publications")
        return {}, False
    required = ("content_id", "platform", "status")
    missing = tuple(
        column for column in required if column not in schema["content_publications"]
    )
    if missing:
        missing_columns["content_publications"] = missing
        return {}, False
    rows = _fetch_dicts(
        conn,
        """SELECT content_id, platform, status, published_at, updated_at
           FROM content_publications
           ORDER BY content_id ASC, platform ASC""",
        (),
    )
    return {
        (int(row["content_id"]), _normalize_label(row.get("platform"))): row
        for row in rows
    }, True


def _load_latest_engagement(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> tuple[dict[tuple[int, str], float], dict[str, bool]]:
    engagement: dict[tuple[int, str], float] = {}
    availability: dict[str, bool] = {}
    loaded_tables: set[str] = set()
    for platform, table in ENGAGEMENT_TABLES.items():
        availability[table] = table in schema
        if table in loaded_tables:
            continue
        loaded_tables.add(table)
        if table not in schema:
            missing_tables.add(table)
            continue
        required = ("id", "content_id", "engagement_score", "fetched_at")
        missing = tuple(column for column in required if column not in schema[table])
        if missing:
            missing_columns[table] = missing
            continue
        rows = _fetch_dicts(
            conn,
            f"""SELECT e.content_id, e.engagement_score
                FROM {table} e
                WHERE e.id = (
                    SELECT latest.id
                    FROM {table} latest
                    WHERE latest.content_id = e.content_id
                    ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                    LIMIT 1
                )
                ORDER BY e.content_id ASC""",
            (),
        )
        for row in rows:
            score = row.get("engagement_score")
            if score is None:
                continue
            for mapped_platform, mapped_table in ENGAGEMENT_TABLES.items():
                if mapped_table == table:
                    engagement[(int(row["content_id"]), mapped_platform)] = float(score)
    return engagement, availability


def _build_groups(
    *,
    variants: list[dict[str, Any]],
    publications: dict[tuple[int, str], dict[str, Any]],
    engagement: dict[tuple[int, str], float],
    min_sample: int,
) -> list[ContentVariantOutcomeGroup]:
    buckets: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for variant in variants:
        status = "selected" if variant["selected"] else "unselected"
        key = (variant["platform"], variant["variant_type"], status)
        buckets.setdefault(key, []).append(variant)

    groups: list[ContentVariantOutcomeGroup] = []
    for (platform, variant_type, selection_status), rows in buckets.items():
        published_count = 0
        scores: list[float] = []
        no_engagement_count = 0
        low_engagement_count = 0
        for row in rows:
            publication = publications.get((row["content_id"], platform), {})
            published = publication.get("status") == "published"
            if published:
                published_count += 1
            score = engagement.get((row["content_id"], platform))
            if row["selected"] and published and score is not None:
                scores.append(score)
                if score < LOW_ENGAGEMENT_SCORE:
                    low_engagement_count += 1
            elif row["selected"] and published:
                no_engagement_count += 1

        variant_count = len(rows)
        snapshot_count = len(scores)
        average = round(mean(scores), 2) if scores else None
        publication_rate = round(published_count / variant_count, 4) if variant_count else 0.0
        snapshot_rate = round(snapshot_count / published_count, 4) if published_count else 0.0
        low_rate = round(low_engagement_count / snapshot_count, 4) if snapshot_count else 0.0
        sample_status = "included" if variant_count >= min_sample else "low_sample"
        groups.append(
            ContentVariantOutcomeGroup(
                platform=platform,
                variant_type=variant_type,
                selection_status=selection_status,
                variant_count=variant_count,
                published_count=published_count,
                unpublished_count=variant_count - published_count,
                engagement_snapshot_count=snapshot_count,
                no_engagement_count=no_engagement_count,
                low_engagement_count=low_engagement_count,
                average_engagement_score=average,
                publication_rate=publication_rate,
                engagement_snapshot_rate=snapshot_rate,
                low_engagement_rate=low_rate,
                sample_status=sample_status,
                recommendation=_recommendation(
                    selection_status=selection_status,
                    sample_status=sample_status,
                    average_engagement_score=average,
                    no_engagement_count=no_engagement_count,
                    low_engagement_count=low_engagement_count,
                    published_count=published_count,
                ),
            )
        )
    groups.sort(
        key=lambda group: (
            group.platform,
            group.variant_type,
            0 if group.selection_status == "selected" else 1,
        )
    )
    return groups


def _recommendation(
    *,
    selection_status: str,
    sample_status: str,
    average_engagement_score: float | None,
    no_engagement_count: int,
    low_engagement_count: int,
    published_count: int,
) -> str:
    if sample_status == "low_sample":
        return "collect_more_data"
    if selection_status != "selected":
        return "compare_if_selected"
    if published_count and no_engagement_count == published_count:
        return "instrument_engagement"
    if average_engagement_score is not None and average_engagement_score < LOW_ENGAGEMENT_SCORE:
        return "review_variant_type"
    if low_engagement_count:
        return "monitor_mixed_outcomes"
    return "keep_using"


def _build_recommendations(
    groups: list[ContentVariantOutcomeGroup],
    *,
    min_sample: int,
) -> list[str]:
    recommendations = []
    for group in groups:
        if group.recommendation == "review_variant_type":
            recommendations.append(
                f"Review {group.platform}/{group.variant_type}: "
                f"{group.variant_count} selected variants meet min_sample={min_sample}, "
                f"avg engagement {_format_score(group.average_engagement_score)} is below "
                f"{LOW_ENGAGEMENT_SCORE:.1f}."
            )
        elif group.recommendation == "instrument_engagement":
            recommendations.append(
                f"Add engagement snapshots for {group.platform}/{group.variant_type}: "
                f"{group.no_engagement_count} published selected variants have no snapshot."
            )
    return recommendations


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


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


def _fetch_dicts(
    conn: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...] | list[Any],
) -> list[dict[str, Any]]:
    cursor = conn.execute(query, params)
    names = [column[0] for column in cursor.description or ()]
    return [
        {
            name: row[name] if hasattr(row, "keys") else row[index]
            for index, name in enumerate(names)
        }
        for row in cursor.fetchall()
    ]


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize_label(value: Any) -> str:
    return str(value or "unknown").strip().lower() or "unknown"


def _format_score(value: float | None) -> str:
    return "n/a" if value is None else f"{value:.2f}"
