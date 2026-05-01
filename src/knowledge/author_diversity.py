"""Report author and domain diversity in linked knowledge usage."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 30
DEFAULT_TOP_N = 5
DEFAULT_MIN_USAGE = 5
DEFAULT_STALE_AFTER_DAYS = 180
SOURCE_TYPE_FLOOR = 0.15
UNKNOWN_VALUES = {"", "unknown", "n/a", "none", "null", "-"}
EXPECTED_SOURCE_TYPES = (
    "own_post",
    "own_conversation",
    "curated_x",
    "curated_article",
    "curated_newsletter",
)


@dataclass(frozen=True)
class UsageBucket:
    """One source usage bucket in the diversity report."""

    label: str
    usage_count: int
    share: float
    knowledge_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["knowledge_ids"] = list(self.knowledge_ids)
        return payload


@dataclass(frozen=True)
class UnderusedSourceType:
    """A source type available in the store but underused in linked content."""

    source_type: str
    usage_count: int
    share: float
    available_count: int
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class KnowledgeAuthorDiversityReport:
    """Diversity summary for recent content knowledge links."""

    generated_at: str
    days: int
    top_n: int
    min_usage: int
    stale_after_days: int
    total_usage_count: int
    unique_content_count: int
    unique_knowledge_count: int
    unknown_author_count: int
    unknown_domain_count: int
    stale_usage_count: int
    missing_source_date_count: int
    top_authors: tuple[UsageBucket, ...]
    top_domains: tuple[UsageBucket, ...]
    source_types: tuple[UsageBucket, ...]
    concentration_metrics: dict[str, float]
    warnings: tuple[str, ...]
    recommended_underused_source_types: tuple[UnderusedSourceType, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "days": self.days,
            "top_n": self.top_n,
            "min_usage": self.min_usage,
            "stale_after_days": self.stale_after_days,
            "total_usage_count": self.total_usage_count,
            "unique_content_count": self.unique_content_count,
            "unique_knowledge_count": self.unique_knowledge_count,
            "unknown_author_count": self.unknown_author_count,
            "unknown_domain_count": self.unknown_domain_count,
            "stale_usage_count": self.stale_usage_count,
            "missing_source_date_count": self.missing_source_date_count,
            "top_authors": [bucket.to_dict() for bucket in self.top_authors],
            "top_domains": [bucket.to_dict() for bucket in self.top_domains],
            "source_types": [bucket.to_dict() for bucket in self.source_types],
            "concentration_metrics": dict(sorted(self.concentration_metrics.items())),
            "warnings": list(self.warnings),
            "recommended_underused_source_types": [
                recommendation.to_dict()
                for recommendation in self.recommended_underused_source_types
            ],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
        }


def build_knowledge_author_diversity_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    top_n: int = DEFAULT_TOP_N,
    min_usage: int = DEFAULT_MIN_USAGE,
    stale_after_days: int = DEFAULT_STALE_AFTER_DAYS,
    now: datetime | None = None,
) -> KnowledgeAuthorDiversityReport:
    """Build a read-only diversity report from recent knowledge link rows."""
    if days < 1:
        raise ValueError("days must be at least 1")
    if top_n < 1:
        raise ValueError("top_n must be at least 1")
    if min_usage < 1:
        raise ValueError("min_usage must be at least 1")
    if stale_after_days < 1:
        raise ValueError("stale_after_days must be at least 1")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_aware(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)

    missing_tables = tuple(
        table
        for table in ("knowledge", "content_knowledge_links")
        if table not in schema
    )
    missing_columns = _missing_columns(schema)
    if missing_tables:
        return _empty_report(
            generated_at,
            days,
            top_n,
            min_usage,
            stale_after_days,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    rows = _load_usage_rows(conn, schema, cutoff)
    author_buckets = _usage_buckets(rows, total=len(rows), key="author_label", top_n=top_n)
    domain_buckets = _usage_buckets(rows, total=len(rows), key="domain_label", top_n=top_n)
    source_type_buckets = _usage_buckets(rows, total=len(rows), key="source_type_label", top_n=top_n)

    unique_content_ids = {
        int(row["content_id"])
        for row in rows
        if row.get("content_id") is not None
    }
    unique_knowledge_ids = {
        int(row["knowledge_id"])
        for row in rows
        if row.get("knowledge_id") is not None
    }
    unknown_author_count = sum(1 for row in rows if row["author_label"] == "(unknown author)")
    unknown_domain_count = sum(1 for row in rows if row["domain_label"] == "(unknown domain)")
    stale_usage_count = sum(1 for row in rows if _is_stale(row.get("source_date"), generated_at, stale_after_days))
    missing_source_date_count = sum(1 for row in rows if not row.get("source_date"))

    concentration_metrics = _concentration_metrics(
        rows,
        author_buckets,
        domain_buckets,
        unknown_author_count,
        unknown_domain_count,
        stale_usage_count,
    )
    recommendations = _underused_source_types(conn, schema, source_type_buckets, total=len(rows))
    warnings = _warnings(
        total=len(rows),
        min_usage=min_usage,
        author_buckets=author_buckets,
        domain_buckets=domain_buckets,
        unknown_author_count=unknown_author_count,
        unknown_domain_count=unknown_domain_count,
        stale_usage_count=stale_usage_count,
    )

    return KnowledgeAuthorDiversityReport(
        generated_at=generated_at.isoformat(),
        days=days,
        top_n=top_n,
        min_usage=min_usage,
        stale_after_days=stale_after_days,
        total_usage_count=len(rows),
        unique_content_count=len(unique_content_ids),
        unique_knowledge_count=len(unique_knowledge_ids),
        unknown_author_count=unknown_author_count,
        unknown_domain_count=unknown_domain_count,
        stale_usage_count=stale_usage_count,
        missing_source_date_count=missing_source_date_count,
        top_authors=tuple(author_buckets),
        top_domains=tuple(domain_buckets),
        source_types=tuple(source_type_buckets),
        concentration_metrics=concentration_metrics,
        warnings=tuple(warnings),
        recommended_underused_source_types=tuple(recommendations),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_knowledge_author_diversity_json(report: KnowledgeAuthorDiversityReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_knowledge_author_diversity_text(report: KnowledgeAuthorDiversityReport) -> str:
    """Render a stable human-readable diversity report."""
    lines = [
        "Knowledge Author Diversity Report",
        f"Generated: {report.generated_at}",
        f"Window: {report.days} days",
        (
            "Usage: "
            f"{report.total_usage_count} links, "
            f"{report.unique_knowledge_count} knowledge rows, "
            f"{report.unique_content_count} content rows"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if any(report.missing_columns.values()):
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in report.missing_columns.items()
            if columns
        ]
        lines.append(f"Missing optional columns: {'; '.join(missing)}")
    lines.append("")

    if report.total_usage_count == 0:
        lines.append("No knowledge links found in the selected window.")
        return "\n".join(lines)

    metrics = report.concentration_metrics
    lines.extend(
        [
            "Concentration:",
            f"  Top author share: {metrics['top_author_share']:.0%}",
            f"  Top domain share: {metrics['top_domain_share']:.0%}",
            f"  Unknown author share: {metrics['unknown_author_share']:.0%}",
            f"  Unknown domain share: {metrics['unknown_domain_share']:.0%}",
            f"  Stale usage share: {metrics['stale_usage_share']:.0%}",
            "",
            "Top authors:",
        ]
    )
    lines.extend(_format_buckets(report.top_authors))
    lines.append("")
    lines.append("Top domains:")
    lines.extend(_format_buckets(report.top_domains))
    lines.append("")
    lines.append("Source types:")
    lines.extend(_format_buckets(report.source_types))

    if report.warnings:
        lines.append("")
        lines.append("Warnings:")
        lines.extend(f"  - {warning}" for warning in report.warnings)

    if report.recommended_underused_source_types:
        lines.append("")
        lines.append("Recommended underused source types:")
        for recommendation in report.recommended_underused_source_types:
            lines.append(
                "  - {source_type}: {reason} "
                "(usage={usage_count}, available={available_count})".format(
                    source_type=recommendation.source_type,
                    reason=recommendation.reason,
                    usage_count=recommendation.usage_count,
                    available_count=recommendation.available_count,
                )
            )

    return "\n".join(lines)


def normalize_domain(url: str | None) -> str | None:
    """Return a stable lower-case host from a URL-like value."""
    value = (url or "").strip()
    if _is_unknown(value):
        return None
    parsed = urlparse(value if "://" in value else f"https://{value}")
    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    names = [row[0] for row in rows]
    return {
        name: {column[1] for column in conn.execute(f"PRAGMA table_info({name})")}
        for name in names
    }


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "knowledge": (
            "id",
            "source_type",
            "source_url",
            "author",
            "published_at",
            "ingested_at",
            "created_at",
        ),
        "content_knowledge_links": (
            "content_id",
            "knowledge_id",
            "created_at",
        ),
        "generated_content": ("id", "created_at"),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema or table == "generated_content"
    }


def _load_usage_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
) -> list[dict[str, Any]]:
    link_columns = schema.get("content_knowledge_links", set())
    knowledge_columns = schema.get("knowledge", set())
    generated_columns = schema.get("generated_content", set())
    join_generated = (
        "generated_content" in schema
        and "id" in generated_columns
        and "content_id" in link_columns
    )
    join_knowledge = "knowledge_id" in link_columns and "id" in knowledge_columns
    selected_knowledge_columns = knowledge_columns if join_knowledge else set()
    timestamp_expr = _usage_timestamp_expr(link_columns, generated_columns if join_generated else set())
    where = ""
    params: list[Any] = []
    if timestamp_expr != "NULL":
        where = f"WHERE {timestamp_expr} >= ?"
        params.append(cutoff.isoformat())
    generated_join = (
        "LEFT JOIN generated_content gc ON gc.id = ckl.content_id"
        if join_generated
        else ""
    )
    knowledge_join = "LEFT JOIN knowledge k ON k.id = ckl.knowledge_id" if join_knowledge else ""
    cursor = conn.execute(
        f"""SELECT {_column_expr(link_columns, "id", "ckl", "link_id")},
                  {_column_expr(link_columns, "content_id", "ckl", "content_id")},
                  {_column_expr(link_columns, "knowledge_id", "ckl", "knowledge_id")},
                  {_column_expr(link_columns, "created_at", "ckl", "linked_at")},
                  {_column_expr(selected_knowledge_columns, "id", "k", "matched_knowledge_id")},
                  {_column_expr(selected_knowledge_columns, "source_type", "k", "source_type")},
                  {_column_expr(selected_knowledge_columns, "source_url", "k", "source_url")},
                  {_column_expr(selected_knowledge_columns, "author", "k", "author")},
                  {_column_expr(selected_knowledge_columns, "published_at", "k", "published_at")},
                  {_column_expr(selected_knowledge_columns, "ingested_at", "k", "ingested_at")},
                  {_column_expr(selected_knowledge_columns, "created_at", "k", "knowledge_created_at")},
                  {_column_expr(selected_knowledge_columns, "metadata", "k", "metadata")},
                  {timestamp_expr} AS usage_timestamp
           FROM content_knowledge_links ckl
           {knowledge_join}
           {generated_join}
           {where}
           ORDER BY usage_timestamp DESC, link_id DESC""",
        params,
    )
    names = [description[0] for description in cursor.description]
    rows = [dict(zip(names, row)) for row in cursor.fetchall()]
    for row in rows:
        metadata = _metadata_dict(row.get("metadata"))
        source_url = _first_present(
            row.get("source_url"),
            metadata.get("canonical_url"),
            metadata.get("source_url"),
            (metadata.get("link_metadata") or {}).get("canonical_url")
            if isinstance(metadata.get("link_metadata"), dict)
            else None,
        )
        author = _clean_string(row.get("author"))
        domain = normalize_domain(source_url)
        row["author_label"] = author if author else "(unknown author)"
        row["domain_label"] = domain if domain else "(unknown domain)"
        row["source_type_label"] = _clean_string(row.get("source_type")) or "(unknown source type)"
        row["source_date"] = _first_present(
            row.get("published_at"),
            row.get("ingested_at"),
            row.get("knowledge_created_at"),
        )
    return rows


def _column_expr(columns: set[str], column: str, alias: str, output: str) -> str:
    if column in columns:
        return f"{alias}.{column} AS {output}"
    return f"NULL AS {output}"


def _usage_timestamp_expr(link_columns: set[str], generated_columns: set[str]) -> str:
    parts = []
    if "created_at" in link_columns:
        parts.append("ckl.created_at")
    if "created_at" in generated_columns:
        parts.append("gc.created_at")
    if not parts:
        return "NULL"
    if len(parts) == 1:
        return parts[0]
    return f"COALESCE({', '.join(parts)})"


def _usage_buckets(
    rows: list[dict[str, Any]],
    *,
    total: int,
    key: str,
    top_n: int,
) -> list[UsageBucket]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        label = str(row[key])
        bucket = grouped.setdefault(label, {"count": 0, "knowledge_ids": set()})
        bucket["count"] += 1
        if row.get("knowledge_id") is not None:
            bucket["knowledge_ids"].add(int(row["knowledge_id"]))
    buckets = [
        UsageBucket(
            label=label,
            usage_count=int(data["count"]),
            share=round((int(data["count"]) / total) if total else 0.0, 3),
            knowledge_ids=tuple(sorted(data["knowledge_ids"])),
        )
        for label, data in grouped.items()
    ]
    buckets.sort(key=lambda bucket: (-bucket.usage_count, bucket.label))
    return buckets[:top_n]


def _concentration_metrics(
    rows: list[dict[str, Any]],
    author_buckets: list[UsageBucket],
    domain_buckets: list[UsageBucket],
    unknown_author_count: int,
    unknown_domain_count: int,
    stale_usage_count: int,
) -> dict[str, float]:
    total = len(rows)
    return {
        "top_author_share": author_buckets[0].share if author_buckets else 0.0,
        "top_domain_share": domain_buckets[0].share if domain_buckets else 0.0,
        "unknown_author_share": round(unknown_author_count / total, 3) if total else 0.0,
        "unknown_domain_share": round(unknown_domain_count / total, 3) if total else 0.0,
        "stale_usage_share": round(stale_usage_count / total, 3) if total else 0.0,
    }


def _warnings(
    *,
    total: int,
    min_usage: int,
    author_buckets: list[UsageBucket],
    domain_buckets: list[UsageBucket],
    unknown_author_count: int,
    unknown_domain_count: int,
    stale_usage_count: int,
) -> list[str]:
    warnings: list[str] = []
    if total < min_usage:
        return [
            f"minimum usage not met: {total}/{min_usage} linked knowledge uses in window"
        ]
    if author_buckets and author_buckets[0].label != "(unknown author)" and author_buckets[0].share >= 0.5:
        bucket = author_buckets[0]
        warnings.append(
            f"author concentration: {bucket.label} accounts for {bucket.share:.0%} of linked usage ({bucket.usage_count}/{total})"
        )
    if domain_buckets and domain_buckets[0].label != "(unknown domain)" and domain_buckets[0].share >= 0.5:
        bucket = domain_buckets[0]
        warnings.append(
            f"domain concentration: {bucket.label} accounts for {bucket.share:.0%} of linked usage ({bucket.usage_count}/{total})"
        )
    if unknown_author_count / total >= 0.25:
        warnings.append(
            f"unknown author data: {unknown_author_count}/{total} linked uses lack author metadata"
        )
    if unknown_domain_count / total >= 0.25:
        warnings.append(
            f"unknown domain data: {unknown_domain_count}/{total} linked uses lack source_url/domain metadata"
        )
    if stale_usage_count / total >= 0.25:
        warnings.append(
            f"stale source usage: {stale_usage_count}/{total} linked uses are older than the stale threshold"
        )
    return warnings


def _underused_source_types(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    source_type_buckets: list[UsageBucket],
    *,
    total: int,
) -> list[UnderusedSourceType]:
    if "knowledge" not in schema or "source_type" not in schema["knowledge"]:
        return []
    available = _available_source_types(conn, schema)
    usage_by_type = {bucket.label: bucket for bucket in source_type_buckets}
    recommendations: list[UnderusedSourceType] = []
    for source_type in sorted(available):
        if source_type not in EXPECTED_SOURCE_TYPES:
            continue
        bucket = usage_by_type.get(source_type)
        usage_count = bucket.usage_count if bucket else 0
        share = bucket.share if bucket else 0.0
        if share >= SOURCE_TYPE_FLOOR:
            continue
        reason = (
            "available but unused in linked content"
            if usage_count == 0
            else f"below {SOURCE_TYPE_FLOOR:.0%} source-type share"
        )
        recommendations.append(
            UnderusedSourceType(
                source_type=source_type,
                usage_count=usage_count,
                share=share if total else 0.0,
                available_count=available[source_type],
                reason=reason,
            )
        )
    recommendations.sort(
        key=lambda item: (
            item.usage_count,
            -item.available_count,
            item.source_type,
        )
    )
    return recommendations


def _available_source_types(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[str, int]:
    knowledge_columns = schema["knowledge"]
    where = ""
    if "approved" in knowledge_columns:
        where = "WHERE approved = 1"
    rows = conn.execute(
        f"""SELECT source_type, COUNT(*) AS available_count
            FROM knowledge
            {where}
            GROUP BY source_type"""
    ).fetchall()
    available: dict[str, int] = {}
    for row in rows:
        source_type = row["source_type"] if isinstance(row, sqlite3.Row) else row[0]
        available_count = row["available_count"] if isinstance(row, sqlite3.Row) else row[1]
        if source_type:
            available[str(source_type)] = int(available_count)
    return available


def _empty_report(
    generated_at: datetime,
    days: int,
    top_n: int,
    min_usage: int,
    stale_after_days: int,
    *,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> KnowledgeAuthorDiversityReport:
    return KnowledgeAuthorDiversityReport(
        generated_at=generated_at.isoformat(),
        days=days,
        top_n=top_n,
        min_usage=min_usage,
        stale_after_days=stale_after_days,
        total_usage_count=0,
        unique_content_count=0,
        unique_knowledge_count=0,
        unknown_author_count=0,
        unknown_domain_count=0,
        stale_usage_count=0,
        missing_source_date_count=0,
        top_authors=(),
        top_domains=(),
        source_types=(),
        concentration_metrics={
            "top_author_share": 0.0,
            "top_domain_share": 0.0,
            "unknown_author_share": 0.0,
            "unknown_domain_share": 0.0,
            "stale_usage_share": 0.0,
        },
        warnings=(),
        recommended_underused_source_types=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _metadata_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _first_present(*values: Any) -> str | None:
    for value in values:
        text = _clean_string(value)
        if text:
            return text
    return None


def _clean_string(value: Any) -> str | None:
    text = str(value or "").strip()
    if _is_unknown(text):
        return None
    return text


def _is_unknown(value: str) -> bool:
    return value.strip().lower() in UNKNOWN_VALUES


def _is_stale(value: Any, now: datetime, stale_after_days: int) -> bool:
    parsed = _parse_datetime(value)
    if parsed is None:
        return False
    return parsed < now - timedelta(days=stale_after_days)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_aware(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return _ensure_aware(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        return None


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_buckets(buckets: tuple[UsageBucket, ...]) -> list[str]:
    if not buckets:
        return ["  - none"]
    return [
        f"  - {bucket.label}: {bucket.usage_count} ({bucket.share:.0%})"
        for bucket in buckets
    ]
