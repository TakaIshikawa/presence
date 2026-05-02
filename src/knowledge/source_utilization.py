"""Report ingested knowledge sources with low downstream utilization."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_MIN_ITEMS = 2
DEFAULT_UNUSED_THRESHOLD = 0.5
UNKNOWN_SOURCE_TYPE = "(unknown source type)"
UNKNOWN_AUTHOR = "(unknown author)"


@dataclass(frozen=True)
class KnowledgeSourceUtilization:
    """One grouped knowledge source utilization bucket."""

    source_type: str
    source_url: str | None
    author: str | None
    item_count: int
    unused_count: int
    used_count: int
    link_count: int
    content_link_count: int
    reply_link_count: int
    unused_percentage: float
    first_ingested_at: str | None
    last_ingested_at: str | None
    knowledge_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["knowledge_ids"] = list(self.knowledge_ids)
        return payload


@dataclass(frozen=True)
class KnowledgeSourceUtilizationReport:
    """Read-only utilization report for ingested knowledge sources."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    sources: tuple[KnowledgeSourceUtilization, ...]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "knowledge_source_utilization",
            "availability": dict(sorted(self.availability.items())),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "source_count": len(self.sources),
            "sources": [source.to_dict() for source in self.sources],
            "totals": dict(sorted(self.totals.items())),
        }


def build_knowledge_source_utilization_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_items: int = DEFAULT_MIN_ITEMS,
    unused_threshold: float = DEFAULT_UNUSED_THRESHOLD,
    now: datetime | None = None,
) -> KnowledgeSourceUtilizationReport:
    """Aggregate ingested knowledge sources by utilization without mutating data."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_items <= 0:
        raise ValueError("min_items must be positive")
    if not 0 <= unused_threshold <= 1:
        raise ValueError("unused_threshold must be between 0 and 1")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    missing_tables = tuple(table for table in ("knowledge",) if table not in schema)
    missing_columns = _missing_columns(schema)
    availability = {
        "knowledge": "knowledge" in schema,
        "content_knowledge_links": _link_table_available(
            schema, "content_knowledge_links"
        ),
        "reply_knowledge_links": _link_table_available(schema, "reply_knowledge_links"),
    }

    if missing_tables:
        return _empty_report(
            generated_at=generated_at,
            days=days,
            min_items=min_items,
            unused_threshold=unused_threshold,
            availability=availability,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    knowledge_rows = _load_knowledge_rows(conn, schema, cutoff=cutoff)
    link_counts = _load_link_counts(conn, schema)
    sources = tuple(
        _ranked_sources(
            knowledge_rows,
            link_counts,
            min_items=min_items,
            unused_threshold=unused_threshold,
        )
    )
    total_links = sum(counts["content"] + counts["reply"] for counts in link_counts.values())
    total_unused = sum(
        1
        for row in knowledge_rows
        if sum(link_counts.get(row["id"], {"content": 0, "reply": 0}).values()) == 0
    )

    return KnowledgeSourceUtilizationReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "min_items": min_items,
            "unused_threshold": unused_threshold,
        },
        totals={
            "knowledge_item_count": len(knowledge_rows),
            "unused_item_count": total_unused,
            "used_item_count": len(knowledge_rows) - total_unused,
            "link_count": total_links,
            "reported_source_count": len(sources),
        },
        sources=sources,
        availability=availability,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_knowledge_source_utilization_json(
    report: KnowledgeSourceUtilizationReport,
) -> str:
    """Serialize a utilization report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_knowledge_source_utilization_text(
    report: KnowledgeSourceUtilizationReport,
) -> str:
    """Render a stable human-readable utilization report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Knowledge Source Utilization",
        f"Generated: {report.generated_at}",
        (
            f"Window: {filters['days']} days; "
            f"min_items={filters['min_items']}; "
            f"unused_threshold={filters['unused_threshold']:.0%}"
        ),
        (
            "Totals: "
            f"{totals['knowledge_item_count']} knowledge rows, "
            f"{totals['unused_item_count']} unused, "
            f"{totals['link_count']} links"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    missing = [
        f"{table}({', '.join(columns)})"
        for table, columns in report.missing_columns.items()
        if columns
    ]
    if missing:
        lines.append(f"Missing optional columns: {'; '.join(missing)}")
    unavailable_links = [
        table
        for table in ("content_knowledge_links", "reply_knowledge_links")
        if not report.availability.get(table, False)
    ]
    if unavailable_links:
        lines.append(f"Unavailable link tables: {', '.join(unavailable_links)}")
    lines.append("")

    if not report.sources:
        lines.append("No underutilized knowledge sources found.")
        return "\n".join(lines)

    lines.append("Underutilized sources:")
    for source in report.sources:
        label = _source_label(source.source_type, source.source_url, source.author)
        lines.append(
            "  - {label}: unused={unused_percentage:.0%} "
            "items={item_count} used={used_count} links={link_count} "
            "last_ingested={last_ingested_at}".format(
                label=label,
                unused_percentage=source.unused_percentage,
                item_count=source.item_count,
                used_count=source.used_count,
                link_count=source.link_count,
                last_ingested_at=source.last_ingested_at or "-",
            )
        )
    return "\n".join(lines)


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
            "ingested_at",
            "created_at",
            "approved",
        ),
        "content_knowledge_links": ("knowledge_id",),
        "reply_knowledge_links": ("knowledge_id",),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _link_table_available(schema: dict[str, set[str]], table: str) -> bool:
    return table in schema and "knowledge_id" in schema[table]


def _load_knowledge_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema.get("knowledge", set())
    timestamp_expr = _knowledge_timestamp_expr(columns)
    where: list[str] = []
    params: list[Any] = []
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} >= ?")
        params.append(cutoff.isoformat())
    if "approved" in columns:
        where.append("COALESCE(approved, 0) = 1")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    cursor = conn.execute(
        f"""SELECT {_column_expr(columns, "id", "id")},
                  {_column_expr(columns, "source_type", "source_type")},
                  {_column_expr(columns, "source_url", "source_url")},
                  {_column_expr(columns, "author", "author")},
                  {_column_expr(columns, "ingested_at", "ingested_at")},
                  {_column_expr(columns, "created_at", "created_at")},
                  {timestamp_expr} AS effective_ingested_at
           FROM knowledge
           {where_sql}
           ORDER BY effective_ingested_at DESC, id ASC""",
        params,
    )
    rows = [dict(row) for row in cursor.fetchall()]
    return [row for row in rows if row.get("id") is not None]


def _knowledge_timestamp_expr(columns: set[str]) -> str:
    parts = [column for column in ("ingested_at", "created_at") if column in columns]
    if not parts:
        return "NULL"
    if len(parts) == 1:
        return parts[0]
    return "COALESCE(ingested_at, created_at)"


def _column_expr(columns: set[str], column: str, output: str) -> str:
    if column in columns:
        return f"{column} AS {output}"
    return f"NULL AS {output}"


def _load_link_counts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, int]]:
    counts: dict[int, dict[str, int]] = {}
    for table, key in (
        ("content_knowledge_links", "content"),
        ("reply_knowledge_links", "reply"),
    ):
        if not _link_table_available(schema, table):
            continue
        rows = conn.execute(
            f"""SELECT knowledge_id, COUNT(*) AS link_count
                FROM {table}
                WHERE knowledge_id IS NOT NULL
                GROUP BY knowledge_id"""
        ).fetchall()
        for row in rows:
            knowledge_id = int(row["knowledge_id"])
            bucket = counts.setdefault(knowledge_id, {"content": 0, "reply": 0})
            bucket[key] = int(row["link_count"] or 0)
    return counts


def _ranked_sources(
    knowledge_rows: list[dict[str, Any]],
    link_counts: dict[int, dict[str, int]],
    *,
    min_items: int,
    unused_threshold: float,
) -> list[KnowledgeSourceUtilization]:
    grouped: dict[tuple[str, str | None, str | None], list[dict[str, Any]]] = {}
    for row in knowledge_rows:
        key = (
            _clean(row.get("source_type")) or UNKNOWN_SOURCE_TYPE,
            _clean(row.get("source_url")),
            _clean(row.get("author")),
        )
        grouped.setdefault(key, []).append(row)

    sources: list[KnowledgeSourceUtilization] = []
    for (source_type, source_url, author), rows in grouped.items():
        item_count = len(rows)
        if item_count < min_items:
            continue
        ids = tuple(sorted(int(row["id"]) for row in rows))
        content_link_count = sum(link_counts.get(knowledge_id, {}).get("content", 0) for knowledge_id in ids)
        reply_link_count = sum(link_counts.get(knowledge_id, {}).get("reply", 0) for knowledge_id in ids)
        link_count = content_link_count + reply_link_count
        used_count = sum(
            1
            for knowledge_id in ids
            if sum(link_counts.get(knowledge_id, {"content": 0, "reply": 0}).values()) > 0
        )
        unused_count = item_count - used_count
        unused_percentage = round(unused_count / item_count, 3) if item_count else 0.0
        if unused_percentage < unused_threshold:
            continue
        ingested = tuple(
            sorted(
                timestamp
                for timestamp in (_clean(row.get("effective_ingested_at")) for row in rows)
                if timestamp
            )
        )
        sources.append(
            KnowledgeSourceUtilization(
                source_type=source_type,
                source_url=source_url,
                author=author,
                item_count=item_count,
                unused_count=unused_count,
                used_count=used_count,
                link_count=link_count,
                content_link_count=content_link_count,
                reply_link_count=reply_link_count,
                unused_percentage=unused_percentage,
                first_ingested_at=ingested[0] if ingested else None,
                last_ingested_at=ingested[-1] if ingested else None,
                knowledge_ids=ids,
            )
        )
    sources.sort(
        key=lambda source: (
            -source.unused_percentage,
            -source.item_count,
            source.link_count,
            source.last_ingested_at or "",
            source.source_type,
            source.source_url or "",
            source.author or "",
        )
    )
    return sources


def _empty_report(
    *,
    generated_at: datetime,
    days: int,
    min_items: int,
    unused_threshold: float,
    availability: dict[str, bool],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> KnowledgeSourceUtilizationReport:
    return KnowledgeSourceUtilizationReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "min_items": min_items,
            "unused_threshold": unused_threshold,
        },
        totals={
            "knowledge_item_count": 0,
            "unused_item_count": 0,
            "used_item_count": 0,
            "link_count": 0,
            "reported_source_count": 0,
        },
        sources=(),
        availability=availability,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _source_label(source_type: str, source_url: str | None, author: str | None) -> str:
    parts = [source_type]
    if author:
        parts.append(f"author={author}")
    else:
        parts.append(f"author={UNKNOWN_AUTHOR}")
    if source_url:
        parts.append(source_url)
    return " ".join(parts)
