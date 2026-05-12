"""Report knowledge source author/domain recency balance."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_FRESHNESS_WINDOW_DAYS = 90
DEFAULT_HEAVY_USAGE_COUNT = 3
DEFAULT_DOMINANCE_THRESHOLD = 0.5
DEFAULT_MIN_ITEMS = 1
UNKNOWN_AUTHOR = "(unknown author)"
UNKNOWN_DOMAIN = "(unknown domain)"
UNKNOWN_VALUES = {"", "unknown", "n/a", "none", "null", "-"}
HANDLE_PREFIX_RE = re.compile(r"^(?:@|https?://(?:www\.)?(?:x|twitter)\.com/)+", re.I)


@dataclass(frozen=True)
class KnowledgeSourceAuthorRecencyRow:
    """One author/domain/recency bucket in the source balance report."""

    author: str
    display_author: str
    domain: str
    recency_bucket: str
    item_count: int
    usage_count: int
    usage_share: float
    oldest_at: str | None
    newest_at: str | None
    knowledge_ids: tuple[int, ...]
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["knowledge_ids"] = list(self.knowledge_ids)
        return payload


@dataclass(frozen=True)
class KnowledgeSourceAuthorRecencyReport:
    """Read-only report for stale or overused knowledge source authors."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[KnowledgeSourceAuthorRecencyRow, ...]
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, tuple[str, ...]]
    availability: dict[str, bool]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "knowledge_source_author_recency",
            "availability": dict(sorted(self.availability.items())),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_knowledge_source_author_recency_report(
    db_or_conn: Any,
    *,
    freshness_window_days: int = DEFAULT_FRESHNESS_WINDOW_DAYS,
    heavy_usage_count: int = DEFAULT_HEAVY_USAGE_COUNT,
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
    min_items: int = DEFAULT_MIN_ITEMS,
    now: datetime | None = None,
) -> KnowledgeSourceAuthorRecencyReport:
    """Build a report grouped by normalized author, domain, and recency bucket."""
    if freshness_window_days <= 0:
        raise ValueError("freshness_window_days must be positive")
    if heavy_usage_count <= 0:
        raise ValueError("heavy_usage_count must be positive")
    if not 0 <= dominance_threshold <= 1:
        raise ValueError("dominance_threshold must be between 0 and 1")
    if min_items <= 0:
        raise ValueError("min_items must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    freshness_cutoff = generated_at - timedelta(days=freshness_window_days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("knowledge",) if table not in schema)
    missing_columns = _missing_columns(schema)
    availability = {
        "knowledge": "knowledge" in schema,
        "content_knowledge_links": _link_table_available(schema, "content_knowledge_links"),
        "reply_knowledge_links": _link_table_available(schema, "reply_knowledge_links"),
    }
    if missing_tables:
        return _empty_report(
            generated_at,
            freshness_window_days,
            heavy_usage_count,
            dominance_threshold,
            min_items,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            availability=availability,
        )

    knowledge_rows = _load_knowledge_rows(conn, schema)
    usage_counts = _load_usage_counts(conn, schema)
    report_rows = tuple(
        _recency_rows(
            knowledge_rows,
            usage_counts,
            freshness_cutoff=freshness_cutoff,
            heavy_usage_count=heavy_usage_count,
            dominance_threshold=dominance_threshold,
            min_items=min_items,
        )
    )
    action_counts: dict[str, int] = {}
    for row in report_rows:
        action_counts[row.recommended_action] = action_counts.get(row.recommended_action, 0) + 1

    return KnowledgeSourceAuthorRecencyReport(
        generated_at=generated_at.isoformat(),
        filters={
            "freshness_window_days": freshness_window_days,
            "heavy_usage_count": heavy_usage_count,
            "dominance_threshold": dominance_threshold,
            "min_items": min_items,
        },
        totals={
            "knowledge_item_count": len(knowledge_rows),
            "reported_group_count": len(report_rows),
            "usage_count": sum(usage_counts.values()),
            "action_counts": dict(sorted(action_counts.items())),
        },
        rows=report_rows,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        availability=availability,
    )


def format_knowledge_source_author_recency_json(
    report: KnowledgeSourceAuthorRecencyReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_knowledge_source_author_recency_text(
    report: KnowledgeSourceAuthorRecencyReport,
) -> str:
    """Render a stable human-readable source author recency report."""
    lines = [
        "Knowledge Source Author Recency",
        f"Generated: {report.generated_at}",
        (
            f"Freshness window: {report.filters['freshness_window_days']} days; "
            f"heavy_usage_count={report.filters['heavy_usage_count']}; "
            f"dominance_threshold={report.filters['dominance_threshold']:.0%}"
        ),
        (
            f"Groups: {report.totals['reported_group_count']} from "
            f"{report.totals['knowledge_item_count']} knowledge rows"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + missing)
    if not report.rows:
        lines.append("No knowledge source author recency groups found.")
        return "\n".join(lines)

    lines.append("Author/domain recency groups:")
    for row in report.rows:
        lines.append(
            f"- {row.display_author} @ {row.domain}: bucket={row.recency_bucket} "
            f"items={row.item_count} usage={row.usage_count} "
            f"share={row.usage_share:.0%} action={row.recommended_action} "
            f"newest={row.newest_at or '-'}"
        )
    return "\n".join(lines)


def normalize_author(value: Any) -> str | None:
    """Normalize author names and handles for grouping."""
    text = str(value or "").strip()
    if not text:
        return None
    text = HANDLE_PREFIX_RE.sub("", text).strip()
    text = text.split("?", 1)[0].split("#", 1)[0].strip().strip("/")
    normalized = " ".join(text.casefold().split())
    return None if normalized in UNKNOWN_VALUES else normalized


def normalize_domain(value: Any) -> str | None:
    """Normalize a source URL to a lowercase registrable-looking domain label."""
    text = str(value or "").strip()
    if not text:
        return None
    parsed = urlparse(text if "://" in text else f"https://{text}")
    domain = (parsed.netloc or parsed.path.split("/", 1)[0]).casefold()
    if domain.startswith("www."):
        domain = domain[4:]
    return None if domain in UNKNOWN_VALUES else domain or None


def _recency_rows(
    rows: list[dict[str, Any]],
    usage_counts: dict[int, int],
    *,
    freshness_cutoff: datetime,
    heavy_usage_count: int,
    dominance_threshold: float,
    min_items: int,
) -> list[KnowledgeSourceAuthorRecencyRow]:
    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    display_values: dict[str, set[str]] = defaultdict(set)
    for row in rows:
        timestamp = _parse_timestamp(_source_timestamp(row))
        bucket = _recency_bucket(timestamp, freshness_cutoff)
        author = normalize_author(row.get("author")) or UNKNOWN_AUTHOR
        raw_author = str(row.get("author") or "").strip()
        if raw_author:
            display_values[author].add(raw_author)
        domain = normalize_domain(row.get("source_url")) or UNKNOWN_DOMAIN
        grouped[(author, domain, bucket)].append(row)

    total_usage = sum(usage_counts.values())
    output: list[KnowledgeSourceAuthorRecencyRow] = []
    for (author, domain, bucket), group_rows in grouped.items():
        if len(group_rows) < min_items:
            continue
        ids = tuple(sorted(int(row["id"]) for row in group_rows if row.get("id") is not None))
        usage_count = sum(usage_counts.get(knowledge_id, 0) for knowledge_id in ids)
        timestamps = sorted(
            timestamp.isoformat()
            for timestamp in (_parse_timestamp(_source_timestamp(row)) for row in group_rows)
            if timestamp is not None
        )
        usage_share = round(usage_count / total_usage, 3) if total_usage else 0.0
        output.append(
            KnowledgeSourceAuthorRecencyRow(
                author=author,
                display_author=_display_author(author, display_values.get(author, set())),
                domain=domain,
                recency_bucket=bucket,
                item_count=len(group_rows),
                usage_count=usage_count,
                usage_share=usage_share,
                oldest_at=timestamps[0] if timestamps else None,
                newest_at=timestamps[-1] if timestamps else None,
                knowledge_ids=ids,
                recommended_action=_recommended_action(
                    recency_bucket=bucket,
                    usage_count=usage_count,
                    usage_share=usage_share,
                    heavy_usage_count=heavy_usage_count,
                    dominance_threshold=dominance_threshold,
                ),
            )
        )
    output.sort(
        key=lambda row: (
            _action_rank(row.recommended_action),
            -row.usage_count,
            -row.item_count,
            row.author,
            row.domain,
            row.recency_bucket,
        )
    )
    return output


def _recommended_action(
    *,
    recency_bucket: str,
    usage_count: int,
    usage_share: float,
    heavy_usage_count: int,
    dominance_threshold: float,
) -> str:
    heavy = usage_count >= heavy_usage_count
    if heavy and recency_bucket in {"stale", "undated"}:
        return "refresh_author_sources"
    if heavy and usage_share >= dominance_threshold:
        return "diversify_author"
    return "ok"


def _recency_bucket(timestamp: datetime | None, freshness_cutoff: datetime) -> str:
    if timestamp is None:
        return "undated"
    return "fresh" if timestamp >= freshness_cutoff else "stale"


def _load_knowledge_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema.get("knowledge", set())
    cursor = conn.execute(
        f"""SELECT {_column_expr(columns, "id", "id")},
                  {_column_expr(columns, "source_url", "source_url")},
                  {_column_expr(columns, "author", "author")},
                  {_column_expr(columns, "published_at", "published_at")},
                  {_column_expr(columns, "ingested_at", "ingested_at")},
                  {_column_expr(columns, "created_at", "created_at")}
           FROM knowledge
           ORDER BY id ASC"""
    )
    return [dict(row) for row in cursor.fetchall()]


def _load_usage_counts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, int]:
    counts: dict[int, int] = defaultdict(int)
    for table in ("content_knowledge_links", "reply_knowledge_links"):
        if not _link_table_available(schema, table):
            continue
        rows = conn.execute(
            f"""SELECT knowledge_id, COUNT(*) AS usage_count
                FROM {table}
                WHERE knowledge_id IS NOT NULL
                GROUP BY knowledge_id"""
        ).fetchall()
        for row in rows:
            counts[int(row["knowledge_id"])] += int(row["usage_count"])
    return dict(counts)


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "knowledge": (
            "id",
            "source_url",
            "author",
            "published_at",
            "ingested_at",
            "created_at",
        ),
        "content_knowledge_links": ("knowledge_id",),
        "reply_knowledge_links": ("knowledge_id",),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _empty_report(
    generated_at: datetime,
    freshness_window_days: int,
    heavy_usage_count: int,
    dominance_threshold: float,
    min_items: int,
    *,
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
    availability: dict[str, bool],
) -> KnowledgeSourceAuthorRecencyReport:
    return KnowledgeSourceAuthorRecencyReport(
        generated_at=generated_at.isoformat(),
        filters={
            "freshness_window_days": freshness_window_days,
            "heavy_usage_count": heavy_usage_count,
            "dominance_threshold": dominance_threshold,
            "min_items": min_items,
        },
        totals={
            "knowledge_item_count": 0,
            "reported_group_count": 0,
            "usage_count": 0,
            "action_counts": {},
        },
        rows=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        availability=availability,
    )


def _link_table_available(schema: dict[str, set[str]], table: str) -> bool:
    return table in schema and "knowledge_id" in schema[table]


def _column_expr(columns: set[str], column: str, output: str) -> str:
    if column in columns:
        return f"{column} AS {output}"
    return f"NULL AS {output}"


def _source_timestamp(row: dict[str, Any]) -> Any:
    return row.get("published_at") or row.get("ingested_at") or row.get("created_at")


def _parse_timestamp(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _display_author(author: str, values: set[str]) -> str:
    if author == UNKNOWN_AUTHOR:
        return UNKNOWN_AUTHOR
    if not values:
        return author
    return sorted(values, key=lambda value: (len(value), value.casefold()))[0]


def _action_rank(action: str) -> int:
    return {
        "refresh_author_sources": 0,
        "diversify_author": 1,
        "ok": 2,
    }.get(action, 99)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    names = [row[0] for row in rows]
    return {
        name: {column[1] for column in conn.execute(f"PRAGMA table_info({name})")}
        for name in names
    }


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
