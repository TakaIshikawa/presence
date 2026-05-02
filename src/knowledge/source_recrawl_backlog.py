"""Plan curated source recrawls from source health and knowledge freshness."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_STALE_DAYS = 14
DEFAULT_FAILURE_BACKOFF_DAYS = 3
DEFAULT_LIMIT = 100

RECRAWL_NOW = "recrawl_now"
BACKOFF = "backoff"
NEEDS_FEED_URL = "needs_feed_url"
HEALTHY = "healthy"

RECOMMENDATION_ORDER = (RECRAWL_NOW, BACKOFF, NEEDS_FEED_URL, HEALTHY)
TERMINAL_STATUSES = {"inactive", "rejected", "retired"}
FEED_SOURCE_TYPES = {"blog", "newsletter"}
KNOWLEDGE_SOURCE_TYPES = {
    "curated_x": "x_account",
    "curated_article": "blog",
    "curated_newsletter": "newsletter",
}


@dataclass(frozen=True)
class SourceRecrawlBacklogItem:
    """One curated source recrawl recommendation."""

    source_id: int
    source_type: str
    identifier: str
    status: str
    recommendation: str
    reason: str
    rank_score: int
    feed_url: str | None
    last_success_at: str | None
    last_failure_at: str | None
    consecutive_failures: int
    next_eligible_at: str | None
    feed_etag: str | None
    feed_last_modified: str | None
    knowledge_item_count: int
    latest_knowledge_at: str | None
    freshness_at: str | None
    freshness_age_days: int | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceRecrawlBacklogReport:
    """Read-only recrawl plan for curated sources."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    recommendations: tuple[SourceRecrawlBacklogItem, ...]
    missing_feed_url: tuple[SourceRecrawlBacklogItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "source_recrawl_backlog",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_feed_url": [item.to_dict() for item in self.missing_feed_url],
            "missing_tables": list(self.missing_tables),
            "recommendations": [item.to_dict() for item in self.recommendations],
            "totals": dict(sorted(self.totals.items())),
        }


def build_source_recrawl_backlog_report(
    db_or_conn: Any,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    failure_backoff_days: int = DEFAULT_FAILURE_BACKOFF_DAYS,
    source_type: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> SourceRecrawlBacklogReport:
    """Rank curated sources by recrawl urgency."""
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if failure_backoff_days <= 0:
        raise ValueError("failure_backoff_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    source_type_filter = _clean_optional(source_type, "source_type")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "failure_backoff_days": failure_backoff_days,
        "limit": limit,
        "source_type": source_type_filter,
        "stale_days": stale_days,
        "stale_before": (generated_at - timedelta(days=stale_days)).isoformat(),
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

    knowledge_freshness = _knowledge_freshness(conn, schema)
    rows = _load_sources(conn, source_type=source_type_filter)
    items = [
        _classify_source(
            row,
            knowledge_freshness=knowledge_freshness,
            stale_days=stale_days,
            failure_backoff_days=failure_backoff_days,
            generated_at=generated_at,
        )
        for row in rows
    ]
    items.sort(key=_sort_key)
    recommendations = tuple(items[:limit])
    missing_feed_url = tuple(item for item in recommendations if item.recommendation == NEEDS_FEED_URL)
    return SourceRecrawlBacklogReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(recommendations),
        recommendations=recommendations,
        missing_feed_url=missing_feed_url,
        missing_tables=(),
        missing_columns=_missing_optional_columns(schema),
    )


def format_source_recrawl_backlog_json(report: SourceRecrawlBacklogReport) -> str:
    """Serialize the recrawl backlog as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_recrawl_backlog_text(report: SourceRecrawlBacklogReport) -> str:
    """Render the recrawl backlog for terminal review."""
    totals = report.totals
    lines = [
        "Curated Source Recrawl Backlog",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"stale_days={report.filters['stale_days']} "
            f"failure_backoff_days={report.filters['failure_backoff_days']} "
            f"source_type={report.filters['source_type'] or 'all'} "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"sources={totals['source_count']} "
            f"recrawl_now={totals['recrawl_now_count']} "
            f"backoff={totals['backoff_count']} "
            f"needs_feed_url={totals['needs_feed_url_count']} "
            f"healthy={totals['healthy_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        ]
        lines.append("Missing columns: " + "; ".join(missing))
    if not report.recommendations:
        lines.append("No curated sources found for recrawl planning.")
        return "\n".join(lines)

    lines.append("By source_type: " + _format_counts(totals["by_source_type"]))
    lines.append("By recommendation: " + _format_counts(totals["by_recommendation"]))
    if report.missing_feed_url:
        lines.append("")
        lines.append("Missing feed_url:")
        for item in report.missing_feed_url:
            lines.append(
                f"  - #{item.source_id} {item.source_type}:{item.identifier} "
                f"status={item.status} reason={item.reason}"
            )
    lines.append("")
    lines.append("Recommendations:")
    for item in report.recommendations:
        lines.append(
            f"  - #{item.source_id} {item.source_type}:{item.identifier} "
            f"recommendation={item.recommendation} score={item.rank_score} "
            f"freshness_at={item.freshness_at or '-'} age_days={item.freshness_age_days if item.freshness_age_days is not None else '-'}"
        )
        details = [
            f"reason={item.reason}",
            f"failures={item.consecutive_failures}",
        ]
        if item.next_eligible_at:
            details.append(f"next_eligible_at={item.next_eligible_at}")
        if item.feed_etag or item.feed_last_modified:
            details.append(f"feed_cache=etag:{item.feed_etag or '-'} modified:{item.feed_last_modified or '-'}")
        if item.knowledge_item_count:
            details.append(
                f"knowledge_items={item.knowledge_item_count} latest={item.latest_knowledge_at or '-'}"
            )
        lines.append("      " + " ".join(details))
    return "\n".join(lines)


def _classify_source(
    row: dict[str, Any],
    *,
    knowledge_freshness: dict[tuple[str, str], tuple[int, str | None]],
    stale_days: int,
    failure_backoff_days: int,
    generated_at: datetime,
) -> SourceRecrawlBacklogItem:
    source_type = _clean(row.get("source_type")) or "unknown"
    identifier = _clean(row.get("identifier")) or ""
    failures = _int(row.get("consecutive_failures"))
    last_failure_at = _clean(row.get("last_failure_at"))
    last_failure = _parse_datetime(last_failure_at)
    last_success_at = _clean(row.get("last_success_at"))
    status = _clean(row.get("status")) or "active"
    feed_url = _clean(row.get("feed_url"))
    knowledge_count, latest_knowledge_at = knowledge_freshness.get(
        _source_key(source_type, identifier),
        (0, None),
    )
    freshness_at = _max_timestamp(
        last_success_at,
        _clean(row.get("feed_last_modified")),
        latest_knowledge_at,
    )
    freshness_age_days = _age_days(_parse_datetime(freshness_at), generated_at)
    next_eligible_at = None

    if source_type in FEED_SOURCE_TYPES and not feed_url:
        recommendation = NEEDS_FEED_URL
        reason = "feed_url_missing"
        rank_score = 700
    elif failures >= 2 and last_failure is not None:
        next_eligible = last_failure + timedelta(days=failure_backoff_days)
        next_eligible_at = next_eligible.isoformat()
        if next_eligible > generated_at:
            recommendation = BACKOFF
            reason = "recent_repeated_failures"
            rank_score = 800 + failures
        else:
            recommendation = RECRAWL_NOW
            reason = "failure_backoff_elapsed"
            rank_score = 1200 + failures
    elif freshness_at is None:
        recommendation = RECRAWL_NOW
        reason = "never_successfully_crawled"
        rank_score = 1100
    elif freshness_age_days is not None and freshness_age_days >= stale_days:
        recommendation = RECRAWL_NOW
        reason = "stale_success"
        rank_score = 1000 + freshness_age_days
    else:
        recommendation = HEALTHY
        reason = "fresh_enough"
        rank_score = max(0, freshness_age_days or 0)

    return SourceRecrawlBacklogItem(
        source_id=int(row.get("id") or 0),
        source_type=source_type,
        identifier=identifier,
        status=status,
        recommendation=recommendation,
        reason=reason,
        rank_score=rank_score,
        feed_url=feed_url,
        last_success_at=last_success_at,
        last_failure_at=last_failure_at,
        consecutive_failures=failures,
        next_eligible_at=next_eligible_at,
        feed_etag=_clean(row.get("feed_etag")),
        feed_last_modified=_clean(row.get("feed_last_modified")),
        knowledge_item_count=knowledge_count,
        latest_knowledge_at=latest_knowledge_at,
        freshness_at=freshness_at,
        freshness_age_days=freshness_age_days,
    )


def _load_sources(conn: sqlite3.Connection, *, source_type: str | None) -> list[dict[str, Any]]:
    where = ["LOWER(COALESCE(status, 'active')) NOT IN (?, ?, ?)"]
    params: list[Any] = sorted(TERMINAL_STATUSES)
    if source_type is not None:
        where.append("source_type = ?")
        params.append(source_type)
    rows = conn.execute(
        f"""SELECT id, source_type, identifier, status, feed_url, last_success_at,
                   last_failure_at, consecutive_failures, feed_etag, feed_last_modified
            FROM curated_sources
            WHERE {' AND '.join(where)}
            ORDER BY source_type ASC, identifier ASC, id ASC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _knowledge_freshness(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[tuple[str, str], tuple[int, str | None]]:
    if "knowledge" not in schema:
        return {}
    columns = schema["knowledge"]
    select = [
        _column_expr(columns, "source_type"),
        _column_expr(columns, "source_id"),
        _column_expr(columns, "source_url"),
        _column_expr(columns, "author"),
        _column_expr(columns, "published_at"),
        _column_expr(columns, "ingested_at"),
        _column_expr(columns, "created_at"),
    ]
    rows = conn.execute(f"SELECT {', '.join(select)} FROM knowledge ORDER BY id ASC").fetchall()
    buckets: dict[tuple[str, str], tuple[int, str | None]] = {}
    for raw in rows:
        row = dict(raw)
        latest = _max_timestamp(
            _clean(row.get("published_at")),
            _clean(row.get("ingested_at")),
            _clean(row.get("created_at")),
        )
        for key in _candidate_source_keys(row):
            count, current_latest = buckets.get(key, (0, None))
            buckets[key] = (count + 1, _max_timestamp(current_latest, latest))
    return buckets


def _candidate_source_keys(row: dict[str, Any]) -> set[tuple[str, str]]:
    source_type = KNOWLEDGE_SOURCE_TYPES.get(_clean(row.get("source_type")) or "")
    if not source_type:
        return set()
    values = {
        _normalize_identifier(row.get("author")),
        _normalize_identifier(row.get("source_id")),
        _normalize_identifier(_host(row.get("source_url"))),
        _normalize_identifier(_host(row.get("source_id"))),
    }
    values.discard("")
    return {(source_type, value) for value in values}


def _totals(items: tuple[SourceRecrawlBacklogItem, ...]) -> dict[str, Any]:
    by_recommendation = Counter(item.recommendation for item in items)
    return {
        "source_count": len(items),
        "recrawl_now_count": by_recommendation[RECRAWL_NOW],
        "backoff_count": by_recommendation[BACKOFF],
        "needs_feed_url_count": by_recommendation[NEEDS_FEED_URL],
        "healthy_count": by_recommendation[HEALTHY],
        "by_recommendation": _ordered_counts(by_recommendation, RECOMMENDATION_ORDER),
        "by_source_type": dict(sorted(Counter(item.source_type for item in items).items())),
    }


def _schema_gaps(
    schema: dict[str, set[str]],
) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "curated_sources": (
            "id",
            "source_type",
            "identifier",
            "status",
            "feed_url",
            "last_success_at",
            "last_failure_at",
            "consecutive_failures",
            "feed_etag",
            "feed_last_modified",
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


def _missing_optional_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    requirements = {
        "knowledge": (
            "source_type",
            "source_id",
            "source_url",
            "author",
            "published_at",
            "ingested_at",
            "created_at",
        )
    }
    missing: dict[str, tuple[str, ...]] = {}
    for table, columns in requirements.items():
        if table not in schema:
            continue
        absent = tuple(column for column in columns if column not in schema[table])
        if absent:
            missing[table] = absent
    return missing


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> SourceRecrawlBacklogReport:
    return SourceRecrawlBacklogReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(()),
        recommendations=(),
        missing_feed_url=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _sort_key(item: SourceRecrawlBacklogItem) -> tuple[int, int, str, str, int]:
    order = {
        RECRAWL_NOW: 0,
        BACKOFF: 1,
        NEEDS_FEED_URL: 2,
        HEALTHY: 3,
    }
    return (
        order[item.recommendation],
        -item.rank_score,
        item.source_type,
        item.identifier,
        item.source_id,
    )


def _source_key(source_type: str, identifier: str) -> tuple[str, str]:
    return (source_type, _normalize_identifier(identifier))


def _ordered_counts(counter: Counter[str], order: tuple[str, ...]) -> dict[str, int]:
    return {key: counter[key] for key in order if counter[key]}


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
        name = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(name)] = {
            str(column[1])
            for column in conn.execute(f"PRAGMA table_info({name})").fetchall()
        }
    return schema


def _column_expr(columns: set[str], name: str) -> str:
    if name in columns:
        return f"{name} AS {name}"
    return f"NULL AS {name}"


def _max_timestamp(*values: str | None) -> str | None:
    best_value = None
    best_dt = None
    for value in values:
        parsed = _parse_datetime(value)
        if parsed is None:
            continue
        if best_dt is None or parsed > best_dt:
            best_dt = parsed
            best_value = value
    return best_value


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return _as_utc(value)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_days(value: datetime | None, generated_at: datetime) -> int | None:
    if value is None:
        return None
    return max(0, int((generated_at - value).total_seconds() // 86400))


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_optional(value: str | None, name: str) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        raise ValueError(f"{name} must not be blank")
    return cleaned


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _host(value: Any) -> str:
    text = _clean(value) or ""
    parsed = urlparse(text)
    return parsed.netloc or text


def _normalize_identifier(value: Any) -> str:
    text = (_clean(value) or "").casefold()
    if text.startswith("@"):
        text = text[1:]
    if text.startswith("www."):
        text = text[4:]
    return text.rstrip("/")


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "none"
    return ", ".join(f"{key}={value}" for key, value in sorted(counts.items()))
