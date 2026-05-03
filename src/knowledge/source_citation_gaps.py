"""Report curated knowledge sources with citation gaps in generated output."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import math
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 90
DEFAULT_MIN_AGE_DAYS = 14
DEFAULT_LIMIT = 25
CURATED_SOURCE_TYPES = frozenset({"curated_x", "curated_article", "curated_newsletter"})
TRUST_KEYS = (
    "trust_score",
    "source_trust",
    "credibility_score",
    "quality_score",
    "authority_score",
)
TIER_SCORES = {
    "gold": 1.0,
    "trusted": 0.9,
    "high": 0.85,
    "primary": 0.85,
    "silver": 0.75,
    "medium": 0.6,
    "bronze": 0.45,
    "low": 0.3,
}


@dataclass(frozen=True)
class SourceCitationGap:
    """One curated source that has no citations in the configured lookback."""

    knowledge_id: int
    source_type: str | None
    source_id: str | None
    source_url: str | None
    author: str | None
    title: str | None
    source_age_days: int | None
    usage_count: int
    recent_usage_count: int
    last_cited_at: str | None
    trust_score: float | None
    priority_score: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceCitationGapReport:
    """Deterministic citation-gap report for knowledge sources."""

    generated_at: str
    filters: dict[str, Any]
    total_source_count: int
    gap_count: int
    gaps: tuple[SourceCitationGap, ...]
    usage_table_availability: dict[str, bool]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "source_citation_gaps",
            "filters": dict(self.filters),
            "gap_count": self.gap_count,
            "gaps": [gap.to_dict() for gap in self.gaps],
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "total_source_count": self.total_source_count,
            "usage_table_availability": dict(sorted(self.usage_table_availability.items())),
            "warnings": list(self.warnings),
        }


def build_source_citation_gap_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_age_days: int = DEFAULT_MIN_AGE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> SourceCitationGapReport:
    """Return curated knowledge sources not cited in the recent output window."""

    if days <= 0:
        raise ValueError("days must be positive")
    if min_age_days < 0:
        raise ValueError("min_age_days must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {"days": days, "min_age_days": min_age_days, "limit": limit}
    availability = {
        "content_knowledge_links": _has_columns(schema, "content_knowledge_links", {"knowledge_id"}),
        "generated_content": _has_columns(schema, "generated_content", {"id"}),
        "newsletter_sends": _has_columns(schema, "newsletter_sends", {"source_content_ids"}),
    }

    if "knowledge" not in schema:
        return SourceCitationGapReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            total_source_count=0,
            gap_count=0,
            gaps=(),
            usage_table_availability=availability,
            missing_tables=("knowledge",),
        )

    missing_columns = _missing_columns(schema)
    if "id" in missing_columns.get("knowledge", ()):
        return SourceCitationGapReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            total_source_count=0,
            gap_count=0,
            gaps=(),
            usage_table_availability=availability,
            missing_columns=missing_columns,
        )

    warnings: list[str] = []
    sources = _load_sources(
        conn,
        schema,
        now=generated_at,
        min_age_days=min_age_days,
        warnings=warnings,
    )
    usage = _load_usage(conn, schema, cutoff=generated_at - timedelta(days=days), warnings=warnings)
    gaps = [
        _gap_from_source(source, usage.get(int(source["knowledge_id"]), {}), now=generated_at)
        for source in sources
        if int(usage.get(int(source["knowledge_id"]), {}).get("recent_usage_count", 0)) == 0
    ]
    gaps.sort(
        key=lambda gap: (
            -gap.priority_score,
            gap.usage_count,
            -(gap.source_age_days or 0),
            gap.source_type or "",
            gap.source_id or "",
            gap.knowledge_id,
        )
    )
    selected = tuple(gaps[:limit])
    return SourceCitationGapReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        total_source_count=len(sources),
        gap_count=len(selected),
        gaps=selected,
        usage_table_availability=availability,
        missing_columns=missing_columns,
        warnings=tuple(sorted(set(warnings))),
    )


def format_source_citation_gap_json(report: SourceCitationGapReport) -> str:
    """Serialize a citation-gap report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_citation_gap_text(report: SourceCitationGapReport) -> str:
    """Render a compact text report for terminal review."""

    filters = report.filters
    lines = [
        "Source Citation Gaps",
        f"Generated: {report.generated_at}",
        (
            f"Filters: days={filters['days']} min_age_days={filters['min_age_days']} "
            f"limit={filters['limit']}"
        ),
        f"Sources: {report.total_source_count} checked; gaps={report.gap_count}",
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = [
            f"{table}.{column}"
            for table, columns in sorted(report.missing_columns.items())
            for column in columns
        ]
        if missing:
            lines.append("Missing columns: " + ", ".join(missing))
    unavailable = [
        table for table, available in sorted(report.usage_table_availability.items()) if not available
    ]
    if unavailable:
        lines.append("Unavailable usage tables: " + ", ".join(unavailable))
    if report.warnings:
        lines.append(f"Warnings: {len(report.warnings)}")
        lines.extend(f"  - {warning}" for warning in report.warnings)
    if not report.gaps:
        lines.append("No source citation gaps found.")
        return "\n".join(lines)

    lines.append("Gaps:")
    for gap in report.gaps:
        label = gap.title or gap.source_url or gap.source_id or f"knowledge:{gap.knowledge_id}"
        age = gap.source_age_days if gap.source_age_days is not None else "n/a"
        trust = f"{gap.trust_score:.2f}" if gap.trust_score is not None else "n/a"
        lines.append(
            "  - "
            f"knowledge_id={gap.knowledge_id} score={gap.priority_score:.3f} "
            f"age_days={age} usage={gap.usage_count} last_cited={gap.last_cited_at or '-'} "
            f"trust={trust} source={label}"
        )
    return "\n".join(lines)


def _load_sources(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    now: datetime,
    min_age_days: int,
    warnings: list[str],
) -> list[dict[str, Any]]:
    columns = schema["knowledge"]
    timestamp_expr = _source_timestamp_expr(columns)
    where = []
    params: list[Any] = []
    if "approved" in columns:
        where.append("COALESCE(approved, 0) = 1")
    if "source_type" in columns:
        where.append("source_type IN ({})".format(", ".join("?" for _ in CURATED_SOURCE_TYPES)))
        params.extend(sorted(CURATED_SOURCE_TYPES))
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} <= ?")
        params.append((now - timedelta(days=min_age_days)).isoformat())
    sql = f"""SELECT {_column_expr(columns, 'id', 'knowledge_id')},
                     {_column_expr(columns, 'source_type', 'source_type')},
                     {_column_expr(columns, 'source_id', 'source_id')},
                     {_column_expr(columns, 'source_url', 'source_url')},
                     {_column_expr(columns, 'author', 'author')},
                     {_column_expr(columns, 'published_at', 'published_at')},
                     {_column_expr(columns, 'ingested_at', 'ingested_at')},
                     {_column_expr(columns, 'created_at', 'created_at')},
                     {_column_expr(columns, 'metadata', 'metadata')},
                     {timestamp_expr} AS source_timestamp
              FROM knowledge"""
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY knowledge_id ASC"
    rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    sources: list[dict[str, Any]] = []
    for row in rows:
        if row.get("knowledge_id") is None:
            continue
        metadata = _metadata_object(row.get("metadata"), "knowledge", row["knowledge_id"], warnings)
        timestamp = _parse_timestamp(row.get("source_timestamp"))
        source_age_days = (now.date() - timestamp.date()).days if timestamp else None
        sources.append(
            {
                **row,
                "knowledge_id": int(row["knowledge_id"]),
                "title": _first_clean(
                    metadata.get("title"),
                    metadata.get("headline"),
                    metadata.get("link_title"),
                ),
                "source_age_days": source_age_days,
                "trust_score": _trust_score(metadata),
            }
        )
    return sources


def _load_usage(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    warnings: list[str],
) -> dict[int, dict[str, Any]]:
    usage: dict[int, dict[str, Any]] = {}
    if not _has_columns(schema, "content_knowledge_links", {"knowledge_id"}):
        return usage

    timestamp_expr = _link_timestamp_expr(schema)
    rows = conn.execute(
        f"""SELECT ckl.knowledge_id AS knowledge_id,
                  COUNT(*) AS usage_count,
                  SUM(CASE WHEN {timestamp_expr} >= ? THEN 1 ELSE 0 END) AS recent_usage_count,
                  MAX({timestamp_expr}) AS last_cited_at
           FROM content_knowledge_links ckl
           {_generated_content_join(schema)}
           WHERE ckl.knowledge_id IS NOT NULL
           GROUP BY ckl.knowledge_id""",
        (cutoff.isoformat(),),
    ).fetchall()
    for row in rows:
        knowledge_id = int(row["knowledge_id"])
        usage[knowledge_id] = {
            "usage_count": int(row["usage_count"] or 0),
            "recent_usage_count": int(row["recent_usage_count"] or 0),
            "last_cited_at": _clean(row["last_cited_at"]),
        }

    for citation in _newsletter_citations(conn, schema, cutoff=cutoff, warnings=warnings):
        bucket = usage.setdefault(
            citation["knowledge_id"],
            {"usage_count": 0, "recent_usage_count": 0, "last_cited_at": None},
        )
        bucket["usage_count"] = int(bucket["usage_count"]) + 1
        bucket["recent_usage_count"] = int(bucket["recent_usage_count"]) + citation["recent"]
        last_cited = _max_timestamp(bucket.get("last_cited_at"), citation.get("cited_at"))
        bucket["last_cited_at"] = last_cited
    return usage


def _newsletter_citations(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    warnings: list[str],
) -> Iterable[dict[str, Any]]:
    if not _has_columns(schema, "newsletter_sends", {"source_content_ids"}):
        return ()
    if not _has_columns(schema, "content_knowledge_links", {"content_id", "knowledge_id"}):
        return ()
    send_columns = schema["newsletter_sends"]
    sent_expr = _column_expr(send_columns, "sent_at", "sent_at")
    rows = conn.execute(
        f"""SELECT {_column_expr(send_columns, 'id', 'id')},
                  {sent_expr},
                  source_content_ids
           FROM newsletter_sends
           WHERE source_content_ids IS NOT NULL
           ORDER BY id ASC"""
    ).fetchall()
    citations: list[dict[str, Any]] = []
    for row in rows:
        sent_at = _clean(row["sent_at"])
        for content_id in _parse_int_list(row["source_content_ids"], "newsletter_sends", row["id"], warnings):
            linked = conn.execute(
                """SELECT DISTINCT knowledge_id
                   FROM content_knowledge_links
                   WHERE content_id = ? AND knowledge_id IS NOT NULL""",
                (content_id,),
            ).fetchall()
            cited_at = _parse_timestamp(sent_at)
            for link in linked:
                citations.append(
                    {
                        "knowledge_id": int(link["knowledge_id"]),
                        "cited_at": sent_at,
                        "recent": 1 if cited_at and cited_at >= cutoff else 0,
                    }
                )
    return citations


def _gap_from_source(
    source: Mapping[str, Any],
    usage: Mapping[str, Any],
    *,
    now: datetime,
) -> SourceCitationGap:
    usage_count = int(usage.get("usage_count", 0) or 0)
    age = source.get("source_age_days")
    trust = source.get("trust_score")
    priority = _priority_score(
        source_age_days=age if isinstance(age, int) else None,
        trust_score=trust if isinstance(trust, float) else None,
        usage_count=usage_count,
        last_cited_at=usage.get("last_cited_at"),
        now=now,
    )
    return SourceCitationGap(
        knowledge_id=int(source["knowledge_id"]),
        source_type=_clean(source.get("source_type")),
        source_id=_clean(source.get("source_id")),
        source_url=_clean(source.get("source_url")),
        author=_clean(source.get("author")),
        title=_clean(source.get("title")),
        source_age_days=age if isinstance(age, int) else None,
        usage_count=usage_count,
        recent_usage_count=int(usage.get("recent_usage_count", 0) or 0),
        last_cited_at=_clean(usage.get("last_cited_at")),
        trust_score=trust if isinstance(trust, float) else None,
        priority_score=priority,
    )


def _priority_score(
    *,
    source_age_days: int | None,
    trust_score: float | None,
    usage_count: int,
    last_cited_at: Any,
    now: datetime,
) -> float:
    age_component = 0.0
    if source_age_days is not None:
        age_component = min(40.0, math.log1p(max(source_age_days, 0)) * 8.0)
    trust_component = 30.0 * (trust_score if trust_score is not None else 0.5)
    unused_bonus = 35.0 if usage_count == 0 else max(0.0, 18.0 - min(usage_count, 6) * 3.0)
    stale_component = 0.0
    last_cited = _parse_timestamp(last_cited_at)
    if last_cited:
        stale_component = min(15.0, max((now - last_cited).days, 0) / 12.0)
    return round(age_component + trust_component + unused_bonus + stale_component, 3)


def _source_timestamp_expr(columns: set[str]) -> str:
    parts = [column for column in ("published_at", "ingested_at", "created_at") if column in columns]
    if not parts:
        return "NULL"
    if len(parts) == 1:
        return parts[0]
    return "COALESCE(" + ", ".join(parts) + ")"


def _link_timestamp_expr(schema: dict[str, set[str]]) -> str:
    ckl_columns = schema.get("content_knowledge_links", set())
    gc_columns = schema.get("generated_content", set())
    parts = []
    if "created_at" in ckl_columns:
        parts.append("ckl.created_at")
    if (
        "content_id" in ckl_columns
        and "created_at" in gc_columns
        and _has_columns(schema, "generated_content", {"id"})
    ):
        parts.append("gc.created_at")
    if not parts:
        return "'1970-01-01T00:00:00+00:00'"
    if len(parts) == 1:
        return parts[0]
    return "COALESCE(" + ", ".join(parts) + ")"


def _generated_content_join(schema: dict[str, set[str]]) -> str:
    if _has_columns(schema, "generated_content", {"id"}) and "content_id" in schema.get(
        "content_knowledge_links", set()
    ):
        return "LEFT JOIN generated_content gc ON gc.id = ckl.content_id"
    return ""


def _metadata_object(raw_value: Any, table: str, row_id: Any, warnings: list[str]) -> dict[str, Any]:
    if isinstance(raw_value, dict):
        return raw_value
    if raw_value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(raw_value))
    except json.JSONDecodeError as exc:
        warnings.append(f"{table}:{row_id}.metadata malformed JSON: {exc.msg}")
        return {}
    if not isinstance(parsed, dict):
        warnings.append(f"{table}:{row_id}.metadata is not a JSON object")
        return {}
    return parsed


def _trust_score(metadata: Mapping[str, Any]) -> float | None:
    for key in TRUST_KEYS:
        score = _float_or_none(metadata.get(key))
        if score is not None:
            return max(0.0, min(score if score <= 1 else score / 100.0, 1.0))
    tier = _clean(metadata.get("source_tier") or metadata.get("tier") or metadata.get("trust_tier"))
    if tier:
        return TIER_SCORES.get(tier.lower(), 0.5)
    return None


def _parse_int_list(raw_value: Any, table: str, row_id: Any, warnings: list[str]) -> list[int]:
    if raw_value in (None, ""):
        return []
    try:
        parsed = json.loads(str(raw_value))
    except json.JSONDecodeError as exc:
        warnings.append(f"{table}:{row_id}.source_content_ids malformed JSON: {exc.msg}")
        return []
    if not isinstance(parsed, list):
        warnings.append(f"{table}:{row_id}.source_content_ids is not a JSON list")
        return []
    ids: list[int] = []
    for item in parsed:
        try:
            value = int(item)
        except (TypeError, ValueError):
            continue
        if value > 0 and value not in ids:
            ids.append(value)
    return ids


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row["name"]): {
            str(column["name"])
            for column in conn.execute(f"PRAGMA table_info({row['name']})").fetchall()
        }
        for row in rows
    }


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        "knowledge": ("id", "source_type", "source_id", "source_url", "author", "approved"),
        "content_knowledge_links": ("knowledge_id",),
        "newsletter_sends": ("source_content_ids",),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
        if any(column not in schema.get(table, set()) for column in columns)
    }


def _has_columns(schema: dict[str, set[str]], table: str, columns: set[str]) -> bool:
    return table in schema and columns.issubset(schema[table])


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object with conn")
    conn.row_factory = sqlite3.Row
    return conn


def _column_expr(columns: set[str], column: str, output: str) -> str:
    return f"{column} AS {output}" if column in columns else f"NULL AS {output}"


def _parse_timestamp(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _ensure_utc(parsed)


def _max_timestamp(left: Any, right: Any) -> str | None:
    left_dt = _parse_timestamp(left)
    right_dt = _parse_timestamp(right)
    if left_dt and right_dt:
        return (left if left_dt >= right_dt else right) or None
    return _clean(left) or _clean(right)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _first_clean(*values: Any) -> str | None:
    for value in values:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return None


def _clean(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
