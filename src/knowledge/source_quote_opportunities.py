"""Rank unused source excerpts for quote-backed content generation."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Mapping, Sequence


DEFAULT_DAYS = 90
DEFAULT_LIMIT = 25
DEFAULT_MIN_SCORE = 0.0

TEXT_COLUMNS = ("content", "insight", "note", "source", "published_url", "tweet_id")
METADATA_COLUMNS = ("metadata", "source_metadata", "source_material")
TOPIC_KEYS = ("topic", "topics", "tags", "keywords", "category", "theme", "themes")
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
STOPWORDS = {
    "about",
    "after",
    "again",
    "against",
    "also",
    "because",
    "before",
    "being",
    "between",
    "could",
    "every",
    "from",
    "have",
    "into",
    "more",
    "only",
    "source",
    "that",
    "their",
    "there",
    "these",
    "this",
    "through",
    "when",
    "where",
    "with",
    "without",
    "would",
}

WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9'-]{2,}")
WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class SourceQuoteOpportunity:
    """One ranked source excerpt opportunity."""

    source_id: str
    knowledge_id: int | None
    title: str | None
    url: str | None
    excerpt: str
    topic_terms: tuple[str, ...]
    freshness_days: int | None
    usage_count: int
    opportunity_score: float
    suggested_content_angle: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "excerpt": self.excerpt,
            "freshness_days": self.freshness_days,
            "knowledge_id": self.knowledge_id,
            "opportunity_score": self.opportunity_score,
            "source_id": self.source_id,
            "suggested_content_angle": self.suggested_content_angle,
            "title": self.title,
            "topic_terms": list(self.topic_terms),
            "url": self.url,
            "usage_count": self.usage_count,
        }


@dataclass(frozen=True)
class SourceQuoteOpportunityReport:
    """Deterministic quote-opportunity export."""

    generated_at: str
    filters: dict[str, Any]
    total_candidates: int
    opportunity_count: int
    opportunities: tuple[SourceQuoteOpportunity, ...]
    warnings: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "source_quote_opportunities",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "opportunities": [item.to_dict() for item in self.opportunities],
            "opportunity_count": self.opportunity_count,
            "total_candidates": self.total_candidates,
            "warnings": list(self.warnings),
        }


def build_source_quote_opportunity_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_score: float = DEFAULT_MIN_SCORE,
    now: datetime | None = None,
) -> SourceQuoteOpportunityReport:
    """Build ranked quote opportunities from knowledge and downstream usage."""

    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_score < 0:
        raise ValueError("min_score must be non-negative")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    filters = {"days": days, "limit": limit, "min_score": min_score}
    if "knowledge" not in schema:
        return SourceQuoteOpportunityReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            total_candidates=0,
            opportunity_count=0,
            opportunities=(),
            missing_tables=("knowledge",),
        )

    required = {"knowledge": ("id", "content")}
    missing_columns = {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in required.items()
        if table in schema
        if any(column not in schema.get(table, set()) for column in columns)
    }
    if missing_columns:
        return SourceQuoteOpportunityReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            total_candidates=0,
            opportunity_count=0,
            opportunities=(),
            missing_columns=missing_columns,
        )

    warning_list: list[str] = []
    candidates = _knowledge_candidates(
        conn,
        schema,
        cutoff=generated_at - timedelta(days=days),
        warnings=warning_list,
    )
    usage = _usage_counts(conn, schema, candidates, warnings=warning_list)
    topic_counts = Counter(
        term for candidate in candidates for term in candidate["topic_terms"][:3]
    )
    opportunities = [
        _opportunity(candidate, usage.get(candidate["knowledge_id"], 0), topic_counts, generated_at)
        for candidate in candidates
    ]
    opportunities = [
        item for item in opportunities if item.opportunity_score >= min_score
    ]
    opportunities.sort(
        key=lambda item: (
            -item.opportunity_score,
            item.usage_count,
            item.freshness_days if item.freshness_days is not None else 999999,
            item.source_id,
            item.knowledge_id or 0,
        )
    )
    selected = tuple(opportunities[:limit])
    return SourceQuoteOpportunityReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        total_candidates=len(candidates),
        opportunity_count=len(selected),
        opportunities=selected,
        warnings=tuple(sorted(set(warning_list))),
    )


def format_source_quote_opportunities_json(report: SourceQuoteOpportunityReport) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_quote_opportunities_text(report: SourceQuoteOpportunityReport) -> str:
    """Render a compact text report for review."""

    filters = report.filters
    lines = [
        "Source Quote Opportunities",
        f"Generated: {report.generated_at}",
        (
            f"Filters: days={filters['days']} limit={filters['limit']} "
            f"min_score={filters['min_score']}"
        ),
        f"Opportunities: {report.opportunity_count}/{report.total_candidates}",
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + ", ".join(
                f"{table}.{column}"
                for table, columns in sorted(report.missing_columns.items())
                for column in columns
            )
        )
    if report.warnings:
        lines.append(f"Warnings: {len(report.warnings)}")
        lines.extend(f"  - {warning}" for warning in report.warnings)
    if not report.opportunities:
        lines.append("No source quote opportunities matched.")
        return "\n".join(lines)

    lines.append("Candidates:")
    for item in report.opportunities:
        title = item.title or item.url or item.source_id
        topics = ", ".join(item.topic_terms) if item.topic_terms else "-"
        freshness = item.freshness_days if item.freshness_days is not None else "n/a"
        lines.append(
            "  - "
            f"source_id={item.source_id} score={item.opportunity_score:.3f} "
            f"usage={item.usage_count} freshness_days={freshness} title={title}"
        )
        lines.append(f"    topics={topics}")
        lines.append(f"    angle={item.suggested_content_angle}")
        lines.append(f"    excerpt={item.excerpt!r}")
    return "\n".join(lines)


def _knowledge_candidates(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    warnings: list[str],
) -> list[dict[str, Any]]:
    columns = schema["knowledge"]
    date_expr = _knowledge_date_expr(columns)
    where = []
    params: list[Any] = []
    if "approved" in columns:
        where.append("COALESCE(approved, 0) = 1")
    if date_expr != "NULL":
        where.append(f"{date_expr} >= ?")
        params.append(cutoff.isoformat())
    sql = f"""SELECT {_column_expr(columns, 'id')},
                     {_column_expr(columns, 'source_type')},
                     {_column_expr(columns, 'source_id')},
                     {_column_expr(columns, 'source_url')},
                     {_column_expr(columns, 'author')},
                     {_column_expr(columns, 'content')},
                     {_column_expr(columns, 'insight')},
                     {_column_expr(columns, 'published_at')},
                     {_column_expr(columns, 'ingested_at')},
                     {_column_expr(columns, 'created_at')},
                     {_column_expr(columns, 'metadata')}
              FROM knowledge"""
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY datetime({date_expr}) DESC, id ASC" if date_expr != "NULL" else " ORDER BY id ASC"
    rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
    candidates: list[dict[str, Any]] = []
    for row in rows:
        metadata = _metadata_object(row.get("metadata"), "knowledge", row.get("id"), "metadata", warnings)
        excerpt = _excerpt(metadata, row)
        if not excerpt:
            continue
        source_id = _clean(row.get("source_id") or row.get("source_url") or row.get("id"))
        title = _first_clean(
            metadata.get("title"),
            metadata.get("headline"),
            metadata.get("link_title"),
            row.get("source_id"),
        )
        topics = _topic_terms(metadata, row)
        candidates.append(
            {
                "knowledge_id": int(row["id"]),
                "source_id": source_id or str(row["id"]),
                "url": _clean(row.get("source_url") or metadata.get("url")),
                "title": title,
                "excerpt": excerpt,
                "topic_terms": topics,
                "freshness_at": _parse_timestamp(
                    row.get("published_at") or row.get("ingested_at") or row.get("created_at")
                ),
                "trust": _trust_score(metadata),
            }
        )
    return candidates


def _usage_counts(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    candidates: Sequence[dict[str, Any]],
    *,
    warnings: list[str],
) -> dict[int, int]:
    counts: Counter[int] = Counter()
    ids = {int(item["knowledge_id"]) for item in candidates}
    by_source_id = {
        str(item["source_id"]).lower(): int(item["knowledge_id"])
        for item in candidates
        if item.get("source_id")
    }
    by_url = {
        str(item["url"]).lower(): int(item["knowledge_id"])
        for item in candidates
        if item.get("url")
    }
    if not ids:
        return {}
    if "content_knowledge_links" in schema and "knowledge_id" in schema["content_knowledge_links"]:
        rows = conn.execute(
            """SELECT knowledge_id, COUNT(*) AS usage_count
               FROM content_knowledge_links
               WHERE knowledge_id IS NOT NULL
               GROUP BY knowledge_id"""
        ).fetchall()
        for row in rows:
            knowledge_id = int(row["knowledge_id"])
            if knowledge_id in ids:
                counts[knowledge_id] += int(row["usage_count"] or 0)

    for table in ("generated_content", "content_ideas"):
        if table not in schema:
            continue
        table_columns = schema[table]
        selected = ["id"] if "id" in table_columns else ["rowid AS id"]
        selected.extend(column for column in (*TEXT_COLUMNS, *METADATA_COLUMNS) if column in table_columns)
        if len(selected) == 1:
            continue
        for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table} ORDER BY id ASC"):
            payload = dict(row)
            referenced = set()
            for column in METADATA_COLUMNS:
                if column in payload:
                    metadata = _metadata_object(payload.get(column), table, payload.get("id"), column, warnings)
                    referenced.update(_references_from_value(metadata, by_source_id, by_url, ids))
            for column in TEXT_COLUMNS:
                if column in payload:
                    referenced.update(
                        _references_from_value(
                            payload.get(column),
                            by_source_id,
                            by_url,
                            ids,
                            match_source_ids=False,
                        )
                    )
            for knowledge_id in referenced:
                counts[knowledge_id] += 1
    return dict(counts)


def _opportunity(
    candidate: dict[str, Any],
    usage_count: int,
    topic_counts: Counter[str],
    now: datetime,
) -> SourceQuoteOpportunity:
    freshness_at = candidate.get("freshness_at")
    freshness_days = (now.date() - freshness_at.date()).days if freshness_at else None
    freshness_score = 12.0
    if freshness_days is not None:
        freshness_score = max(0.0, 25.0 * (1.0 - min(freshness_days, 180) / 180.0))
    trust_score = 25.0 * float(candidate.get("trust") or 0.5)
    length_score = _length_score(candidate["excerpt"])
    topic_terms = tuple(candidate["topic_terms"])
    if topic_terms:
        rarest = min(topic_counts.get(term, 1) for term in topic_terms[:3])
        diversity_score = max(3.0, 15.0 / rarest)
    else:
        diversity_score = 3.0
    usage_penalty = min(45.0, usage_count * 22.0)
    score = max(0.0, freshness_score + trust_score + length_score + diversity_score - usage_penalty)
    return SourceQuoteOpportunity(
        source_id=str(candidate["source_id"]),
        knowledge_id=int(candidate["knowledge_id"]),
        title=candidate.get("title"),
        url=candidate.get("url"),
        excerpt=candidate["excerpt"],
        topic_terms=topic_terms,
        freshness_days=freshness_days,
        usage_count=usage_count,
        opportunity_score=round(score, 3),
        suggested_content_angle=_suggested_angle(topic_terms, candidate["excerpt"]),
    )


def _references_from_value(
    value: Any,
    by_source_id: Mapping[str, int],
    by_url: Mapping[str, int],
    ids: set[int],
    *,
    match_source_ids: bool = True,
) -> set[int]:
    references: set[int] = set()
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key).lower()
            if key_text in {"knowledge_id", "source_content_id"}:
                parsed = _int_or_none(item)
                if parsed in ids:
                    references.add(int(parsed))
            if key_text in {"knowledge_ids", "source_ids", "source_content_ids"}:
                for parsed in _int_values(item):
                    if parsed in ids:
                        references.add(parsed)
            references.update(_references_from_value(item, by_source_id, by_url, ids))
        return references
    if isinstance(value, (list, tuple, set)):
        for item in value:
            references.update(
                _references_from_value(
                    item,
                    by_source_id,
                    by_url,
                    ids,
                    match_source_ids=match_source_ids,
                )
            )
        return references
    text = _clean(value)
    if not text:
        return references
    lower = text.lower()
    if match_source_ids:
        for source_id, knowledge_id in by_source_id.items():
            if source_id and _contains_identifier(lower, source_id):
                references.add(knowledge_id)
    for url, knowledge_id in by_url.items():
        if url and url in lower:
            references.add(knowledge_id)
    return references


def _contains_identifier(text: str, identifier: str) -> bool:
    return re.search(rf"(?<![A-Za-z0-9_-]){re.escape(identifier)}(?![A-Za-z0-9_-])", text) is not None


def _metadata_object(
    raw_value: Any,
    table: str,
    row_id: Any,
    column: str,
    warnings: list[str],
) -> dict[str, Any]:
    if isinstance(raw_value, dict):
        return raw_value
    if raw_value in (None, ""):
        return {}
    try:
        parsed = json.loads(str(raw_value))
    except json.JSONDecodeError as exc:
        warnings.append(f"{table}:{row_id}.{column} malformed JSON: {exc.msg}")
        return {}
    if not isinstance(parsed, dict):
        warnings.append(f"{table}:{row_id}.{column} metadata is not a JSON object")
        return {}
    return parsed


def _excerpt(metadata: Mapping[str, Any], row: Mapping[str, Any]) -> str | None:
    value = _first_clean(
        metadata.get("quote"),
        metadata.get("excerpt"),
        metadata.get("summary_excerpt"),
        row.get("insight"),
        row.get("content"),
    )
    if not value:
        return None
    return _shorten(WHITESPACE_RE.sub(" ", value), 420)


def _topic_terms(metadata: Mapping[str, Any], row: Mapping[str, Any]) -> tuple[str, ...]:
    terms: list[str] = []
    for key in TOPIC_KEYS:
        terms.extend(_string_values(metadata.get(key)))
    text = " ".join(str(item or "") for item in (row.get("insight"), row.get("content")))
    counter = Counter(
        word.lower()
        for word in WORD_RE.findall(text)
        if word.lower() not in STOPWORDS and len(word) > 3
    )
    terms.extend(word for word, _count in counter.most_common(8))
    return tuple(_dedupe_terms(terms)[:8])


def _trust_score(metadata: Mapping[str, Any]) -> float:
    for key in TRUST_KEYS:
        score = _float_or_none(metadata.get(key))
        if score is not None:
            return max(0.0, min(score if score <= 1 else score / 100.0, 1.0))
    tier = _clean(metadata.get("source_tier") or metadata.get("tier") or metadata.get("trust_tier"))
    if tier:
        return TIER_SCORES.get(tier.lower(), 0.5)
    return 0.5


def _length_score(text: str) -> float:
    length = len(text)
    if 120 <= length <= 300:
        return 20.0
    if length < 120:
        return max(4.0, 20.0 * length / 120.0)
    return max(6.0, 20.0 * (1.0 - min(length - 300, 300) / 300.0))


def _suggested_angle(topic_terms: Sequence[str], excerpt: str) -> str:
    topic = topic_terms[0] if topic_terms else "this source"
    cue = "why it matters"
    lowered = excerpt.lower()
    if any(word in lowered for word in ("risk", "fails", "failure", "problem")):
        cue = "the hidden risk"
    elif any(word in lowered for word in ("better", "improve", "works", "effective")):
        cue = "what works"
    return f"Use the quote to frame {cue} in {topic}."


def _knowledge_date_expr(columns: set[str]) -> str:
    candidates = [column for column in ("published_at", "ingested_at", "created_at") if column in columns]
    if not candidates:
        return "NULL"
    return "COALESCE(" + ", ".join(candidates) + ")"


def _column_expr(columns: set[str], column: str) -> str:
    return f"{column} AS {column}" if column in columns else f"NULL AS {column}"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        str(row["name"]): {
            str(column["name"])
            for column in conn.execute(f"PRAGMA table_info({row['name']})").fetchall()
        }
        for row in rows
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object with conn")
    conn.row_factory = sqlite3.Row
    return conn


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
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _string_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if _clean(item)]
    return [str(value)] if _clean(value) else []


def _dedupe_terms(values: Sequence[str]) -> list[str]:
    terms: list[str] = []
    seen: set[str] = set()
    for value in values:
        for part in re.split(r"[,;/|]", str(value)):
            term = WHITESPACE_RE.sub(" ", part.strip().lower())
            if not term or term in seen or term in STOPWORDS:
                continue
            seen.add(term)
            terms.append(term)
    return terms


def _shorten(text: str, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_values(value: Any) -> list[int]:
    if isinstance(value, (list, tuple, set)):
        return [parsed for item in value if (parsed := _int_or_none(item)) is not None]
    parsed = _int_or_none(value)
    return [] if parsed is None else [parsed]
