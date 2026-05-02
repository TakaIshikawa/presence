"""Plan source reattachment for knowledge rows with weak attribution."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlsplit

from .link_metadata_enricher import normalize_canonical_url


DEFAULT_LIMIT = 50
DEFAULT_MIN_CONFIDENCE = 0.55
OPTIONAL_LINK_METADATA_TABLES = ("link_metadata",)
STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "how",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "this",
    "to",
    "with",
}


@dataclass(frozen=True)
class ReattachmentCandidate:
    source_table: str
    source_row_id: int | None
    source_type: str
    source_id: str | None
    source_url: str | None
    canonical_url: str | None
    title: str | None
    published_at: str | None
    confidence: float
    reason_codes: tuple[str, ...]
    recommended_update: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class OrphanKnowledgeItem:
    knowledge_id: int
    source_type: str
    source_id: str | None
    source_url: str | None
    title: str | None
    published_at: str | None
    reason_codes: tuple[str, ...]
    candidates: tuple[ReattachmentCandidate, ...] = field(default_factory=tuple)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class KnowledgeSourceReattachmentPlan:
    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    orphaned_items: tuple[OrphanKnowledgeItem, ...]
    missing_required_tables: tuple[str, ...] = ()
    missing_required_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)
    missing_optional_tables: tuple[str, ...] = ()
    capability_warnings: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class _CandidateSource:
    source_table: str
    source_row_id: int | None
    source_type: str
    source_id: str | None
    source_url: str | None
    canonical_url: str | None
    title: str | None
    published_at: str | None
    text: str


def build_knowledge_source_reattachment_plan(
    db_or_conn: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    source_type: str | None = None,
    now: datetime | None = None,
) -> KnowledgeSourceReattachmentPlan:
    """Return read-only source reattachment recommendations for orphaned knowledge."""

    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_confidence < 0 or min_confidence > 1:
        raise ValueError("min_confidence must be between 0 and 1")

    conn = _connection(db_or_conn)
    now = _normalize_datetime(now or datetime.now(timezone.utc))
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, tuple[str, ...]] = {}
    if "knowledge" not in schema:
        missing_tables.append("knowledge")
    else:
        required = {
            "id",
            "source_type",
            "source_id",
            "source_url",
            "content",
            "insight",
            "published_at",
            "ingested_at",
            "metadata",
            "created_at",
        }
        missing = tuple(sorted(required - schema["knowledge"]))
        if missing:
            missing_columns["knowledge"] = missing

    optional_missing = tuple(
        table for table in OPTIONAL_LINK_METADATA_TABLES if table not in schema
    )
    warnings = tuple(
        f"Optional link metadata table '{table}' is unavailable; using embedded knowledge metadata, curated sources, and nearby ingested items only."
        for table in optional_missing
    )

    filters = {
        "limit": limit,
        "min_confidence": min_confidence,
        "source_type": source_type,
    }
    if missing_tables or missing_columns:
        return KnowledgeSourceReattachmentPlan(
            artifact_type="knowledge_source_reattachment_plan",
            generated_at=now.isoformat(),
            filters=filters,
            totals={
                "orphaned_item_count": 0,
                "item_with_candidate_count": 0,
                "candidate_count": 0,
            },
            orphaned_items=(),
            missing_required_tables=tuple(missing_tables),
            missing_required_columns=missing_columns,
            missing_optional_tables=optional_missing,
            capability_warnings=warnings,
        )

    orphans = _load_orphan_rows(conn, limit=limit, source_type=source_type)
    candidate_sources = _load_candidate_sources(conn, schema, source_type=source_type)
    items = tuple(
        _plan_item(row, candidate_sources, min_confidence=min_confidence, now=now)
        for row in orphans
    )
    return KnowledgeSourceReattachmentPlan(
        artifact_type="knowledge_source_reattachment_plan",
        generated_at=now.isoformat(),
        filters=filters,
        totals={
            "orphaned_item_count": len(items),
            "item_with_candidate_count": sum(1 for item in items if item.candidates),
            "candidate_count": sum(len(item.candidates) for item in items),
        },
        orphaned_items=items,
        missing_optional_tables=optional_missing,
        capability_warnings=warnings,
    )


def format_knowledge_source_reattachment_json(
    plan: KnowledgeSourceReattachmentPlan,
) -> str:
    """Render a source reattachment plan as stable JSON."""

    return json.dumps(plan.as_dict(), indent=2, sort_keys=True)


def format_knowledge_source_reattachment_text(
    plan: KnowledgeSourceReattachmentPlan,
) -> str:
    """Render a concise terminal report."""

    totals = plan.totals
    lines = [
        "Knowledge Source Reattachment Plan",
        (
            "Filters: "
            f"source_type={plan.filters['source_type'] or 'all'} "
            f"limit={plan.filters['limit']} "
            f"min_confidence={plan.filters['min_confidence']}"
        ),
        (
            "Totals: "
            f"orphaned={totals['orphaned_item_count']} "
            f"with_candidates={totals['item_with_candidate_count']} "
            f"candidates={totals['candidate_count']}"
        ),
    ]
    if plan.missing_required_tables:
        lines.append("Missing required tables: " + ", ".join(plan.missing_required_tables))
    if plan.missing_required_columns:
        for table, columns in sorted(plan.missing_required_columns.items()):
            lines.append(f"Missing required columns on {table}: {', '.join(columns)}")
    for warning in plan.capability_warnings:
        lines.append(f"Capability warning: {warning}")

    if not plan.orphaned_items:
        lines.append("No orphaned knowledge rows found.")
        return "\n".join(lines)

    for item in plan.orphaned_items:
        title = _shorten(item.title or item.source_id or item.source_url or "-", 72)
        lines.append("")
        lines.append(
            f"- knowledge #{item.knowledge_id} {item.source_type} "
            f"reasons={','.join(item.reason_codes)} title={title}"
        )
        if not item.candidates:
            lines.append("  candidates: none above threshold")
            continue
        for candidate in item.candidates:
            label = _shorten(candidate.title or candidate.source_url or candidate.source_id or "-", 72)
            target_url = candidate.canonical_url or candidate.source_url or "-"
            lines.append(
                "  candidate "
                f"{candidate.confidence:.2f} {candidate.source_table}"
                f":{candidate.source_row_id or '-'} reasons={','.join(candidate.reason_codes)} "
                f"title={label} url={target_url}"
            )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3 connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row["name"]: {info[1] for info in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _load_orphan_rows(
    conn: sqlite3.Connection,
    *,
    limit: int,
    source_type: str | None,
) -> list[dict[str, Any]]:
    filters = [_orphan_filter_sql()]
    params: list[Any] = []
    if source_type:
        filters.append("source_type = ?")
        params.append(source_type)
    params.append(limit)
    rows = conn.execute(
        f"""SELECT id, source_type, source_id, source_url, content, insight,
                  published_at, ingested_at, metadata, created_at
           FROM knowledge
           WHERE {' AND '.join(filters)}
           ORDER BY COALESCE(published_at, ingested_at, created_at) DESC, id DESC
           LIMIT ?""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _orphan_filter_sql() -> str:
    return """(
        COALESCE(TRIM(source_url), '') = ''
        OR LOWER(TRIM(source_url)) IN ('none', 'unknown', 'n/a')
        OR (TRIM(source_url) NOT LIKE 'http://%' AND TRIM(source_url) NOT LIKE 'https://%')
    )"""


def _load_candidate_sources(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    source_type: str | None,
) -> list[_CandidateSource]:
    candidates: list[_CandidateSource] = []
    candidates.extend(_knowledge_sources(conn, source_type=source_type))
    if "curated_sources" in schema:
        candidates.extend(_curated_sources(conn, schema["curated_sources"]))
    if "link_metadata" in schema:
        candidates.extend(_link_metadata_sources(conn, schema["link_metadata"]))
    return candidates


def _knowledge_sources(
    conn: sqlite3.Connection,
    *,
    source_type: str | None,
) -> list[_CandidateSource]:
    params: list[Any] = []
    filters = [
        "COALESCE(TRIM(source_url), '') != ''",
        "(TRIM(source_url) LIKE 'http://%' OR TRIM(source_url) LIKE 'https://%')",
    ]
    if source_type:
        filters.append("source_type = ?")
        params.append(source_type)
    rows = conn.execute(
        f"""SELECT id, source_type, source_id, source_url, content, insight,
                  published_at, ingested_at, metadata, created_at
           FROM knowledge
           WHERE {' AND '.join(filters)}
           ORDER BY COALESCE(published_at, ingested_at, created_at) DESC, id DESC
           LIMIT 500""",
        params,
    ).fetchall()
    candidates = []
    for row in rows:
        data = dict(row)
        metadata = _parse_metadata(data.get("metadata"))
        link_metadata = _link_metadata(metadata)
        title = _clean_string(link_metadata.get("title")) or _clean_string(data.get("insight"))
        source_url = _clean_string(data.get("source_url"))
        canonical_url = _clean_string(link_metadata.get("canonical_url")) or source_url
        candidates.append(
            _CandidateSource(
                source_table="knowledge",
                source_row_id=int(data["id"]),
                source_type=data.get("source_type") or "",
                source_id=_clean_string(data.get("source_id")),
                source_url=source_url,
                canonical_url=_normalize_url(canonical_url),
                title=title,
                published_at=_clean_string(data.get("published_at") or data.get("ingested_at") or data.get("created_at")),
                text=" ".join(
                    value
                    for value in (
                        title or "",
                        _clean_string(data.get("content")) or "",
                        _clean_string(data.get("insight")) or "",
                    )
                    if value
                ),
            )
        )
    return candidates


def _curated_sources(
    conn: sqlite3.Connection,
    columns: set[str],
) -> list[_CandidateSource]:
    required = {"id", "source_type", "identifier"}
    if not required.issubset(columns):
        return []
    select = {
        "id": "id",
        "source_type": "source_type",
        "identifier": "identifier",
        "feed_url": _column_expr(columns, "feed_url"),
        "canonical_url": _column_expr(columns, "canonical_url"),
        "link_title": _column_expr(columns, "link_title"),
        "name": _column_expr(columns, "name"),
        "published_at": _column_expr(columns, "published_at"),
        "site_name": _column_expr(columns, "site_name"),
    }
    rows = conn.execute(
        f"""SELECT {select['id']} AS id,
                  {select['source_type']} AS source_type,
                  {select['identifier']} AS identifier,
                  {select['feed_url']} AS feed_url,
                  {select['canonical_url']} AS canonical_url,
                  {select['link_title']} AS link_title,
                  {select['name']} AS name,
                  {select['published_at']} AS published_at,
                  {select['site_name']} AS site_name
           FROM curated_sources
           ORDER BY id DESC
           LIMIT 500"""
    ).fetchall()
    candidates = []
    for row in rows:
        data = dict(row)
        source_url = _clean_string(data.get("canonical_url") or data.get("feed_url"))
        title = _clean_string(data.get("link_title") or data.get("name"))
        candidates.append(
            _CandidateSource(
                source_table="curated_sources",
                source_row_id=int(data["id"]),
                source_type=data.get("source_type") or "",
                source_id=_clean_string(data.get("identifier")),
                source_url=_clean_string(data.get("feed_url")),
                canonical_url=_normalize_url(source_url),
                title=title,
                published_at=_clean_string(data.get("published_at")),
                text=" ".join(
                    value
                    for value in (
                        title or "",
                        _clean_string(data.get("site_name")) or "",
                        _clean_string(data.get("identifier")) or "",
                    )
                    if value
                ),
            )
        )
    return candidates


def _link_metadata_sources(
    conn: sqlite3.Connection,
    columns: set[str],
) -> list[_CandidateSource]:
    url_column = _first_existing(columns, ("canonical_url", "url", "source_url"))
    if not url_column:
        return []
    id_column = _first_existing(columns, ("id", "rowid"))
    title_column = _first_existing(columns, ("title", "link_title"))
    type_column = _first_existing(columns, ("source_type", "type"))
    source_id_column = _first_existing(columns, ("source_id", "identifier"))
    published_column = _first_existing(columns, ("published_at", "created_at", "updated_at"))
    rows = conn.execute(
        f"""SELECT {id_column or 'rowid'} AS id,
                  {_column_expr(columns, type_column)} AS source_type,
                  {_column_expr(columns, source_id_column)} AS source_id,
                  {_column_expr(columns, url_column)} AS source_url,
                  {_column_expr(columns, 'canonical_url')} AS canonical_url,
                  {_column_expr(columns, title_column)} AS title,
                  {_column_expr(columns, published_column)} AS published_at
           FROM link_metadata
           WHERE COALESCE(TRIM({_column_expr(columns, url_column)}), '') != ''
           ORDER BY id DESC
           LIMIT 500"""
    ).fetchall()
    candidates = []
    for row in rows:
        data = dict(row)
        source_url = _clean_string(data.get("source_url"))
        canonical_url = _clean_string(data.get("canonical_url")) or source_url
        title = _clean_string(data.get("title"))
        candidates.append(
            _CandidateSource(
                source_table="link_metadata",
                source_row_id=int(data["id"]) if data.get("id") is not None else None,
                source_type=data.get("source_type") or "link_metadata",
                source_id=_clean_string(data.get("source_id")),
                source_url=source_url,
                canonical_url=_normalize_url(canonical_url),
                title=title,
                published_at=_clean_string(data.get("published_at")),
                text=title or "",
            )
        )
    return candidates


def _plan_item(
    row: dict[str, Any],
    candidate_sources: list[_CandidateSource],
    *,
    min_confidence: float,
    now: datetime,
) -> OrphanKnowledgeItem:
    metadata = _parse_metadata(row.get("metadata"))
    link_metadata = _link_metadata(metadata)
    title = (
        _clean_string(link_metadata.get("title"))
        or _clean_string(metadata.get("title"))
        or _clean_string(row.get("insight"))
    )
    reason_codes = _orphan_reason_codes(row, link_metadata)
    source_text = " ".join(
        value
        for value in (
            title or "",
            _clean_string(row.get("content")) or "",
            _clean_string(row.get("insight")) or "",
        )
        if value
    )
    url_hints = _url_hints(row, metadata, link_metadata)
    candidates = []
    for candidate in candidate_sources:
        if candidate.source_table == "knowledge" and candidate.source_row_id == int(row["id"]):
            continue
        scored = _score_candidate(
            candidate,
            source_text=source_text,
            url_hints=url_hints,
            source_published_at=_clean_string(row.get("published_at") or row.get("ingested_at") or row.get("created_at")),
            knowledge_id=int(row["id"]),
            now=now,
        )
        if scored and scored.confidence >= min_confidence:
            candidates.append(scored)
    candidates = sorted(
        candidates,
        key=lambda item: (
            -item.confidence,
            item.source_table,
            item.source_row_id or 0,
            item.source_url or "",
        ),
    )[:5]
    return OrphanKnowledgeItem(
        knowledge_id=int(row["id"]),
        source_type=row.get("source_type") or "",
        source_id=_clean_string(row.get("source_id")),
        source_url=_clean_string(row.get("source_url")),
        title=title,
        published_at=_clean_string(row.get("published_at") or row.get("ingested_at") or row.get("created_at")),
        reason_codes=tuple(reason_codes),
        candidates=tuple(candidates),
    )


def _score_candidate(
    candidate: _CandidateSource,
    *,
    source_text: str,
    url_hints: set[str],
    source_published_at: str | None,
    knowledge_id: int,
    now: datetime,
) -> ReattachmentCandidate | None:
    candidate_urls = {
        url
        for url in (
            _normalize_url(candidate.source_url),
            _normalize_url(candidate.canonical_url),
        )
        if url
    }
    exact_url = bool(url_hints & candidate_urls)
    overlap = _token_overlap(source_text, candidate.text)
    recency = _recency_score(source_published_at, candidate.published_at, now)

    score = 0.0
    reasons: list[str] = []
    if exact_url:
        score += 0.78
        reasons.append("exact_url_match")
    if overlap >= 0.2:
        score += min(0.62, overlap * 0.75)
        reasons.append("title_overlap_match")
    if recency > 0:
        score += recency
        reasons.append("recency_proximity")
    if candidate.source_table == "curated_sources":
        score += 0.03
        reasons.append("curated_source_record")
    elif candidate.source_table == "link_metadata":
        score += 0.04
        reasons.append("link_metadata_record")
    elif candidate.source_table == "knowledge":
        score += 0.02
        reasons.append("nearby_ingested_item")

    score = round(min(score, 1.0), 3)
    if score <= 0:
        return None
    target_url = candidate.canonical_url or candidate.source_url
    update_fields = {
        key: value
        for key, value in {
            "source_url": target_url,
            "source_id": candidate.source_id or target_url,
            "metadata.link_metadata.canonical_url": candidate.canonical_url or target_url,
            "metadata.link_metadata.title": candidate.title,
            "metadata.source_reattachment_candidate": {
                "source_table": candidate.source_table,
                "source_row_id": candidate.source_row_id,
                "confidence": score,
                "reason_codes": reasons,
            },
        }.items()
        if value
    }
    return ReattachmentCandidate(
        source_table=candidate.source_table,
        source_row_id=candidate.source_row_id,
        source_type=candidate.source_type,
        source_id=candidate.source_id,
        source_url=candidate.source_url,
        canonical_url=candidate.canonical_url,
        title=candidate.title,
        published_at=candidate.published_at,
        confidence=score,
        reason_codes=tuple(reasons),
        recommended_update={
            "table": "knowledge",
            "row_id": knowledge_id,
            "set": update_fields,
        },
    )


def _orphan_reason_codes(row: dict[str, Any], link_metadata: dict[str, Any]) -> list[str]:
    source_url = _clean_string(row.get("source_url"))
    source_id = _clean_string(row.get("source_id"))
    reasons = []
    if not source_url:
        reasons.append("missing_source_url")
    elif not _is_http_url(source_url):
        reasons.append("weak_source_url")
    if not source_id:
        reasons.append("missing_source_id")
    if not _clean_string(link_metadata.get("canonical_url")):
        reasons.append("missing_canonical_url")
    return reasons or ["weak_source_attribution"]


def _url_hints(
    row: dict[str, Any],
    metadata: dict[str, Any],
    link_metadata: dict[str, Any],
) -> set[str]:
    hints = set()
    for value in (
        row.get("source_url"),
        row.get("source_id"),
        metadata.get("source_url"),
        metadata.get("url"),
        metadata.get("canonical_url"),
        link_metadata.get("canonical_url"),
        link_metadata.get("url"),
    ):
        normalized = _normalize_url(_clean_string(value))
        if normalized:
            hints.add(normalized)
    return hints


def _normalize_url(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip()
    if not text:
        return None
    if "://" not in text and "." in text.split("/", 1)[0]:
        text = "https://" + text
    if not _is_http_url(text):
        return None
    return normalize_canonical_url(text)


def _is_http_url(value: str | None) -> bool:
    if not value:
        return False
    parts = urlsplit(value.strip())
    return parts.scheme in {"http", "https"} and bool(parts.netloc)


def _token_overlap(left: str, right: str) -> float:
    left_tokens = _tokens(left)
    right_tokens = _tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if len(token) > 2 and token not in STOPWORDS
    }


def _recency_score(
    left_value: str | None,
    right_value: str | None,
    now: datetime,
) -> float:
    left = _parse_datetime(left_value) or now
    right = _parse_datetime(right_value)
    if right is None:
        return 0.0
    delta_days = abs((left - right).total_seconds()) / 86400
    if delta_days <= 1:
        return 0.1
    if delta_days <= 7:
        return 0.07
    if delta_days <= 30:
        return 0.04
    return 0.0


def _parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _normalize_datetime(parsed)


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _link_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    value = metadata.get("link_metadata")
    return value if isinstance(value, dict) else {}


def _clean_string(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    text = " ".join(value.split())
    return text or None


def _column_expr(columns: set[str], name: str | None, table_alias: str | None = None) -> str:
    if name and name in columns:
        return f"{table_alias}.{name}" if table_alias else name
    return "NULL"


def _first_existing(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _shorten(value: str, limit: int) -> str:
    text = value.replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."
