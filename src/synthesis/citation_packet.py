"""Build operator citation packets for generated-content claims."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any

from synthesis.claim_checker import Claim, ClaimChecker


STATUS_UNSUPPORTED = "unsupported"
STATUS_WEAK = "weakly_supported"
STATUS_SUPPORTED = "supported"


@dataclass(frozen=True)
class CitationPacketSource:
    """Source context attached to one reviewed claim."""

    knowledge_id: int | None
    link_id: int | None
    relevance_score: float | None
    source_type: str | None
    source_id: str | None
    title: str | None
    url: str | None
    canonical_url: str | None
    author: str | None
    freshness: dict[str, Any]
    license: dict[str, Any]
    matched_terms: list[str]
    evidence_text: str | None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CitationPacketClaim:
    """One extracted claim plus review context."""

    text: str
    kind: str
    support_status: str
    matched_terms: list[str]
    review_notes: list[str]
    sources: list[CitationPacketSource]


@dataclass(frozen=True)
class CitationPacket:
    """JSON-serializable citation review packet."""

    artifact_type: str
    content_id: int
    available: bool
    unavailable_reason: str | None
    missing_required_tables: list[str]
    generated_at: str
    include_supported: bool
    content: dict[str, Any] | None
    claim_check: dict[str, Any] | None
    claim_count: int
    included_claim_count: int
    unsupported_count: int
    weakly_supported_count: int
    supported_count: int
    claims: list[CitationPacketClaim]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_citation_packet(
    db: Any,
    content_id: int,
    *,
    include_supported: bool = False,
    now: datetime | None = None,
) -> CitationPacket:
    """Assemble generated claims and linked source metadata for review."""

    if content_id < 1:
        raise ValueError("content_id must be positive")

    generated_at = _normalize_datetime(now or datetime.now(timezone.utc))
    conn = _connection(db)
    schema = _schema(conn)
    required = ["generated_content", "content_claim_checks", "content_knowledge_links", "knowledge"]
    missing = [table for table in required if table not in schema]
    if missing:
        return _unavailable_packet(
            content_id=content_id,
            include_supported=include_supported,
            generated_at=generated_at,
            missing=missing,
        )

    content = _load_content(conn, schema, content_id)
    if content is None:
        raise ValueError(f"Content ID {content_id} not found")

    claim_check = _load_claim_check(conn, schema, content_id)
    sources = _load_knowledge_sources(conn, schema, content_id, generated_at)
    claims = _build_claims(
        text=str(content.get("content") or ""),
        sources=sources,
        include_supported=include_supported,
    )
    all_claims = _build_claims(
        text=str(content.get("content") or ""),
        sources=sources,
        include_supported=True,
    )
    counts = {
        STATUS_UNSUPPORTED: sum(1 for claim in all_claims if claim.support_status == STATUS_UNSUPPORTED),
        STATUS_WEAK: sum(1 for claim in all_claims if claim.support_status == STATUS_WEAK),
        STATUS_SUPPORTED: sum(1 for claim in all_claims if claim.support_status == STATUS_SUPPORTED),
    }
    return CitationPacket(
        artifact_type="citation_packet",
        content_id=content_id,
        available=True,
        unavailable_reason=None,
        missing_required_tables=[],
        generated_at=generated_at.isoformat(),
        include_supported=include_supported,
        content=_content_metadata(content),
        claim_check=_claim_check_payload(claim_check),
        claim_count=len(all_claims),
        included_claim_count=len(claims),
        unsupported_count=counts[STATUS_UNSUPPORTED],
        weakly_supported_count=counts[STATUS_WEAK],
        supported_count=counts[STATUS_SUPPORTED],
        claims=claims,
    )


def format_json_packet(packet: CitationPacket) -> str:
    """Render a citation packet as deterministic JSON."""

    return json.dumps(packet.as_dict(), indent=2, sort_keys=True, default=str)


def format_markdown_packet(packet: CitationPacket) -> str:
    """Render a citation packet as stable markdown for operator review."""

    if not packet.available:
        missing = ", ".join(packet.missing_required_tables) or "-"
        return "\n".join(
            [
                f"# Citation Packet: Content #{packet.content_id}",
                "",
                "Unavailable",
                "",
                f"- Reason: {packet.unavailable_reason or 'required data unavailable'}",
                f"- Missing required tables: {missing}",
            ]
        )

    content = packet.content or {}
    claim_check = packet.claim_check or {}
    lines = [
        f"# Citation Packet: Content #{packet.content_id}",
        "",
        "## Content",
        f"- Type: {content.get('content_type') or '-'}",
        f"- Format: {content.get('content_format') or '-'}",
        f"- Created: {content.get('created_at') or '-'}",
        "",
        str(content.get("text") or ""),
        "",
        "## Summary",
        f"- Claim check: {claim_check.get('status') or 'unchecked'}",
        f"- Included claims: {packet.included_claim_count} of {packet.claim_count}",
        f"- Unsupported: {packet.unsupported_count}",
        f"- Weakly supported: {packet.weakly_supported_count}",
        f"- Supported: {packet.supported_count}",
    ]
    if claim_check.get("annotation_text"):
        lines.extend(["- Claim-check notes:"] + [f"  - {line}" for line in claim_check["annotation_text"].splitlines()])

    if not packet.claims:
        lines.extend(["", "## Claims", "- none"])
        return "\n".join(lines)

    for index, claim in enumerate(packet.claims, start=1):
        lines.extend(
            [
                "",
                f"## Claim {index}: {claim.support_status}",
                claim.text,
                "",
                f"- Kind: {claim.kind}",
                f"- Matched terms: {', '.join(claim.matched_terms) or 'none'}",
                "- Review notes:",
            ]
        )
        lines.extend(f"  - {note}" for note in claim.review_notes)
        lines.append("- Sources:")
        if claim.sources:
            for source in claim.sources:
                source_label = source.title or source.source_id or f"knowledge #{source.knowledge_id or '-'}"
                url = source.canonical_url or source.url or "-"
                age = source.freshness.get("age_days")
                age_text = f"{age:.2f}" if isinstance(age, (int, float)) else "-"
                license_status = source.license.get("status") or "-"
                lines.append(f"  - {source_label}")
                lines.append(f"    - URL: {url}")
                lines.append(f"    - Freshness: {source.freshness.get('source_timestamp') or '-'} (age_days={age_text})")
                lines.append(f"    - License: {license_status}, approved={_yes_no(source.license.get('approved'))}")
                lines.append(f"    - Matched terms: {', '.join(source.matched_terms) or 'none'}")
        else:
            lines.append("  - none")
    return "\n".join(lines)


def _build_claims(
    *,
    text: str,
    sources: list[CitationPacketSource],
    include_supported: bool,
) -> list[CitationPacketClaim]:
    checker = ClaimChecker()
    claims = checker.extract_claims(text)
    packet_claims = [_claim_packet(claim, sources, checker) for claim in claims]
    packet_claims.sort(key=lambda claim: (_status_sort_key(claim.support_status), claim.text))
    if include_supported:
        return packet_claims
    return [claim for claim in packet_claims if claim.support_status != STATUS_SUPPORTED]


def _claim_packet(
    claim: Claim,
    sources: list[CitationPacketSource],
    checker: ClaimChecker,
) -> CitationPacketClaim:
    matched_sources = []
    partial_terms: set[str] = set()
    supported_terms: set[str] = set()
    for source in sources:
        evidence_norm = checker._normalize(source.evidence_text or "")
        supported, matched_terms, _reason = checker._claim_supported(claim, evidence_norm)
        if matched_terms:
            partial_terms.update(matched_terms)
            matched_sources.append(_source_with_matches(source, matched_terms))
        if supported:
            supported_terms.update(matched_terms)

    if supported_terms:
        support_status = STATUS_SUPPORTED
        matched = sorted(supported_terms)
    else:
        support_status = STATUS_WEAK if partial_terms else STATUS_UNSUPPORTED
        matched = sorted(partial_terms)
    return CitationPacketClaim(
        text=claim.text,
        kind=claim.kind,
        support_status=support_status,
        matched_terms=matched,
        review_notes=_review_notes(support_status, matched_sources),
        sources=matched_sources,
    )


def _review_notes(status: str, sources: list[CitationPacketSource]) -> list[str]:
    if status == STATUS_SUPPORTED:
        notes = ["Claim terms are covered by linked source evidence."]
    elif status == STATUS_WEAK:
        notes = ["Only partial claim terms appear in linked source evidence; verify before publishing."]
    else:
        notes = ["No linked source evidence matched this claim; revise, remove, or add support."]

    if not sources:
        notes.append("No source metadata is attached to this claim.")
        return notes
    if any(not (source.canonical_url or source.url) for source in sources):
        notes.append("At least one matching source lacks a traceable URL.")
    if any(source.license.get("status") == "restricted" for source in sources):
        notes.append("At least one matching source is marked restricted.")
    if any(source.license.get("approved") is False for source in sources):
        notes.append("At least one matching source is not approved.")
    return notes


def _load_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> dict[str, Any] | None:
    columns = schema["generated_content"]
    row = conn.execute(
        f"""SELECT id,
                  {_column_expr(columns, "content_type")} AS content_type,
                  {_column_expr(columns, "content_format")} AS content_format,
                  {_column_expr(columns, "content")} AS content,
                  {_column_expr(columns, "created_at")} AS created_at,
                  {_column_expr(columns, "published")} AS published,
                  {_column_expr(columns, "published_url")} AS published_url
           FROM generated_content
           WHERE id = ?""",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def _load_claim_check(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
) -> dict[str, Any] | None:
    columns = schema["content_claim_checks"]
    row = conn.execute(
        f"""SELECT content_id,
                  {_column_expr(columns, "supported_count")} AS supported_count,
                  {_column_expr(columns, "unsupported_count")} AS unsupported_count,
                  {_column_expr(columns, "annotation_text")} AS annotation_text,
                  {_column_expr(columns, "created_at")} AS created_at,
                  {_column_expr(columns, "updated_at")} AS updated_at
           FROM content_claim_checks
           WHERE content_id = ?""",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def _load_knowledge_sources(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    now: datetime,
) -> list[CitationPacketSource]:
    link_columns = schema["content_knowledge_links"]
    knowledge_columns = schema["knowledge"]
    rows = conn.execute(
        f"""SELECT {_column_expr(link_columns, "id", "ckl")} AS link_id,
                  ckl.knowledge_id,
                  {_column_expr(link_columns, "relevance_score", "ckl")} AS relevance_score,
                  k.id AS matched_knowledge_id,
                  {_column_expr(knowledge_columns, "source_type", "k")} AS source_type,
                  {_column_expr(knowledge_columns, "source_id", "k")} AS source_id,
                  {_column_expr(knowledge_columns, "source_url", "k")} AS source_url,
                  {_column_expr(knowledge_columns, "author", "k")} AS author,
                  {_column_expr(knowledge_columns, "content", "k")} AS content,
                  {_column_expr(knowledge_columns, "insight", "k")} AS insight,
                  {_column_expr(knowledge_columns, "license", "k")} AS license,
                  {_column_expr(knowledge_columns, "approved", "k")} AS approved,
                  {_column_expr(knowledge_columns, "attribution_required", "k")} AS attribution_required,
                  {_column_expr(knowledge_columns, "published_at", "k")} AS published_at,
                  {_column_expr(knowledge_columns, "ingested_at", "k")} AS ingested_at,
                  {_column_expr(knowledge_columns, "created_at", "k")} AS knowledge_created_at,
                  {_column_expr(knowledge_columns, "metadata", "k")} AS metadata
           FROM content_knowledge_links ckl
           LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
           WHERE ckl.content_id = ?
           ORDER BY ckl.relevance_score DESC, ckl.id ASC""",
        (content_id,),
    ).fetchall()
    return [_source_from_row(dict(row), now) for row in rows]


def _source_from_row(row: dict[str, Any], now: datetime) -> CitationPacketSource:
    metadata = _metadata(row.get("metadata"))
    link_metadata = metadata.get("link_metadata") if isinstance(metadata.get("link_metadata"), dict) else {}
    canonical_url = _clean_string(link_metadata.get("canonical_url"))
    title = _clean_string(link_metadata.get("title")) or _clean_string(row.get("source_id"))
    timestamp = (
        _clean_string(row.get("published_at"))
        or _clean_string(link_metadata.get("published_at"))
        or _clean_string(row.get("ingested_at"))
        or _clean_string(row.get("knowledge_created_at"))
    )
    evidence = " ".join(
        str(value)
        for value in (
            row.get("author"),
            row.get("source_id"),
            row.get("source_url"),
            row.get("content"),
            row.get("insight"),
        )
        if value
    )
    return CitationPacketSource(
        knowledge_id=_int_or_none(row.get("knowledge_id")),
        link_id=_int_or_none(row.get("link_id")),
        relevance_score=_float_or_none(row.get("relevance_score")),
        source_type=_clean_string(row.get("source_type")),
        source_id=_clean_string(row.get("source_id")),
        title=title,
        url=_clean_string(row.get("source_url")),
        canonical_url=canonical_url,
        author=_clean_string(row.get("author")),
        freshness={
            "source_timestamp": timestamp,
            "age_days": _round_days(_age_days(timestamp, now)),
        },
        license={
            "status": _clean_string(row.get("license")) or "attribution_required",
            "approved": _bool_or_none(row.get("approved")),
            "attribution_required": bool(row.get("attribution_required")),
        },
        matched_terms=[],
        evidence_text=evidence or None,
        metadata=metadata,
    )


def _source_with_matches(
    source: CitationPacketSource,
    matched_terms: list[str],
) -> CitationPacketSource:
    return CitationPacketSource(
        knowledge_id=source.knowledge_id,
        link_id=source.link_id,
        relevance_score=source.relevance_score,
        source_type=source.source_type,
        source_id=source.source_id,
        title=source.title,
        url=source.url,
        canonical_url=source.canonical_url,
        author=source.author,
        freshness=dict(source.freshness),
        license=dict(source.license),
        matched_terms=sorted(set(matched_terms)),
        evidence_text=source.evidence_text,
        metadata=dict(source.metadata),
    )


def _content_metadata(content: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": content.get("id"),
        "content_type": content.get("content_type"),
        "content_format": content.get("content_format"),
        "created_at": content.get("created_at"),
        "publication_status": _content_status(content.get("published")),
        "published_url": content.get("published_url"),
        "text": content.get("content"),
    }


def _claim_check_payload(summary: dict[str, Any] | None) -> dict[str, Any]:
    if summary is None:
        return {
            "checked": False,
            "status": "unchecked",
            "supported_count": 0,
            "unsupported_count": 0,
            "annotation_text": None,
            "created_at": None,
            "updated_at": None,
        }
    unsupported = int(summary.get("unsupported_count") or 0)
    return {
        "checked": True,
        "status": "unsupported" if unsupported else "supported",
        "supported_count": int(summary.get("supported_count") or 0),
        "unsupported_count": unsupported,
        "annotation_text": summary.get("annotation_text"),
        "created_at": summary.get("created_at"),
        "updated_at": summary.get("updated_at"),
    }


def _unavailable_packet(
    *,
    content_id: int,
    include_supported: bool,
    generated_at: datetime,
    missing: list[str],
) -> CitationPacket:
    return CitationPacket(
        artifact_type="citation_packet",
        content_id=content_id,
        available=False,
        unavailable_reason="required claim or citation tables are unavailable",
        missing_required_tables=missing,
        generated_at=generated_at.isoformat(),
        include_supported=include_supported,
        content=None,
        claim_check=None,
        claim_count=0,
        included_claim_count=0,
        unsupported_count=0,
        weakly_supported_count=0,
        supported_count=0,
        claims=[],
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        name = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[name] = {
            column[1] for column in conn.execute(f"PRAGMA table_info({name})").fetchall()
        }
    return schema


def _connection(db: Any) -> sqlite3.Connection:
    conn = getattr(db, "conn", db)
    conn.row_factory = sqlite3.Row
    return conn


def _column_expr(columns: set[str], column: str, alias: str | None = None) -> str:
    if column not in columns:
        return "NULL"
    prefix = f"{alias}." if alias else ""
    return f"{prefix}{column}"


def _metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value or "{}")
        except json.JSONDecodeError:
            value = {}
    return value if isinstance(value, dict) else {}


def _content_status(value: Any) -> str:
    if value == 1:
        return "published"
    if value == -1:
        return "abandoned"
    return "unpublished"


def _normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return _normalize_datetime(value)
    text = _clean_string(value)
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return _normalize_datetime(datetime.fromisoformat(text))
    except ValueError:
        return None


def _age_days(value: Any, now: datetime) -> float | None:
    parsed = _parse_datetime(value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds(), 0.0) / 86400.0


def _round_days(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 2)


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _clean_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _yes_no(value: Any) -> str:
    if value is None:
        return "-"
    return "yes" if bool(value) else "no"


def _status_sort_key(status: str) -> int:
    return {
        STATUS_UNSUPPORTED: 0,
        STATUS_WEAK: 1,
        STATUS_SUPPORTED: 2,
    }.get(status, 9)
