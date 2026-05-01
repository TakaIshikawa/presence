"""Build source attribution packets for generated content."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import sqlite3
from typing import Any


LICENSE_OPEN = "open"
LICENSE_ATTRIBUTION_REQUIRED = "attribution_required"
LICENSE_RESTRICTED = "restricted"


@dataclass(frozen=True)
class AttributionSourceEntry:
    """One linked knowledge source normalized for attribution review."""

    knowledge_id: int | None
    link_id: int | None
    relevance_score: float | None
    source_type: str | None
    source_id: str | None
    source_url: str | None
    author: str | None
    license: str
    attribution_required: bool
    excerpt: str | None
    insight: str | None
    warnings: list[str]


@dataclass(frozen=True)
class AttributionPacket:
    """JSON-serializable attribution packet for one generated-content row."""

    artifact_type: str
    content_id: int
    content: dict[str, Any]
    source_count: int
    warning_count: int
    include_open: bool
    sources: list[AttributionSourceEntry]
    warnings: list[str]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_attribution_packet(
    db: Any,
    content_id: int,
    *,
    include_open: bool = True,
) -> AttributionPacket:
    """Gather linked knowledge sources for generated content attribution.

    The packet is read-only and orders linked knowledge by relevance descending,
    then by knowledge id ascending for stable operator review.
    """

    if content_id < 1:
        raise ValueError("content_id must be positive")

    conn = _connection(db)
    schema = _schema(conn)
    missing = [
        table
        for table in ("generated_content", "content_knowledge_links", "knowledge")
        if table not in schema
    ]
    if missing:
        raise ValueError(f"Missing required tables: {', '.join(missing)}")

    content = _load_content(conn, schema, content_id)
    if content is None:
        raise ValueError(f"Content ID {content_id} not found")

    sources = _load_sources(conn, schema, content_id, include_open=include_open)
    warnings = [
        f"knowledge #{source.knowledge_id}: {warning}"
        for source in sources
        for warning in source.warnings
    ]
    return AttributionPacket(
        artifact_type="attribution_packet",
        content_id=content_id,
        content=content,
        source_count=len(sources),
        warning_count=len(warnings),
        include_open=include_open,
        sources=sources,
        warnings=warnings,
    )


def format_attribution_packet_json(packet: AttributionPacket) -> str:
    """Render an attribution packet as deterministic JSON."""

    return json.dumps(packet.as_dict(), indent=2, sort_keys=True, default=str)


def format_attribution_packet_text(packet: AttributionPacket) -> str:
    """Render an attribution packet as copyable source attribution text."""

    content_type = packet.content.get("content_type") or "-"
    content_format = packet.content.get("content_format") or "-"
    lines = [
        f"Attribution Packet: Content #{packet.content_id}",
        f"Type: {content_type}",
        f"Format: {content_format}",
        f"Sources: {packet.source_count}",
        f"Warnings: {packet.warning_count}",
    ]

    if packet.warnings:
        lines.extend(["", "Warnings:"])
        lines.extend(f"- {warning}" for warning in packet.warnings)

    lines.extend(["", "Source Attributions:"])
    if not packet.sources:
        lines.append("- none")
        return "\n".join(lines)

    for index, source in enumerate(packet.sources, start=1):
        label = source.source_url or source.source_id or f"knowledge #{source.knowledge_id}"
        lines.extend(
            [
                f"{index}. {label}",
                f"   Author: {source.author or '-'}",
                f"   URL: {source.source_url or '-'}",
                f"   License: {source.license}",
                f"   Attribution required: {_yes_no(source.attribution_required)}",
                f"   Relevance: {_format_score(source.relevance_score)}",
            ]
        )
        if source.insight:
            lines.append(f"   Insight: {source.insight}")
        if source.excerpt and source.excerpt != source.insight:
            lines.append(f"   Excerpt: {source.excerpt}")
        if source.warnings:
            lines.append(f"   Warnings: {'; '.join(source.warnings)}")
    return "\n".join(lines)


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
                  {_column_expr(columns, "content")} AS text,
                  {_column_expr(columns, "created_at")} AS created_at
           FROM generated_content
           WHERE id = ?""",
        (content_id,),
    ).fetchone()
    return dict(row) if row else None


def _load_sources(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_id: int,
    *,
    include_open: bool,
) -> list[AttributionSourceEntry]:
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
                  {_column_expr(knowledge_columns, "attribution_required", "k")} AS attribution_required
           FROM content_knowledge_links ckl
           LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
           WHERE ckl.content_id = ?
           ORDER BY ckl.relevance_score DESC, ckl.knowledge_id ASC""",
        (content_id,),
    ).fetchall()

    sources = [_source_from_row(dict(row)) for row in rows]
    if include_open:
        return sources
    return [
        source
        for source in sources
        if source.license != LICENSE_OPEN or source.attribution_required or source.warnings
    ]


def _source_from_row(row: dict[str, Any]) -> AttributionSourceEntry:
    license_value = _normalize_license(row.get("license"))
    attribution_required = _requires_attribution(row, license_value)
    warnings = _warnings(row, license_value, attribution_required)
    return AttributionSourceEntry(
        knowledge_id=_int_or_none(row.get("knowledge_id")),
        link_id=_int_or_none(row.get("link_id")),
        relevance_score=_float_or_none(row.get("relevance_score")),
        source_type=_clean_string(row.get("source_type")),
        source_id=_clean_string(row.get("source_id")),
        source_url=_clean_string(row.get("source_url")),
        author=_clean_string(row.get("author")),
        license=license_value,
        attribution_required=attribution_required,
        excerpt=_snippet(row.get("content")),
        insight=_snippet(row.get("insight")),
        warnings=warnings,
    )


def _warnings(
    row: dict[str, Any],
    license_value: str,
    attribution_required: bool,
) -> list[str]:
    if row.get("matched_knowledge_id") is None:
        return ["Linked knowledge row no longer exists."]

    warnings: list[str] = []
    if license_value == LICENSE_RESTRICTED:
        warnings.append("Source license is restricted; do not publish derived work without approval.")
    if attribution_required:
        warnings.append("Source requires attribution.")
        if not _clean_string(row.get("source_url")):
            warnings.append("Attribution-required source is missing source_url.")
        if not _clean_string(row.get("author")):
            warnings.append("Attribution-required source is missing author.")
    if row.get("license") is None:
        warnings.append("Source license metadata is missing; treated as attribution_required.")
    return warnings


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


def _requires_attribution(row: dict[str, Any], license_value: str) -> bool:
    if license_value in {LICENSE_ATTRIBUTION_REQUIRED, LICENSE_RESTRICTED}:
        return True
    return bool(row.get("attribution_required"))


def _normalize_license(value: Any) -> str:
    text = str(value or LICENSE_ATTRIBUTION_REQUIRED).strip().lower()
    return text or LICENSE_ATTRIBUTION_REQUIRED


def _snippet(value: Any, limit: int = 220) -> str | None:
    text = _clean_string(value)
    if not text:
        return None
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _clean_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


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


def _format_score(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _yes_no(value: bool) -> str:
    return "yes" if value else "no"
