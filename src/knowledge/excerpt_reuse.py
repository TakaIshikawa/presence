"""Detect generated content that reuses long excerpts from knowledge items."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


DEFAULT_MIN_TOKENS = 12
DEFAULT_SIMILARITY_THRESHOLD = 0.2
DEFAULT_LIMIT = 50
KNOWLEDGE_TEXT_FIELDS = ("title", "summary", "insight", "content", "text")
TOKEN_RE = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class TokenSpan:
    """Longest contiguous token overlap between two texts."""

    token_count: int
    generated_start: int
    knowledge_start: int
    generated_excerpt: str
    knowledge_excerpt: str
    similarity: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExcerptReuseFinding:
    """One generated_content row that copies a long knowledge excerpt."""

    content_id: int
    content_type: str | None
    knowledge_id: int
    knowledge_identifier: str
    knowledge_field: str
    overlap_token_count: int
    similarity: float
    generated_excerpt: str
    knowledge_excerpt: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ExcerptReuseReport:
    """Aggregate report for knowledge excerpt reuse."""

    generated_at: str
    min_tokens: int
    similarity_threshold: float
    limit: int | None
    generated_content_count: int
    knowledge_item_count: int
    comparison_count: int
    missing_tables: tuple[str, ...]
    missing_columns: dict[str, list[str]]
    findings: tuple[ExcerptReuseFinding, ...]

    @property
    def summary(self) -> dict[str, Any]:
        return {
            "total_findings": len(self.findings),
            "generated_content_scanned": self.generated_content_count,
            "knowledge_items_scanned": self.knowledge_item_count,
            "comparisons": self.comparison_count,
        }

    def to_dict(self) -> dict[str, Any]:
        return {
            "comparison_count": self.comparison_count,
            "findings": [finding.to_dict() for finding in self.findings],
            "generated_at": self.generated_at,
            "generated_content_count": self.generated_content_count,
            "knowledge_item_count": self.knowledge_item_count,
            "limit": self.limit,
            "min_tokens": self.min_tokens,
            "missing_columns": self.missing_columns,
            "missing_tables": list(self.missing_tables),
            "similarity_threshold": self.similarity_threshold,
            "summary": self.summary,
        }


def normalize_text(text: str | None) -> str:
    """Return lowercase token text with punctuation and spacing normalized."""
    return " ".join(tokenize_text(text))


def tokenize_text(text: str | None) -> list[str]:
    """Tokenize text for stable excerpt comparison."""
    return TOKEN_RE.findall((text or "").lower())


def longest_shared_token_span(
    generated_text: str | None,
    knowledge_text: str | None,
) -> TokenSpan:
    """Compute the longest contiguous token span shared by both texts."""
    generated_tokens = tokenize_text(generated_text)
    knowledge_tokens = tokenize_text(knowledge_text)
    if not generated_tokens or not knowledge_tokens:
        return TokenSpan(0, 0, 0, "", "", 0.0)

    previous = [0] * (len(knowledge_tokens) + 1)
    best_count = 0
    best_generated_end = 0
    best_knowledge_end = 0
    for generated_index, generated_token in enumerate(generated_tokens, start=1):
        current = [0] * (len(knowledge_tokens) + 1)
        for knowledge_index, knowledge_token in enumerate(knowledge_tokens, start=1):
            if generated_token != knowledge_token:
                continue
            count = previous[knowledge_index - 1] + 1
            current[knowledge_index] = count
            if count > best_count:
                best_count = count
                best_generated_end = generated_index
                best_knowledge_end = knowledge_index
        previous = current

    generated_start = best_generated_end - best_count
    knowledge_start = best_knowledge_end - best_count
    denominator = min(len(generated_tokens), len(knowledge_tokens))
    similarity = best_count / denominator if denominator else 0.0
    return TokenSpan(
        token_count=best_count,
        generated_start=generated_start,
        knowledge_start=knowledge_start,
        generated_excerpt=_preview_tokens(generated_tokens, generated_start, best_count),
        knowledge_excerpt=_preview_tokens(knowledge_tokens, knowledge_start, best_count),
        similarity=round(similarity, 3),
    )


def build_knowledge_excerpt_reuse_report(
    db_or_conn: Any,
    *,
    min_tokens: int = DEFAULT_MIN_TOKENS,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ExcerptReuseReport:
    """Compare generated content against knowledge text fields."""
    if min_tokens < 1:
        raise ValueError("min_tokens must be at least 1")
    if not 0 < similarity_threshold <= 1:
        raise ValueError("similarity_threshold must be greater than 0 and at most 1")
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")

    conn = _connection(db_or_conn)
    conn.row_factory = sqlite3.Row
    schema = _schema(conn)
    missing_tables = tuple(
        table for table in ("generated_content", "knowledge") if table not in schema
    )
    missing_columns = _missing_columns(schema)
    generated_rows = _load_generated_content(conn, schema)
    knowledge_rows = _load_knowledge_items(conn, schema)

    findings: list[ExcerptReuseFinding] = []
    comparison_count = 0
    for content_row in generated_rows:
        for knowledge_row in knowledge_rows:
            for field_name, field_text in knowledge_row["text_fields"]:
                comparison_count += 1
                span = longest_shared_token_span(content_row["content"], field_text)
                if (
                    span.token_count < min_tokens
                    or span.similarity < similarity_threshold
                ):
                    continue
                findings.append(
                    ExcerptReuseFinding(
                        content_id=int(content_row["id"]),
                        content_type=content_row.get("content_type"),
                        knowledge_id=int(knowledge_row["id"]),
                        knowledge_identifier=knowledge_row["identifier"],
                        knowledge_field=field_name,
                        overlap_token_count=span.token_count,
                        similarity=span.similarity,
                        generated_excerpt=span.generated_excerpt,
                        knowledge_excerpt=span.knowledge_excerpt,
                    )
                )

    findings.sort(
        key=lambda finding: (
            -finding.overlap_token_count,
            -finding.similarity,
            finding.content_id,
            finding.knowledge_id,
            finding.knowledge_field,
        )
    )
    if limit is not None:
        findings = findings[:limit]

    generated_at = _ensure_utc(now or datetime.now(timezone.utc)).isoformat()
    return ExcerptReuseReport(
        generated_at=generated_at,
        min_tokens=min_tokens,
        similarity_threshold=similarity_threshold,
        limit=limit,
        generated_content_count=len(generated_rows),
        knowledge_item_count=len(knowledge_rows),
        comparison_count=comparison_count,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        findings=tuple(findings),
    )


def format_knowledge_excerpt_reuse_json(report: ExcerptReuseReport) -> str:
    """Render the excerpt reuse report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_knowledge_excerpt_reuse_text(report: ExcerptReuseReport) -> str:
    """Render the excerpt reuse report as stable terminal text."""
    summary = report.summary
    lines = [
        "KNOWLEDGE EXCERPT REUSE",
        f"Generated at: {report.generated_at}",
        (
            "Thresholds: "
            f"min_tokens={report.min_tokens} "
            f"similarity_threshold={report.similarity_threshold:g}"
        ),
        f"Limit: {report.limit if report.limit is not None else 'none'}",
        (
            "Summary: "
            f"findings={summary['total_findings']} "
            f"generated_content={summary['generated_content_scanned']} "
            f"knowledge_items={summary['knowledge_items_scanned']} "
            f"comparisons={summary['comparisons']}"
        ),
    ]
    if report.missing_tables:
        lines.append(f"Missing tables: {', '.join(report.missing_tables)}")
    if report.missing_columns:
        for table, columns in report.missing_columns.items():
            lines.append(f"Missing columns on {table}: {', '.join(columns)}")

    if not report.findings:
        lines.append("")
        lines.append("No long knowledge excerpt reuse found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Findings:")
    for finding in report.findings:
        lines.append(
            f"  - content_id={finding.content_id} "
            f"knowledge={finding.knowledge_identifier} "
            f"field={finding.knowledge_field} "
            f"overlap_tokens={finding.overlap_token_count} "
            f"similarity={finding.similarity:.3f}"
        )
        lines.append(f"    generated: {finding.generated_excerpt}")
        lines.append(f"    knowledge:  {finding.knowledge_excerpt}")
    return "\n".join(lines)


def _preview_tokens(tokens: list[str], start: int, count: int) -> str:
    if count <= 0:
        return ""
    return " ".join(tokens[start : start + count])


def _load_generated_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema.get("generated_content", set())
    if not {"id", "content"}.issubset(columns):
        return []
    content_type_expr = "content_type" if "content_type" in columns else "NULL AS content_type"
    rows = conn.execute(
        f"""SELECT id, {content_type_expr}, content
            FROM generated_content
            WHERE COALESCE(content, '') != ''
            ORDER BY id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_knowledge_items(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> list[dict[str, Any]]:
    columns = schema.get("knowledge", set())
    if "id" not in columns:
        return []
    text_fields = [field for field in KNOWLEDGE_TEXT_FIELDS if field in columns]
    if not text_fields:
        return []
    optional = [
        _column_expr(columns, "source_type"),
        _column_expr(columns, "source_id"),
        _column_expr(columns, "source_url"),
        _column_expr(columns, "author"),
    ]
    selected_text = ", ".join(text_fields)
    rows = conn.execute(
        f"""SELECT id, {', '.join(optional)}, {selected_text}
            FROM knowledge
            ORDER BY id ASC"""
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row)
        fields = [
            (field, str(row_dict.get(field) or ""))
            for field in text_fields
            if str(row_dict.get(field) or "").strip()
        ]
        if not fields:
            continue
        row_dict["identifier"] = _knowledge_identifier(row_dict)
        row_dict["text_fields"] = fields
        items.append(row_dict)
    return items


def _knowledge_identifier(row: dict[str, Any]) -> str:
    source_type = str(row.get("source_type") or "").strip()
    source_id = str(row.get("source_id") or "").strip()
    source_url = str(row.get("source_url") or "").strip()
    author = str(row.get("author") or "").strip()
    if source_type and source_id:
        return f"{source_type}:{source_id}"
    if source_url:
        return source_url
    if author:
        return f"author:{author}"
    return f"knowledge:{row['id']}"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table'"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row[0]
        schema[table] = {info[1] for info in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    missing: dict[str, list[str]] = {}
    generated_columns = schema.get("generated_content")
    if generated_columns is not None:
        required_generated = ("id", "content")
        missing_generated = [
            column for column in required_generated if column not in generated_columns
        ]
        if missing_generated:
            missing["generated_content"] = missing_generated
    knowledge_columns = schema.get("knowledge")
    if knowledge_columns is not None:
        missing_knowledge = []
        if "id" not in knowledge_columns:
            missing_knowledge.append("id")
        if not any(field in knowledge_columns for field in KNOWLEDGE_TEXT_FIELDS):
            missing_knowledge.extend(KNOWLEDGE_TEXT_FIELDS)
        if missing_knowledge:
            missing["knowledge"] = missing_knowledge
    return {key: missing[key] for key in sorted(missing)}


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    return conn


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
