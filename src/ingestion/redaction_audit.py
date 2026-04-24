"""Audit persisted text for values covered by ingestion redaction rules."""

from __future__ import annotations

import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

from ingestion.redaction import RedactionPattern, Redactor


SUPPORTED_TABLES = (
    "claude_messages",
    "github_activity",
    "generated_content",
    "reply_queue",
    "knowledge",
)


@dataclass(frozen=True)
class TableAuditSpec:
    table: str
    date_columns: tuple[str, ...]
    text_fields: tuple[str, ...]


TABLE_SPECS: dict[str, TableAuditSpec] = {
    "claude_messages": TableAuditSpec(
        table="claude_messages",
        date_columns=("timestamp", "created_at"),
        text_fields=("project_path", "prompt_text"),
    ),
    "github_activity": TableAuditSpec(
        table="github_activity",
        date_columns=("ingested_at", "updated_at", "created_at_github"),
        text_fields=("title", "body", "author", "url", "labels", "metadata"),
    ),
    "generated_content": TableAuditSpec(
        table="generated_content",
        date_columns=("created_at", "published_at", "last_retry_at"),
        text_fields=(
            "source_commits",
            "source_messages",
            "source_activity_ids",
            "content",
            "eval_feedback",
            "published_url",
            "tweet_id",
            "image_path",
            "image_prompt",
            "image_alt_text",
        ),
    ),
    "reply_queue": TableAuditSpec(
        table="reply_queue",
        date_columns=("detected_at", "reviewed_at", "posted_at"),
        text_fields=(
            "inbound_tweet_id",
            "inbound_author_handle",
            "inbound_author_id",
            "inbound_text",
            "our_tweet_id",
            "inbound_url",
            "inbound_cid",
            "our_platform_id",
            "platform_metadata",
            "our_post_text",
            "draft_text",
            "relationship_context",
            "quality_flags",
            "posted_tweet_id",
            "posted_platform_id",
        ),
    ),
    "knowledge": TableAuditSpec(
        table="knowledge",
        date_columns=("created_at", "ingested_at", "published_at"),
        text_fields=("source_id", "source_url", "author", "content", "insight"),
    ),
}


@dataclass(frozen=True)
class RedactionAuditMatch:
    """A redaction audit hit without raw matched values."""

    table: str
    row_id: int | str
    field: str
    pattern_label: str
    redacted_preview: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def audit_redaction_leaks(
    conn: sqlite3.Connection,
    *,
    days: int = 30,
    tables: Sequence[str] | None = None,
    patterns: Iterable[str | dict[str, Any]] | None = None,
    now: datetime | None = None,
    preview_chars: int = 160,
) -> list[RedactionAuditMatch]:
    """Return redaction-pattern matches in supported tables.

    The returned preview is built from redacted context only. Raw regex matches are
    used for detection but are never included in returned values.
    """
    if days < 1:
        raise ValueError("days must be at least 1")
    selected_tables = tuple(tables or SUPPORTED_TABLES)
    unknown = sorted(set(selected_tables) - set(SUPPORTED_TABLES))
    if unknown:
        raise ValueError(f"Unsupported table(s): {', '.join(unknown)}")

    redactor = Redactor(patterns)
    scan_now = now or datetime.now(timezone.utc)
    if scan_now.tzinfo is None:
        scan_now = scan_now.replace(tzinfo=timezone.utc)
    cutoff = (scan_now.astimezone(timezone.utc) - timedelta(days=days)).isoformat()

    matches: list[RedactionAuditMatch] = []
    for table in selected_tables:
        spec = TABLE_SPECS[table]
        available_columns = _table_columns(conn, table)
        if not available_columns:
            continue

        fields = tuple(field for field in spec.text_fields if field in available_columns)
        if not fields:
            continue

        for row in _iter_recent_rows(conn, spec, available_columns, fields, cutoff):
            row_id = row["id"] if "id" in row.keys() else ""
            for field in fields:
                value = row[field]
                if value is None:
                    continue
                text = str(value)
                if not text:
                    continue
                for pattern in redactor.patterns:
                    if not pattern.regex.search(text):
                        continue
                    preview = build_redacted_preview(
                        text,
                        pattern,
                        redactor=redactor,
                        max_chars=preview_chars,
                    )
                    matches.append(
                        RedactionAuditMatch(
                            table=table,
                            row_id=row_id,
                            field=field,
                            pattern_label=pattern.name,
                            redacted_preview=preview,
                        )
                    )
    return matches


def build_audit_payload(
    matches: Sequence[RedactionAuditMatch],
    *,
    days: int,
    tables: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build deterministic JSON-serializable audit output grouped by table."""
    grouped = {table: [] for table in (tables or SUPPORTED_TABLES)}
    for match in matches:
        grouped.setdefault(match.table, []).append(match.to_dict())
    return {
        "days": days,
        "tables": list(tables or SUPPORTED_TABLES),
        "total_matches": len(matches),
        "matches": grouped,
    }


def build_redacted_preview(
    text: str,
    pattern: RedactionPattern,
    *,
    redactor: Redactor,
    max_chars: int = 160,
    context_chars: int = 56,
) -> str:
    """Return a compact preview around the first match without raw match text."""
    match = pattern.regex.search(text)
    if not match:
        return _shorten(redactor.redact(text), max_chars)

    before_start = max(0, match.start() - context_chars)
    after_end = min(len(text), match.end() + context_chars)
    before = redactor.redact(text[before_start:match.start()])
    redacted_match = redactor.redact(pattern.apply(match.group(0)))
    after = redactor.redact(text[match.end():after_end])

    preview = f"{before}{redacted_match}{after}".replace("\n", " ").strip()
    if before_start > 0:
        preview = "..." + preview
    if after_end < len(text):
        preview = preview + "..."
    return _shorten(preview, max_chars)


def _iter_recent_rows(
    conn: sqlite3.Connection,
    spec: TableAuditSpec,
    available_columns: set[str],
    fields: Sequence[str],
    cutoff: str,
) -> list[sqlite3.Row]:
    columns = ["id"] if "id" in available_columns else []
    columns.extend(fields)
    quoted_columns = ", ".join(_quote_identifier(column) for column in columns)
    date_columns = [column for column in spec.date_columns if column in available_columns]

    where = ""
    params: tuple[str, ...] = ()
    if date_columns:
        date_expr = "COALESCE({})".format(
            ", ".join(_quote_identifier(column) for column in date_columns)
        )
        where = f" WHERE datetime({date_expr}) >= datetime(?)"
        params = (cutoff,)

    sql = (
        f"SELECT {quoted_columns} FROM {_quote_identifier(spec.table)}"
        f"{where} ORDER BY {_quote_identifier('id') if 'id' in available_columns else 'rowid'}"
    )
    return list(conn.execute(sql, params).fetchall())


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = conn.execute(f"PRAGMA table_info({_quote_identifier(table)})").fetchall()
    except sqlite3.Error:
        return set()
    return {row[1] for row in rows}


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _shorten(value: str, limit: int) -> str:
    text = value.replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."
