"""Export rejected or revised generated content as salvage idea seeds."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 25
VALID_FEEDBACK_TYPES = {"all", "reject", "revise"}
VALID_RECOMMENDATIONS = {
    "rewrite_from_replacement",
    "revisit_source_material",
    "avoid_pattern",
}
EXCERPT_CHARS = 220


@dataclass(frozen=True)
class ContentFeedbackSalvageExport:
    feedback_id: int
    content_id: int
    content_type: str
    feedback_type: str
    notes: str
    replacement_text: str
    generated_content: str
    source_commits: list[str]
    source_messages: list[str]
    source_activity_ids: list[str]
    created_at: str
    salvage_recommendation: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_content_feedback_salvage_exports(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    feedback_type: str = "all",
    content_type: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> list[ContentFeedbackSalvageExport]:
    """Return recent reject/revise feedback records with generated content context."""
    if days <= 0:
        raise ValueError("days must be positive")
    if feedback_type not in VALID_FEEDBACK_TYPES:
        raise ValueError(f"invalid feedback_type: {feedback_type}")
    if limit is not None and limit <= 0:
        return []

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "content_feedback" not in schema or "generated_content" not in schema:
        return []

    now = _aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    rows = _feedback_rows(
        conn,
        schema,
        feedback_type=feedback_type,
        content_type=content_type,
        cutoff=cutoff,
        now=now,
    )
    exports = [_export_from_row(row) for row in rows]
    return exports[:limit] if limit is not None else exports


def format_content_feedback_salvage_json(
    exports: list[ContentFeedbackSalvageExport],
) -> str:
    """Render salvage exports as deterministic JSON."""
    return json.dumps([export.to_dict() for export in exports], indent=2, sort_keys=True)


def format_content_feedback_salvage_text(
    exports: list[ContentFeedbackSalvageExport],
) -> str:
    """Render salvage exports as stable human-readable text."""
    lines = [f"salvage_items={len(exports)}"]
    lines.append(f"{'ID':>4s}  {'Type':12s}  {'Feedback':8s}  {'Recommendation':24s}  Notes / replacement")
    lines.append(f"{'-' * 4:>4s}  {'-' * 12:12s}  {'-' * 8:8s}  {'-' * 24:24s}  {'-' * 60}")
    if not exports:
        lines.append("   -  none          none      none                      no salvage feedback")
        return "\n".join(lines)

    for export in exports:
        signal = export.replacement_text or export.notes or export.generated_content
        lines.append(
            f"{export.content_id:4d}  "
            f"{_shorten(export.content_type, 12):12s}  "
            f"{export.feedback_type[:8]:8s}  "
            f"{export.salvage_recommendation[:24]:24s}  "
            f"{_shorten(signal, 90)}"
        )
    return "\n".join(lines)


def _feedback_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    feedback_type: str,
    content_type: str | None,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    feedback_columns = schema["content_feedback"]
    content_columns = schema["generated_content"]
    required_feedback = {"id", "content_id", "feedback_type"}
    required_content = {"id"}
    if not required_feedback.issubset(feedback_columns) or not required_content.issubset(content_columns):
        return []

    notes_expr = _column_expr(feedback_columns, "notes", "cf")
    replacement_expr = _column_expr(feedback_columns, "replacement_text", "cf")
    created_expr = _column_expr(feedback_columns, "created_at", "cf")
    content_expr = _column_expr(content_columns, "content", "gc")
    content_type_expr = _column_expr(content_columns, "content_type", "gc")
    source_commits_expr = _column_expr(content_columns, "source_commits", "gc")
    source_messages_expr = _column_expr(content_columns, "source_messages", "gc")
    source_activity_expr = _column_expr(content_columns, "source_activity_ids", "gc")

    filters = ["cf.feedback_type IN ('reject', 'revise')"]
    params: list[Any] = []
    if feedback_type != "all":
        filters.append("cf.feedback_type = ?")
        params.append(feedback_type)
    if content_type:
        filters.append("gc.content_type = ?")
        params.append(content_type)

    raw_rows = conn.execute(
        f"""SELECT cf.id AS feedback_id,
                  cf.content_id,
                  cf.feedback_type,
                  {notes_expr} AS notes,
                  {replacement_expr} AS replacement_text,
                  {created_expr} AS created_at,
                  {content_expr} AS generated_content,
                  {content_type_expr} AS content_type,
                  {source_commits_expr} AS source_commits,
                  {source_messages_expr} AS source_messages,
                  {source_activity_expr} AS source_activity_ids
           FROM content_feedback cf
           INNER JOIN generated_content gc ON gc.id = cf.content_id
           WHERE {' AND '.join(filters)}
           ORDER BY cf.created_at DESC, cf.id DESC""",
        params,
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        row_dict = dict(row)
        created_at = _parse_timestamp(row_dict.get("created_at")) or now
        if created_at < cutoff or created_at > now:
            continue
        row_dict["created_at"] = created_at.isoformat()
        rows.append(row_dict)
    return rows


def _export_from_row(row: dict[str, Any]) -> ContentFeedbackSalvageExport:
    source_commits = _json_list(row.get("source_commits"))
    source_messages = _json_list(row.get("source_messages"))
    source_activity_ids = _json_list(row.get("source_activity_ids"))
    notes = _clean_text(row.get("notes"))
    replacement_text = _clean_text(row.get("replacement_text"))
    feedback_type = str(row.get("feedback_type") or "")
    recommendation = _salvage_recommendation(
        feedback_type=feedback_type,
        replacement_text=replacement_text,
        source_ids=[*source_commits, *source_messages, *source_activity_ids],
    )
    return ContentFeedbackSalvageExport(
        feedback_id=int(row["feedback_id"]),
        content_id=int(row["content_id"]),
        content_type=_clean_text(row.get("content_type")) or "unknown",
        feedback_type=feedback_type,
        notes=notes,
        replacement_text=replacement_text,
        generated_content=_clean_text(row.get("generated_content")),
        source_commits=source_commits,
        source_messages=source_messages,
        source_activity_ids=source_activity_ids,
        created_at=str(row.get("created_at") or ""),
        salvage_recommendation=recommendation,
    )


def _salvage_recommendation(
    *,
    feedback_type: str,
    replacement_text: str,
    source_ids: list[str],
) -> str:
    if replacement_text:
        return "rewrite_from_replacement"
    if feedback_type == "revise" and source_ids:
        return "revisit_source_material"
    return "avoid_pattern"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
        schema[table] = {
            column["name"] if isinstance(column, sqlite3.Row) else column[1]
            for column in columns
        }
    return schema


def _column_expr(columns: set[str], column: str, alias: str) -> str:
    return f"{alias}.{column}" if column in columns else "NULL"


def _json_list(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item not in (None, "")]
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item) for item in parsed if item not in (None, "")]


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        return _aware(datetime.fromisoformat(text))
    except ValueError:
        return None


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clean_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _shorten(value: str | None, width: int = EXCERPT_CHARS) -> str:
    text = _clean_text(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)].rstrip() + "..."
