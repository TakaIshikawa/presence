"""Report generated visual content with missing or weak alt text."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_MIN_CHARS = 20
PLACEHOLDER_ALT_TEXT = {
    "alt text",
    "image",
    "photo",
    "picture",
    "screenshot",
    "chart",
    "graphic",
    "generated image",
    "ai generated image",
    "visual",
}
SEVERITY_ORDER = {"error": 0, "warning": 1}


@dataclass(frozen=True)
class VisualAltTextBacklogRow:
    content_id: int
    severity: str
    issue_type: str
    image_path: str | None
    image_prompt: str | None
    image_alt_text: str | None
    alt_text_length: int
    content_type: str | None
    created_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class VisualAltTextBacklogReport:
    generated_at: str
    min_chars: int
    rows: tuple[VisualAltTextBacklogRow, ...]
    totals: dict[str, int]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "visual_alt_text_backlog",
            "generated_at": self.generated_at,
            "min_chars": self.min_chars,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_visual_alt_text_backlog_report(
    db_or_conn: Any,
    *,
    min_chars: int = DEFAULT_MIN_CHARS,
    now: datetime | None = None,
) -> VisualAltTextBacklogReport:
    """Build a backlog of visual generated_content rows needing alt-text work."""
    if min_chars <= 0:
        raise ValueError("min_chars must be positive")
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return _report(generated_at, min_chars, (), ("generated_content",), {})

    columns = schema["generated_content"]
    required = ("id",)
    optional = (
        "image_path",
        "image_prompt",
        "image_alt_text",
        "content_type",
        "created_at",
    )
    missing_required = tuple(column for column in required if column not in columns)
    missing_optional = tuple(column for column in optional if column not in columns)
    missing_columns = {}
    if missing_required:
        missing_columns["generated_content"] = missing_required + missing_optional
        return _report(generated_at, min_chars, (), (), missing_columns)
    if "image_path" not in columns and "image_prompt" not in columns:
        missing_columns["generated_content"] = missing_optional
        return _report(generated_at, min_chars, (), (), missing_columns)
    if missing_optional:
        missing_columns["generated_content"] = missing_optional

    rows = tuple(
        sorted(
            (
                row
                for record in _visual_records(conn, columns)
                for row in [_row_issue(record, min_chars=min_chars)]
                if row is not None
            ),
            key=lambda row: (
                SEVERITY_ORDER.get(row.severity, 99),
                row.created_at or "",
                row.content_id,
            ),
        )
    )
    return _report(generated_at, min_chars, rows, (), missing_columns)


def format_visual_alt_text_backlog_json(report: VisualAltTextBacklogReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_visual_alt_text_backlog_text(report: VisualAltTextBacklogReport) -> str:
    lines = [
        "Visual Alt Text Backlog",
        f"Generated: {report.generated_at}",
        f"Minimum characters: {report.min_chars}",
        (
            "Totals: "
            f"rows={report.totals['row_count']} "
            f"missing={report.totals['missing_alt_text']} "
            f"placeholder={report.totals['placeholder_alt_text']} "
            f"too_short={report.totals['too_short_alt_text']}"
        ),
        "",
    ]
    if not report.rows:
        lines.append("No visual alt-text backlog rows found.")
        return "\n".join(lines)
    for row in report.rows:
        lines.append(
            f"- content={row.content_id} severity={row.severity} "
            f"issue={row.issue_type} length={row.alt_text_length}"
        )
    return "\n".join(lines)


def _visual_records(conn: sqlite3.Connection, columns: set[str]) -> list[sqlite3.Row]:
    select_columns = [
        "id",
        _column_expr(columns, "image_path"),
        _column_expr(columns, "image_prompt"),
        _column_expr(columns, "image_alt_text"),
        _column_expr(columns, "content_type"),
        _column_expr(columns, "created_at"),
    ]
    terms = []
    if "image_path" in columns:
        terms.append("(image_path IS NOT NULL AND trim(image_path) != '')")
    if "image_prompt" in columns:
        terms.append("(image_prompt IS NOT NULL AND trim(image_prompt) != '')")
    where = " OR ".join(terms) or "0"
    return conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM generated_content
            WHERE {where}
            ORDER BY id ASC"""
    ).fetchall()


def _row_issue(
    record: sqlite3.Row,
    *,
    min_chars: int,
) -> VisualAltTextBacklogRow | None:
    alt_text = _clean(record["image_alt_text"])
    normalized_alt = _normalize(alt_text)
    if not alt_text:
        issue_type = "missing_alt_text"
        severity = "error"
    elif normalized_alt in PLACEHOLDER_ALT_TEXT:
        issue_type = "placeholder_alt_text"
        severity = "warning"
    elif len(alt_text) < min_chars:
        issue_type = "too_short_alt_text"
        severity = "warning"
    else:
        return None
    return VisualAltTextBacklogRow(
        content_id=int(record["id"]),
        severity=severity,
        issue_type=issue_type,
        image_path=_clean(record["image_path"]),
        image_prompt=_clean(record["image_prompt"]),
        image_alt_text=alt_text,
        alt_text_length=len(alt_text or ""),
        content_type=_clean(record["content_type"]),
        created_at=_clean(record["created_at"]),
    )


def _report(
    generated_at: datetime,
    min_chars: int,
    rows: tuple[VisualAltTextBacklogRow, ...],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> VisualAltTextBacklogReport:
    totals = {
        "row_count": len(rows),
        "missing_alt_text": 0,
        "placeholder_alt_text": 0,
        "too_short_alt_text": 0,
    }
    for row in rows:
        totals[row.issue_type] += 1
    return VisualAltTextBacklogReport(
        generated_at=generated_at.isoformat(),
        min_chars=min_chars,
        rows=rows,
        totals=totals,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in tables:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {str(info[1]) for info in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    return column if column in columns else f"{default} AS {column}"


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _normalize(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (value or "").lower()).strip()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
