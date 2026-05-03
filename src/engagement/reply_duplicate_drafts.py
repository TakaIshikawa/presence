"""Find repeated pending reply draft copy."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_THRESHOLD = 2
DEFAULT_LIMIT = 25
DEFAULT_EXAMPLE_LIMIT = 5

TABLE = "reply_queue"

_URL_RE = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)
_MENTION_RE = re.compile(r"(?<!\w)@[a-z0-9_.-]+", re.IGNORECASE)
_PUNCT_RE = re.compile(r"[^\w\s]")
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class ReplyDuplicateDraftExample:
    """One reply row participating in a duplicate draft cluster."""

    reply_id: int | None
    handle: str | None
    status: str
    detected_at: str | None
    draft_snippet: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ReplyDuplicateDraftGroup:
    """Drafts sharing the same normalized copy signature."""

    normalized_signature: str
    duplicate_count: int
    newest_detected_at: str | None
    representative_draft_snippet: str
    reply_ids: tuple[int, ...]
    examples: tuple[ReplyDuplicateDraftExample, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "duplicate_count": self.duplicate_count,
            "examples": [example.to_dict() for example in self.examples],
            "newest_detected_at": self.newest_detected_at,
            "normalized_signature": self.normalized_signature,
            "reply_ids": list(self.reply_ids),
            "representative_draft_snippet": self.representative_draft_snippet,
        }


@dataclass(frozen=True)
class ReplyDuplicateDraftReport:
    """Operational report for repeated pending reply draft copy."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    groups: tuple[ReplyDuplicateDraftGroup, ...]
    source_table: str | None = TABLE
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_duplicate_drafts",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "groups": [group.to_dict() for group in self.groups],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "source_table": self.source_table,
            "totals": dict(sorted(self.totals.items())),
        }


def build_reply_duplicate_drafts_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    threshold: int = DEFAULT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyDuplicateDraftReport:
    """Group pending reply drafts with equivalent normalized copy."""

    if days <= 0:
        raise ValueError("days must be positive")
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "status": "pending",
        "threshold": threshold,
    }

    if _looks_like_rows(db_or_rows):
        raw_rows = [_mapping(row) for row in db_or_rows]
        columns = set().union(*(row.keys() for row in raw_rows)) if raw_rows else set()
        source_table: str | None = "rows"
        missing_tables: tuple[str, ...] = ()
    else:
        conn = _connection(db_or_rows)
        columns = _table_columns(conn, TABLE)
        if not columns:
            return _empty_report(
                generated_at,
                filters,
                missing_tables=(TABLE,),
            )
        raw_rows = _load_rows(conn, columns, cutoff=cutoff, now=generated_at)
        source_table = TABLE
        missing_tables = ()

    missing = _missing_columns(columns)
    if "draft_text" in missing:
        return _empty_report(
            generated_at,
            filters,
            source_table=source_table,
            missing_columns={TABLE: missing},
        )

    rows = [
        _normalize_row(row, columns=columns, cutoff=cutoff, now=generated_at)
        for row in raw_rows
    ]
    rows = [row for row in rows if row is not None]
    drafted_rows = [row for row in rows if row["has_draft"]]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in drafted_rows:
        if row["normalized_signature"]:
            grouped[row["normalized_signature"]].append(row)

    groups = [
        _build_group(signature, matches)
        for signature, matches in grouped.items()
        if len(matches) >= threshold
    ]
    groups.sort(key=_group_sort_key)
    groups = groups[:limit]

    return ReplyDuplicateDraftReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "duplicate_draft_count": sum(group.duplicate_count for group in groups),
            "duplicate_groups": len(groups),
            "drafted_rows": len(drafted_rows),
            "rows_scanned": len(rows),
        },
        groups=tuple(groups),
        source_table=source_table,
        missing_tables=missing_tables,
        missing_columns={TABLE: missing} if missing else {},
    )


def normalize_reply_draft_signature(value: Any) -> str:
    """Normalize reply draft text for duplicate-copy grouping."""

    if value is None:
        return ""
    normalized = str(value).casefold()
    normalized = normalized.replace("&amp;", " and ")
    normalized = _URL_RE.sub(" ", normalized)
    normalized = _MENTION_RE.sub(" ", normalized)
    normalized = normalized.replace("'", "")
    normalized = _PUNCT_RE.sub(" ", normalized)
    return _SPACE_RE.sub(" ", normalized).strip()


def format_reply_duplicate_drafts_json(report: ReplyDuplicateDraftReport) -> str:
    """Serialize the duplicate-drafts report as deterministic JSON."""

    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_duplicate_drafts_text(report: ReplyDuplicateDraftReport) -> str:
    """Render a concise human-readable duplicate-drafts report."""

    filters = report.filters
    totals = report.totals
    lines = [
        "Reply Duplicate Drafts Report",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} status={filters['status']} "
            f"threshold={filters['threshold']} limit={filters['limit']}"
        ),
        (
            "Totals: "
            f"rows_scanned={totals['rows_scanned']} drafted_rows={totals['drafted_rows']} "
            f"duplicate_groups={totals['duplicate_groups']} "
            f"duplicate_draft_count={totals['duplicate_draft_count']}"
        ),
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
    if not report.groups:
        lines.append("No duplicate pending reply drafts matched.")
        return "\n".join(lines)

    lines.extend(["", "Duplicate groups:"])
    for index, group in enumerate(report.groups, start=1):
        lines.append(
            f"{index}. count={group.duplicate_count} newest={group.newest_detected_at or '-'} "
            f"snippet={group.representative_draft_snippet!r}"
        )
        for example in group.examples:
            handle = example.handle or "unknown"
            reply_id = "-" if example.reply_id is None else str(example.reply_id)
            lines.append(
                f"   - reply_queue:{reply_id} @{handle} "
                f"status={example.status} detected={example.detected_at or '-'}"
            )
    return "\n".join(lines)


def _build_group(signature: str, rows: list[dict[str, Any]]) -> ReplyDuplicateDraftGroup:
    ordered = sorted(rows, key=_row_sort_key)
    newest = max(rows, key=_newest_sort_key)
    examples = tuple(
        ReplyDuplicateDraftExample(
            reply_id=row["reply_id"],
            handle=row["handle"],
            status=row["status"],
            detected_at=row["detected_at"],
            draft_snippet=row["draft_snippet"],
        )
        for row in ordered[:DEFAULT_EXAMPLE_LIMIT]
    )
    reply_ids = tuple(
        row["reply_id"]
        for row in sorted(rows, key=_reply_id_sort_key)
        if row["reply_id"] is not None
    )
    representative = min(rows, key=_row_sort_key)["draft_snippet"]
    return ReplyDuplicateDraftGroup(
        normalized_signature=signature,
        duplicate_count=len(rows),
        newest_detected_at=newest["detected_at"],
        representative_draft_snippet=representative,
        reply_ids=reply_ids,
        examples=examples,
    )


def _group_sort_key(group: ReplyDuplicateDraftGroup) -> tuple[int, int, str]:
    return (
        -group.duplicate_count,
        -_timestamp_sort_value(group.newest_detected_at),
        group.normalized_signature,
    )


def _row_sort_key(row: dict[str, Any]) -> tuple[int, int]:
    return (-_timestamp_sort_value(row["detected_at"]), row["reply_id"] or 0)


def _reply_id_sort_key(row: dict[str, Any]) -> int:
    return row["reply_id"] if row["reply_id"] is not None else 10**12


def _newest_sort_key(row: dict[str, Any]) -> tuple[int, int]:
    return (_timestamp_sort_value(row["detected_at"]), row["reply_id"] or 0)


def _normalize_row(
    row: dict[str, Any],
    *,
    columns: set[str],
    cutoff: datetime,
    now: datetime,
) -> dict[str, Any] | None:
    status = _clean(_value(row, columns, "status")) or "pending"
    if "status" in columns and status.casefold() != "pending":
        return None
    detected_at = _parse_datetime(_value(row, columns, "detected_at"))
    if detected_at is not None and not (cutoff <= detected_at <= now):
        return None
    draft_text = str(_value(row, columns, "draft_text") or "")
    return {
        "reply_id": _int_or_none(row.get("id") or row.get("reply_queue_id") or row.get("rowid")),
        "handle": _clean(_value(row, columns, "inbound_author_handle")),
        "status": status,
        "detected_at": (
            detected_at.isoformat()
            if detected_at
            else _clean(_value(row, columns, "detected_at"))
        ),
        "draft_snippet": _shorten(draft_text, 140),
        "has_draft": bool(draft_text.strip()),
        "normalized_signature": normalize_reply_draft_signature(draft_text),
    }


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id", "rowid"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "status", "'pending'"),
        _column_expr(columns, "detected_at"),
        _column_expr(columns, "draft_text"),
    ]
    where = []
    params: list[Any] = []
    if "status" in columns:
        where.append("LOWER(COALESCE(status, 'pending')) = 'pending'")
    if "detected_at" in columns:
        where.append("(detected_at IS NULL OR datetime(detected_at) >= datetime(?))")
        where.append("(detected_at IS NULL OR datetime(detected_at) <= datetime(?))")
        params.extend([cutoff.isoformat(), now.isoformat()])
    query = f"SELECT {', '.join(select_columns)} FROM {_quote_identifier(TABLE)}"
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY " + _order_clause(columns)
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _missing_columns(columns: set[str]) -> tuple[str, ...]:
    missing = []
    if "draft_text" not in columns:
        missing.append("draft_text")
    for optional in ("detected_at", "inbound_author_handle", "status"):
        if optional not in columns:
            missing.append(optional)
    return tuple(missing)


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) DESC")
    parts.append("id ASC" if "id" in columns else "rowid ASC")
    return ", ".join(parts)


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    if column in columns:
        return f"{_quote_identifier(column)} AS {_quote_identifier(column)}"
    return f"{default} AS {_quote_identifier(column)}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3 connection or database wrapper with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, (list, tuple))


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return dict(row)


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {
            str(row[1])
            for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
    except sqlite3.Error:
        return set()


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _value(row: dict[str, Any], columns: set[str], column: str) -> Any:
    return row.get(column) if column in columns else None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _shorten(value: str, limit: int) -> str:
    normalized = _SPACE_RE.sub(" ", value.strip())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "..."


def _parse_datetime(value: Any) -> datetime | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    try:
        parsed = datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(cleaned, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timestamp_sort_value(value: str | None) -> int:
    timestamp = _parse_datetime(value) if value else datetime(1, 1, 1, tzinfo=timezone.utc)
    return (
        timestamp.toordinal() * 86_400
        + timestamp.hour * 3_600
        + timestamp.minute * 60
        + timestamp.second
    )


def _empty_totals() -> dict[str, int]:
    return {
        "duplicate_draft_count": 0,
        "duplicate_groups": 0,
        "drafted_rows": 0,
        "rows_scanned": 0,
    }


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    source_table: str | None = None,
    missing_tables: tuple[str, ...] = (),
    missing_columns: dict[str, tuple[str, ...]] | None = None,
) -> ReplyDuplicateDraftReport:
    return ReplyDuplicateDraftReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_empty_totals(),
        groups=(),
        source_table=source_table,
        missing_tables=missing_tables,
        missing_columns=missing_columns or {},
    )
