"""Report reply drafts that required repeated edits during review."""

from __future__ import annotations

import csv
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import io
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 50
DEFAULT_MIN_EDITS = 1

REPLY_TABLE = "reply_queue"
EVENT_TABLE = "reply_review_events"


@dataclass(frozen=True)
class ReplyEditChurnRow:
    """One reply draft and its review edit churn signals."""

    reply_queue_id: int
    platform: str | None
    inbound_author_handle: str | None
    intent: str | None
    priority: str | None
    status: str | None
    quality_score: float | None
    quality_flags: tuple[str, ...]
    draft_length: int
    edit_count: int
    review_event_count: int
    first_event_at: str | None
    last_event_at: str | None
    detected_at: str | None
    churn_score: float

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["quality_flags"] = list(self.quality_flags)
        return payload


@dataclass(frozen=True)
class ReplyEditChurnReport:
    """Aggregate reply edit churn report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[ReplyEditChurnRow, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "reply_edit_churn",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "totals": dict(sorted(self.totals.items())),
        }


def build_reply_edit_churn_report(
    db_or_conn: Any,
    *,
    platform: str | None = None,
    status: str | None = None,
    intent: str | None = None,
    priority: str | None = None,
    start_date: str | datetime | None = None,
    end_date: str | datetime | None = None,
    min_edits: int = DEFAULT_MIN_EDITS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ReplyEditChurnReport:
    """Build a deterministic report of replies with repeated review edits."""
    if min_edits < 0:
        raise ValueError("min_edits must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    start = _parse_filter_date(start_date, "start_date")
    end = _parse_filter_date(end_date, "end_date")
    if start and end and start > end:
        raise ValueError("start_date must be before or equal to end_date")

    filters = {
        "end_date": end.isoformat() if end else None,
        "intent": intent,
        "limit": limit,
        "min_edits": min_edits,
        "platform": platform,
        "priority": priority,
        "start_date": start.isoformat() if start else None,
        "status": status,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(
        table for table in (REPLY_TABLE, EVENT_TABLE) if table not in schema
    )
    missing_columns = _missing_columns(schema)
    if REPLY_TABLE not in schema or "id" not in schema.get(REPLY_TABLE, set()):
        return ReplyEditChurnReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_empty_totals(),
            rows=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    reply_rows = _load_reply_rows(
        conn,
        schema[REPLY_TABLE],
        platform=platform,
        status=status,
        intent=intent,
        priority=priority,
        start_date=start,
        end_date=end,
    )
    events_by_reply = _load_event_stats(
        conn,
        schema.get(EVENT_TABLE, set()),
        [row["reply_queue_id"] for row in reply_rows],
    )

    all_rows = [
        _build_row(row, events_by_reply.get(row["reply_queue_id"], _empty_event_stats()))
        for row in reply_rows
    ]
    matched_rows = [row for row in all_rows if row.edit_count >= min_edits]
    matched_rows.sort(key=_row_sort_key)
    emitted_rows = matched_rows[:limit]

    edit_counts = [row.edit_count for row in all_rows]
    churn_scores = [row.churn_score for row in all_rows]
    return ReplyEditChurnReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "edit_event_count": sum(edit_counts),
            "max_churn_score": max(churn_scores) if churn_scores else 0.0,
            "reply_count": len(all_rows),
            "reply_with_edit_count": sum(1 for count in edit_counts if count > 0),
            "review_event_count": sum(row.review_event_count for row in all_rows),
            "row_count": len(emitted_rows),
            "total_churn_score": round(sum(churn_scores), 3),
        },
        rows=tuple(emitted_rows),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_reply_edit_churn_json(report: ReplyEditChurnReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_edit_churn_csv(report: ReplyEditChurnReport) -> str:
    """Serialize the report as CSV with one row per reply draft."""
    output = io.StringIO()
    fieldnames = [
        "reply_queue_id",
        "platform",
        "inbound_author_handle",
        "intent",
        "priority",
        "status",
        "quality_score",
        "quality_flags",
        "draft_length",
        "edit_count",
        "review_event_count",
        "first_event_at",
        "last_event_at",
        "detected_at",
        "churn_score",
    ]
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for item in report.rows:
        row = item.to_dict()
        row["quality_flags"] = json.dumps(row["quality_flags"], sort_keys=True)
        writer.writerow(row)
    return output.getvalue().rstrip("\r\n")


def _build_row(row: dict[str, Any], events: dict[str, Any]) -> ReplyEditChurnRow:
    quality_score = _float_or_none(row.get("quality_score"))
    quality_flags = tuple(_parse_flags(row.get("quality_flags")))
    edit_count = int(events["edit_count"])
    review_event_count = int(events["review_event_count"])
    churn_score = _churn_score(
        edit_count=edit_count,
        review_event_count=review_event_count,
        quality_score=quality_score,
        quality_flag_count=len(quality_flags),
    )
    return ReplyEditChurnRow(
        reply_queue_id=int(row["reply_queue_id"]),
        platform=row.get("platform"),
        inbound_author_handle=row.get("inbound_author_handle"),
        intent=row.get("intent"),
        priority=row.get("priority"),
        status=row.get("status"),
        quality_score=quality_score,
        quality_flags=quality_flags,
        draft_length=len(str(row.get("draft_text") or "")),
        edit_count=edit_count,
        review_event_count=review_event_count,
        first_event_at=events["first_event_at"],
        last_event_at=events["last_event_at"],
        detected_at=row.get("detected_at"),
        churn_score=churn_score,
    )


def _churn_score(
    *,
    edit_count: int,
    review_event_count: int,
    quality_score: float | None,
    quality_flag_count: int,
) -> float:
    quality_penalty = 0.0
    if quality_score is not None:
        quality_penalty = max(0.0, 7.0 - quality_score)
    raw = (
        edit_count * 10.0
        + max(0, review_event_count - edit_count) * 1.5
        + quality_flag_count * 2.0
        + quality_penalty
    )
    return round(raw, 3)


def _load_reply_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    platform: str | None,
    status: str | None,
    intent: str | None,
    priority: str | None,
    start_date: datetime | None,
    end_date: datetime | None,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id", alias="reply_queue_id"),
        _column_expr(columns, "platform", "'x'"),
        _column_expr(columns, "inbound_author_handle"),
        _column_expr(columns, "intent", "'other'"),
        _column_expr(columns, "priority", "'normal'"),
        _column_expr(columns, "status", "'pending'"),
        _column_expr(columns, "quality_score"),
        _column_expr(columns, "quality_flags"),
        _column_expr(columns, "draft_text"),
        _column_expr(columns, "detected_at"),
    ]
    where: list[str] = []
    params: list[Any] = []
    for column, value in (
        ("platform", platform),
        ("status", status),
        ("intent", intent),
        ("priority", priority),
    ):
        if value is not None and column in columns:
            where.append(f"{column} = ?")
            params.append(value)
    if start_date is not None and "detected_at" in columns:
        where.append("datetime(detected_at) >= datetime(?)")
        params.append(start_date.isoformat())
    if end_date is not None and "detected_at" in columns:
        where.append("datetime(detected_at) <= datetime(?)")
        params.append(end_date.isoformat())
    where_sql = f" WHERE {' AND '.join(where)}" if where else ""
    order_sql = (
        "datetime(detected_at) ASC, id ASC"
        if "detected_at" in columns
        else "id ASC"
    )
    rows = conn.execute(
        f"SELECT {', '.join(select_columns)} FROM {REPLY_TABLE}{where_sql} ORDER BY {order_sql}",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows if row["reply_queue_id"] is not None]


def _load_event_stats(
    conn: sqlite3.Connection,
    columns: set[str],
    reply_ids: list[int],
) -> dict[int, dict[str, Any]]:
    required = {"reply_queue_id"}
    if not reply_ids or not columns or not required.issubset(columns):
        return {}
    event_type_expr = _column_expr(columns, "event_type")
    created_at_expr = _column_expr(columns, "created_at")
    placeholders = ", ".join("?" for _ in reply_ids)
    rows = conn.execute(
        f"""SELECT reply_queue_id,
                  {event_type_expr},
                  {created_at_expr}
             FROM {EVENT_TABLE}
             WHERE reply_queue_id IN ({placeholders})
             ORDER BY reply_queue_id ASC, datetime(created_at) ASC, rowid ASC""",
        tuple(reply_ids),
    ).fetchall()
    grouped: dict[int, dict[str, Any]] = {}
    for raw in rows:
        row = dict(raw)
        reply_id = _int_or_none(row.get("reply_queue_id"))
        if reply_id is None:
            continue
        stats = grouped.setdefault(reply_id, _empty_event_stats())
        stats["review_event_count"] += 1
        if str(row.get("event_type") or "").strip().casefold() == "edited":
            stats["edit_count"] += 1
        created_at = row.get("created_at")
        if created_at:
            if stats["first_event_at"] is None:
                stats["first_event_at"] = str(created_at)
            stats["last_event_at"] = str(created_at)
    return grouped


def _empty_event_stats() -> dict[str, Any]:
    return {
        "edit_count": 0,
        "first_event_at": None,
        "last_event_at": None,
        "review_event_count": 0,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, tuple[str, ...]]:
    expected = {
        REPLY_TABLE: (
            "id",
            "platform",
            "inbound_author_handle",
            "intent",
            "priority",
            "status",
            "quality_score",
            "quality_flags",
            "draft_text",
            "detected_at",
        ),
        EVENT_TABLE: (
            "reply_queue_id",
            "event_type",
            "created_at",
        ),
    }
    return {
        table: tuple(column for column in columns if column not in schema.get(table, set()))
        for table, columns in expected.items()
        if table in schema
    }


def _column_expr(columns: set[str], column: str, default: str = "NULL", *, alias: str | None = None) -> str:
    target = alias or column
    if column in columns:
        return column if target == column else f"{column} AS {target}"
    return f"{default} AS {target}"


def _parse_flags(raw: Any) -> list[str]:
    if raw in (None, ""):
        return []
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (TypeError, json.JSONDecodeError):
        return []
    if not isinstance(parsed, list):
        return []
    return sorted(str(item) for item in parsed if item is not None and str(item))


def _parse_filter_date(value: str | datetime | None, name: str) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO-8601 date or datetime") from exc
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _row_sort_key(row: ReplyEditChurnRow) -> tuple[Any, ...]:
    return (-row.edit_count, row.detected_at or "", row.reply_queue_id)


def _empty_totals() -> dict[str, Any]:
    return {
        "edit_event_count": 0,
        "max_churn_score": 0.0,
        "reply_count": 0,
        "reply_with_edit_count": 0,
        "review_event_count": 0,
        "row_count": 0,
        "total_churn_score": 0.0,
    }
