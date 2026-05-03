"""Report Claude Code session interruption resumption by day."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from statistics import median
from typing import Any, Iterable, Mapping

from ingestion.claude_session_topic_drift import (
    DEFAULT_DAYS,
    _column_expr,
    _connection,
    _ensure_utc,
    _looks_like_rows,
    _optional_text,
    _parse_datetime,
    _schema,
    jaccard_distance,
    tokenize_prompt_keywords,
)


DEFAULT_RESUME_THRESHOLD = 0.55
_TABLE = "claude_messages"
_REQUIRED_COLUMNS = ("session_id", "timestamp")
_OPTIONAL_COLUMNS = (
    "id",
    "message_uuid",
    "project_path",
    "prompt_text",
    "response_text",
    "content",
    "text",
    "message",
    "body",
    "transcript",
)
_INTERRUPTION_RE = re.compile(
    r"\b("
    r"user\s+(?:cancelled|canceled)|"
    r"(?:was\s+)?(?:cancelled|canceled|interrupted|aborted)|"
    r"session\s+(?:was\s+)?interrupted|"
    r"tool\s+call\s+(?:failed\s+because\s+it\s+)?(?:was\s+)?aborted|"
    r"stopped\s+(?:by|after)\s+user"
    r")\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ClaudeSessionInterruptionResumptionDay:
    """Daily interruption resumption counts."""

    day: str
    interrupted: int
    resumed: int
    abandoned: int
    median_minutes_to_resume: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionInterruptionResumptionReport:
    """Claude session interruption resumption report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    rows: tuple[ClaudeSessionInterruptionResumptionDay, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_interruption_resumption",
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


@dataclass(frozen=True)
class _SessionMessage:
    session_id: str
    timestamp: datetime | None
    timestamp_text: str | None
    project_path: str | None
    text: str
    keywords: frozenset[str]
    sort_id: str


@dataclass(frozen=True)
class _InterruptionEvent:
    session_id: str
    timestamp: datetime | None
    project_path: str | None
    keywords: frozenset[str]


def build_claude_session_interruption_resumption_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> ClaudeSessionInterruptionResumptionReport:
    """Build a deterministic daily report of interrupted and resumed Claude work."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "resume_threshold": DEFAULT_RESUME_THRESHOLD,
    }

    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}
    if _looks_like_rows(db_or_rows):
        raw_rows = [_mapping(row) for row in db_or_rows]
        raw_rows = _filter_rows(raw_rows, cutoff=cutoff)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        if _TABLE not in schema:
            missing_tables = (_TABLE,)
            raw_rows = []
        else:
            columns = schema[_TABLE]
            missing_columns = _missing_columns(columns)
            raw_rows = _load_rows(conn, columns, cutoff=cutoff)

    messages = _messages_from_rows(raw_rows)
    interruptions = _detect_interruptions(messages)
    daily = _daily_rows(interruptions, messages)
    return ClaudeSessionInterruptionResumptionReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "abandoned": sum(row.abandoned for row in daily),
            "interrupted": sum(row.interrupted for row in daily),
            "messages_scanned": len(messages),
            "resumed": sum(row.resumed for row in daily),
            "sessions_scanned": len({message.session_id for message in messages}),
        },
        rows=tuple(daily),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_session_interruption_resumption_json(
    report: ClaudeSessionInterruptionResumptionReport,
) -> str:
    """Serialize an interruption resumption report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _daily_rows(
    interruptions: Iterable[_InterruptionEvent],
    messages: tuple[_SessionMessage, ...],
) -> list[ClaudeSessionInterruptionResumptionDay]:
    grouped: dict[str, list[float | None]] = defaultdict(list)
    for interruption in interruptions:
        day = interruption.timestamp.date().isoformat() if interruption.timestamp else "unknown"
        resumed_at = _find_resume(interruption, messages)
        if resumed_at is None or interruption.timestamp is None:
            grouped[day].append(None)
        else:
            grouped[day].append((resumed_at - interruption.timestamp).total_seconds() / 60)

    rows: list[ClaudeSessionInterruptionResumptionDay] = []
    for day, resume_minutes in sorted(grouped.items()):
        resumed_minutes = [value for value in resume_minutes if value is not None]
        resumed = len(resumed_minutes)
        interrupted = len(resume_minutes)
        median_minutes = round(float(median(resumed_minutes)), 2) if resumed_minutes else None
        rows.append(
            ClaudeSessionInterruptionResumptionDay(
                day=day,
                interrupted=interrupted,
                resumed=resumed,
                abandoned=interrupted - resumed,
                median_minutes_to_resume=median_minutes,
            )
        )
    return rows


def _find_resume(
    interruption: _InterruptionEvent,
    messages: tuple[_SessionMessage, ...],
) -> datetime | None:
    if interruption.timestamp is None:
        return None
    candidates: list[_SessionMessage] = []
    for message in messages:
        if message.timestamp is None or message.timestamp <= interruption.timestamp:
            continue
        if message.session_id == interruption.session_id:
            continue
        if interruption.project_path and message.project_path != interruption.project_path:
            continue
        if not _is_same_work(interruption.keywords, message.keywords):
            continue
        candidates.append(message)
    if not candidates:
        return None
    return min(message.timestamp for message in candidates if message.timestamp is not None)


def _is_same_work(left: frozenset[str], right: frozenset[str]) -> bool:
    if not left or not right:
        return False
    return jaccard_distance(left, right) <= DEFAULT_RESUME_THRESHOLD


def _detect_interruptions(messages: tuple[_SessionMessage, ...]) -> tuple[_InterruptionEvent, ...]:
    by_session: dict[str, list[_SessionMessage]] = defaultdict(list)
    for message in messages:
        by_session[message.session_id].append(message)

    interruptions: list[_InterruptionEvent] = []
    for session_messages in by_session.values():
        ordered = sorted(session_messages, key=_message_sort_key)
        for index, message in enumerate(ordered):
            if not _INTERRUPTION_RE.search(message.text):
                continue
            context_keywords = set(message.keywords)
            for previous in ordered[max(0, index - 2) : index]:
                context_keywords.update(previous.keywords)
            interruptions.append(
                _InterruptionEvent(
                    session_id=message.session_id,
                    timestamp=message.timestamp,
                    project_path=message.project_path,
                    keywords=frozenset(context_keywords),
                )
            )
    return tuple(sorted(interruptions, key=_interruption_sort_key))


def _messages_from_rows(rows: Iterable[Mapping[str, Any]]) -> tuple[_SessionMessage, ...]:
    messages = []
    for row in rows:
        text = _row_text(row)
        if not text:
            continue
        timestamp_text = _optional_text(row.get("timestamp"))
        messages.append(
            _SessionMessage(
                session_id=str(row.get("session_id") or "unknown-session"),
                timestamp=_parse_datetime(timestamp_text),
                timestamp_text=timestamp_text,
                project_path=_optional_text(row.get("project_path")),
                text=text,
                keywords=tokenize_prompt_keywords(_strip_interruption_words(text)),
                sort_id=str(row.get("id") or row.get("message_uuid") or ""),
            )
        )
    return tuple(sorted(messages, key=_message_sort_key))


def _row_text(row: Mapping[str, Any]) -> str:
    parts = []
    for key in ("prompt_text", "response_text", "content", "text", "message", "body", "transcript"):
        text = _content_text(row.get(key))
        if text:
            parts.append(text)
    return "\n".join(parts)


def _content_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, Mapping):
        return _content_text(value.get("text") or value.get("content"))
    if isinstance(value, list):
        return "\n".join(part for item in value if (part := _content_text(item)))
    return ""


def _strip_interruption_words(text: str) -> str:
    return _INTERRUPTION_RE.sub(" ", text)


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    filtered = []
    for row in rows:
        timestamp = _parse_datetime(row.get("timestamp"))
        if timestamp is None or timestamp >= cutoff:
            filtered.append(row)
    return filtered


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id", "rowid"),
        _column_expr(columns, "session_id"),
        _column_expr(columns, "message_uuid"),
        _column_expr(columns, "project_path"),
        _column_expr(columns, "timestamp"),
        *[_column_expr(columns, column) for column in _OPTIONAL_COLUMNS if column not in {"id", "message_uuid", "project_path"}],
    ]
    where_sql = "WHERE timestamp >= ?" if "timestamp" in columns else ""
    params: list[Any] = [cutoff.isoformat()] if "timestamp" in columns else []
    order_sql = "timestamp ASC, id ASC" if {"timestamp", "id"}.issubset(columns) else "rowid ASC"
    cursor = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM {_TABLE}
            {where_sql}
            ORDER BY {order_sql}""",
        params,
    )
    column_names = [description[0] for description in cursor.description]
    return [
        dict(row)
        if isinstance(row, Mapping)
        else dict(zip(column_names, row, strict=False))
        for row in cursor.fetchall()
    ]


def _missing_columns(columns: set[str]) -> dict[str, tuple[str, ...]]:
    missing = tuple(
        column
        for column in (*_REQUIRED_COLUMNS, *_OPTIONAL_COLUMNS)
        if column not in columns
    )
    return {_TABLE: missing} if missing else {}


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row)


def _message_sort_key(message: _SessionMessage) -> tuple[str, str, str]:
    return (
        message.timestamp.isoformat() if message.timestamp else str(message.timestamp_text or ""),
        message.session_id,
        message.sort_id,
    )


def _interruption_sort_key(event: _InterruptionEvent) -> tuple[str, str]:
    return (
        event.timestamp.isoformat() if event.timestamp else "",
        event.session_id,
    )
