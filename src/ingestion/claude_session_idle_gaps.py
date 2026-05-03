"""Report long idle gaps inside Claude Code sessions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Iterable, Mapping

from ingestion.claude_command_retry_recovery import (
    DEFAULT_DAYS,
    SOURCE_TABLE_CANDIDATES,
    TIMESTAMP_COLUMNS,
    _connection,
    _ensure_utc,
    _first_text,
    _is_row_iterable,
    _metadata,
    _parse_datetime,
    _schema,
    _tool_name,
    load_claude_command_event_rows,
)


DEFAULT_MIN_GAP_MINUTES = 30
SESSION_COLUMNS = ("session_id", "sessionId")


@dataclass(frozen=True)
class ClaudeSessionIdleGapRow:
    """One long gap between adjacent events in a Claude session."""

    session_id: str
    previous_event_at: str
    next_event_at: str
    gap_minutes: float
    previous_tool_name: str
    next_tool_name: str
    gap_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionIdleGapReport:
    """Claude session idle gap report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionIdleGapRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_idle_gaps",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _IdleEvent:
    session_id: str
    timestamp: str
    timestamp_at: datetime
    tool_name: str
    ordinal: int


def build_claude_session_idle_gaps_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_gap_minutes: int = DEFAULT_MIN_GAP_MINUTES,
    session_id: str | None = None,
    now: datetime | None = None,
) -> ClaudeSessionIdleGapReport:
    """Build a deterministic report of long idle gaps within sessions."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_gap_minutes <= 0:
        raise ValueError("min_gap_minutes must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    source_tables: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()

    if _is_row_iterable(db_or_rows):
        raw_rows = [dict(row) for row in db_or_rows]
        source_tables = tuple(
            sorted({str(row.get("_source_table") or "rows") for row in raw_rows})
        ) or ("rows",)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        source_tables = tuple(table for table in SOURCE_TABLE_CANDIDATES if table in schema)
        missing_tables = () if source_tables else SOURCE_TABLE_CANDIDATES
        raw_rows = [
            row
            for table in source_tables
            for row in load_claude_command_event_rows(conn, table, schema[table], cutoff=cutoff)
        ]

    events, malformed_metadata_count, malformed_timestamp_count = load_idle_gap_events(
        raw_rows,
        cutoff=cutoff,
        session_id=session_id,
    )
    rows = tuple(detect_idle_gaps(events, min_gap_minutes=min_gap_minutes))

    return ClaudeSessionIdleGapReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "min_gap_minutes": min_gap_minutes,
            "session_id": session_id,
        },
        totals={
            "event_count": len(events),
            "gap_count": len(rows),
            "malformed_metadata_count": malformed_metadata_count,
            "malformed_timestamp_count": malformed_timestamp_count,
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
        },
        rows=rows,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_idle_gap_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    cutoff: datetime,
    session_id: str | None = None,
) -> tuple[list[_IdleEvent], int, int]:
    """Normalize Claude rows into timestamped session events."""
    events: list[_IdleEvent] = []
    malformed_metadata_count = 0
    malformed_timestamp_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        row_session_id = (
            _first_text(row, SESSION_COLUMNS)
            or _first_text(metadata, SESSION_COLUMNS)
            or "unknown-session"
        )
        if session_id and row_session_id != session_id:
            continue
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        timestamp_at = _parse_datetime(timestamp)
        if timestamp_at is None:
            malformed_timestamp_count += 1
            continue
        if timestamp_at < cutoff:
            continue
        events.append(
            _IdleEvent(
                session_id=row_session_id,
                timestamp=timestamp_at.isoformat(),
                timestamp_at=timestamp_at,
                tool_name=_tool_name(row, metadata),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count, malformed_timestamp_count


def detect_idle_gaps(
    events: Iterable[_IdleEvent],
    *,
    min_gap_minutes: int = DEFAULT_MIN_GAP_MINUTES,
) -> list[ClaudeSessionIdleGapRow]:
    """Find adjacent event gaps meeting the configured threshold."""
    sessions: dict[str, list[_IdleEvent]] = {}
    for event in events:
        sessions.setdefault(event.session_id, []).append(event)

    rows: list[ClaudeSessionIdleGapRow] = []
    threshold = timedelta(minutes=min_gap_minutes)
    for session_id, session_events in sorted(sessions.items()):
        ordered = sorted(session_events, key=_event_sort_key)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            gap = current.timestamp_at - previous.timestamp_at
            if gap >= threshold:
                rows.append(_gap_row(session_id, previous, current, gap))
    return sorted(rows, key=_row_sort_key)


def format_claude_session_idle_gaps_json(report: ClaudeSessionIdleGapReport) -> str:
    """Serialize a Claude session idle gap report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _gap_row(
    session_id: str,
    previous: _IdleEvent,
    current: _IdleEvent,
    gap: timedelta,
) -> ClaudeSessionIdleGapRow:
    gap_minutes = round(gap.total_seconds() / 60, 3)
    digest = hashlib.sha256(
        f"{session_id}:{previous.timestamp}:{current.timestamp}".encode("utf-8")
    ).hexdigest()[:12]
    return ClaudeSessionIdleGapRow(
        session_id=session_id,
        previous_event_at=previous.timestamp,
        next_event_at=current.timestamp,
        gap_minutes=gap_minutes,
        previous_tool_name=previous.tool_name,
        next_tool_name=current.tool_name,
        gap_id=f"claude_session_idle_gap_{digest}",
    )


def _event_sort_key(event: _IdleEvent) -> tuple[str, str, int]:
    return (event.timestamp_at.isoformat(), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudeSessionIdleGapRow) -> tuple[float, str, str]:
    return (-row.gap_minutes, row.session_id, row.previous_event_at)
