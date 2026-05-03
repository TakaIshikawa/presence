"""Report dense Claude Code tool-use bursts inside fixed time windows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
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


DEFAULT_WINDOW_MINUTES = 5
DEFAULT_MIN_TOOLS = 10
COMMAND_ID_COLUMNS = (
    "command_id",
    "commandId",
    "tool_use_id",
    "toolUseId",
    "event_id",
    "eventId",
    "uuid",
    "id",
)
SESSION_COLUMNS = ("session_id", "sessionId")


@dataclass(frozen=True)
class ClaudeSessionToolBurstRow:
    """One session/window with unusually dense tool use."""

    session_id: str
    window_start: str
    window_end: str
    tool_count: int
    distinct_tool_count: int
    dominant_tool_name: str
    representative_command_ids: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["representative_command_ids"] = list(self.representative_command_ids)
        return payload


@dataclass(frozen=True)
class ClaudeSessionToolBurstReport:
    """Claude session tool burst report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionToolBurstRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_tool_bursts",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _ToolEvent:
    session_id: str
    timestamp: str
    timestamp_at: datetime
    tool_name: str
    command_id: str
    source_table: str
    ordinal: int


def build_claude_session_tool_bursts_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    min_tools: int = DEFAULT_MIN_TOOLS,
    now: datetime | None = None,
) -> ClaudeSessionToolBurstReport:
    """Build a deterministic report of dense Claude tool-use windows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if window_minutes <= 0:
        raise ValueError("window_minutes must be positive")
    if min_tools <= 0:
        raise ValueError("min_tools must be positive")

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

    events, malformed_metadata_count, malformed_timestamp_count = load_tool_burst_events(
        raw_rows,
        cutoff=cutoff,
    )
    rows = tuple(
        detect_tool_bursts(
            events,
            window_minutes=window_minutes,
            min_tools=min_tools,
        )
    )

    return ClaudeSessionToolBurstReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "min_tools": min_tools,
            "window_minutes": window_minutes,
        },
        totals={
            "burst_count": len(rows),
            "malformed_metadata_count": malformed_metadata_count,
            "malformed_timestamp_count": malformed_timestamp_count,
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
            "tool_event_count": len(events),
        },
        rows=rows,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_tool_burst_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    cutoff: datetime,
) -> tuple[list[_ToolEvent], int, int]:
    """Normalize Claude rows into timestamped tool events."""
    events: list[_ToolEvent] = []
    malformed_metadata_count = 0
    malformed_timestamp_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        tool_name = _tool_name(row, metadata)
        if tool_name == "unknown":
            continue
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        timestamp_at = _parse_datetime(timestamp)
        if timestamp_at is None:
            malformed_timestamp_count += 1
            continue
        if timestamp_at < cutoff:
            continue
        session_id = (
            _first_text(row, SESSION_COLUMNS)
            or _first_text(metadata, SESSION_COLUMNS)
            or "unknown-session"
        )
        events.append(
            _ToolEvent(
                session_id=session_id,
                timestamp=timestamp_at.isoformat(),
                timestamp_at=timestamp_at,
                tool_name=tool_name,
                command_id=_command_id(row, metadata, ordinal),
                source_table=str(row.get("_source_table") or "rows"),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count, malformed_timestamp_count


def detect_tool_bursts(
    events: Iterable[_ToolEvent],
    *,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    min_tools: int = DEFAULT_MIN_TOOLS,
) -> list[ClaudeSessionToolBurstRow]:
    """Group tool events by session and fixed UTC windows."""
    window_seconds = window_minutes * 60
    grouped: dict[tuple[str, datetime], list[_ToolEvent]] = {}
    for event in events:
        grouped.setdefault((event.session_id, _window_start(event.timestamp_at, window_seconds)), []).append(event)

    rows = [
        _burst_row(session_id, window_start, tuple(sorted(group, key=_event_sort_key)), window_minutes)
        for (session_id, window_start), group in grouped.items()
        if len(group) >= min_tools
    ]
    return sorted(rows, key=_row_sort_key)


def format_claude_session_tool_bursts_json(report: ClaudeSessionToolBurstReport) -> str:
    """Serialize a Claude session tool burst report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _burst_row(
    session_id: str,
    window_start: datetime,
    events: tuple[_ToolEvent, ...],
    window_minutes: int,
) -> ClaudeSessionToolBurstRow:
    tool_counts: dict[str, int] = {}
    for event in events:
        tool_counts[event.tool_name] = tool_counts.get(event.tool_name, 0) + 1
    dominant_tool_name = sorted(tool_counts.items(), key=lambda item: (-item[1], item[0]))[0][0]
    return ClaudeSessionToolBurstRow(
        session_id=session_id,
        window_start=window_start.isoformat(),
        window_end=(window_start + timedelta(minutes=window_minutes)).isoformat(),
        tool_count=len(events),
        distinct_tool_count=len(tool_counts),
        dominant_tool_name=dominant_tool_name,
        representative_command_ids=tuple(dict.fromkeys(event.command_id for event in events))[:5],
    )


def _window_start(timestamp: datetime, window_seconds: int) -> datetime:
    epoch_seconds = int(timestamp.timestamp())
    return datetime.fromtimestamp(
        epoch_seconds - (epoch_seconds % window_seconds),
        tz=timezone.utc,
    )


def _command_id(row: Mapping[str, Any], metadata: Mapping[str, Any], ordinal: int) -> str:
    value = _first_text(row, COMMAND_ID_COLUMNS) or _first_text(metadata, COMMAND_ID_COLUMNS)
    return value or f"row-{ordinal}"


def _event_sort_key(event: _ToolEvent) -> tuple[str, str, int]:
    return (event.timestamp_at.isoformat(), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudeSessionToolBurstRow) -> tuple[str, str, int]:
    return (row.window_start, row.session_id, -row.tool_count)
