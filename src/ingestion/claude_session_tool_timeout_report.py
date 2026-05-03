"""Report Claude Code tool timeout events by day and tool."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Iterable

from ingestion.claude_command_retry_recovery import (
    SOURCE_TABLE_CANDIDATES,
    TIMESTAMP_COLUMNS,
    _connection,
    _ensure_utc,
    _first_text,
    _is_row_iterable,
    _parse_datetime,
    _schema,
    load_claude_command_event_rows,
)
from ingestion.claude_tool_error_taxonomy import (
    DEFAULT_DAYS,
    ClaudeToolErrorTaxonomyEvent,
    load_claude_tool_error_events,
    normalize_tool_filter,
)


@dataclass(frozen=True)
class ClaudeSessionToolTimeoutRow:
    """One daily tool timeout group."""

    day: str
    tool_name: str
    timeout_count: int
    session_count: int
    first_seen_at: str | None
    last_seen_at: str | None
    representative_session_ids: tuple[str, ...]
    timeout_id: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["representative_session_ids"] = list(self.representative_session_ids)
        return payload


@dataclass(frozen=True)
class ClaudeSessionToolTimeoutReport:
    """Claude session tool timeout report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionToolTimeoutRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_tool_timeout_report",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_session_tool_timeout_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    tool: str | None = None,
    now: datetime | None = None,
) -> ClaudeSessionToolTimeoutReport:
    """Build a deterministic report of Claude tool timeout events."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    normalized_tool = normalize_tool_filter(tool)
    source_tables: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()

    if _is_row_iterable(db_or_rows):
        raw_rows = _filter_rows([dict(row) for row in db_or_rows], cutoff=cutoff)
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

    events, malformed_metadata_count = load_claude_tool_error_events(
        raw_rows,
        tool=normalized_tool,
    )
    timeout_events = tuple(event for event in events if event.error_class == "timeout")
    rows = _group_timeout_events(timeout_events)
    return ClaudeSessionToolTimeoutReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "tool": normalized_tool,
        },
        totals={
            "malformed_metadata_count": malformed_metadata_count,
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in timeout_events}),
            "timeout_count": len(timeout_events),
            "tool_count": len({event.tool_name for event in timeout_events}),
        },
        rows=tuple(rows),
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def format_claude_session_tool_timeout_json(
    report: ClaudeSessionToolTimeoutReport,
) -> str:
    """Serialize a Claude session tool timeout report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _group_timeout_events(
    events: Iterable[ClaudeToolErrorTaxonomyEvent],
) -> list[ClaudeSessionToolTimeoutRow]:
    grouped: dict[tuple[str, str], list[ClaudeToolErrorTaxonomyEvent]] = {}
    for event in events:
        day = _event_day(event)
        grouped.setdefault((day, event.tool_name), []).append(event)

    rows = [
        _timeout_row(day, tool_name, tuple(sorted(group, key=_event_sort_key)))
        for (day, tool_name), group in grouped.items()
    ]
    return sorted(rows, key=_row_sort_key)


def _timeout_row(
    day: str,
    tool_name: str,
    events: tuple[ClaudeToolErrorTaxonomyEvent, ...],
) -> ClaudeSessionToolTimeoutRow:
    digest = hashlib.sha256(f"{day}:{tool_name}".encode("utf-8")).hexdigest()[:12]
    sessions = tuple(dict.fromkeys(event.session_id for event in events))
    return ClaudeSessionToolTimeoutRow(
        day=day,
        tool_name=tool_name,
        timeout_count=len(events),
        session_count=len(set(sessions)),
        first_seen_at=events[0].timestamp,
        last_seen_at=events[-1].timestamp,
        representative_session_ids=tuple(sorted(sessions)[:3]),
        timeout_id=f"claude_session_tool_timeout_{digest}",
    )


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    filtered = []
    for row in rows:
        timestamp = _parse_datetime(_first_text(row, TIMESTAMP_COLUMNS))
        if timestamp is None or timestamp >= cutoff:
            filtered.append(row)
    return filtered


def _event_day(event: ClaudeToolErrorTaxonomyEvent) -> str:
    timestamp = _parse_datetime(event.timestamp)
    return timestamp.date().isoformat() if timestamp else "unknown"


def _event_sort_key(event: ClaudeToolErrorTaxonomyEvent) -> tuple[str, str, str]:
    timestamp = _parse_datetime(event.timestamp)
    return (
        timestamp.isoformat() if timestamp else str(event.timestamp or ""),
        event.session_id,
        event.tool_name,
    )


def _row_sort_key(row: ClaudeSessionToolTimeoutRow) -> tuple[str, str]:
    return (row.day, row.tool_name)
