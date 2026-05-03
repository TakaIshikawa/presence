"""Detect tool outputs likely truncated before synthesis or audit use."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Iterable, Mapping

from ingestion.claude_command_retry_recovery import (
    _connection,
    _ensure_utc,
    _is_row_iterable,
    _metadata,
    _parse_datetime,
    _schema,
    _tool_name,
    load_claude_command_event_rows,
)


SOURCE_TABLE_CANDIDATES = (
    "claude_messages",
    "claude_message_events",
    "claude_session_events",
)


DEFAULT_DAYS = 14
DEFAULT_REPRESENTATIVE_LIMIT = 3

TRUNCATION_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("truncated", re.compile(r"\btruncated\b", re.IGNORECASE)),
    ("output_omitted", re.compile(r"\boutput\s+omitted\b", re.IGNORECASE)),
    ("lines_omitted", re.compile(r"\blines?\s+omitted\b", re.IGNORECASE)),
    ("output_truncated", re.compile(r"\boutput.*truncated\b", re.IGNORECASE)),
)


@dataclass(frozen=True)
class ClaudeSessionOutputTruncationEvent:
    """One normalized truncated tool output event."""

    session_id: str
    tool_name: str
    timestamp: str | None
    marker: str
    excerpt: str
    source_table: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionOutputTruncationRow:
    """One grouped truncated output row."""

    session_id: str
    tool_name: str
    marker: str
    occurrence_count: int
    first_seen_at: str | None
    last_seen_at: str | None
    representative_excerpt: str
    source_table: str
    truncation_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionOutputTruncationReport:
    """Claude session output truncation report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionOutputTruncationRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_output_truncation",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_session_output_truncation_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> ClaudeSessionOutputTruncationReport:
    """Build a deterministic report of truncated Claude tool outputs."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    source_tables: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()

    if _is_row_iterable(db_or_rows):
        raw_rows = [dict(row) for row in db_or_rows]
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

    events, malformed_metadata_count = load_truncation_events(raw_rows)
    rows = group_truncation_events(events)

    return ClaudeSessionOutputTruncationReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
        },
        totals={
            "malformed_metadata_count": malformed_metadata_count,
            "reported_group_count": len(rows),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
            "truncation_event_count": len(events),
        },
        rows=tuple(rows),
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_truncation_events(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[ClaudeSessionOutputTruncationEvent], int]:
    """Normalize raw Claude rows into truncation events."""
    events: list[ClaudeSessionOutputTruncationEvent] = []
    malformed_metadata_count = 0

    for row in rows:
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1

        tool_name = _tool_name(row, metadata)
        session_id = (
            _first_text(row, ("session_id", "sessionId"))
            or _first_text(metadata, ("session_id", "sessionId"))
            or "unknown-session"
        )
        timestamp = (
            _first_text(row, ("timestamp", "created_at", "event_time", "event_at"))
            or _first_text(metadata, ("timestamp", "created_at", "event_time", "event_at"))
        )
        source_table = str(row.get("_source_table") or "unknown")

        # Track which fields we've already checked to avoid duplicates
        checked_values = set()

        # Scan plain text fields in the row
        for field in ("message", "content", "output", "error", "result"):
            value = row.get(field)
            if value and isinstance(value, str):
                value_id = id(value)
                if value_id not in checked_values:
                    checked_values.add(value_id)
                    for marker, pattern in TRUNCATION_PATTERNS:
                        if pattern.search(value):
                            events.append(
                                ClaudeSessionOutputTruncationEvent(
                                    session_id=session_id,
                                    tool_name=tool_name,
                                    timestamp=timestamp,
                                    marker=marker,
                                    excerpt=_excerpt(value),
                                    source_table=source_table,
                                )
                            )
                            break

        # Scan nested JSON fields in metadata
        if isinstance(metadata, Mapping):
            for field in ("output", "result", "error", "message", "text"):
                value = metadata.get(field)
                if value:
                    value_str = str(value)
                    value_id = id(value_str)
                    if value_id not in checked_values:
                        checked_values.add(value_id)
                        for marker, pattern in TRUNCATION_PATTERNS:
                            if pattern.search(value_str):
                                events.append(
                                    ClaudeSessionOutputTruncationEvent(
                                        session_id=session_id,
                                        tool_name=tool_name,
                                        timestamp=timestamp,
                                        marker=marker,
                                        excerpt=_excerpt(value_str),
                                        source_table=source_table,
                                    )
                                )
                                break

        # Scan nested content field if it's a dict
        content_value = row.get("content")
        if isinstance(content_value, Mapping):
            for field in ("output", "result", "error", "message", "text"):
                value = content_value.get(field)
                if value:
                    value_str = str(value)
                    value_id = id(value_str)
                    if value_id not in checked_values:
                        checked_values.add(value_id)
                        for marker, pattern in TRUNCATION_PATTERNS:
                            if pattern.search(value_str):
                                events.append(
                                    ClaudeSessionOutputTruncationEvent(
                                        session_id=session_id,
                                        tool_name=tool_name,
                                        timestamp=timestamp,
                                        marker=marker,
                                        excerpt=_excerpt(value_str),
                                        source_table=source_table,
                                    )
                                )
                                break

        # Check explicit truncation flags in metadata
        if isinstance(metadata, Mapping):
            truncated_flag = metadata.get("truncated")
            if truncated_flag:
                events.append(
                    ClaudeSessionOutputTruncationEvent(
                        session_id=session_id,
                        tool_name=tool_name,
                        timestamp=timestamp,
                        marker="truncated_flag",
                        excerpt=f"truncated={truncated_flag}",
                        source_table=source_table,
                    )
                )

    return sorted(events, key=_event_sort_key), malformed_metadata_count


def group_truncation_events(
    events: Iterable[ClaudeSessionOutputTruncationEvent],
) -> list[ClaudeSessionOutputTruncationRow]:
    """Group truncation events into deterministic rows."""
    grouped: dict[tuple[str, str, str], list[ClaudeSessionOutputTruncationEvent]] = {}
    for event in events:
        key = (event.session_id, event.tool_name, event.marker)
        grouped.setdefault(key, []).append(event)

    rows = [_truncation_row(key, tuple(sorted(group, key=_event_sort_key))) for key, group in grouped.items()]
    return sorted(rows, key=_row_sort_key)


def format_claude_session_output_truncation_json(report: ClaudeSessionOutputTruncationReport) -> str:
    """Serialize a truncation report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_output_truncation_text(report: ClaudeSessionOutputTruncationReport) -> str:
    """Render a compact truncation report."""
    totals = report.totals
    lines = [
        "Claude Session Output Truncation Report",
        f"Generated: {report.generated_at}",
        f"Filters: days={report.filters['days']}",
        (
            "Totals: "
            f"rows={totals['rows_scanned']} events={totals['truncation_event_count']} "
            f"sessions={totals['session_count']} groups={totals['reported_group_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.rows:
        lines.extend(["", "No truncated outputs detected."])
        return "\n".join(lines)

    lines.extend(["", "Truncated Outputs:"])
    for row in report.rows:
        lines.append(
            f"- session={row.session_id} tool={row.tool_name} marker={row.marker} "
            f"occurrences={row.occurrence_count}"
        )
        lines.append(f"  first_seen={row.first_seen_at or '-'} last_seen={row.last_seen_at or '-'}")
        lines.append(f"  excerpt: {row.representative_excerpt}")
        lines.append(f"  source_table: {row.source_table}")

    return "\n".join(lines)


def _truncation_row(
    key: tuple[str, str, str],
    events: tuple[ClaudeSessionOutputTruncationEvent, ...],
) -> ClaudeSessionOutputTruncationRow:
    session_id, tool_name, marker = key
    digest = hashlib.sha256(f"{session_id}:{tool_name}:{marker}".encode("utf-8")).hexdigest()[:12]
    excerpts = tuple(dict.fromkeys(event.excerpt for event in events if event.excerpt))
    source_tables = {event.source_table for event in events}
    return ClaudeSessionOutputTruncationRow(
        session_id=session_id,
        tool_name=tool_name,
        marker=marker,
        occurrence_count=len(events),
        first_seen_at=events[0].timestamp,
        last_seen_at=events[-1].timestamp,
        representative_excerpt=excerpts[0] if excerpts else "",
        source_table=sorted(source_tables)[0] if source_tables else "unknown",
        truncation_id=f"claude_truncation_{digest}",
    )


def _event_sort_key(event: ClaudeSessionOutputTruncationEvent) -> tuple[str, str, str, str]:
    return (
        _timestamp_sort(event.timestamp),
        event.session_id,
        event.tool_name,
        event.marker,
    )


def _row_sort_key(row: ClaudeSessionOutputTruncationRow) -> tuple[int, str, str, str]:
    return (-row.occurrence_count, row.session_id, row.tool_name, row.marker)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _first_text(source: Mapping[str, Any], columns: tuple[str, ...]) -> str | None:
    for column in columns:
        value = source.get(column)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _excerpt(value: Any, *, limit: int = 120) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."
