"""Report model changes inside Claude Code sessions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
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
    _nested_text,
    _parse_datetime,
    _schema,
    load_claude_command_event_rows,
)


SESSION_COLUMNS = ("session_id", "sessionId")
EVENT_ID_COLUMNS = ("event_id", "eventId", "message_id", "messageId", "uuid", "id")
MODEL_COLUMNS = (
    "model",
    "model_name",
    "modelName",
    "claude_model",
    "assistant_model",
    "requested_model",
)


@dataclass(frozen=True)
class ClaudeSessionModelSwitchRow:
    """One adjacent model change within a Claude session."""

    session_id: str
    switched_at: str
    from_model: str
    to_model: str
    previous_event_id: str
    next_event_id: str
    switch_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionModelSwitchReport:
    """Claude session model switch report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionModelSwitchRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_model_switches",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _ModelEvent:
    session_id: str
    timestamp: str
    timestamp_at: datetime
    model: str
    event_id: str
    ordinal: int


def build_claude_session_model_switches_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    session_id: str | None = None,
    model: str | None = None,
    now: datetime | None = None,
) -> ClaudeSessionModelSwitchReport:
    """Build a deterministic report of adjacent model switches."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    normalized_model = normalize_model(model) if model else None
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

    events, malformed_metadata_count, malformed_timestamp_count = load_model_switch_events(
        raw_rows,
        cutoff=cutoff,
        session_id=session_id,
    )
    all_rows = detect_model_switches(events)
    rows = tuple(
        row
        for row in all_rows
        if normalized_model is None or normalized_model in {row.from_model, row.to_model}
    )

    return ClaudeSessionModelSwitchReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "model": normalized_model,
            "session_id": session_id,
        },
        totals={
            "event_count": len(events),
            "malformed_metadata_count": malformed_metadata_count,
            "malformed_timestamp_count": malformed_timestamp_count,
            "model_count": len({event.model for event in events}),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
            "switch_count": len(rows),
            "unfiltered_switch_count": len(all_rows),
        },
        rows=rows,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_model_switch_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    cutoff: datetime,
    session_id: str | None = None,
) -> tuple[list[_ModelEvent], int, int]:
    """Normalize Claude rows into timestamped model events."""
    events: list[_ModelEvent] = []
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
            _ModelEvent(
                session_id=row_session_id,
                timestamp=timestamp_at.isoformat(),
                timestamp_at=timestamp_at,
                model=_model_name(row, metadata),
                event_id=_event_id(row, metadata, ordinal),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count, malformed_timestamp_count


def detect_model_switches(events: Iterable[_ModelEvent]) -> list[ClaudeSessionModelSwitchRow]:
    """Find adjacent events where normalized model names differ."""
    sessions: dict[str, list[_ModelEvent]] = {}
    for event in events:
        sessions.setdefault(event.session_id, []).append(event)

    rows: list[ClaudeSessionModelSwitchRow] = []
    for session_id, session_events in sorted(sessions.items()):
        ordered = sorted(session_events, key=_event_sort_key)
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if previous.model != current.model:
                rows.append(_switch_row(session_id, previous, current))
    return sorted(rows, key=_row_sort_key)


def normalize_model(value: Any) -> str:
    """Normalize model text for grouping and filters."""
    text = " ".join(str(value or "").strip().split()).lower()
    return re.sub(r"[^a-z0-9_.:-]+", "-", text).strip("-") or "unknown"


def format_claude_session_model_switches_json(report: ClaudeSessionModelSwitchReport) -> str:
    """Serialize a Claude session model switch report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _model_name(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    text = _first_text(row, MODEL_COLUMNS) or _first_text(metadata, MODEL_COLUMNS)
    if not text:
        for path in (
            ("model", "name"),
            ("message", "model"),
            ("request", "model"),
            ("response", "model"),
            ("usage", "model"),
            ("metadata", "model"),
        ):
            text = _nested_text(metadata, path)
            if text:
                break
    return normalize_model(text)


def _event_id(row: Mapping[str, Any], metadata: Mapping[str, Any], ordinal: int) -> str:
    return _first_text(row, EVENT_ID_COLUMNS) or _first_text(metadata, EVENT_ID_COLUMNS) or f"row-{ordinal}"


def _switch_row(
    session_id: str,
    previous: _ModelEvent,
    current: _ModelEvent,
) -> ClaudeSessionModelSwitchRow:
    digest = hashlib.sha256(
        f"{session_id}:{previous.event_id}:{current.event_id}:{previous.model}:{current.model}".encode("utf-8")
    ).hexdigest()[:12]
    return ClaudeSessionModelSwitchRow(
        session_id=session_id,
        switched_at=current.timestamp,
        from_model=previous.model,
        to_model=current.model,
        previous_event_id=previous.event_id,
        next_event_id=current.event_id,
        switch_id=f"claude_session_model_switch_{digest}",
    )


def _event_sort_key(event: _ModelEvent) -> tuple[str, str, int]:
    return (event.timestamp_at.isoformat(), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudeSessionModelSwitchRow) -> tuple[str, str, str, str]:
    return (row.switched_at, row.session_id, row.from_model, row.to_model)
