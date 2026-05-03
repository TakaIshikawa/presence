"""Report Claude shell command runtimes grouped by day and duration bucket."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Iterable, Mapping

from ingestion.claude_session_approval_decision_audit import (
    METADATA_COLUMNS,
    SESSION_COLUMNS,
    SOURCE_TABLE_CANDIDATES,
    TIMESTAMP_COLUMNS,
    _connection,
    _ensure_utc,
    _first_existing,
    _first_text,
    _metadata,
    _nested_text,
    _parse_datetime,
    _quote_identifier,
    _schema,
)
from ingestion.claude_session_command_exit_codes import _command, _looks_like_rows, _mapping
from ingestion.claude_tool_error_taxonomy import DEFAULT_DAYS


DEFAULT_LIMIT = 50
UNKNOWN_DURATION_BUCKET = "unknown"
_DURATION_BUCKETS = (
    ("lt_1s", 0, 1_000),
    ("1s_to_5s", 1_000, 5_000),
    ("5s_to_30s", 5_000, 30_000),
    ("30s_to_2m", 30_000, 120_000),
    ("gte_2m", 120_000, None),
)
_DURATION_BUCKET_ORDER = {
    name: index for index, (name, _minimum, _maximum) in enumerate(_DURATION_BUCKETS)
}
_DURATION_BUCKET_ORDER[UNKNOWN_DURATION_BUCKET] = len(_DURATION_BUCKETS)
_DURATION_COLUMNS = ("duration_ms", "elapsed_ms", "runtime_ms")
_START_COLUMNS = (
    "started_at",
    "startedAt",
    "start_time",
    "startTime",
    "started",
    "start",
)
_COMPLETED_COLUMNS = (
    "completed_at",
    "completedAt",
    "completed_time",
    "completedTime",
    "finished_at",
    "finishedAt",
    "ended_at",
    "endedAt",
    "end_time",
    "endTime",
    "completed",
    "finished",
    "ended",
    "end",
)


@dataclass(frozen=True)
class ClaudeSessionCommandDurationBucketRow:
    """One daily command duration bucket."""

    day: str
    duration_bucket: str
    command_event_count: int
    session_count: int
    min_duration_ms: int | None
    max_duration_ms: int | None
    first_seen_at: str | None
    last_seen_at: str | None
    representative_session_ids: tuple[str, ...]
    representative_commands: tuple[str, ...]
    report_id: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["representative_commands"] = list(self.representative_commands)
        payload["representative_session_ids"] = list(self.representative_session_ids)
        return payload


@dataclass(frozen=True)
class ClaudeSessionCommandDurationBucketReport:
    """Claude session command duration bucket report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionCommandDurationBucketRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_command_duration_buckets",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _CommandDurationEvent:
    session_id: str
    timestamp: str | None
    command: str
    duration_ms: int | None
    ordinal: int


def build_claude_session_command_duration_buckets_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    include_unknown_duration: bool = False,
    now: datetime | None = None,
) -> ClaudeSessionCommandDurationBucketReport:
    """Build a deterministic report of shell command duration buckets."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    if _looks_like_rows(db_or_rows):
        raw_rows = _filter_rows([_mapping(row) for row in db_or_rows], cutoff=cutoff)
        source_tables = tuple(
            sorted({str(row.get("_source_table") or "rows") for row in raw_rows})
        ) or ("rows",)
        missing_tables: tuple[str, ...] = ()
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        source_tables = tuple(table for table in SOURCE_TABLE_CANDIDATES if table in schema)
        missing_tables = () if source_tables else SOURCE_TABLE_CANDIDATES
        raw_rows = [
            row
            for table in source_tables
            for row in _load_rows(conn, table, schema[table], cutoff=cutoff)
        ]

    events, malformed_metadata_count = load_command_duration_events(raw_rows)
    all_command_event_count = len(events)
    known_duration_events = [event for event in events if event.duration_ms is not None]
    if not include_unknown_duration:
        events = known_duration_events

    rows = tuple(_group_command_duration_events(events)[:limit])
    return ClaudeSessionCommandDurationBucketReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "include_unknown_duration": include_unknown_duration,
            "limit": limit,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
        },
        totals={
            "bucket_count": len(rows),
            "command_event_count": len(events),
            "known_duration_event_count": len(known_duration_events),
            "malformed_metadata_count": malformed_metadata_count,
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
            "unknown_duration_event_count": all_command_event_count - len(known_duration_events),
        },
        rows=rows,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_command_duration_events(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[_CommandDurationEvent], int]:
    """Normalize rows into command events with parsed durations."""
    events: list[_CommandDurationEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        command = _command(row, metadata)
        if command == "unknown command" and not _row_has_command(row) and not _row_has_command(metadata):
            continue
        timestamp = (
            _first_text(row, TIMESTAMP_COLUMNS)
            or _first_text(metadata, TIMESTAMP_COLUMNS)
            or _first_text(row, _START_COLUMNS)
            or _first_text(metadata, _START_COLUMNS)
            or _first_text(row, _COMPLETED_COLUMNS)
            or _first_text(metadata, _COMPLETED_COLUMNS)
        )
        events.append(
            _CommandDurationEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                timestamp=timestamp,
                command=command,
                duration_ms=_duration_ms(row, metadata),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def format_claude_session_command_duration_buckets_json(
    report: ClaudeSessionCommandDurationBucketReport,
) -> str:
    """Serialize a command duration bucket report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _group_command_duration_events(
    events: Iterable[_CommandDurationEvent],
) -> list[ClaudeSessionCommandDurationBucketRow]:
    grouped: dict[tuple[str, str], list[_CommandDurationEvent]] = {}
    for event in events:
        grouped.setdefault((_event_day(event), _duration_bucket(event.duration_ms)), []).append(event)
    rows = [
        _duration_bucket_row(day, duration_bucket, tuple(sorted(group, key=_event_sort_key)))
        for (day, duration_bucket), group in grouped.items()
    ]
    return sorted(rows, key=_row_sort_key)


def _duration_bucket_row(
    day: str,
    duration_bucket: str,
    events: tuple[_CommandDurationEvent, ...],
) -> ClaudeSessionCommandDurationBucketRow:
    known_durations = sorted(
        event.duration_ms for event in events if event.duration_ms is not None
    )
    digest = hashlib.sha256(f"{day}:{duration_bucket}".encode("utf-8")).hexdigest()[:12]
    return ClaudeSessionCommandDurationBucketRow(
        day=day,
        duration_bucket=duration_bucket,
        command_event_count=len(events),
        session_count=len({event.session_id for event in events}),
        min_duration_ms=known_durations[0] if known_durations else None,
        max_duration_ms=known_durations[-1] if known_durations else None,
        first_seen_at=events[0].timestamp,
        last_seen_at=events[-1].timestamp,
        representative_session_ids=tuple(sorted({event.session_id for event in events})[:3]),
        representative_commands=tuple(dict.fromkeys(event.command for event in events))[:5],
        report_id=f"claude_session_command_duration_buckets_{digest}",
    )


def _duration_ms(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> int | None:
    for source in (row, metadata):
        for column in _DURATION_COLUMNS:
            parsed = _nonnegative_int(source.get(column))
            if parsed is not None:
                return parsed
    for path in (
        ("result", "duration_ms"),
        ("result", "elapsed_ms"),
        ("result", "runtime_ms"),
        ("tool_result", "duration_ms"),
        ("tool_result", "elapsed_ms"),
        ("tool_result", "runtime_ms"),
        ("command", "duration_ms"),
        ("command", "elapsed_ms"),
        ("command", "runtime_ms"),
    ):
        parsed = _nonnegative_int(_nested_text(metadata, path))
        if parsed is not None:
            return parsed
    return _duration_from_timestamps(row, metadata)


def _duration_from_timestamps(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> int | None:
    starts = [
        _first_text(row, _START_COLUMNS),
        _first_text(metadata, _START_COLUMNS),
        _nested_text(metadata, ("result", "started_at")),
        _nested_text(metadata, ("result", "startedAt")),
        _nested_text(metadata, ("tool_result", "started_at")),
        _nested_text(metadata, ("tool_result", "startedAt")),
        _nested_text(metadata, ("command", "started_at")),
        _nested_text(metadata, ("command", "startedAt")),
    ]
    completions = [
        _first_text(row, _COMPLETED_COLUMNS),
        _first_text(metadata, _COMPLETED_COLUMNS),
        _nested_text(metadata, ("result", "completed_at")),
        _nested_text(metadata, ("result", "completedAt")),
        _nested_text(metadata, ("tool_result", "completed_at")),
        _nested_text(metadata, ("tool_result", "completedAt")),
        _nested_text(metadata, ("command", "completed_at")),
        _nested_text(metadata, ("command", "completedAt")),
    ]
    for start_value in starts:
        start = _parse_datetime(start_value)
        if not start:
            continue
        for completed_value in completions:
            completed = _parse_datetime(completed_value)
            if completed:
                return max(0, int((completed - start).total_seconds() * 1000))
    return None


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(str(value).strip())
    except ValueError:
        return None
    if parsed < 0:
        return None
    return int(parsed)


def _duration_bucket(duration_ms: int | None) -> str:
    if duration_ms is None:
        return UNKNOWN_DURATION_BUCKET
    for name, minimum, maximum in _DURATION_BUCKETS:
        if duration_ms >= minimum and (maximum is None or duration_ms < maximum):
            return name
    return UNKNOWN_DURATION_BUCKET


def _row_has_command(row: Mapping[str, Any]) -> bool:
    if _command(row, row) != "unknown command":
        return True
    for path in (
        ("tool_input", "command"),
        ("input", "command"),
        ("tool", "input", "command"),
        ("tool_use", "input", "command"),
    ):
        if _nested_text(row, path):
            return True
    return False


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if (timestamp := _parse_datetime(_first_text(row, TIMESTAMP_COLUMNS))) is None
        or timestamp >= cutoff
    ]


def _load_rows(conn: Any, table: str, columns: set[str], *, cutoff: datetime) -> list[dict[str, Any]]:
    select_columns = ", ".join(_quote_identifier(column) for column in sorted(columns))
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    where_sql = ""
    params: tuple[str, ...] = ()
    if timestamp_column:
        where_sql = f" WHERE {_quote_identifier(timestamp_column)} IS NULL OR {_quote_identifier(timestamp_column)} >= ?"
        params = (cutoff.isoformat(),)
    order_sql = f"{_quote_identifier(timestamp_column) if timestamp_column else 'rowid'} ASC, rowid ASC"
    cursor = conn.execute(
        f"SELECT {select_columns} FROM {_quote_identifier(table)}{where_sql} ORDER BY {order_sql}",
        params,
    )
    column_names = [description[0] for description in cursor.description]
    return [
        {
            **(
                dict(row)
                if isinstance(row, Mapping)
                else dict(zip(column_names, row, strict=False))
            ),
            "_source_table": table,
        }
        for row in cursor.fetchall()
    ]


def _event_day(event: _CommandDurationEvent) -> str:
    timestamp = _parse_datetime(event.timestamp)
    return timestamp.date().isoformat() if timestamp else "unknown"


def _event_sort_key(event: _CommandDurationEvent) -> tuple[str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudeSessionCommandDurationBucketRow) -> tuple[str, int]:
    return (row.day, _DURATION_BUCKET_ORDER.get(row.duration_bucket, 99))


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")
