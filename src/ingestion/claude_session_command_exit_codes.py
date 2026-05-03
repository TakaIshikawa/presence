"""Report Claude shell command outcomes grouped by exit code."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Iterable, Mapping

from ingestion.claude_session_approval_decision_audit import (
    COMMAND_COLUMNS,
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
from ingestion.claude_tool_error_taxonomy import DEFAULT_DAYS


EXIT_CODE_RE = re.compile(r"\bexit(?:ed)?(?:\s+with)?\s+code\s+(-?\d+)\b", re.IGNORECASE)


@dataclass(frozen=True)
class ClaudeSessionCommandExitCodeRow:
    """One daily command exit-code group."""

    day: str
    exit_code: int
    failure_count: int
    session_count: int
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
class ClaudeSessionCommandExitCodeReport:
    """Claude session command exit-code report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionCommandExitCodeRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_command_exit_codes",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _CommandExitEvent:
    session_id: str
    timestamp: str | None
    command: str
    exit_code: int
    ordinal: int


def build_claude_session_command_exit_codes_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    exit_code: int | None = None,
    include_zero: bool = False,
    now: datetime | None = None,
) -> ClaudeSessionCommandExitCodeReport:
    """Build a deterministic report of shell command exit codes."""
    if days <= 0:
        raise ValueError("days must be positive")

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

    events, malformed_metadata_count = load_command_exit_events(raw_rows)
    if exit_code is not None:
        events = [event for event in events if event.exit_code == exit_code]
    if not include_zero:
        events = [event for event in events if event.exit_code != 0]
    rows = tuple(_group_command_exit_events(events))
    return ClaudeSessionCommandExitCodeReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "exit_code": exit_code,
            "include_zero": include_zero,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
        },
        totals={
            "command_event_count": len(events),
            "exit_code_group_count": len(rows),
            "malformed_metadata_count": malformed_metadata_count,
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
        },
        rows=rows,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_command_exit_events(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[_CommandExitEvent], int]:
    """Normalize rows into command events with parsed exit codes."""
    events: list[_CommandExitEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        parsed_exit_code = _exit_code(row, metadata)
        if parsed_exit_code is None:
            continue
        events.append(
            _CommandExitEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                timestamp=_first_text(row, TIMESTAMP_COLUMNS)
                or _first_text(metadata, TIMESTAMP_COLUMNS),
                command=_command(row, metadata),
                exit_code=parsed_exit_code,
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def format_claude_session_command_exit_codes_json(
    report: ClaudeSessionCommandExitCodeReport,
) -> str:
    """Serialize a command exit-code report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _group_command_exit_events(
    events: Iterable[_CommandExitEvent],
) -> list[ClaudeSessionCommandExitCodeRow]:
    grouped: dict[tuple[str, int], list[_CommandExitEvent]] = {}
    for event in events:
        grouped.setdefault((_event_day(event), event.exit_code), []).append(event)
    rows = [
        _exit_code_row(day, exit_code, tuple(sorted(group, key=_event_sort_key)))
        for (day, exit_code), group in grouped.items()
    ]
    return sorted(rows, key=_row_sort_key)


def _exit_code_row(
    day: str,
    exit_code: int,
    events: tuple[_CommandExitEvent, ...],
) -> ClaudeSessionCommandExitCodeRow:
    digest = hashlib.sha256(f"{day}:{exit_code}".encode("utf-8")).hexdigest()[:12]
    return ClaudeSessionCommandExitCodeRow(
        day=day,
        exit_code=exit_code,
        failure_count=len(events),
        session_count=len({event.session_id for event in events}),
        first_seen_at=events[0].timestamp,
        last_seen_at=events[-1].timestamp,
        representative_session_ids=tuple(sorted({event.session_id for event in events})[:3]),
        representative_commands=tuple(dict.fromkeys(event.command for event in events))[:5],
        report_id=f"claude_session_command_exit_codes_{digest}",
    )


def _exit_code(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> int | None:
    for source in (row, metadata):
        for key in ("exit_code", "exitCode", "returncode", "return_code", "status_code"):
            parsed = _int_value(source.get(key))
            if parsed is not None:
                return parsed
    for path in (
        ("result", "exit_code"),
        ("tool_result", "exit_code"),
        ("output", "exit_code"),
        ("command", "exit_code"),
    ):
        parsed = _int_value(_nested_text(metadata, path))
        if parsed is not None:
            return parsed
    text = " ".join(
        value
        for value in (
            _first_text(row, ("status", "output", "error_message", "content", "result")),
            _first_text(metadata, ("status", "output", "error_message", "content", "result")),
            _nested_text(metadata, ("tool_result", "error")),
        )
        if value
    )
    match = EXIT_CODE_RE.search(text)
    return int(match.group(1)) if match else None


def _int_value(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
        return int(value.strip())
    return None


def _command(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    for source in (row, metadata):
        for column in COMMAND_COLUMNS:
            value = source.get(column)
            if isinstance(value, str) and value.strip():
                return _summary(value)
            if isinstance(value, Mapping):
                nested = _first_text(value, COMMAND_COLUMNS)
                if nested:
                    return _summary(nested)
    for path in (
        ("tool_input", "command"),
        ("input", "command"),
        ("tool", "input", "command"),
        ("tool_use", "input", "command"),
    ):
        nested = _nested_text(metadata, path)
        if nested:
            return _summary(nested)
    return "unknown command"


def _summary(value: Any, *, limit: int = 240) -> str:
    text = " ".join(str(value).strip().strip("`'\"").split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


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


def _event_day(event: _CommandExitEvent) -> str:
    timestamp = _parse_datetime(event.timestamp)
    return timestamp.date().isoformat() if timestamp else "unknown"


def _event_sort_key(event: _CommandExitEvent) -> tuple[str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudeSessionCommandExitCodeRow) -> tuple[str, int]:
    return (row.day, row.exit_code)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _looks_like_rows(value: Any) -> bool:
    return not hasattr(value, "execute") and not hasattr(value, "conn") and not isinstance(
        value,
        (str, bytes),
    )
