"""Report Claude sessions where tool argument shapes drift over time."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Iterable, Mapping

from ingestion.claude_session_approval_decision_audit import (
    METADATA_COLUMNS,
    SESSION_COLUMNS,
    SOURCE_TABLE_CANDIDATES,
    TIMESTAMP_COLUMNS,
    TOOL_COLUMNS,
    _connection,
    _ensure_utc,
    _first_existing,
    _first_text,
    _metadata,
    _nested_text,
    _parse_datetime,
    _quote_identifier,
    _schema,
    _tool_name,
)
from ingestion.claude_tool_error_taxonomy import DEFAULT_DAYS, normalize_tool_filter


DEFAULT_LIMIT = 50

ARGUMENT_COLUMNS = ("arguments", "args", "input", "tool_input", "parameters", "params")


@dataclass(frozen=True)
class ClaudeSessionToolArgumentDriftRow:
    """One session/tool pair with changing argument key sets."""

    session_id: str
    tool_name: str
    call_count: int
    distinct_argument_key_sets: tuple[tuple[str, ...], ...]
    first_seen_at: str | None
    last_seen_at: str | None
    drift_id: str
    representative_argument_keys: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["distinct_argument_key_sets"] = [
            list(key_set) for key_set in self.distinct_argument_key_sets
        ]
        payload["representative_argument_keys"] = list(self.representative_argument_keys)
        return payload


@dataclass(frozen=True)
class ClaudeSessionToolArgumentDriftReport:
    """Claude session tool argument drift report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionToolArgumentDriftRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_tool_argument_drift",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _ToolArgumentEvent:
    session_id: str
    tool_name: str
    timestamp: str | None
    argument_keys: tuple[str, ...]
    ordinal: int


def build_claude_session_tool_argument_drift_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    tool: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionToolArgumentDriftReport:
    """Build a deterministic report of changing tool argument shapes."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    normalized_tool = normalize_tool_filter(tool)

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

    events, malformed_metadata_count = load_tool_argument_events(raw_rows, tool=normalized_tool)
    drift_rows = detect_tool_argument_drift(events)
    reported = tuple(drift_rows[:limit])
    return ClaudeSessionToolArgumentDriftReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "limit": limit,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "tool": normalized_tool,
        },
        totals={
            "drift_count": len(drift_rows),
            "malformed_metadata_count": malformed_metadata_count,
            "reported_count": len(reported),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
            "tool_call_count": len(events),
        },
        rows=reported,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_tool_argument_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    tool: str | None = None,
) -> tuple[list[_ToolArgumentEvent], int]:
    """Normalize parsed Claude tool rows into argument-shape events."""
    events: list[_ToolArgumentEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        tool_name = _tool_name(row, metadata)
        if tool and tool_name != tool:
            continue
        if tool_name == "unknown" and not _argument_mapping(row, metadata):
            continue
        events.append(
            _ToolArgumentEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                tool_name=tool_name,
                timestamp=_first_text(row, TIMESTAMP_COLUMNS)
                or _first_text(metadata, TIMESTAMP_COLUMNS),
                argument_keys=_argument_keys(row, metadata),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def detect_tool_argument_drift(
    events: Iterable[_ToolArgumentEvent],
) -> list[ClaudeSessionToolArgumentDriftRow]:
    """Group events and return session/tool pairs with more than one key set."""
    grouped: dict[tuple[str, str], list[_ToolArgumentEvent]] = {}
    for event in events:
        grouped.setdefault((event.session_id, event.tool_name), []).append(event)

    rows: list[ClaudeSessionToolArgumentDriftRow] = []
    for (session_id, tool_name), group in sorted(grouped.items()):
        ordered = tuple(sorted(group, key=_event_sort_key))
        key_sets = tuple(sorted({event.argument_keys for event in ordered}))
        if len(key_sets) <= 1:
            continue
        rows.append(_drift_row(session_id, tool_name, ordered, key_sets))
    return sorted(rows, key=_row_sort_key)


def format_claude_session_tool_argument_drift_json(
    report: ClaudeSessionToolArgumentDriftReport,
) -> str:
    """Serialize a tool argument drift report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _drift_row(
    session_id: str,
    tool_name: str,
    events: tuple[_ToolArgumentEvent, ...],
    key_sets: tuple[tuple[str, ...], ...],
) -> ClaudeSessionToolArgumentDriftRow:
    digest_source = json.dumps(
        {
            "key_sets": [list(keys) for keys in key_sets],
            "session_id": session_id,
            "tool_name": tool_name,
        },
        sort_keys=True,
    )
    digest = hashlib.sha256(digest_source.encode("utf-8")).hexdigest()[:12]
    representative_keys = tuple(sorted({key for key_set in key_sets for key in key_set}))
    return ClaudeSessionToolArgumentDriftRow(
        session_id=session_id,
        tool_name=tool_name,
        call_count=len(events),
        distinct_argument_key_sets=key_sets,
        first_seen_at=events[0].timestamp,
        last_seen_at=events[-1].timestamp,
        drift_id=f"claude_session_tool_argument_drift_{digest}",
        representative_argument_keys=representative_keys,
    )


def _argument_keys(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> tuple[str, ...]:
    argument_mapping = _argument_mapping(row, metadata)
    return tuple(sorted(_normalize_key(key) for key in argument_mapping if _normalize_key(key)))


def _argument_mapping(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    for source in (row, metadata):
        for column in ARGUMENT_COLUMNS:
            value = source.get(column)
            if isinstance(value, Mapping):
                return value
            if isinstance(value, str) and value.strip() and column in {"arguments", "args", "input"}:
                parsed = _json_object(value)
                if parsed:
                    return parsed
    for path in (
        ("tool_use", "input"),
        ("tool", "input"),
        ("tool", "arguments"),
        ("request", "input"),
        ("message", "tool_use", "input"),
    ):
        current: Any = metadata
        for key in path:
            if not isinstance(current, Mapping):
                current = None
                break
            current = current.get(key)
        if isinstance(current, Mapping):
            return current
    command = _first_text(row, ("command", "cmd", "shell_command")) or _nested_text(
        metadata,
        ("tool_use", "input", "command"),
    )
    if command:
        return {"command": command}
    return {}


def _json_object(value: str) -> Mapping[str, Any] | None:
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, Mapping) else None


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    filtered = []
    for row in rows:
        timestamp = _parse_datetime(_first_text(row, TIMESTAMP_COLUMNS))
        if timestamp is None or timestamp >= cutoff:
            filtered.append(row)
    return filtered


def _load_rows(
    conn: Any,
    table: str,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
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


def _event_sort_key(event: _ToolArgumentEvent) -> tuple[str, str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.tool_name, event.ordinal)


def _row_sort_key(row: ClaudeSessionToolArgumentDriftRow) -> tuple[str, str, str]:
    return (_timestamp_sort(row.first_seen_at), row.session_id, row.tool_name)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _normalize_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9_.-]+", "_", str(value).lower()).strip("_")


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _looks_like_rows(value: Any) -> bool:
    return not hasattr(value, "execute") and not hasattr(value, "conn") and not isinstance(
        value,
        (str, bytes),
    )
