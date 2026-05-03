"""Report Claude sessions where tool result shapes drift over time."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Iterable, Mapping

from ingestion.claude_session_approval_decision_audit import (
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
    _tool_name,
)
from ingestion.claude_tool_error_taxonomy import DEFAULT_DAYS, normalize_tool_filter


DEFAULT_LIMIT = 50
RESULT_COLUMNS = ("result", "output", "tool_result", "response", "payload")


@dataclass(frozen=True)
class ClaudeSessionToolResultDriftRow:
    """One session/tool pair with changing result key sets."""

    session_id: str
    tool_name: str
    result_count: int
    distinct_result_key_sets: tuple[tuple[str, ...], ...]
    first_seen_at: str | None
    last_seen_at: str | None
    drift_id: str
    representative_result_keys: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["distinct_result_key_sets"] = [
            list(key_set) for key_set in self.distinct_result_key_sets
        ]
        payload["representative_result_keys"] = list(self.representative_result_keys)
        return payload


@dataclass(frozen=True)
class ClaudeSessionToolResultDriftReport:
    """Claude session tool result drift report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionToolResultDriftRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_tool_result_drift",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _ToolResultEvent:
    session_id: str
    tool_name: str
    timestamp: str | None
    result_keys: tuple[str, ...]
    ordinal: int


def build_claude_session_tool_result_drift_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    tool: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionToolResultDriftReport:
    """Build a deterministic report of changing tool result shapes."""
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

    events, malformed_metadata_count = load_tool_result_events(raw_rows, tool=normalized_tool)
    drift_rows = detect_tool_result_drift(events)
    reported = tuple(drift_rows[:limit])
    return ClaudeSessionToolResultDriftReport(
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
            "tool_result_count": len(events),
        },
        rows=reported,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_tool_result_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    tool: str | None = None,
) -> tuple[list[_ToolResultEvent], int]:
    """Normalize parsed Claude tool rows into result-shape events."""
    events: list[_ToolResultEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        result_mapping = _result_mapping(row, metadata)
        tool_name = _tool_name(row, metadata)
        if tool and tool_name != tool:
            continue
        if tool_name == "unknown" and not result_mapping:
            continue
        if not result_mapping:
            continue
        events.append(
            _ToolResultEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                tool_name=tool_name,
                timestamp=_first_text(row, TIMESTAMP_COLUMNS)
                or _first_text(metadata, TIMESTAMP_COLUMNS),
                result_keys=_result_keys(result_mapping),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def detect_tool_result_drift(
    events: Iterable[_ToolResultEvent],
) -> list[ClaudeSessionToolResultDriftRow]:
    """Group events and return session/tool pairs with more than one result key set."""
    grouped: dict[tuple[str, str], list[_ToolResultEvent]] = {}
    for event in events:
        grouped.setdefault((event.session_id, event.tool_name), []).append(event)

    rows: list[ClaudeSessionToolResultDriftRow] = []
    for (session_id, tool_name), group in sorted(grouped.items()):
        ordered = tuple(sorted(group, key=_event_sort_key))
        key_sets = tuple(sorted({event.result_keys for event in ordered}))
        if len(key_sets) <= 1:
            continue
        rows.append(_drift_row(session_id, tool_name, ordered, key_sets))
    return sorted(rows, key=_row_sort_key)


def format_claude_session_tool_result_drift_json(
    report: ClaudeSessionToolResultDriftReport,
) -> str:
    """Serialize a tool result drift report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _drift_row(
    session_id: str,
    tool_name: str,
    events: tuple[_ToolResultEvent, ...],
    key_sets: tuple[tuple[str, ...], ...],
) -> ClaudeSessionToolResultDriftRow:
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
    return ClaudeSessionToolResultDriftRow(
        session_id=session_id,
        tool_name=tool_name,
        result_count=len(events),
        distinct_result_key_sets=key_sets,
        first_seen_at=events[0].timestamp,
        last_seen_at=events[-1].timestamp,
        drift_id=f"claude_session_tool_result_drift_{digest}",
        representative_result_keys=representative_keys,
    )


def _result_keys(result_mapping: Mapping[str, Any]) -> tuple[str, ...]:
    return tuple(sorted(_normalize_key(key) for key in result_mapping if _normalize_key(key)))


def _result_mapping(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    for source in (row, metadata):
        for column in RESULT_COLUMNS:
            value = source.get(column)
            parsed = _mapping_value(value)
            if parsed:
                return parsed
    for path in (
        ("result",),
        ("tool_result",),
        ("output",),
        ("tool", "result"),
        ("tool", "output"),
        ("message", "tool_result"),
        ("response", "result"),
    ):
        parsed = _mapping_value(_nested_value(metadata, path))
        if parsed:
            return parsed
    text = _first_text(row, ("content", "text", "message")) or _first_text(
        metadata,
        ("content", "text", "message"),
    )
    return {"content": text} if text else {}


def _mapping_value(value: Any) -> Mapping[str, Any] | None:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str) and value.strip():
        return _json_object(value)
    return None


def _nested_value(source: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = source
    for key in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


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


def _event_sort_key(event: _ToolResultEvent) -> tuple[str, str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.tool_name, event.ordinal)


def _row_sort_key(row: ClaudeSessionToolResultDriftRow) -> tuple[str, str, str]:
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
