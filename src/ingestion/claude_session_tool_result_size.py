"""Report oversized Claude tool results grouped by tool, session, and size bucket."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
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
    _parse_datetime,
    _quote_identifier,
    _schema,
    _tool_name,
)
from ingestion.claude_tool_error_taxonomy import DEFAULT_DAYS, normalize_tool_filter


DEFAULT_LIMIT = 50
DEFAULT_MIN_SIZE = 4_096
SIZE_BUCKETS: tuple[tuple[str, int | None, int | None], ...] = (
    ("empty", 0, 0),
    ("1-1kb", 1, 1_023),
    ("1kb-9kb", 1_024, 9_999),
    ("10kb-99kb", 10_000, 99_999),
    ("100kb-999kb", 100_000, 999_999),
    ("1mb+", 1_000_000, None),
)
RESULT_TEXT_COLUMNS = (
    "result",
    "tool_result",
    "output",
    "stdout",
    "stderr",
    "content",
    "text",
    "message",
    "error",
    "error_message",
)


@dataclass(frozen=True)
class ClaudeSessionToolResultSizeExample:
    """One large tool result example."""

    session_id: str
    tool_name: str
    result_size: int
    timestamp: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionToolResultSizeRow:
    """One tool/session/size-bucket group."""

    tool_name: str
    session_id: str
    size_bucket: str
    result_count: int
    total_result_size: int
    max_result_size: int
    first_seen_at: str | None
    last_seen_at: str | None
    top_examples: tuple[ClaudeSessionToolResultSizeExample, ...]
    report_id: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["top_examples"] = [example.to_dict() for example in self.top_examples]
        return payload


@dataclass(frozen=True)
class ClaudeSessionToolResultSizeReport:
    """Claude session tool result size report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionToolResultSizeRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_tool_result_size",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _ToolResultSizeEvent:
    session_id: str
    tool_name: str
    timestamp: str | None
    result_size: int
    ordinal: int


def build_claude_session_tool_result_size_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    tool: str | None = None,
    min_size: int = DEFAULT_MIN_SIZE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionToolResultSizeReport:
    """Build a deterministic report of Claude tool result sizes."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_size < 0:
        raise ValueError("min_size must be nonnegative")
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

    events, malformed_metadata_count = load_tool_result_size_events(raw_rows, tool=normalized_tool)
    oversized = [event for event in events if event.result_size >= min_size]
    rows = tuple(_group_tool_result_size_events(oversized)[:limit])
    return ClaudeSessionToolResultSizeReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "limit": limit,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "min_size": min_size,
            "tool": normalized_tool,
        },
        totals={
            "malformed_metadata_count": malformed_metadata_count,
            "oversized_result_count": len(oversized),
            "result_event_count": len(events),
            "result_group_count": len(rows),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in oversized}),
            "zero_size_result_count": sum(1 for event in events if event.result_size == 0),
        },
        rows=rows,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_tool_result_size_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    tool: str | None = None,
) -> tuple[list[_ToolResultSizeEvent], int]:
    """Normalize parsed Claude rows into tool result size events."""
    events: list[_ToolResultSizeEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        tool_name = _tool_name(row, metadata)
        if tool and tool_name != tool:
            continue
        events.append(
            _ToolResultSizeEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                tool_name=tool_name,
                timestamp=_first_text(row, TIMESTAMP_COLUMNS)
                or _first_text(metadata, TIMESTAMP_COLUMNS),
                result_size=_result_size(row, metadata),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def format_claude_session_tool_result_size_json(
    report: ClaudeSessionToolResultSizeReport,
) -> str:
    """Serialize a tool result size report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _group_tool_result_size_events(
    events: Iterable[_ToolResultSizeEvent],
) -> list[ClaudeSessionToolResultSizeRow]:
    grouped: dict[tuple[str, str, str], list[_ToolResultSizeEvent]] = {}
    for event in events:
        grouped.setdefault(
            (event.tool_name, event.session_id, _size_bucket(event.result_size)),
            [],
        ).append(event)
    rows = [
        _result_size_row(tool_name, session_id, size_bucket, tuple(group))
        for (tool_name, session_id, size_bucket), group in grouped.items()
    ]
    return sorted(rows, key=_row_sort_key)


def _result_size_row(
    tool_name: str,
    session_id: str,
    size_bucket: str,
    events: tuple[_ToolResultSizeEvent, ...],
) -> ClaudeSessionToolResultSizeRow:
    ordered = tuple(sorted(events, key=_event_sort_key))
    top = tuple(sorted(ordered, key=_top_event_sort_key)[:3])
    digest = hashlib.sha256(
        f"{tool_name}:{session_id}:{size_bucket}".encode("utf-8")
    ).hexdigest()[:12]
    return ClaudeSessionToolResultSizeRow(
        tool_name=tool_name,
        session_id=session_id,
        size_bucket=size_bucket,
        result_count=len(ordered),
        total_result_size=sum(event.result_size for event in ordered),
        max_result_size=max(event.result_size for event in ordered),
        first_seen_at=ordered[0].timestamp,
        last_seen_at=ordered[-1].timestamp,
        top_examples=tuple(
            ClaudeSessionToolResultSizeExample(
                session_id=event.session_id,
                tool_name=event.tool_name,
                result_size=event.result_size,
                timestamp=event.timestamp,
            )
            for event in top
        ),
        report_id=f"claude_session_tool_result_size_{digest}",
    )


def _result_size(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> int:
    value = _result_payload(row, metadata)
    if value is None:
        return 0
    if isinstance(value, bytes):
        return len(value)
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    return len(json.dumps(value, sort_keys=True, default=str).encode("utf-8"))


def _result_payload(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> Any:
    for source in (row, metadata):
        for column in RESULT_TEXT_COLUMNS:
            value = source.get(column)
            if value is not None:
                return _text_from_payload(value)
    for path in (
        ("tool_result", "content"),
        ("tool_result", "result"),
        ("tool_result", "output"),
        ("result", "content"),
        ("message", "content"),
        ("response", "content"),
    ):
        value = _nested_value(metadata, path)
        if value is not None:
            return _text_from_payload(value)
    return None


def _text_from_payload(value: Any) -> Any:
    if isinstance(value, Mapping):
        for key in RESULT_TEXT_COLUMNS:
            nested = value.get(key)
            if nested is not None:
                return _text_from_payload(nested)
        if value.get("type") == "text" and value.get("text") is not None:
            return value["text"]
    if isinstance(value, list):
        parts = [_text_from_payload(item) for item in value]
        text_parts = [str(part) for part in parts if part is not None]
        return "\n".join(text_parts) if text_parts else None
    return value


def _nested_value(source: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    current: Any = source
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _size_bucket(result_size: int) -> str:
    for label, lower, upper in SIZE_BUCKETS:
        if (lower is None or result_size >= lower) and (upper is None or result_size <= upper):
            return label
    return "unknown"


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


def _event_sort_key(event: _ToolResultSizeEvent) -> tuple[str, str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.tool_name, event.ordinal)


def _top_event_sort_key(event: _ToolResultSizeEvent) -> tuple[int, str, int]:
    return (-event.result_size, _timestamp_sort(event.timestamp), event.ordinal)


def _row_sort_key(row: ClaudeSessionToolResultSizeRow) -> tuple[int, str, str, str]:
    return (-row.max_result_size, row.tool_name, row.session_id, row.size_bucket)


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
