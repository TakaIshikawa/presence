"""Report unusually long-running Claude shell commands."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
from typing import Any, Iterable, Mapping

from ingestion.claude_session_approval_decision_audit import (
    COMMAND_LIKE_TOOLS,
    SESSION_COLUMNS,
    SOURCE_TABLE_CANDIDATES,
    TIMESTAMP_COLUMNS,
    _command,
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
from ingestion.claude_tool_error_taxonomy import DEFAULT_DAYS


DEFAULT_LIMIT = 50
DEFAULT_MIN_DURATION_MS = 1_000
DURATION_BUCKETS: tuple[tuple[str, int | None, int | None], ...] = (
    ("1s-9s", 1_000, 9_999),
    ("10s-59s", 10_000, 59_999),
    ("1m-4m", 60_000, 299_999),
    ("5m-14m", 300_000, 899_999),
    ("15m+", 900_000, None),
)


@dataclass(frozen=True)
class ClaudeSessionCommandDurationExample:
    """One long-running command example."""

    session_id: str
    command_prefix: str
    duration_ms: float
    timestamp: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionCommandDurationOutlierRow:
    """One session/command-prefix/duration-bucket group."""

    session_id: str
    command_prefix: str
    duration_bucket: str
    command_count: int
    max_duration_ms: float
    first_seen_at: str | None
    last_seen_at: str | None
    top_examples: tuple[ClaudeSessionCommandDurationExample, ...]
    report_id: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["top_examples"] = [example.to_dict() for example in self.top_examples]
        return payload


@dataclass(frozen=True)
class ClaudeSessionCommandDurationOutlierReport:
    """Claude session command duration outlier report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionCommandDurationOutlierRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_command_duration_outliers",
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
    command_prefix: str
    duration_ms: float
    ordinal: int


def build_claude_session_command_duration_outliers_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_duration_ms: int = DEFAULT_MIN_DURATION_MS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionCommandDurationOutlierReport:
    """Build a deterministic report of long-running shell command events."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_duration_ms < 0:
        raise ValueError("min_duration_ms must be nonnegative")
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

    events, malformed_metadata_count, skipped_missing_duration_count = load_command_duration_events(raw_rows)
    outliers = [event for event in events if event.duration_ms >= min_duration_ms]
    rows = tuple(_group_command_duration_events(outliers)[:limit])
    return ClaudeSessionCommandDurationOutlierReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "limit": limit,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "min_duration_ms": min_duration_ms,
        },
        totals={
            "command_event_count": len(events),
            "duration_group_count": len(rows),
            "malformed_metadata_count": malformed_metadata_count,
            "outlier_event_count": len(outliers),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in outliers}),
            "skipped_missing_duration_count": skipped_missing_duration_count,
        },
        rows=rows,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_command_duration_events(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[_CommandDurationEvent], int, int]:
    """Normalize parsed Claude rows into shell command duration events."""
    events: list[_CommandDurationEvent] = []
    malformed_metadata_count = 0
    skipped_missing_duration_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        tool_name = _tool_name(row, metadata)
        command = _command(row, metadata)
        if not command or (tool_name not in COMMAND_LIKE_TOOLS and tool_name != "unknown"):
            continue
        duration_ms = _duration_ms(row, metadata)
        if duration_ms is None:
            skipped_missing_duration_count += 1
            continue
        events.append(
            _CommandDurationEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                timestamp=_first_text(row, TIMESTAMP_COLUMNS)
                or _first_text(metadata, TIMESTAMP_COLUMNS),
                command=command,
                command_prefix=_command_prefix(command),
                duration_ms=duration_ms,
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count, skipped_missing_duration_count


def format_claude_session_command_duration_outliers_json(
    report: ClaudeSessionCommandDurationOutlierReport,
) -> str:
    """Serialize a command duration outlier report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_command_duration_outliers_text(
    report: ClaudeSessionCommandDurationOutlierReport,
) -> str:
    """Format a compact human-readable command duration outlier table."""
    lines = ["session_id | command_prefix | bucket | count | max_ms"]
    lines.extend(
        f"{row.session_id} | {row.command_prefix} | {row.duration_bucket} | "
        f"{row.command_count} | {row.max_duration_ms:g}"
        for row in report.rows
    )
    return "\n".join(lines)


def _group_command_duration_events(
    events: Iterable[_CommandDurationEvent],
) -> list[ClaudeSessionCommandDurationOutlierRow]:
    grouped: dict[tuple[str, str, str], list[_CommandDurationEvent]] = {}
    for event in events:
        grouped.setdefault(
            (event.session_id, event.command_prefix, _duration_bucket(event.duration_ms)),
            [],
        ).append(event)
    rows = [
        _duration_row(session_id, command_prefix, duration_bucket, tuple(group))
        for (session_id, command_prefix, duration_bucket), group in grouped.items()
    ]
    return sorted(rows, key=_row_sort_key)


def _duration_row(
    session_id: str,
    command_prefix: str,
    duration_bucket: str,
    events: tuple[_CommandDurationEvent, ...],
) -> ClaudeSessionCommandDurationOutlierRow:
    ordered = tuple(sorted(events, key=_event_sort_key))
    top = tuple(sorted(ordered, key=_top_event_sort_key)[:3])
    digest = hashlib.sha256(
        f"{session_id}:{command_prefix}:{duration_bucket}".encode("utf-8")
    ).hexdigest()[:12]
    return ClaudeSessionCommandDurationOutlierRow(
        session_id=session_id,
        command_prefix=command_prefix,
        duration_bucket=duration_bucket,
        command_count=len(ordered),
        max_duration_ms=max(event.duration_ms for event in ordered),
        first_seen_at=ordered[0].timestamp,
        last_seen_at=ordered[-1].timestamp,
        top_examples=tuple(
            ClaudeSessionCommandDurationExample(
                session_id=event.session_id,
                command_prefix=event.command_prefix,
                duration_ms=event.duration_ms,
                timestamp=event.timestamp,
            )
            for event in top
        ),
        report_id=f"claude_session_command_duration_outliers_{digest}",
    )


def _duration_ms(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> float | None:
    for source in (row, metadata):
        for key in ("duration_ms", "durationMillis", "elapsed_ms", "elapsedMillis", "runtime_ms"):
            parsed = _float_value(source.get(key))
            if parsed is not None:
                return parsed
        for key in ("duration_seconds", "duration_secs", "elapsed_seconds", "elapsed"):
            parsed = _float_value(source.get(key))
            if parsed is not None:
                return parsed * 1_000
    for path in (
        ("result", "duration_ms"),
        ("tool_result", "duration_ms"),
        ("command", "duration_ms"),
        ("metrics", "duration_ms"),
    ):
        parsed = _float_value(_nested_text(metadata, path))
        if parsed is not None:
            return parsed
    return None


def _duration_bucket(duration_ms: float) -> str:
    for label, lower, upper in DURATION_BUCKETS:
        if (lower is None or duration_ms >= lower) and (upper is None or duration_ms <= upper):
            return label
    return "<1s"


def _command_prefix(command: str) -> str:
    return " ".join(command.split()[:3]) or "unknown-command"


def _float_value(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


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


def _event_sort_key(event: _CommandDurationEvent) -> tuple[str, str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.command_prefix, event.ordinal)


def _top_event_sort_key(event: _CommandDurationEvent) -> tuple[float, str, int]:
    return (-event.duration_ms, _timestamp_sort(event.timestamp), event.ordinal)


def _row_sort_key(row: ClaudeSessionCommandDurationOutlierRow) -> tuple[float, str, str, str]:
    return (-row.max_duration_ms, row.session_id, row.command_prefix, row.duration_bucket)


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
