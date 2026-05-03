"""Report environment context captured in Claude session events."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
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
from ingestion.claude_tool_error_taxonomy import DEFAULT_DAYS


CONTEXT_FIELDS = ("cwd", "git_branch", "model", "sandbox_mode", "approval_mode")


@dataclass(frozen=True)
class ClaudeSessionEnvContextRow:
    """One Claude session with captured environment context."""

    session_id: str
    first_seen_at: str | None
    last_seen_at: str | None
    context_event_count: int
    distinct_value_counts: dict[str, int]
    cwd_changed: bool
    git_branch_changed: bool
    representative_values: dict[str, tuple[str, ...]]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["distinct_value_counts"] = dict(sorted(self.distinct_value_counts.items()))
        payload["representative_values"] = {
            key: list(values) for key, values in sorted(self.representative_values.items())
        }
        return payload


@dataclass(frozen=True)
class ClaudeSessionEnvContextReport:
    """Claude session environment context report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionEnvContextRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_env_context_report",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _EnvContextEvent:
    session_id: str
    timestamp: str | None
    values: Mapping[str, str]
    ordinal: int


def build_claude_session_env_context_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> ClaudeSessionEnvContextReport:
    """Build a deterministic report of Claude session environment context."""
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

    events, malformed_metadata_count = load_env_context_events(raw_rows)
    rows = tuple(_group_env_context_events(events))
    return ClaudeSessionEnvContextReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
        },
        totals={
            "context_event_count": len(events),
            "malformed_metadata_count": malformed_metadata_count,
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
        },
        rows=rows,
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_env_context_events(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[_EnvContextEvent], int]:
    """Normalize rows into events that contain at least one context field."""
    events: list[_EnvContextEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        values = _context_values(row, metadata)
        if not values:
            continue
        events.append(
            _EnvContextEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                timestamp=_first_text(row, TIMESTAMP_COLUMNS)
                or _first_text(metadata, TIMESTAMP_COLUMNS),
                values=values,
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def format_claude_session_env_context_json(report: ClaudeSessionEnvContextReport) -> str:
    """Serialize an environment context report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_env_context_text(report: ClaudeSessionEnvContextReport) -> str:
    """Render a concise human-readable environment context report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Session Environment Context",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} lookback_start={filters['lookback_start']} "
            f"lookback_end={filters['lookback_end']}"
        ),
        (
            "Totals: "
            f"rows={totals['rows_scanned']} context_events={totals['context_event_count']} "
            f"sessions={totals['session_count']} "
            f"malformed_metadata={totals['malformed_metadata_count']}"
        ),
    ]
    if report.source_tables:
        lines.append("Source tables: " + ", ".join(report.source_tables))
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.rows:
        lines.extend(["", "No Claude environment context events found."])
        return "\n".join(lines)

    lines.extend(["", "Sessions:"])
    for row in report.rows:
        values = "; ".join(
            f"{key}={','.join(values)}"
            for key, values in sorted(row.representative_values.items())
        ) or "-"
        lines.append(
            f"- session={row.session_id} events={row.context_event_count} "
            f"cwd_changed={row.cwd_changed} git_branch_changed={row.git_branch_changed} "
            f"first_seen={row.first_seen_at or '-'} last_seen={row.last_seen_at or '-'} "
            f"values={values}"
        )
    return "\n".join(lines)


def _group_env_context_events(
    events: Iterable[_EnvContextEvent],
) -> list[ClaudeSessionEnvContextRow]:
    grouped: dict[str, list[_EnvContextEvent]] = {}
    for event in events:
        grouped.setdefault(event.session_id, []).append(event)
    rows = []
    for session_id, group in sorted(grouped.items()):
        ordered = tuple(sorted(group, key=_event_sort_key))
        values_by_field = {
            field: tuple(sorted({event.values[field] for event in ordered if field in event.values}))
            for field in CONTEXT_FIELDS
        }
        values_by_field = {key: values for key, values in values_by_field.items() if values}
        rows.append(
            ClaudeSessionEnvContextRow(
                session_id=session_id,
                first_seen_at=ordered[0].timestamp,
                last_seen_at=ordered[-1].timestamp,
                context_event_count=len(ordered),
                distinct_value_counts={
                    key: len(values) for key, values in sorted(values_by_field.items())
                },
                cwd_changed=len(values_by_field.get("cwd", ())) > 1,
                git_branch_changed=len(values_by_field.get("git_branch", ())) > 1,
                representative_values={
                    key: values[:3] for key, values in sorted(values_by_field.items())
                },
            )
        )
    return sorted(rows, key=_row_sort_key)


def _context_values(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> dict[str, str]:
    field_paths = {
        "cwd": (("cwd",), ("project_path",), ("working_directory",), ("environment", "cwd")),
        "git_branch": (("git_branch",), ("branch",), ("environment", "git_branch"), ("git", "branch")),
        "model": (("model",), ("model_name",), ("environment", "model")),
        "sandbox_mode": (("sandbox_mode",), ("sandbox", "mode"), ("environment", "sandbox_mode")),
        "approval_mode": (("approval_mode",), ("approval", "mode"), ("environment", "approval_mode")),
    }
    values: dict[str, str] = {}
    for field, paths in field_paths.items():
        for source in (row, metadata):
            value = _first_text(source, (field,))
            if value:
                values[field] = value
                break
        if field in values:
            continue
        for path in paths:
            value = _nested_text(metadata, path)
            if value:
                values[field] = value
                break
    return values


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


def _event_sort_key(event: _EnvContextEvent) -> tuple[str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudeSessionEnvContextRow) -> tuple[str, str]:
    return (_timestamp_sort(row.first_seen_at), row.session_id)


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
