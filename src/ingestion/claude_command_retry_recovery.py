"""Report failed Claude Code commands that were followed by recovery commands."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping

from ingestion.claude_command_failure_summary import normalize_command_prefix


DEFAULT_DAYS = 14
DEFAULT_WINDOW_MINUTES = 30

SOURCE_TABLE_CANDIDATES = (
    "claude_session_events",
    "claude_tool_events",
    "claude_events",
)
SESSION_COLUMNS = ("session_id", "sessionId")
TIMESTAMP_COLUMNS = ("timestamp", "created_at", "event_time", "event_at")
PROJECT_COLUMNS = ("project_path", "cwd", "working_directory")
TOOL_COLUMNS = ("tool_name", "tool", "toolName", "name")
STATUS_COLUMNS = ("status", "outcome")
COMMAND_COLUMNS = ("command", "cmd", "shell_command", "input")
ERROR_COLUMNS = ("error", "error_message", "stderr", "output", "result", "content", "message")
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload")

FAILURE_RE = re.compile(
    r"\b(error|failed|failure|exception|traceback|timeout|timed out|denied|"
    r"non[- ]?zero|exit code [1-9]\d*)\b",
    re.IGNORECASE,
)
SUCCESS_RE = re.compile(r"\b(success|succeeded|passed|completed|ok|exit code 0)\b", re.IGNORECASE)


@dataclass(frozen=True)
class ClaudeCommandEvent:
    """One normalized Claude command/tool event."""

    session_id: str
    project_path: str | None
    timestamp: str | None
    command: str
    command_prefix: str
    status: str
    source_table: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeCommandRetryRecoveryRow:
    """One failed command and its recovery status."""

    session_id: str
    project_path: str | None
    failed_at: str | None
    failed_command: str
    failed_command_prefix: str
    recovered_at: str | None
    recovered_command: str | None
    recovered_command_prefix: str | None
    elapsed_seconds: int | None
    recovery_category: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeCommandRetryRecoveryReport:
    """Claude command retry recovery report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeCommandRetryRecoveryRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_command_retry_recovery",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_command_retry_recovery_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    window_minutes: int = DEFAULT_WINDOW_MINUTES,
    now: datetime | None = None,
) -> ClaudeCommandRetryRecoveryReport:
    """Build a deterministic report of failure-to-success command recoveries."""
    if days <= 0:
        raise ValueError("days must be positive")
    if window_minutes <= 0:
        raise ValueError("window_minutes must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    source_tables: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()
    rows_scanned = 0

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
    rows_scanned = len(raw_rows)
    events, malformed_metadata_count = load_claude_command_events(raw_rows)
    grouped = group_command_events_by_session(events)
    recovery_rows = detect_command_retry_recoveries(
        grouped,
        window=timedelta(minutes=window_minutes),
    )

    return ClaudeCommandRetryRecoveryReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "window_minutes": window_minutes,
        },
        totals={
            "failure_count": sum(1 for event in events if event.status == "failed"),
            "malformed_metadata_count": malformed_metadata_count,
            "recovered_count": sum(1 for row in recovery_rows if row.recovered_command),
            "rows_scanned": rows_scanned,
            "unrecovered_count": sum(1 for row in recovery_rows if not row.recovered_command),
        },
        rows=tuple(recovery_rows),
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_claude_command_events(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[ClaudeCommandEvent], int]:
    """Normalize raw Claude event rows into command events."""
    events: list[ClaudeCommandEvent] = []
    malformed_metadata_count = 0
    for row in rows:
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        command = _command(row, metadata)
        if not command:
            tool_name = _tool_name(row, metadata)
            if tool_name == "unknown":
                continue
            command = tool_name
        status = _event_status(row, metadata)
        if status not in {"failed", "succeeded"}:
            continue
        session_id = (
            _first_text(row, SESSION_COLUMNS)
            or _first_text(metadata, SESSION_COLUMNS)
            or "unknown-session"
        )
        project_path = _first_text(row, PROJECT_COLUMNS) or _first_text(metadata, PROJECT_COLUMNS)
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        events.append(
            ClaudeCommandEvent(
                session_id=session_id,
                project_path=project_path,
                timestamp=timestamp,
                command=_clean_command(command),
                command_prefix=normalize_command_prefix(command),
                status=status,
                source_table=str(row.get("_source_table") or "unknown"),
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def group_command_events_by_session(
    events: Iterable[ClaudeCommandEvent],
) -> dict[str, tuple[ClaudeCommandEvent, ...]]:
    """Group command events by Claude session with stable ordering."""
    grouped: dict[str, list[ClaudeCommandEvent]] = {}
    for event in events:
        grouped.setdefault(event.session_id, []).append(event)
    return {
        session_id: tuple(sorted(session_events, key=_event_sort_key))
        for session_id, session_events in sorted(grouped.items())
    }


def detect_command_retry_recoveries(
    sessions: Mapping[str, Iterable[ClaudeCommandEvent]],
    *,
    window: timedelta,
) -> list[ClaudeCommandRetryRecoveryRow]:
    """Detect failed commands followed by a successful command in the same session."""
    rows: list[ClaudeCommandRetryRecoveryRow] = []
    for session_id, session_events in sorted(sessions.items()):
        ordered = sorted(session_events, key=_event_sort_key)
        for index, failed in enumerate(ordered):
            if failed.status != "failed":
                continue
            recovered = _first_recovery(failed, ordered[index + 1 :], window)
            rows.append(_recovery_row(session_id, failed, recovered))
    return sorted(rows, key=_row_sort_key)


def format_claude_command_retry_recovery_json(
    report: ClaudeCommandRetryRecoveryReport,
) -> str:
    """Serialize a Claude command retry recovery report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_command_retry_recovery_text(
    report: ClaudeCommandRetryRecoveryReport,
) -> str:
    """Render a concise command-line retry recovery report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Command Retry Recovery",
        f"Generated: {report.generated_at}",
        f"Filters: days={filters['days']} window_minutes={filters['window_minutes']}",
        (
            "Totals: "
            f"rows={totals['rows_scanned']} failures={totals['failure_count']} "
            f"recovered={totals['recovered_count']} unrecovered={totals['unrecovered_count']} "
            f"malformed_metadata={totals['malformed_metadata_count']}"
        ),
    ]
    if report.source_tables:
        lines.append("Source tables: " + ", ".join(report.source_tables))
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.rows:
        lines.extend(["", "No failed Claude commands found."])
        return "\n".join(lines)

    lines.extend(["", "Recoveries:"])
    for row in report.rows:
        elapsed = "-" if row.elapsed_seconds is None else f"{row.elapsed_seconds}s"
        lines.append(
            f"- session={row.session_id} category={row.recovery_category} "
            f"elapsed={elapsed} failed_at={row.failed_at or '-'} recovered_at={row.recovered_at or '-'}"
        )
        lines.append(f"  failed={row.failed_command}")
        lines.append(f"  recovered={row.recovered_command or '-'}")
    return "\n".join(lines)


def load_claude_command_event_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    """Load raw command-event rows from a Claude event table."""
    select_columns = ", ".join(_quote_identifier(column) for column in sorted(columns))
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    where_sql = ""
    params: list[Any] = []
    if timestamp_column:
        where_sql = f"WHERE {_quote_identifier(timestamp_column)} >= ?"
        params.append(cutoff.isoformat())
    order_sql = f"{_quote_identifier(timestamp_column) if timestamp_column else 'rowid'} ASC, rowid ASC"
    cursor = conn.execute(
        f"SELECT {select_columns} FROM {_quote_identifier(table)} {where_sql} ORDER BY {order_sql}",
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


def _first_recovery(
    failed: ClaudeCommandEvent,
    candidates: Iterable[ClaudeCommandEvent],
    window: timedelta,
) -> ClaudeCommandEvent | None:
    failed_at = _parse_datetime(failed.timestamp)
    if not failed_at:
        return None
    for candidate in candidates:
        if candidate.status != "succeeded":
            continue
        candidate_at = _parse_datetime(candidate.timestamp)
        if not candidate_at:
            continue
        elapsed = candidate_at - failed_at
        if elapsed.total_seconds() < 0:
            continue
        if elapsed <= window:
            return candidate
        break
    return None


def _recovery_row(
    session_id: str,
    failed: ClaudeCommandEvent,
    recovered: ClaudeCommandEvent | None,
) -> ClaudeCommandRetryRecoveryRow:
    elapsed_seconds = None
    if recovered:
        failed_at = _parse_datetime(failed.timestamp)
        recovered_at = _parse_datetime(recovered.timestamp)
        if failed_at and recovered_at:
            elapsed_seconds = int((recovered_at - failed_at).total_seconds())
    return ClaudeCommandRetryRecoveryRow(
        session_id=session_id,
        project_path=failed.project_path,
        failed_at=failed.timestamp,
        failed_command=failed.command,
        failed_command_prefix=failed.command_prefix,
        recovered_at=recovered.timestamp if recovered else None,
        recovered_command=recovered.command if recovered else None,
        recovered_command_prefix=recovered.command_prefix if recovered else None,
        elapsed_seconds=elapsed_seconds,
        recovery_category=_recovery_category(failed, recovered),
    )


def _recovery_category(
    failed: ClaudeCommandEvent,
    recovered: ClaudeCommandEvent | None,
) -> str:
    if not recovered:
        return "unrecovered"
    if failed.command_prefix == recovered.command_prefix:
        return "same_command_retry"
    if _command_family(failed.command_prefix) == _command_family(recovered.command_prefix):
        return "same_command_family"
    return "follow_up_success"


def _command_family(command_prefix: str) -> str:
    first = command_prefix.split(" ", 1)[0] if command_prefix else ""
    if command_prefix in {"uv run pytest", "python -m pytest"}:
        return "pytest"
    if first in {"npm", "pnpm", "yarn"}:
        return "javascript_package"
    return first


def _event_status(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    status = _first_text(row, STATUS_COLUMNS) or _first_text(metadata, STATUS_COLUMNS)
    error_text = _error_text(row, metadata)
    status_text = (status or "").lower()
    if status_text in {"error", "failed", "failure", "exception", "errored"}:
        return "failed"
    if status_text in {"success", "succeeded", "passed", "complete", "completed", "ok"}:
        return "succeeded"
    exit_code = metadata.get("exit_code") if "exit_code" in metadata else metadata.get("exitCode")
    if isinstance(exit_code, int):
        return "succeeded" if exit_code == 0 else "failed"
    if bool(metadata.get("is_error") or metadata.get("failed")):
        return "failed"
    if error_text and FAILURE_RE.search(error_text):
        return "failed"
    if (status and SUCCESS_RE.search(status)) or (error_text and SUCCESS_RE.search(error_text)):
        return "succeeded"
    return None


def _metadata(row: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    for column in METADATA_COLUMNS:
        value = row.get(column)
        if isinstance(value, Mapping):
            return dict(value), False
        if isinstance(value, str) and value.strip():
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}, True
            return (dict(parsed), False) if isinstance(parsed, Mapping) else ({}, False)
    return {}, False


def _command(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    for source in (row, metadata):
        for column in COMMAND_COLUMNS:
            value = source.get(column)
            if isinstance(value, str) and value.strip():
                return value
            if isinstance(value, Mapping):
                nested = _first_text(value, COMMAND_COLUMNS)
                if nested:
                    return nested
    for path in (
        ("tool_input", "command"),
        ("input", "command"),
        ("tool", "input", "command"),
        ("tool_use", "input", "command"),
    ):
        nested = _nested_text(metadata, path)
        if nested:
            return nested
    return None


def _error_text(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    parts: list[str] = []
    for source in (row, metadata):
        for column in ERROR_COLUMNS:
            value = source.get(column)
            if isinstance(value, (str, int, float)) and str(value).strip():
                parts.append(str(value))
            elif isinstance(value, Mapping):
                nested = _first_text(value, ERROR_COLUMNS)
                if nested:
                    parts.append(nested)
    for path in (
        ("error", "message"),
        ("result", "error"),
        ("response", "error"),
        ("tool_result", "error"),
    ):
        nested = _nested_text(metadata, path)
        if nested:
            parts.append(nested)
    return "\n".join(dict.fromkeys(parts)) or None


def _tool_name(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    text = (
        _first_text(row, TOOL_COLUMNS)
        or _first_text(metadata, TOOL_COLUMNS)
        or _nested_text(metadata, ("tool", "name"))
        or _nested_text(metadata, ("tool_use", "name"))
        or "unknown"
    )
    return re.sub(r"[^a-z0-9_.-]+", "_", text.lower()).strip("_") or "unknown"


def _clean_command(command: str) -> str:
    text = " ".join(str(command).strip().strip("`'\"").split())
    text = re.sub(r"^\$\s*", "", text)
    return text.strip(" .")


def _first_text(source: Mapping[str, Any], columns: tuple[str, ...]) -> str | None:
    for column in columns:
        value = source.get(column)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _nested_text(source: Mapping[str, Any], keys: tuple[str, ...]) -> str | None:
    current: Any = source
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    if current is None:
        return None
    text = str(current).strip()
    return text or None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
    except sqlite3.Error:
        return set()


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _first_existing(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _event_sort_key(event: ClaudeCommandEvent) -> tuple[str, str, str, str]:
    return (
        _timestamp_sort(event.timestamp),
        event.session_id,
        event.status,
        event.command_prefix,
    )


def _row_sort_key(row: ClaudeCommandRetryRecoveryRow) -> tuple[str, str, str]:
    return (
        _timestamp_sort(row.failed_at),
        row.session_id,
        row.failed_command_prefix,
    )


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_row_iterable(value: Any) -> bool:
    return not hasattr(value, "execute") and not hasattr(value, "conn") and not isinstance(value, (str, bytes))
