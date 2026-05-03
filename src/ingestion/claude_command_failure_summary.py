"""Summarize failed Claude Code shell/tool commands by session and error."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import shlex
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 25

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
ERROR_COLUMNS = (
    "error",
    "error_message",
    "stderr",
    "output",
    "result",
    "content",
    "message",
)
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload")

FAILURE_RE = re.compile(
    r"\b(error|failed|failure|exception|traceback|timeout|timed out|denied|"
    r"non[- ]?zero|exit code [1-9]\d*)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ClaudeCommandFailureRow:
    """One grouped Claude command failure pattern."""

    session_id: str
    project_path: str | None
    command_prefix: str
    error_signature: str
    failure_count: int
    first_seen_at: str | None
    last_seen_at: str | None
    representative_command: str
    representative_error_text: str
    source_table: str
    repeated: bool
    suggested_next_action: str
    signature_id: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeCommandFailureSummaryReport:
    """Claude command failure summary report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeCommandFailureRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_command_failure_summary",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_command_failure_summary_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeCommandFailureSummaryReport:
    """Scan parsed Claude session records for failed commands."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    source_tables = tuple(table for table in SOURCE_TABLE_CANDIDATES if table in schema)
    missing_tables = () if source_tables else SOURCE_TABLE_CANDIDATES
    missing_columns = {
        table: _missing_optional_columns(schema[table])
        for table in source_tables
        if _missing_optional_columns(schema[table])
    }
    raw_rows = [
        row
        for table in source_tables
        for row in _load_rows(conn, table, schema[table], cutoff=cutoff)
    ]
    events, malformed_metadata_count = _failure_events(raw_rows)
    rows = _group_events(events)
    rows.sort(key=_row_sort_key)

    return ClaudeCommandFailureSummaryReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "failure_event_count": len(events),
            "group_count": len(rows),
            "malformed_metadata_count": malformed_metadata_count,
            "repeated_group_count": sum(1 for row in rows if row.repeated),
            "rows_scanned": len(raw_rows),
        },
        rows=tuple(rows[:limit]),
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_command_failure_summary_json(
    report: ClaudeCommandFailureSummaryReport,
) -> str:
    """Serialize a Claude command failure summary as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_command_failure_summary_text(
    report: ClaudeCommandFailureSummaryReport,
) -> str:
    """Render a concise command-line failure summary."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Command Failure Summary",
        f"Generated: {report.generated_at}",
        f"Filters: days={filters['days']} limit={filters['limit']}",
        (
            "Totals: "
            f"rows={totals['rows_scanned']} failures={totals['failure_event_count']} "
            f"groups={totals['group_count']} repeated={totals['repeated_group_count']} "
            f"malformed_metadata={totals['malformed_metadata_count']}"
        ),
    ]
    if report.source_tables:
        lines.append("Source tables: " + ", ".join(report.source_tables))
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing optional columns: " + missing)

    if not report.rows:
        lines.extend(["", "No failed Claude commands found."])
        return "\n".join(lines)

    lines.extend(["", "Failures:"])
    for row in report.rows:
        marker = "repeated" if row.repeated else "single"
        lines.append(
            f"- {marker} session={row.session_id} project={row.project_path or '-'} "
            f"count={row.failure_count} first={row.first_seen_at or '-'} "
            f"last={row.last_seen_at or '-'} action={row.suggested_next_action}"
        )
        lines.append(f"  command_prefix={row.command_prefix}")
        lines.append(f"  signature={row.error_signature}")
        lines.append(f"  error={row.representative_error_text}")
    return "\n".join(lines)


def _failure_events(rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    events: list[dict[str, Any]] = []
    malformed_metadata_count = 0
    for row in rows:
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        command = _command(row, metadata)
        error_text = _error_text(row, metadata)
        status = _first_text(row, STATUS_COLUMNS) or _first_text(metadata, STATUS_COLUMNS)
        if not _is_failed(status, error_text, metadata):
            continue
        if not command:
            tool_name = _tool_name(row, metadata)
            if tool_name == "unknown":
                continue
            command = tool_name
        session_id = (
            _first_text(row, SESSION_COLUMNS)
            or _first_text(metadata, SESSION_COLUMNS)
            or "unknown-session"
        )
        project_path = _first_text(row, PROJECT_COLUMNS) or _first_text(metadata, PROJECT_COLUMNS)
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        representative_error = _excerpt(error_text or status or "command failed")
        events.append(
            {
                "command": _clean_command(command),
                "command_prefix": normalize_command_prefix(command),
                "error_signature": normalize_error_signature(error_text or status or "command failed"),
                "project_path": project_path,
                "representative_error_text": representative_error,
                "session_id": session_id,
                "source_table": str(row.get("_source_table") or "unknown"),
                "timestamp": timestamp,
                "timestamp_sort": _timestamp_sort(timestamp),
            }
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def normalize_command_prefix(command: str, *, max_tokens: int = 3) -> str:
    """Normalize a command to its stable leading invocation tokens."""
    text = _clean_command(command).lower()
    try:
        tokens = shlex.split(text)
    except ValueError:
        tokens = text.split()
    tokens = [token for token in tokens if token]
    while tokens and ("=" in tokens[0] and not tokens[0].startswith("-")):
        tokens.pop(0)
    if tokens[:2] == ["env", "-i"]:
        tokens = tokens[2:]
    if tokens and tokens[0] in {"sudo", "command", "exec"}:
        tokens = tokens[1:]
    if tokens[:2] == ["uv", "run"] and len(tokens) >= 3:
        return " ".join(tokens[:3])
    if len(tokens) >= 3 and tokens[0] in {"python", "python3"} and tokens[1] == "-m":
        return " ".join(tokens[:3])
    if len(tokens) >= 3 and tokens[0] in {"npm", "pnpm", "yarn"} and tokens[1] == "run":
        return " ".join(tokens[:3])
    if len(tokens) >= 2 and tokens[0] in {"npm", "pnpm", "yarn"}:
        return " ".join(tokens[:2])
    return " ".join(tokens[:max_tokens]) or "unknown-command"


def normalize_error_signature(text: str) -> str:
    """Normalize volatile error details while preserving the failure mode."""
    value = " ".join(str(text).split()).lower()
    value = re.sub(r"`{1,3}", " ", value)
    value = re.sub(r"https?://\S+", "<url>", value)
    value = re.sub(r"(/[^\s:]+)+", "<path>", value)
    value = re.sub(r"\b[\w.-]+(?:/[\w.-]+)+\b", "<path>", value)
    value = re.sub(r"\b[0-9a-f]{8}-[0-9a-f-]{13,}\b", "<uuid>", value)
    value = re.sub(r"\b[a-f0-9]{7,40}\b", "<hash>", value)
    value = re.sub(r"\b\d+\b", "<num>", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" -:,.")[:240] or "command failed"


def _group_events(events: list[dict[str, Any]]) -> list[ClaudeCommandFailureRow]:
    grouped: dict[tuple[str, str | None, str, str], list[dict[str, Any]]] = {}
    for event in events:
        key = (
            event["session_id"],
            event["project_path"],
            event["command_prefix"],
            event["error_signature"],
        )
        grouped.setdefault(key, []).append(event)

    rows: list[ClaudeCommandFailureRow] = []
    for (
        session_id,
        project_path,
        command_prefix,
        error_signature,
    ), group_events in grouped.items():
        ordered = sorted(group_events, key=_event_sort_key)
        first = ordered[0]
        last = ordered[-1]
        signature_hash = hashlib.sha256(
            f"{session_id}:{project_path}:{command_prefix}:{error_signature}".encode("utf-8")
        ).hexdigest()[:12]
        rows.append(
            ClaudeCommandFailureRow(
                session_id=session_id,
                project_path=project_path,
                command_prefix=command_prefix,
                error_signature=error_signature,
                failure_count=len(ordered),
                first_seen_at=first["timestamp"],
                last_seen_at=last["timestamp"],
                representative_command=first["command"],
                representative_error_text=first["representative_error_text"],
                source_table=first["source_table"],
                repeated=len(ordered) > 1,
                suggested_next_action=_suggested_next_action(command_prefix, error_signature),
                signature_id=f"claude_command_failure_{signature_hash}",
            )
        )
    return rows


def _load_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    select_columns = ", ".join(sorted(columns))
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    where_sql = ""
    params: list[Any] = []
    if timestamp_column:
        where_sql = f"WHERE {timestamp_column} >= ?"
        params.append(cutoff.isoformat())
    order_sql = f"{timestamp_column or 'rowid'} ASC, rowid ASC"
    cursor = conn.execute(
        f"SELECT {select_columns} FROM {table} {where_sql} ORDER BY {order_sql}",
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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in rows}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _missing_optional_columns(columns: set[str]) -> tuple[str, ...]:
    expected_groups = {
        "session_id": SESSION_COLUMNS,
        "timestamp": TIMESTAMP_COLUMNS,
        "project_path": PROJECT_COLUMNS,
        "tool_name": TOOL_COLUMNS,
        "status": STATUS_COLUMNS,
        "command": COMMAND_COLUMNS,
        "error_text": ERROR_COLUMNS,
        "metadata": METADATA_COLUMNS,
    }
    return tuple(
        name
        for name, variants in expected_groups.items()
        if not any(column in columns for column in variants)
    )


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


def _is_failed(
    status: str | None,
    error_text: str | None,
    metadata: Mapping[str, Any],
) -> bool:
    status_text = (status or "").lower()
    if status_text in {"error", "failed", "failure", "exception", "errored"}:
        return True
    exit_code = metadata.get("exit_code") or metadata.get("exitCode")
    if isinstance(exit_code, int) and exit_code != 0:
        return True
    if bool(metadata.get("is_error") or metadata.get("failed")):
        return True
    return bool(error_text and FAILURE_RE.search(error_text))


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
    text = re.sub(
        r"^(?:command failed(?: with exit code \d+)?:|ran|running)\s+",
        "",
        text,
        flags=re.IGNORECASE,
    )
    return text.strip(" .")


def _suggested_next_action(command_prefix: str, error_signature: str) -> str:
    combined = f"{command_prefix} {error_signature}"
    if "permission denied" in combined or "eacces" in combined:
        return "fix_permissions"
    if "timeout" in combined or "timed out" in combined:
        return "raise_timeout_or_reduce_scope"
    if (
        "not found" in combined
        or "no such file" in combined
        or "module named" in combined
        or "cannot find module" in combined
    ):
        return "repair_missing_dependency_or_path"
    if "exit code" in combined or "non-zero" in combined:
        return "repair_command"
    if "json" in combined or "parse" in combined:
        return "inspect_tool_input"
    return "triage_command_failure"


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


def _first_existing(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _excerpt(text: str, width: int = 180) -> str:
    compact = " ".join(str(text).split())
    if len(compact) <= width:
        return compact
    return compact[: max(0, width - 3)].rstrip() + "..."


def _event_sort_key(event: Mapping[str, Any]) -> tuple[str, str, str, str]:
    return (
        _timestamp_sort(event.get("timestamp")),
        str(event.get("session_id") or ""),
        str(event.get("command_prefix") or ""),
        str(event.get("error_signature") or ""),
    )


def _row_sort_key(row: ClaudeCommandFailureRow) -> tuple[int, str, str, str, str]:
    return (
        -row.failure_count,
        _reverse_text(row.last_seen_at),
        row.session_id,
        row.command_prefix,
        row.error_signature,
    )


def _reverse_text(value: Any) -> str:
    return "".join(chr(0x10FFFF - ord(char)) for char in str(value or ""))


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _ensure_utc(value)
    try:
        return _ensure_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None
