"""Audit Claude sessions for command and tool side-effect activity."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import shlex
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_LIMIT = 50

SOURCE_TABLE_CANDIDATES = (
    "claude_session_events",
    "claude_tool_events",
    "claude_events",
)
SESSION_COLUMNS = ("session_id", "sessionId")
TIMESTAMP_COLUMNS = ("timestamp", "created_at", "event_time", "event_at")
PROJECT_COLUMNS = ("project_path", "cwd", "working_directory", "project")
TOOL_COLUMNS = ("tool_name", "tool", "toolName", "name")
COMMAND_COLUMNS = ("command", "cmd", "shell_command", "input")
TEXT_COLUMNS = ("content", "text", "message", "body", "output", "result")
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload")

WRITE_LIKE_TOOLS = {"write", "edit", "multiedit", "notebookedit"}
HIGH_RISK_COMMANDS = {
    "rm",
    "mv",
    "chmod",
    "chown",
    "dd",
    "mkfs",
    "truncate",
    "git",
}
MEDIUM_RISK_COMMANDS = {
    "cp",
    "mkdir",
    "touch",
    "install",
    "npm",
    "pnpm",
    "yarn",
    "pip",
    "uv",
    "poetry",
    "cargo",
    "go",
}
REDIRECT_RE = re.compile(r"(^|[^<])>>?|\btee\s+")


@dataclass(frozen=True)
class ClaudeSessionCommandSideEffectAuditRow:
    """One command or tool event with likely workspace side effects."""

    session_id: str
    project_path: str | None
    timestamp: str | None
    tool_name: str
    command: str
    side_effect_type: str
    severity: str
    source_table: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionCommandSideEffectAuditReport:
    """Claude session command side-effect audit report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionCommandSideEffectAuditRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_command_side_effect_audit",
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


def build_claude_session_command_side_effect_audit_report(
    db_or_rows: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionCommandSideEffectAuditReport:
    """Build a deterministic audit of write-like tools and side-effect commands."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    missing_columns: dict[str, tuple[str, ...]] = {}
    if _looks_like_rows(db_or_rows):
        raw_rows = [_mapping(row) for row in db_or_rows]
        source_tables: tuple[str, ...] = tuple(
            sorted({str(row.get("_source_table") or "rows") for row in raw_rows})
        ) or ("rows",)
        missing_tables: tuple[str, ...] = ()
    else:
        conn = _connection(db_or_rows)
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
            for row in _load_rows(conn, table, schema[table])
        ]

    rows, malformed_metadata_count = _detect_side_effects(raw_rows)
    rows = sorted(rows, key=_row_sort_key)[:limit]
    return ClaudeSessionCommandSideEffectAuditReport(
        generated_at=generated_at.isoformat(),
        filters={"limit": limit},
        totals={
            "flagged_event_count": len(rows),
            "high_severity_count": sum(1 for row in rows if row.severity == "high"),
            "malformed_metadata_count": malformed_metadata_count,
            "rows_scanned": len(raw_rows),
            "session_count": len({row.session_id for row in rows}),
        },
        rows=tuple(rows),
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_session_command_side_effect_audit_json(
    report: ClaudeSessionCommandSideEffectAuditReport,
) -> str:
    """Serialize a Claude session command side-effect audit report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _detect_side_effects(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[ClaudeSessionCommandSideEffectAuditRow], int]:
    audit_rows: list[ClaudeSessionCommandSideEffectAuditRow] = []
    malformed_metadata_count = 0
    for row in rows:
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        tool_name = _tool_name(row, metadata)
        command = _command(row, metadata)
        side_effect_type, severity = _side_effect(tool_name, command)
        if not side_effect_type:
            continue
        audit_rows.append(
            ClaudeSessionCommandSideEffectAuditRow(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                project_path=_first_text(row, PROJECT_COLUMNS)
                or _first_text(metadata, PROJECT_COLUMNS),
                timestamp=_first_text(row, TIMESTAMP_COLUMNS)
                or _first_text(metadata, TIMESTAMP_COLUMNS),
                tool_name=tool_name,
                command=_summary(command or tool_name),
                side_effect_type=side_effect_type,
                severity=severity,
                source_table=str(row.get("_source_table") or "rows"),
            )
        )
    return audit_rows, malformed_metadata_count


def _side_effect(tool_name: str, command: str | None) -> tuple[str | None, str]:
    if tool_name in WRITE_LIKE_TOOLS:
        return "write_like_tool", "high"
    if not command:
        return None, "low"
    normalized = _normalize_command(command)
    first = normalized.split(" ", 1)[0] if normalized else ""
    if first in HIGH_RISK_COMMANDS:
        if first == "git" and not _git_side_effect(normalized):
            return None, "low"
        return "side_effect_command", "high"
    if REDIRECT_RE.search(command):
        return "shell_redirection", "high"
    if first in MEDIUM_RISK_COMMANDS:
        if first in {"npm", "pnpm", "yarn"} and " install" not in f" {normalized} ":
            return None, "low"
        if first == "uv" and " pip install" not in f" {normalized} ":
            return None, "low"
        return "side_effect_command", "medium"
    return None, "low"


def _git_side_effect(command: str) -> bool:
    tokens = command.split()
    return len(tokens) >= 2 and tokens[1] in {
        "add",
        "am",
        "apply",
        "checkout",
        "clean",
        "commit",
        "merge",
        "mv",
        "pull",
        "push",
        "rebase",
        "reset",
        "restore",
        "rm",
        "stash",
    }


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
                return _clean(value)
            if isinstance(value, Mapping):
                nested = _first_text(value, COMMAND_COLUMNS)
                if nested:
                    return _clean(nested)
    for path in (
        ("tool_input", "command"),
        ("input", "command"),
        ("tool", "input", "command"),
        ("tool_use", "input", "command"),
    ):
        nested = _nested_text(metadata, path)
        if nested:
            return _clean(nested)
    return None


def _tool_name(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    text = (
        _first_text(row, TOOL_COLUMNS)
        or _first_text(metadata, TOOL_COLUMNS)
        or _nested_text(metadata, ("tool", "name"))
        or _nested_text(metadata, ("tool_use", "name"))
        or "unknown"
    )
    return re.sub(r"[^a-z0-9_.-]+", "_", text.lower()).strip("_") or "unknown"


def _normalize_command(command: str) -> str:
    try:
        tokens = shlex.split(command.lower())
    except ValueError:
        tokens = command.lower().split()
    while tokens and "=" in tokens[0] and not tokens[0].startswith("-"):
        tokens.pop(0)
    if tokens and tokens[0] in {"sudo", "command", "exec"}:
        tokens = tokens[1:]
    return " ".join(tokens)


def _summary(value: str, *, limit: int = 240) -> str:
    text = _clean(value)
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def _clean(value: Any) -> str:
    return " ".join(str(value).strip().strip("`'\"").split())


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


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    select_columns = ", ".join(_quote_identifier(column) for column in sorted(columns))
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    order_sql = f"{_quote_identifier(timestamp_column) if timestamp_column else 'rowid'} ASC, rowid ASC"
    cursor = conn.execute(
        f"SELECT {select_columns} FROM {_quote_identifier(table)} ORDER BY {order_sql}"
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


def _missing_optional_columns(columns: set[str]) -> tuple[str, ...]:
    groups = {
        "session": SESSION_COLUMNS,
        "timestamp": TIMESTAMP_COLUMNS,
        "tool": TOOL_COLUMNS,
        "command": COMMAND_COLUMNS,
        "metadata": METADATA_COLUMNS,
    }
    return tuple(
        name
        for name, candidates in sorted(groups.items())
        if not _first_existing(columns, candidates)
    )


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


def _row_sort_key(row: ClaudeSessionCommandSideEffectAuditRow) -> tuple[str, str, str]:
    return (_timestamp_sort(row.timestamp), row.session_id, row.command)


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


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _looks_like_rows(value: Any) -> bool:
    return not hasattr(value, "execute") and not hasattr(value, "conn") and not isinstance(
        value,
        (str, bytes),
    )
