"""Report Claude Code session todo completion outcomes."""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 25
TODO_STATUSES = ("pending", "in_progress", "completed", "canceled")

EVENT_TABLE_CANDIDATES = (
    "claude_session_events",
    "claude_tool_events",
    "claude_events",
)
SESSION_COLUMNS = ("session_id", "sessionId")
TIMESTAMP_COLUMNS = ("timestamp", "created_at", "event_time", "event_at")
PROJECT_COLUMNS = ("project_path", "cwd", "working_directory")
TEXT_COLUMNS = (
    "prompt_text",
    "response_text",
    "content",
    "message",
    "text",
    "output",
    "result",
)
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload", "input")

_STATUS_ALIASES = {
    "active": "in_progress",
    "abandoned": "canceled",
    "cancelled": "canceled",
    "complete": "completed",
    "completed": "completed",
    "canceled": "canceled",
    "done": "completed",
    "doing": "in_progress",
    "dropped": "canceled",
    "finished": "completed",
    "fixed": "completed",
    "in progress": "in_progress",
    "in-progress": "in_progress",
    "in_progress": "in_progress",
    "not started": "pending",
    "not_started": "pending",
    "open": "pending",
    "pending": "pending",
    "resolved": "completed",
    "skipped": "canceled",
    "started": "in_progress",
    "todo": "pending",
}
_CHECKBOX_RE = re.compile(r"(?m)^\s*[-*]\s+\[(?P<mark>[ xX~-])\]\s+(?P<text>.+?)\s*$")
_STATUS_LINE_RE = re.compile(
    r"(?im)^\s*[-*]?\s*(?:todo|task|action item)?\s*"
    r"(?P<status>pending|in[_ -]?progress|completed?|done|finished|cancell?ed|skipped)"
    r"\s*[:\-]\s*(?P<text>.+?)\s*$"
)


@dataclass(frozen=True)
class ClaudeSessionTodoCompletion:
    """Todo completion counts for one Claude session."""

    session_id: str
    project_path: str | None
    message_count: int
    event_count: int
    first_seen_at: str | None
    last_seen_at: str | None
    status_counts: dict[str, int]
    todo_count: int
    completed_count: int
    incomplete_count: int
    completion_rate: float | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionTodoCompletionReport:
    """Claude session todo completion report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    sessions: tuple[ClaudeSessionTodoCompletion, ...]
    source_tables: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_todo_completion",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "sessions": [session.to_dict() for session in self.sessions],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
            "warnings": list(self.warnings),
        }


def build_claude_session_todo_completion_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionTodoCompletionReport:
    """Scan Claude session records for todo markers and completion states."""
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
    warnings: list[str] = []
    missing_tables: list[str] = []
    missing_columns: dict[str, tuple[str, ...]] = {}

    if "claude_messages" not in schema:
        missing_tables.append("claude_messages")
        return ClaudeSessionTodoCompletionReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_totals(()),
            sessions=(),
            missing_tables=tuple(missing_tables),
            missing_columns=missing_columns,
        )

    message_columns = schema["claude_messages"]
    missing_message_columns = tuple(
        column for column in ("session_id", "timestamp") if column not in message_columns
    )
    optional_message_columns = tuple(
        column
        for column in ("id", "project_path", "prompt_text", "response_text")
        if column not in message_columns
    )
    if missing_message_columns or optional_message_columns:
        missing_columns["claude_messages"] = missing_message_columns + optional_message_columns

    records = _load_message_records(conn, message_columns, cutoff=cutoff)
    source_tables = tuple(table for table in EVENT_TABLE_CANDIDATES if table in schema)
    if not source_tables:
        missing_tables.extend(EVENT_TABLE_CANDIDATES)
    for table in source_tables:
        table_columns = schema[table]
        optional_missing = _missing_event_columns(table_columns)
        if optional_missing:
            missing_columns[table] = optional_missing
        records.extend(_load_event_records(conn, table, table_columns, cutoff=cutoff))

    sessions = _build_sessions(records, warnings)
    sessions.sort(key=_session_sort_key)

    return ClaudeSessionTodoCompletionReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(sessions),
        sessions=tuple(sessions[:limit]),
        source_tables=("claude_messages",) + source_tables,
        warnings=tuple(warnings),
        missing_tables=tuple(dict.fromkeys(missing_tables)),
        missing_columns=missing_columns,
    )


def format_claude_session_todo_completion_json(
    report: ClaudeSessionTodoCompletionReport,
) -> str:
    """Serialize a todo completion report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_todo_completion_text(
    report: ClaudeSessionTodoCompletionReport,
) -> str:
    """Render a concise human-readable todo completion report."""
    filters = report.filters
    totals = report.totals
    rate = _display_rate(totals.get("completion_rate"))
    lines = [
        "Claude Session Todo Completion",
        f"Generated: {report.generated_at}",
        f"Filters: days={filters['days']} limit={filters['limit']}",
        (
            "Totals: "
            f"sessions={totals['sessions_scanned']} "
            f"sessions_with_todos={totals['sessions_with_todos']} "
            f"todos={totals['todo_count']} completed={totals['completed']} "
            f"incomplete={totals['incomplete']} completion_rate={rate}"
        ),
    ]
    if report.source_tables:
        lines.append("Source tables: " + ", ".join(report.source_tables))
    if report.missing_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
            if columns
        )
        if missing:
            lines.append("Missing columns: " + missing)
    if report.warnings:
        lines.append("Warnings:")
        lines.extend(f"  - {warning}" for warning in report.warnings)

    lines.extend(["", "Sessions:"])
    if not report.sessions:
        lines.append("- none")
    for session in report.sessions:
        counts = ", ".join(
            f"{status}={session.status_counts.get(status, 0)}" for status in TODO_STATUSES
        )
        lines.append(
            f"- session={session.session_id} project={session.project_path or '-'} "
            f"todos={session.todo_count} completion_rate={_display_rate(session.completion_rate)} "
            f"messages={session.message_count} events={session.event_count} "
            f"first={session.first_seen_at or '-'} last={session.last_seen_at or '-'} "
            f"{counts}"
        )
    return "\n".join(lines)


def _build_sessions(
    records: Iterable[dict[str, Any]],
    warnings: list[str],
) -> list[ClaudeSessionTodoCompletion]:
    grouped: dict[tuple[str, str | None], dict[str, Any]] = {}
    seen_todos: dict[tuple[str, str | None], set[tuple[str, str]]] = defaultdict(set)
    for record in records:
        key = (
            str(record.get("session_id") or "unknown-session"),
            _optional_text(record.get("project_path")),
        )
        if key not in grouped:
            grouped[key] = {
                "event_count": 0,
                "message_count": 0,
                "status_counts": Counter(),
                "timestamps": [],
            }
        item = grouped[key]
        item[f"{record['_record_type']}_count"] += 1
        timestamp = _optional_text(record.get("timestamp"))
        if timestamp:
            item["timestamps"].append(timestamp)
        for todo in _extract_todos(record, warnings):
            marker = (todo["status"], todo["text"])
            if marker in seen_todos[key]:
                continue
            seen_todos[key].add(marker)
            item["status_counts"][todo["status"]] += 1

    sessions: list[ClaudeSessionTodoCompletion] = []
    for key, item in grouped.items():
        counts = {status: int(item["status_counts"].get(status, 0)) for status in TODO_STATUSES}
        todo_count = sum(counts.values())
        completed_count = counts["completed"]
        incomplete_count = counts["pending"] + counts["in_progress"]
        timestamps = sorted(item["timestamps"])
        sessions.append(
            ClaudeSessionTodoCompletion(
                session_id=key[0],
                project_path=key[1],
                message_count=int(item["message_count"]),
                event_count=int(item["event_count"]),
                first_seen_at=timestamps[0] if timestamps else None,
                last_seen_at=timestamps[-1] if timestamps else None,
                status_counts=counts,
                todo_count=todo_count,
                completed_count=completed_count,
                incomplete_count=incomplete_count,
                completion_rate=_rate(completed_count, todo_count),
            )
        )
    return sessions


def _extract_todos(record: Mapping[str, Any], warnings: list[str]) -> list[dict[str, str]]:
    todos: list[dict[str, str]] = []
    for value in _record_values(record):
        parsed = _json_value(value, record, warnings)
        if parsed is not None:
            todos.extend(_todos_from_json(parsed))
        if isinstance(value, str):
            todos.extend(_todos_from_text(value))
    return todos


def _record_values(record: Mapping[str, Any]) -> Iterable[Any]:
    for key in TEXT_COLUMNS + METADATA_COLUMNS:
        value = record.get(key)
        if value not in (None, ""):
            yield value


def _todos_from_json(value: Any) -> list[dict[str, str]]:
    todos: list[dict[str, str]] = []
    for item in _walk_json(value):
        if not isinstance(item, Mapping):
            continue
        status = _normalize_status(_first_text(item, ("status", "state", "todo_status")))
        if status is None:
            continue
        text = _first_text(item, ("content", "text", "title", "task", "todo", "description"))
        todos.append({"status": status, "text": _normalize_todo_text(text or json.dumps(item, sort_keys=True))})
    return todos


def _todos_from_text(text: str) -> list[dict[str, str]]:
    todos: list[dict[str, str]] = []
    for match in _CHECKBOX_RE.finditer(text):
        mark = match.group("mark").strip().lower()
        status = "completed" if mark == "x" else "canceled" if mark in {"~", "-"} else "pending"
        todos.append({"status": status, "text": _normalize_todo_text(match.group("text"))})
    for match in _STATUS_LINE_RE.finditer(text):
        status = _normalize_status(match.group("status"))
        if status:
            todos.append({"status": status, "text": _normalize_todo_text(match.group("text"))})
    return todos


def _json_value(value: Any, record: Mapping[str, Any], warnings: list[str]) -> Any | None:
    if isinstance(value, (Mapping, list)):
        return value
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text or text[0] not in "[{":
        return None
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        source = record.get("_source_table") or "claude_messages"
        row_id = record.get("id") or "-"
        warnings.append(f"{source} {row_id} has malformed todo JSON: {exc.msg}")
        return None


def _walk_json(value: Any) -> Iterable[Any]:
    yield value
    if isinstance(value, Mapping):
        for nested in value.values():
            yield from _walk_json(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_json(nested)


def _totals(sessions: Iterable[ClaudeSessionTodoCompletion]) -> dict[str, Any]:
    session_list = list(sessions)
    status_totals = {status: 0 for status in TODO_STATUSES}
    for session in session_list:
        for status in TODO_STATUSES:
            status_totals[status] += session.status_counts.get(status, 0)
    todo_count = sum(status_totals.values())
    completed = status_totals["completed"]
    incomplete = status_totals["pending"] + status_totals["in_progress"]
    return {
        **status_totals,
        "completion_rate": _rate(completed, todo_count),
        "incomplete": incomplete,
        "sessions_scanned": len(session_list),
        "sessions_with_todos": sum(1 for session in session_list if session.todo_count > 0),
        "todo_count": todo_count,
    }


def _load_message_records(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    if "timestamp" not in columns:
        return []
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "session_id"),
        _column_expr(columns, "project_path"),
        _column_expr(columns, "timestamp"),
        _column_expr(columns, "prompt_text"),
        _column_expr(columns, "response_text"),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
              FROM claude_messages
             WHERE timestamp >= ?
             ORDER BY timestamp ASC, id ASC""",
        (_db_time(cutoff),),
    ).fetchall()
    return [
        {
            **_row_dict(row),
            "_record_type": "message",
            "_source_table": "claude_messages",
        }
        for row in rows
    ]


def _load_event_records(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    if timestamp_column is None:
        return []
    order_by = f"{timestamp_column} ASC" + (", id ASC" if "id" in columns else "")
    selected = [
        _column_expr(columns, "id"),
        _alias_expr(columns, SESSION_COLUMNS, "session_id"),
        _alias_expr(columns, PROJECT_COLUMNS, "project_path"),
        _alias_expr(columns, TIMESTAMP_COLUMNS, "timestamp"),
    ]
    selected.extend(
        _column_expr(columns, column)
        for column in dict.fromkeys(TEXT_COLUMNS + METADATA_COLUMNS)
    )
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
              FROM {table}
             WHERE {timestamp_column} >= ?
             ORDER BY {order_by}""",
        (_db_time(cutoff),),
    ).fetchall()
    return [
        {
            **_row_dict(row),
            "_record_type": "event",
            "_source_table": table,
        }
        for row in rows
    ]


def _missing_event_columns(columns: set[str]) -> tuple[str, ...]:
    missing: list[str] = []
    if _first_existing(columns, SESSION_COLUMNS) is None:
        missing.append("session_id")
    if _first_existing(columns, TIMESTAMP_COLUMNS) is None:
        missing.append("timestamp")
    if not any(column in columns for column in TEXT_COLUMNS + METADATA_COLUMNS):
        missing.append("todo_payload")
    return tuple(missing)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {
            str(col["name"] if isinstance(col, sqlite3.Row) else col[1])
            for col in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _row_dict(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row)


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _alias_expr(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    column = _first_existing(columns, candidates)
    return f"{column} AS {alias}" if column else f"NULL AS {alias}"


def _first_existing(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    return next((column for column in candidates if column in columns), None)


def _first_text(row: Mapping[str, Any], keys: Iterable[str]) -> str | None:
    for key in keys:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def _normalize_status(value: Any) -> str | None:
    if value is None:
        return None
    key = re.sub(r"\s+", " ", str(value).strip().casefold().replace("_", " "))
    key = key.replace("cancelled", "canceled")
    return _STATUS_ALIASES.get(key) or _STATUS_ALIASES.get(key.replace(" ", "_"))


def _normalize_todo_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value).strip()).casefold()


def _session_sort_key(session: ClaudeSessionTodoCompletion) -> tuple[int, int, str, str]:
    return (
        0 if session.todo_count else 1,
        -session.incomplete_count,
        session.last_seen_at or "",
        session.session_id,
        session.project_path or "",
    )


def _rate(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return round(numerator / denominator, 4)


def _display_rate(value: Any) -> str:
    return "-" if value is None else f"{float(value):.1%}"


def _db_time(value: datetime) -> str:
    return _ensure_utc(value).isoformat()


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
