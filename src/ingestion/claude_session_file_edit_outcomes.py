"""Report Claude Code file-editing tool outcomes from session logs."""

from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping


FILE_EDIT_TOOLS = {
    "write": "Write",
    "edit": "Edit",
    "multiedit": "MultiEdit",
    "notebookedit": "NotebookEdit",
}
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
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload")
PATH_COLUMNS = ("file_path", "filepath", "path", "notebook_path", "file")
TEXT_COLUMNS = ("content", "text", "message", "body", "error", "error_message", "output", "result")

FAILURE_RE = re.compile(
    r"\b(error|failed|failure|exception|traceback|timeout|timed out|denied|"
    r"old_string not found|no such file|not found)\b",
    re.IGNORECASE,
)
NO_OP_RE = re.compile(
    r"\b(no changes?|unchanged|already (?:exists|matches|up[- ]?to[- ]?date)|"
    r"nothing to (?:change|edit|write)|skipped)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ClaudeSessionFileEditOutcomeEvent:
    """One normalized Claude file-editing tool attempt."""

    session_id: str
    project_path: str | None
    timestamp: str | None
    tool_use_id: str | None
    tool_name: str
    target_path: str | None
    path_category: str
    outcome: str
    result_snippet: str
    source_table: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionFileEditOutcomeRow:
    """Grouped file-editing outcomes by tool and target path category."""

    tool_name: str
    path_category: str
    attempts: int
    successes: int
    failures: int
    missing_results: int
    no_ops: int
    session_count: int
    target_count: int
    target_extension_counts: dict[str, int]
    example_targets: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["example_targets"] = list(self.example_targets)
        payload["target_extension_counts"] = dict(sorted(self.target_extension_counts.items()))
        return payload


@dataclass(frozen=True)
class ClaudeSessionFileEditOutcomeReport:
    """Claude file-editing outcome report."""

    generated_at: str
    totals: dict[str, int]
    rows: tuple[ClaudeSessionFileEditOutcomeRow, ...]
    events: tuple[ClaudeSessionFileEditOutcomeEvent, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_file_edit_outcomes",
            "events": [event.to_dict() for event in self.events],
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_session_file_edit_outcomes_report(
    rows_or_db: Any,
    *,
    now: datetime | None = None,
) -> ClaudeSessionFileEditOutcomeReport:
    """Build a deterministic report of Claude file-editing tool outcomes."""
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    source_tables: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()

    if _looks_like_rows(rows_or_db):
        rows = [_mapping(row) for row in rows_or_db]
    else:
        conn = _connection(rows_or_db)
        schema = _schema(conn)
        source_tables = tuple(table for table in SOURCE_TABLE_CANDIDATES if table in schema)
        missing_tables = () if source_tables else SOURCE_TABLE_CANDIDATES
        rows = [
            row
            for table in source_tables
            for row in _load_rows(conn, table, schema[table])
        ]

    events, malformed_metadata_count = load_claude_session_file_edit_outcome_events(rows)
    report_rows = group_file_edit_outcome_events(events)

    return ClaudeSessionFileEditOutcomeReport(
        generated_at=generated_at.isoformat(),
        totals={
            "attempt_count": len(events),
            "failure_count": sum(1 for event in events if event.outcome == "failure"),
            "malformed_metadata_count": malformed_metadata_count,
            "missing_result_count": sum(1 for event in events if event.outcome == "missing_result"),
            "no_op_count": sum(1 for event in events if event.outcome == "no_op"),
            "rows_scanned": len(rows),
            "session_count": len({event.session_id for event in events}),
            "success_count": sum(1 for event in events if event.outcome == "success"),
        },
        rows=tuple(report_rows),
        events=tuple(events),
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_claude_session_file_edit_outcome_events(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[ClaudeSessionFileEditOutcomeEvent], int]:
    """Normalize raw Claude rows into file-editing tool attempts."""
    tool_uses: list[dict[str, Any]] = []
    results: dict[str, dict[str, Any]] = {}
    malformed_metadata_count = 0

    for index, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        context = _row_context(row, metadata, index=index)
        row_results = _tool_result_blocks(row, metadata)

        for tool_use in _tool_use_blocks(row, metadata):
            tool_name = _canonical_file_edit_tool(_first_text(tool_use, ("name", "tool_name", "toolName", "tool")))
            if not tool_name:
                continue
            tool_use_id = _first_text(tool_use, ("id", "tool_use_id", "toolUseID", "tool_useId"))
            embedded_result = _embedded_result_for_tool_use(tool_use_id, row_results)
            tool_uses.append(
                {
                    **context,
                    "embedded_result": embedded_result,
                    "tool_use": tool_use,
                    "tool_name": tool_name,
                }
            )

        for result in row_results:
            result_id = _first_text(result, ("tool_use_id", "toolUseID", "tool_useId", "id"))
            if result_id:
                results[result_id] = {**context, "tool_result": result}

    events = [
        _event_from_tool_use(
            item,
            results.get(str(item["tool_use"].get("id") or ""))
            or ({"tool_result": item["embedded_result"]} if item.get("embedded_result") else None),
        )
        for item in tool_uses
    ]
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def _embedded_result_for_tool_use(
    tool_use_id: str | None,
    row_results: list[dict[str, Any]],
) -> dict[str, Any] | None:
    if not row_results:
        return None
    for result in row_results:
        result_id = _first_text(result, ("tool_use_id", "toolUseID", "tool_useId", "id"))
        if tool_use_id and result_id == tool_use_id:
            return result
    anonymous_results = [
        result
        for result in row_results
        if not _first_text(result, ("tool_use_id", "toolUseID", "tool_useId", "id"))
    ]
    return anonymous_results[0] if len(anonymous_results) == 1 else None


def group_file_edit_outcome_events(
    events: Iterable[ClaudeSessionFileEditOutcomeEvent],
) -> list[ClaudeSessionFileEditOutcomeRow]:
    """Group file edit attempts by tool and path category."""
    grouped: dict[tuple[str, str], list[ClaudeSessionFileEditOutcomeEvent]] = {}
    for event in events:
        grouped.setdefault((event.tool_name, event.path_category), []).append(event)

    rows: list[ClaudeSessionFileEditOutcomeRow] = []
    for (tool_name, path_category), group in grouped.items():
        outcomes = Counter(event.outcome for event in group)
        targets = tuple(sorted({event.target_path for event in group if event.target_path}))
        target_extension_counts = Counter(_target_extension(event.target_path) for event in group)
        rows.append(
            ClaudeSessionFileEditOutcomeRow(
                tool_name=tool_name,
                path_category=path_category,
                attempts=len(group),
                successes=outcomes["success"],
                failures=outcomes["failure"],
                missing_results=outcomes["missing_result"],
                no_ops=outcomes["no_op"],
                session_count=len({event.session_id for event in group}),
                target_count=len(targets),
                target_extension_counts=dict(sorted(target_extension_counts.items())),
                example_targets=targets[:5],
            )
        )
    return sorted(rows, key=_row_sort_key)


def load_claude_session_log_rows(log_path: str | Path) -> list[dict[str, Any]]:
    """Load a Claude session JSONL file into raw event rows."""
    path = Path(log_path).expanduser()
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {line_number}: {exc}") from exc
            if isinstance(payload, Mapping):
                rows.append({**dict(payload), "_source_table": path.name})
    return rows


def format_claude_session_file_edit_outcomes_json(
    report: ClaudeSessionFileEditOutcomeReport,
) -> str:
    """Serialize a file-edit outcome report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_file_edit_outcomes_text(
    report: ClaudeSessionFileEditOutcomeReport,
) -> str:
    """Render a concise human-readable file-edit outcome report."""
    totals = report.totals
    lines = [
        "Claude Session File Edit Outcomes",
        f"Generated: {report.generated_at}",
        (
            "Totals: "
            f"rows={totals['rows_scanned']} attempts={totals['attempt_count']} "
            f"successes={totals['success_count']} failures={totals['failure_count']} "
            f"missing_results={totals['missing_result_count']} no_ops={totals['no_op_count']} "
            f"malformed_metadata={totals['malformed_metadata_count']}"
        ),
    ]
    if report.source_tables:
        lines.append("Source tables: " + ", ".join(report.source_tables))
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.rows:
        lines.extend(["", "No Claude file-editing tool attempts found."])
        return "\n".join(lines)

    lines.extend(["", "Outcomes:"])
    for row in report.rows:
        lines.append(
            f"- {row.tool_name} {row.path_category}: attempts={row.attempts} "
            f"successes={row.successes} failures={row.failures} "
            f"missing_results={row.missing_results} no_ops={row.no_ops} "
            f"sessions={row.session_count} targets={row.target_count}"
        )
        if row.target_extension_counts:
            extensions = ", ".join(
                f"{extension}={count}"
                for extension, count in sorted(row.target_extension_counts.items())
            )
            lines.append("  extensions: " + extensions)
        if row.example_targets:
            lines.append("  targets: " + ", ".join(row.example_targets))
    return "\n".join(lines)


def _event_from_tool_use(
    item: Mapping[str, Any],
    result_item: Mapping[str, Any] | None,
) -> ClaudeSessionFileEditOutcomeEvent:
    tool_use = item["tool_use"]
    tool_input = tool_use.get("input") if isinstance(tool_use.get("input"), Mapping) else tool_use
    target_path = _target_path(tool_input)
    tool_use_id = _first_text(tool_use, ("id", "tool_use_id", "toolUseID", "tool_useId"))
    result = result_item.get("tool_result") if result_item else None
    result_text = _result_text(result) if isinstance(result, Mapping) else ""
    outcome = _classify_outcome(result if isinstance(result, Mapping) else None, result_text)
    return ClaudeSessionFileEditOutcomeEvent(
        session_id=str(item["session_id"]),
        project_path=item["project_path"],
        timestamp=item["timestamp"],
        tool_use_id=tool_use_id,
        tool_name=str(item["tool_name"]),
        target_path=target_path,
        path_category=_path_category(target_path),
        outcome=outcome,
        result_snippet=_snippet(result_text),
        source_table=str(item["source_table"]),
    )


def _classify_outcome(result: Mapping[str, Any] | None, result_text: str) -> str:
    if result is None:
        return "missing_result"
    status = _first_text(result, STATUS_COLUMNS)
    if _result_is_error(result) or _status_failed(status) or FAILURE_RE.search(result_text):
        return "failure"
    if NO_OP_RE.search(result_text):
        return "no_op"
    return "success"


def _tool_use_blocks(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for source in (row, metadata):
        blocks.extend(_content_blocks(source, block_type="tool_use"))
        nested = source.get("tool_use")
        if isinstance(nested, Mapping):
            blocks.append(dict(nested))
        tool_name = _canonical_file_edit_tool(_first_text(source, TOOL_COLUMNS))
        if tool_name:
            tool_input = source.get("input") if isinstance(source.get("input"), Mapping) else source
            blocks.append(
                {
                    "id": _first_text(source, ("tool_use_id", "toolUseID", "tool_useId", "id")),
                    "name": tool_name,
                    "input": dict(tool_input),
                }
            )
    return _dedupe_blocks(blocks)


def _tool_result_blocks(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    for source in (row, metadata):
        blocks.extend(_content_blocks(source, block_type="tool_result"))
        nested = source.get("tool_result")
        if isinstance(nested, Mapping):
            blocks.append(dict(nested))
        result_id = _first_text(source, ("tool_use_id", "toolUseID", "tool_useId"))
        if result_id or _first_text(source, STATUS_COLUMNS) or _text_values(source):
            blocks.append({**dict(source), "tool_use_id": result_id})
    return _dedupe_blocks(blocks)


def _content_blocks(source: Mapping[str, Any], *, block_type: str) -> list[dict[str, Any]]:
    containers = [source.get("content")]
    message = source.get("message")
    if isinstance(message, Mapping):
        containers.append(message.get("content"))
    blocks: list[dict[str, Any]] = []
    for container in containers:
        if isinstance(container, Mapping) and container.get("type") == block_type:
            blocks.append(dict(container))
        elif isinstance(container, list):
            blocks.extend(
                dict(item)
                for item in container
                if isinstance(item, Mapping) and item.get("type") == block_type
            )
    return blocks


def _dedupe_blocks(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    for block in blocks:
        key = json.dumps(block, sort_keys=True, default=str)
        if key not in seen:
            seen.add(key)
            deduped.append(block)
    return deduped


def _row_context(row: Mapping[str, Any], metadata: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    return {
        "session_id": _first_text(row, SESSION_COLUMNS)
        or _first_text(metadata, SESSION_COLUMNS)
        or "unknown-session",
        "project_path": _first_text(row, PROJECT_COLUMNS) or _first_text(metadata, PROJECT_COLUMNS),
        "timestamp": _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS),
        "source_table": str(row.get("_source_table") or "rows"),
        "row_index": index,
    }


def _target_path(source: Mapping[str, Any]) -> str | None:
    for column in PATH_COLUMNS:
        value = source.get(column)
        if isinstance(value, str) and value.strip():
            return _clean_path(value)
    return None


def _clean_path(value: str) -> str:
    text = str(value).strip().strip("`'\"()[]{}<>,")
    text = re.sub(r"^(?:file://)", "", text)
    text = re.sub(r"(?::\d+)+$", "", text)
    return text.replace("\\", "/")


def _path_category(path: str | None) -> str:
    if not path:
        return "[unknown path]"
    name = Path(path).name
    if name in {"Makefile", "Dockerfile"}:
        return "[no extension]"
    suffix = Path(path).suffix.lower()
    return suffix or "[no extension]"


def _target_extension(path: str | None) -> str:
    if not path:
        return "[unknown]"
    suffix = Path(path).suffix.lower()
    return suffix or "[none]"


def _canonical_file_edit_tool(value: Any) -> str | None:
    key = re.sub(r"[^a-z0-9]+", "", str(value or "").lower())
    return FILE_EDIT_TOOLS.get(key)


def _result_is_error(result: Mapping[str, Any]) -> bool:
    for key in ("is_error", "isError", "error"):
        value = result.get(key)
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip() and key == "error":
            return True
    return False


def _status_failed(status: str | None) -> bool:
    return bool(status and status.lower() in {"error", "failed", "failure", "errored"})


def _result_text(result: Mapping[str, Any]) -> str:
    values = _text_values(result)
    return "\n".join(dict.fromkeys(values))


def _text_values(source: Mapping[str, Any]) -> list[str]:
    values: list[str] = []
    for column in TEXT_COLUMNS:
        value = source.get(column)
        if isinstance(value, str) and value.strip():
            values.append(value)
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, (str, int, float)):
                    values.append(str(item))
                elif isinstance(item, Mapping):
                    values.extend(_text_values(item))
        elif isinstance(value, Mapping):
            values.extend(_text_values(value))
    return values


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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected rows, sqlite3.Connection, or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name"
    ).fetchall()
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str]) -> list[dict[str, Any]]:
    select_columns = ", ".join(sorted(columns))
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    order_sql = f"{timestamp_column or 'rowid'} ASC, rowid ASC"
    cursor = conn.execute(f"SELECT {select_columns} FROM {table} ORDER BY {order_sql}")
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


def _first_existing(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for column in candidates:
        if column in columns:
            return column
    return None


def _first_text(source: Mapping[str, Any], columns: tuple[str, ...]) -> str | None:
    for column in columns:
        value = source.get(column)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _snippet(value: Any, *, limit: int = 160) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


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


def _timestamp_sort(value: str | None) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _event_sort_key(event: ClaudeSessionFileEditOutcomeEvent) -> tuple[str, str, str, str]:
    return (
        _timestamp_sort(event.timestamp),
        event.session_id,
        event.tool_name,
        event.target_path or "",
    )


def _row_sort_key(row: ClaudeSessionFileEditOutcomeRow) -> tuple[int, int, str, str]:
    return (-row.attempts, -row.failures - row.missing_results, row.tool_name, row.path_category)


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row)


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(
        value,
        (str, bytes, sqlite3.Connection),
    ) and not hasattr(value, "conn")
