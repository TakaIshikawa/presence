"""Find Claude Code test commands that fail and later pass without edits."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
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
    "claude_messages",
)
SESSION_COLUMNS = ("session_id", "sessionId")
TIMESTAMP_COLUMNS = ("timestamp", "created_at", "event_time", "event_at")
TOOL_COLUMNS = ("tool_name", "tool", "toolName", "name")
STATUS_COLUMNS = ("status", "outcome")
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload")
TEXT_COLUMNS = (
    "prompt_text",
    "response_text",
    "content",
    "text",
    "message",
    "body",
    "command",
    "output",
    "result",
    "stdout",
    "stderr",
    "error_message",
)
COMMAND_COLUMNS = ("command", "cmd", "input", "shell_command")
EDIT_TOOL_NAMES = {"edit", "write", "multiedit", "notebookedit", "apply_patch"}

FAILURE_RE = re.compile(
    r"\b(fail(?:ed|ing|ure)?|error|traceback|exit code [1-9]\d*|non[- ]?zero)\b",
    re.IGNORECASE,
)
PASS_RE = re.compile(
    r"\b(pass(?:ed|es)?|success(?:ful)?|succeeded|ok|exit code 0|0 failed)\b",
    re.IGNORECASE,
)
TEST_COMMAND_RE = re.compile(
    r"(?:(?:^|[\n\r;|&`$>])\s*)"
    r"((?:uv\s+run\s+)?(?:python(?:3)?\s+-m\s+)?(?:pytest|unittest)\b[^\n\r;&|`]*"
    r"|(?:uv\s+run\s+)?pytest\b[^\n\r;&|`]*"
    r"|(?:uv\s+run\s+)?python(?:3)?\s+-m\s+(?:pytest|unittest)\b[^\n\r;&|`]*"
    r"|(?:npm|pnpm)\s+(?:test|run\s+test)\b[^\n\r;&|`]*)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ClaudeFlakyTestRetry:
    """One fail-then-pass test retry candidate."""

    session_id: str
    project_path: str | None
    command: str
    normalized_command: str
    failure_count: int
    first_failure_timestamp: str | None
    eventual_pass_timestamp: str | None
    evidence_snippets: tuple[str, ...]
    source_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["evidence_snippets"] = list(self.evidence_snippets)
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeFlakyTestRetriesReport:
    """Claude flaky retry candidate report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    retries: tuple[ClaudeFlakyTestRetry, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_flaky_test_retries",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "retries": [retry.to_dict() for retry in self.retries],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_flaky_test_retries_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeFlakyTestRetriesReport:
    """Scan Claude session timelines for failing test commands that later pass."""
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
    rows = [
        row
        for table in source_tables
        for row in _load_rows(conn, table, schema[table], cutoff=cutoff)
    ]
    events, malformed_metadata_count = _events_from_rows(rows)
    retries = _detect_retries(events)
    retries.sort(key=_retry_sort_key)

    return ClaudeFlakyTestRetriesReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "candidate_count": len(retries),
            "edit_events": sum(1 for event in events if event["kind"] == "edit"),
            "malformed_metadata_count": malformed_metadata_count,
            "rows_scanned": len(rows),
            "test_attempts": sum(1 for event in events if event["kind"] == "test_attempt"),
        },
        retries=tuple(retries[:limit]),
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_flaky_test_retries_json(report: ClaudeFlakyTestRetriesReport) -> str:
    """Serialize a Claude flaky retry report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_flaky_test_retries_text(report: ClaudeFlakyTestRetriesReport) -> str:
    """Render a concise human-readable flaky retry digest."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Flaky Test Retries",
        f"Generated: {report.generated_at}",
        f"Filters: days={filters['days']} limit={filters['limit']}",
        (
            "Totals: "
            f"rows={totals['rows_scanned']} attempts={totals['test_attempts']} "
            f"edits={totals['edit_events']} candidates={totals['candidate_count']} "
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

    if not report.retries:
        lines.extend(["", "No flaky test retry candidates found."])
        return "\n".join(lines)

    lines.extend(["", "Candidates:"])
    for retry in report.retries:
        lines.append(
            f"- session={retry.session_id} project={retry.project_path or '-'} "
            f"failures={retry.failure_count} pass={retry.eventual_pass_timestamp or '-'}"
        )
        lines.append(f"  command={retry.command}")
        for snippet in retry.evidence_snippets[:3]:
            lines.append(f"  evidence={snippet}")
    return "\n".join(lines)


def _events_from_rows(rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    events: list[dict[str, Any]] = []
    malformed_metadata_count = 0
    for row in rows:
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        base = _base_event(row, metadata)
        if _is_edit_event(row, metadata):
            events.append({**base, "kind": "edit"})
        for command in _extract_test_commands(row, metadata):
            outcome = _outcome(row, metadata)
            if outcome:
                events.append(
                    {
                        **base,
                        "command": command,
                        "evidence": _excerpt(_event_text(row, metadata)),
                        "kind": "test_attempt",
                        "normalized_command": normalize_test_command(command),
                        "outcome": outcome,
                    }
                )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def normalize_test_command(command: str) -> str:
    """Normalize common test command forms for retry matching."""
    text = " ".join(str(command).strip().strip("`").split())
    text = re.sub(r"^\$\s*", "", text)
    text = re.sub(r"\s*(?:#|&&|\|\||;).*$", "", text).strip()
    try:
        tokens = shlex.split(text)
    except ValueError:
        tokens = text.split()
    tokens = [token for token in tokens if token]
    while tokens and ("=" in tokens[0] and not tokens[0].startswith("-")):
        tokens.pop(0)
    if tokens[:2] == ["uv", "run"]:
        tokens = tokens[2:]
    if len(tokens) >= 3 and tokens[0] in {"python", "python3"} and tokens[1] == "-m":
        tokens = tokens[2:]
    if len(tokens) >= 2 and tokens[0] in {"npm", "pnpm"} and tokens[1] == "run":
        tokens = [tokens[0], *tokens[2:]]
    return " ".join(tokens).lower()


def _detect_retries(events: list[dict[str, Any]]) -> list[ClaudeFlakyTestRetry]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        grouped[event["session_id"]].append(event)

    retries: list[ClaudeFlakyTestRetry] = []
    for session_id, session_events in grouped.items():
        failures: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for event in sorted(session_events, key=_event_sort_key):
            if event["kind"] == "edit":
                failures.clear()
                continue
            if event["kind"] != "test_attempt":
                continue
            normalized = event["normalized_command"]
            if event["outcome"] == "fail":
                failures[normalized].append(event)
            elif event["outcome"] == "pass" and failures.get(normalized):
                failed = failures.pop(normalized)
                evidence = [failure["evidence"] for failure in failed]
                evidence.append(event["evidence"])
                retries.append(
                    ClaudeFlakyTestRetry(
                        session_id=session_id,
                        project_path=event["project_path"],
                        command=event["command"],
                        normalized_command=normalized,
                        failure_count=len(failed),
                        first_failure_timestamp=failed[0]["timestamp"],
                        eventual_pass_timestamp=event["timestamp"],
                        evidence_snippets=tuple(dict.fromkeys(evidence))[:4],
                        source_tables=tuple(
                            sorted({failure["source_table"] for failure in failed} | {event["source_table"]})
                        ),
                    )
                )
    return retries


def _base_event(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "project_path": _first_text(row, ("project_path",)) or _first_text(metadata, ("project_path",)),
        "session_id": _first_text(row, SESSION_COLUMNS)
        or _first_text(metadata, SESSION_COLUMNS)
        or "unknown-session",
        "source_table": str(row.get("_source_table") or "unknown"),
        "timestamp": _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS),
    }


def _extract_test_commands(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> tuple[str, ...]:
    commands: list[str] = []
    for source in (row, metadata):
        for column in COMMAND_COLUMNS:
            value = source.get(column)
            if isinstance(value, str) and value.strip():
                commands.append(value)
            elif isinstance(value, Mapping):
                nested = _first_text(value, COMMAND_COLUMNS)
                if nested:
                    commands.append(nested)
    for path in (
        ("tool_input", "command"),
        ("input", "command"),
        ("tool", "input", "command"),
        ("tool_use", "input", "command"),
    ):
        nested = _nested_text(metadata, path)
        if nested:
            commands.append(nested)
    text = _event_text(row, metadata)
    commands.extend(match.group(1) for match in TEST_COMMAND_RE.finditer(text))
    filtered = []
    for command in commands:
        cleaned = _clean_command(command)
        if cleaned and _is_test_command(cleaned):
            filtered.append(cleaned)
    return tuple(dict.fromkeys(filtered))


def _clean_command(command: str) -> str:
    text = " ".join(command.strip().strip("`'\"").split())
    text = re.sub(r"^(?:command failed(?: with exit code \d+)?:|ran|running)\s+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+(?:failed|passed|succeeded)(?:\b.*)?$", "", text, flags=re.IGNORECASE)
    return text.strip(" .")


def _is_test_command(command: str) -> bool:
    return bool(TEST_COMMAND_RE.search(f"\n{command}"))


def _outcome(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    status = (_first_text(row, STATUS_COLUMNS) or _first_text(metadata, STATUS_COLUMNS) or "").lower()
    text = _event_text(row, metadata)
    if status in {"failed", "failure", "error", "errored"} or FAILURE_RE.search(text):
        return "fail"
    if status in {"passed", "pass", "success", "succeeded", "ok", "completed"} or PASS_RE.search(text):
        return "pass"
    return None


def _is_edit_event(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> bool:
    tool_name = (
        _first_text(row, TOOL_COLUMNS)
        or _first_text(metadata, TOOL_COLUMNS)
        or _nested_text(metadata, ("tool", "name"))
        or _nested_text(metadata, ("tool_use", "name"))
        or ""
    ).lower()
    if tool_name in EDIT_TOOL_NAMES:
        return True
    text = _event_text(row, metadata)
    return bool(
        re.search(r"\b(apply_patch|edited|modified|updated|wrote|write file)\b", text, re.IGNORECASE)
        and re.search(r"\b(src|scripts|tests|app|lib|packages)/[\w./-]+", text)
    )


def _load_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id", "rowid"),
        *[
            _aliased_first_expr(columns, names, names[0])
            for names in (SESSION_COLUMNS, TIMESTAMP_COLUMNS, TOOL_COLUMNS, STATUS_COLUMNS)
        ],
        _column_expr(columns, "project_path"),
        *[
            _column_expr(columns, column)
            for column in (*METADATA_COLUMNS, *TEXT_COLUMNS, *COMMAND_COLUMNS)
        ],
    ]
    select_sql = ", ".join(dict.fromkeys(select_columns))
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    where_sql = ""
    params: list[Any] = []
    if timestamp_column:
        where_sql = f"WHERE {timestamp_column} >= ?"
        params.append(cutoff.isoformat())
    order_sql = f"{timestamp_column or 'rowid'} ASC, id ASC" if "id" in columns else "rowid ASC"
    cursor = conn.execute(
        f"""SELECT {select_sql}
              FROM {table}
              {where_sql}
             ORDER BY {order_sql}""",
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
    optional = (
        "project_path",
        *TOOL_COLUMNS,
        *STATUS_COLUMNS,
        *METADATA_COLUMNS,
        *COMMAND_COLUMNS,
        "response_text",
    )
    return tuple(column for column in optional if column not in columns)


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    return column if column in columns else f"{fallback} AS {column}"


def _aliased_first_expr(columns: set[str], names: tuple[str, ...], alias: str) -> str:
    column = _first_existing(columns, names)
    return f"{column} AS {alias}" if column and column != alias else _column_expr(columns, alias)


def _first_existing(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


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


def _event_text(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for source in (row, metadata):
        for column in (*TEXT_COLUMNS, *COMMAND_COLUMNS, *STATUS_COLUMNS):
            value = source.get(column)
            if isinstance(value, (str, int, float)):
                parts.append(str(value))
            elif isinstance(value, (list, tuple)):
                parts.extend(str(item) for item in value)
            elif isinstance(value, Mapping):
                parts.extend(str(item) for item in value.values() if isinstance(item, (str, int, float)))
    return "\n".join(parts)


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


def _excerpt(text: str, width: int = 180) -> str:
    compact = " ".join(str(text).split())
    if len(compact) <= width:
        return compact
    return compact[: max(0, width - 3)].rstrip() + "..."


def _event_sort_key(event: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        _timestamp_sort(event.get("timestamp")),
        str(event.get("session_id") or ""),
        str(event.get("kind") or ""),
    )


def _retry_sort_key(retry: ClaudeFlakyTestRetry) -> tuple[str, str, str]:
    return (
        retry.eventual_pass_timestamp or "",
        retry.session_id,
        retry.normalized_command,
    )


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
