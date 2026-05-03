"""Report Claude session recovery after denied permission decisions."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
from typing import Any, Iterable, Mapping

from ingestion.claude_command_failure_summary import normalize_command_prefix
from ingestion.claude_command_retry_recovery import _event_status
from ingestion.claude_session_approval_decision_audit import (
    APPROVAL_RE,
    APPROVAL_TOOL_NAMES,
    BLOCKED_DECISIONS,
    COMMAND_COLUMNS,
    METADATA_COLUMNS,
    PROJECT_COLUMNS,
    SESSION_COLUMNS,
    SOURCE_TABLE_CANDIDATES,
    STATUS_COLUMNS,
    TEXT_COLUMNS,
    TIMESTAMP_COLUMNS,
    TOOL_COLUMNS,
    _connection,
    _ensure_utc,
    _event_text,
    _first_existing,
    _first_text,
    _metadata,
    _nested_text,
    _parse_datetime,
    _quote_identifier,
    _schema,
    _summary,
    _tool_name,
)


DEFAULT_LIMIT = 50


@dataclass(frozen=True)
class ClaudeSessionPermissionDenialRecoveryRow:
    """One denied permission decision and its next successful tool call."""

    session_id: str
    project_path: str | None
    denied_at: str | None
    denied_decision: str
    denied_tool: str
    denied_command: str | None
    denied_summary: str
    recovery_at: str | None
    recovery_tool: str | None
    recovery_command: str | None
    recovery_summary: str | None
    elapsed_seconds: int | None
    recovery_bucket: str
    source_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeSessionPermissionDenialRecoveryReport:
    """Claude session permission denial recovery report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionPermissionDenialRecoveryRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_permission_denial_recovery",
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


@dataclass(frozen=True)
class _ClaudeSessionEvent:
    session_id: str
    project_path: str | None
    timestamp: str | None
    tool_name: str
    status: str | None
    command: str | None
    text: str | None
    source_table: str
    ordinal: int
    metadata: Mapping[str, Any]


def build_claude_session_permission_denial_recovery_report(
    db_or_rows: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionPermissionDenialRecoveryReport:
    """Build a deterministic report of denied permissions and recovery outcomes."""
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    missing_columns: dict[str, tuple[str, ...]] = {}

    if _looks_like_rows(db_or_rows):
        raw_rows = [_mapping(row) for row in db_or_rows]
        source_tables = tuple(
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

    events, malformed_metadata_count = _load_events(raw_rows)
    rows = detect_permission_denial_recoveries(events)
    reported = tuple(rows[:limit])

    return ClaudeSessionPermissionDenialRecoveryReport(
        generated_at=generated_at.isoformat(),
        filters={"limit": limit},
        totals={
            "denied_approval_count": len(rows),
            "malformed_metadata_count": malformed_metadata_count,
            "no_recovery_count": sum(1 for row in rows if row.recovery_bucket == "no_recovery"),
            "recovered_count": sum(1 for row in rows if row.recovery_bucket == "recovered"),
            "reported_count": len(reported),
            "retried_same_command_count": sum(
                1 for row in rows if row.recovery_bucket == "retried_same_command"
            ),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
        },
        rows=reported,
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def detect_permission_denial_recoveries(
    events: Iterable[_ClaudeSessionEvent],
) -> list[ClaudeSessionPermissionDenialRecoveryRow]:
    """Classify denied permission decisions by their next successful tool call."""
    sessions: dict[str, list[_ClaudeSessionEvent]] = {}
    for event in events:
        sessions.setdefault(event.session_id, []).append(event)

    rows: list[ClaudeSessionPermissionDenialRecoveryRow] = []
    for session_id, session_events in sorted(sessions.items()):
        ordered = sorted(session_events, key=_event_sort_key)
        for index, event in enumerate(ordered):
            decision = _approval_decision(event)
            if decision not in BLOCKED_DECISIONS and decision != "denied":
                continue
            recovery = _next_successful_tool_call(ordered[index + 1 :])
            rows.append(_recovery_row(session_id, event, recovery, decision or "denied"))
    return sorted(rows, key=_row_sort_key)


def format_claude_session_permission_denial_recovery_json(
    report: ClaudeSessionPermissionDenialRecoveryReport,
) -> str:
    """Serialize a permission denial recovery report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_permission_denial_recovery_text(
    report: ClaudeSessionPermissionDenialRecoveryReport,
) -> str:
    """Render a compact permission denial recovery table."""
    totals = report.totals
    lines = [
        "Claude Session Permission Denial Recovery",
        f"Generated: {report.generated_at}",
        f"Filters: limit={report.filters['limit']}",
        (
            "Totals: "
            f"rows={totals['rows_scanned']} denied={totals['denied_approval_count']} "
            f"recovered={totals['recovered_count']} "
            f"retried_same_command={totals['retried_same_command_count']} "
            f"no_recovery={totals['no_recovery_count']} "
            f"malformed_metadata={totals['malformed_metadata_count']}"
        ),
    ]
    if report.source_tables:
        lines.append("Source tables: " + ", ".join(report.source_tables))
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        for table, columns in sorted(report.missing_columns.items()):
            lines.append(f"Missing columns for {table}: " + ", ".join(columns))
    if not report.rows:
        lines.extend(["", "No denied permission decisions found."])
        return "\n".join(lines)

    lines.extend(["", "Recoveries:"])
    for row in report.rows:
        elapsed = "-" if row.elapsed_seconds is None else f"{row.elapsed_seconds}s"
        lines.append(
            f"- session={row.session_id} bucket={row.recovery_bucket} "
            f"elapsed={elapsed} denied_at={row.denied_at or '-'} "
            f"recovery_at={row.recovery_at or '-'}"
        )
        lines.append(f"  denied={row.denied_tool} {row.denied_command or row.denied_summary}")
        lines.append(f"  recovery={row.recovery_tool or '-'} {row.recovery_command or '-'}")
    return "\n".join(lines)


def _load_events(rows: Iterable[Mapping[str, Any]]) -> tuple[list[_ClaudeSessionEvent], int]:
    events: list[_ClaudeSessionEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        session_id = (
            _first_text(row, SESSION_COLUMNS)
            or _first_text(metadata, SESSION_COLUMNS)
            or "unknown-session"
        )
        events.append(
            _ClaudeSessionEvent(
                session_id=session_id,
                project_path=_first_text(row, PROJECT_COLUMNS) or _first_text(metadata, PROJECT_COLUMNS),
                timestamp=_first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS),
                tool_name=_tool_name(row, metadata),
                status=_decision_text(row, metadata) or _first_text(row, STATUS_COLUMNS),
                command=_command(row, metadata),
                text=_event_text(row, metadata),
                source_table=str(row.get("_source_table") or "rows"),
                ordinal=ordinal,
                metadata=metadata,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def _recovery_row(
    session_id: str,
    denial: _ClaudeSessionEvent,
    recovery: _ClaudeSessionEvent | None,
    decision: str,
) -> ClaudeSessionPermissionDenialRecoveryRow:
    denied_tool = _denied_tool(denial)
    denied_command = denial.command
    elapsed_seconds = _elapsed_seconds(denial.timestamp, recovery.timestamp if recovery else None)
    source_tables = {denial.source_table}
    if recovery:
        source_tables.add(recovery.source_table)
    return ClaudeSessionPermissionDenialRecoveryRow(
        session_id=session_id,
        project_path=denial.project_path or (recovery.project_path if recovery else None),
        denied_at=denial.timestamp,
        denied_decision=decision,
        denied_tool=denied_tool,
        denied_command=denied_command,
        denied_summary=_summary(denial.text or denial.command or denial.status or denied_tool),
        recovery_at=recovery.timestamp if recovery else None,
        recovery_tool=recovery.tool_name if recovery else None,
        recovery_command=recovery.command if recovery else None,
        recovery_summary=_summary(recovery.command or recovery.text or recovery.tool_name) if recovery else None,
        elapsed_seconds=elapsed_seconds,
        recovery_bucket=_recovery_bucket(denial, denied_tool, recovery),
        source_tables=tuple(sorted(source_tables)),
    )


def _next_successful_tool_call(
    candidates: Iterable[_ClaudeSessionEvent],
) -> _ClaudeSessionEvent | None:
    for candidate in candidates:
        if _is_approval_event(candidate):
            continue
        if _is_successful_tool_call(candidate):
            return candidate
    return None


def _is_successful_tool_call(event: _ClaudeSessionEvent) -> bool:
    if event.tool_name == "unknown" and not event.command:
        return False
    status = _status_bucket(event)
    if status == "failed":
        return False
    return status == "succeeded" or bool(event.command or event.tool_name != "unknown")


def _status_bucket(event: _ClaudeSessionEvent) -> str | None:
    return _event_status(
        {
            "status": event.status,
            "command": event.command,
            "content": event.text,
        },
        dict(event.metadata),
    )


def _recovery_bucket(
    denial: _ClaudeSessionEvent,
    denied_tool: str,
    recovery: _ClaudeSessionEvent | None,
) -> str:
    if recovery is None:
        return "no_recovery"
    if _same_denied_action(denial, denied_tool, recovery):
        return "retried_same_command"
    return "recovered"


def _same_denied_action(
    denial: _ClaudeSessionEvent,
    denied_tool: str,
    recovery: _ClaudeSessionEvent,
) -> bool:
    if denial.command and recovery.command:
        return _clean(denial.command) == _clean(recovery.command)
    if denial.command and recovery.command is None:
        return normalize_command_prefix(denial.command) == normalize_command_prefix(recovery.text or "")
    return denied_tool != "unknown" and denied_tool == recovery.tool_name


def _approval_decision(event: _ClaudeSessionEvent) -> str | None:
    if not _is_approval_event(event):
        return None
    text = " ".join(part for part in (event.status, event.text, event.tool_name) if part).lower()
    for decision in sorted(BLOCKED_DECISIONS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(decision)}\b", text):
            return "denied" if decision in {"deny", "denied", "reject", "rejected", "no"} else "cancelled"
    return None


def _is_approval_event(event: _ClaudeSessionEvent) -> bool:
    if event.tool_name in APPROVAL_TOOL_NAMES:
        return True
    status_text = (event.status or "").lower()
    if any(re.search(rf"\b{re.escape(decision)}\b", status_text) for decision in BLOCKED_DECISIONS):
        return bool(APPROVAL_RE.search(event.text or status_text))
    return False


def _denied_tool(event: _ClaudeSessionEvent) -> str:
    for path in (
        ("approval", "tool_name"),
        ("permission", "tool_name"),
        ("request", "tool_name"),
        ("tool_use", "name"),
        ("tool", "name"),
    ):
        value = _nested_text(event.metadata, path)
        if value:
            return _normalize_tool(value)
    text = " ".join(part for part in (event.text, event.command) if part)
    match = re.search(r"\b(Bash|Write|Edit|MultiEdit|Read|WebFetch|WebSearch)\b", text)
    return _normalize_tool(match.group(1)) if match else "unknown"


def _decision_text(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    for source in (row, metadata):
        text = _first_text(source, STATUS_COLUMNS)
        if text:
            return text
    for path in (
        ("approval", "decision"),
        ("permission", "decision"),
        ("permissionDecision",),
        ("approvalDecision",),
        ("result", "decision"),
    ):
        text = _nested_text(metadata, path)
        if text:
            return text
    return None


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
        ("approval", "command"),
        ("permission", "command"),
        ("request", "command"),
    ):
        nested = _nested_text(metadata, path)
        if nested:
            return _clean(nested)
    return None


def _elapsed_seconds(start: str | None, end: str | None) -> int | None:
    start_at = _parse_datetime(start)
    end_at = _parse_datetime(end)
    if not start_at or not end_at:
        return None
    elapsed = int((end_at - start_at).total_seconds())
    return elapsed if elapsed >= 0 else None


def _load_rows(conn: Any, table: str, columns: set[str]) -> list[dict[str, Any]]:
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
        "approval_signal": TOOL_COLUMNS + STATUS_COLUMNS + TEXT_COLUMNS + METADATA_COLUMNS,
        "session": SESSION_COLUMNS,
        "timestamp": TIMESTAMP_COLUMNS,
    }
    return tuple(
        name
        for name, candidates in sorted(groups.items())
        if not _first_existing(columns, candidates)
    )


def _event_sort_key(event: _ClaudeSessionEvent) -> tuple[str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudeSessionPermissionDenialRecoveryRow) -> tuple[str, str, str]:
    return (_timestamp_sort(row.denied_at), row.session_id, row.denied_summary)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _normalize_tool(value: str) -> str:
    return re.sub(r"[^a-z0-9_.-]+", "_", value.lower()).strip("_") or "unknown"


def _clean(value: Any) -> str:
    return " ".join(str(value).strip().strip("`'\"").split())


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _looks_like_rows(value: Any) -> bool:
    return not hasattr(value, "execute") and not hasattr(value, "conn") and not isinstance(
        value,
        (str, bytes),
    )
