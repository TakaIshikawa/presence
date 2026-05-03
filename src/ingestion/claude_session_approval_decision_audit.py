"""Audit Claude sessions for activity after denied approval prompts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_LIMIT = 50
DEFAULT_WINDOW_SIZE = 8

SOURCE_TABLE_CANDIDATES = (
    "claude_session_events",
    "claude_tool_events",
    "claude_events",
)
SESSION_COLUMNS = ("session_id", "sessionId")
TIMESTAMP_COLUMNS = ("timestamp", "created_at", "event_time", "event_at")
PROJECT_COLUMNS = ("project_path", "cwd", "working_directory", "project")
TOOL_COLUMNS = ("tool_name", "tool", "toolName", "name")
STATUS_COLUMNS = ("status", "outcome", "decision", "approval_decision", "permission_decision")
COMMAND_COLUMNS = ("command", "cmd", "shell_command", "input")
TEXT_COLUMNS = (
    "approval_text",
    "prompt",
    "message",
    "content",
    "text",
    "description",
    "reason",
    "output",
    "result",
)
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload")

APPROVAL_TOOL_NAMES = {
    "approval",
    "approval_prompt",
    "permission",
    "permission_prompt",
    "permission_request",
    "user_approval",
}
APPROVED_DECISIONS = {"accept", "accepted", "allow", "allowed", "approve", "approved", "yes"}
BLOCKED_DECISIONS = {
    "cancel",
    "canceled",
    "cancelled",
    "deny",
    "denied",
    "reject",
    "rejected",
    "no",
}
WRITE_LIKE_TOOLS = {"write", "edit", "multiedit", "notebookedit"}
COMMAND_LIKE_TOOLS = {
    "bash",
    "shell",
    "terminal",
    "command",
    "run_command",
    "exec",
}
APPROVAL_RE = re.compile(r"\b(approval|approve|permission|allow|deny|denied|cancelled|canceled)\b", re.I)


@dataclass(frozen=True)
class ClaudeApprovalFollowUpEvidence:
    """One command-like or write-like event after a blocked approval."""

    timestamp: str | None
    tool_name: str
    summary: str
    evidence_type: str
    source_table: str
    event_offset: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionApprovalDecisionAuditRow:
    """One blocked approval followed by suspicious session activity."""

    session_id: str
    project_path: str | None
    approval_at: str | None
    first_follow_up_at: str | None
    last_follow_up_at: str | None
    approval_decision: str
    approval_text: str
    severity: str
    follow_up_count: int
    follow_up_evidence: tuple[ClaudeApprovalFollowUpEvidence, ...]
    source_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["follow_up_evidence"] = [
            evidence.to_dict() for evidence in self.follow_up_evidence
        ]
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeSessionApprovalDecisionAuditReport:
    """Claude session approval decision audit report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionApprovalDecisionAuditRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_approval_decision_audit",
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


def build_claude_session_approval_decision_audit_report(
    db_or_rows: Any,
    *,
    limit: int = DEFAULT_LIMIT,
    window_size: int = DEFAULT_WINDOW_SIZE,
    now: datetime | None = None,
) -> ClaudeSessionApprovalDecisionAuditReport:
    """Build a deterministic audit of denied approvals followed by activity."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if window_size <= 0:
        raise ValueError("window_size must be positive")

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

    events, malformed_metadata_count = _load_events(raw_rows)
    rows = _detect_approval_bypasses(events, window_size=window_size)
    rows = rows[:limit]

    return ClaudeSessionApprovalDecisionAuditReport(
        generated_at=generated_at.isoformat(),
        filters={"limit": limit, "window_size": window_size},
        totals={
            "approved_approval_count": sum(1 for event in events if _approval_decision(event) == "approved"),
            "blocked_approval_count": sum(1 for event in events if _approval_decision(event) in BLOCKED_DECISIONS),
            "flagged_approval_count": len(rows),
            "malformed_metadata_count": malformed_metadata_count,
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
        },
        rows=tuple(rows),
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_session_approval_decision_audit_json(
    report: ClaudeSessionApprovalDecisionAuditReport,
) -> str:
    """Serialize a Claude session approval decision audit report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


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
        project_path = _first_text(row, PROJECT_COLUMNS) or _first_text(metadata, PROJECT_COLUMNS)
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        tool_name = _tool_name(row, metadata)
        events.append(
            _ClaudeSessionEvent(
                session_id=session_id,
                project_path=project_path,
                timestamp=timestamp,
                tool_name=tool_name,
                status=_decision_text(row, metadata) or _first_text(row, STATUS_COLUMNS),
                command=_command(row, metadata),
                text=_event_text(row, metadata),
                source_table=str(row.get("_source_table") or "rows"),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def _detect_approval_bypasses(
    events: Iterable[_ClaudeSessionEvent],
    *,
    window_size: int,
) -> list[ClaudeSessionApprovalDecisionAuditRow]:
    sessions: dict[str, list[_ClaudeSessionEvent]] = {}
    for event in events:
        sessions.setdefault(event.session_id, []).append(event)

    rows: list[ClaudeSessionApprovalDecisionAuditRow] = []
    for session_id, session_events in sorted(sessions.items()):
        ordered = sorted(session_events, key=_event_sort_key)
        for index, event in enumerate(ordered):
            decision = _approval_decision(event)
            if decision not in BLOCKED_DECISIONS:
                continue
            candidates = ordered[index + 1 : index + 1 + window_size]
            evidence = tuple(
                _follow_up_evidence(candidate, offset)
                for offset, candidate in enumerate(candidates, start=1)
                if _is_follow_up_activity(candidate)
            )
            if not evidence:
                continue
            rows.append(_audit_row(session_id, event, evidence))
    return sorted(rows, key=_row_sort_key)


def _audit_row(
    session_id: str,
    approval: _ClaudeSessionEvent,
    evidence: tuple[ClaudeApprovalFollowUpEvidence, ...],
) -> ClaudeSessionApprovalDecisionAuditRow:
    source_tables = {approval.source_table, *(item.source_table for item in evidence)}
    return ClaudeSessionApprovalDecisionAuditRow(
        session_id=session_id,
        project_path=approval.project_path,
        approval_at=approval.timestamp,
        first_follow_up_at=evidence[0].timestamp,
        last_follow_up_at=evidence[-1].timestamp,
        approval_decision=_approval_decision(approval) or "unknown",
        approval_text=_summary(approval.text or approval.command or approval.status or ""),
        severity=_severity(evidence),
        follow_up_count=len(evidence),
        follow_up_evidence=evidence,
        source_tables=tuple(sorted(source_tables)),
    )


def _follow_up_evidence(
    event: _ClaudeSessionEvent,
    offset: int,
) -> ClaudeApprovalFollowUpEvidence:
    evidence_type = "write_like_tool" if _is_write_like(event) else "command_like_activity"
    summary = event.command or event.text or event.tool_name
    return ClaudeApprovalFollowUpEvidence(
        timestamp=event.timestamp,
        tool_name=event.tool_name,
        summary=_summary(summary),
        evidence_type=evidence_type,
        source_table=event.source_table,
        event_offset=offset,
    )


def _approval_decision(event: _ClaudeSessionEvent) -> str | None:
    if not _is_approval_event(event):
        return None
    text = " ".join(part for part in (event.status, event.text, event.tool_name) if part).lower()
    for decision in APPROVED_DECISIONS:
        if re.search(rf"\b{re.escape(decision)}\b", text):
            return "approved"
    for decision in sorted(BLOCKED_DECISIONS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(decision)}\b", text):
            if decision == "cancel":
                return "cancelled"
            return decision
    return None


def _is_approval_event(event: _ClaudeSessionEvent) -> bool:
    return event.tool_name in APPROVAL_TOOL_NAMES or bool(APPROVAL_RE.search(event.text or ""))


def _is_follow_up_activity(event: _ClaudeSessionEvent) -> bool:
    return not _is_approval_event(event) and (_is_write_like(event) or _is_command_like(event))


def _is_write_like(event: _ClaudeSessionEvent) -> bool:
    return event.tool_name in WRITE_LIKE_TOOLS


def _is_command_like(event: _ClaudeSessionEvent) -> bool:
    return event.tool_name in COMMAND_LIKE_TOOLS or bool(event.command)


def _severity(evidence: Iterable[ClaudeApprovalFollowUpEvidence]) -> str:
    evidence_types = {item.evidence_type for item in evidence}
    if "write_like_tool" in evidence_types:
        return "high"
    return "medium"


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
    ):
        nested = _nested_text(metadata, path)
        if nested:
            return _clean(nested)
    return None


def _event_text(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    parts: list[str] = []
    for source in (row, metadata):
        for column in TEXT_COLUMNS:
            value = source.get(column)
            if isinstance(value, (str, int, float)) and str(value).strip():
                parts.append(str(value).strip())
            elif isinstance(value, Mapping):
                nested = _first_text(value, TEXT_COLUMNS)
                if nested:
                    parts.append(nested)
    for path in (
        ("approval", "prompt"),
        ("approval", "text"),
        ("permission", "prompt"),
        ("permission", "text"),
        ("request", "prompt"),
        ("tool_use", "input", "description"),
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
        "status_or_decision": STATUS_COLUMNS,
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


def _event_sort_key(event: _ClaudeSessionEvent) -> tuple[str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudeSessionApprovalDecisionAuditRow) -> tuple[str, str, str]:
    return (_timestamp_sort(row.approval_at), row.session_id, row.approval_text)


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
