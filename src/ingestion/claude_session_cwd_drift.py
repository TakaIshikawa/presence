"""Report Claude session working-directory drift."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
from typing import Any, Iterable, Mapping

from ingestion.claude_session_approval_decision_audit import (
    COMMAND_COLUMNS,
    METADATA_COLUMNS,
    SESSION_COLUMNS,
    SOURCE_TABLE_CANDIDATES,
    TEXT_COLUMNS,
    TIMESTAMP_COLUMNS,
    TOOL_COLUMNS,
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


DEFAULT_LIMIT = 50
DEFAULT_MIN_CWD_CHANGES = 2

CWD_COLUMNS = ("cwd", "working_directory", "workingDirectory")
PROJECT_ROOT_COLUMNS = ("project_root", "repo_root", "project_path", "project")
COMMAND_LIKE_TOOLS = {"bash", "shell", "terminal", "command", "run_command", "exec"}


@dataclass(frozen=True)
class ClaudeSessionCwdDriftCommand:
    """One command/tool event that contributed to cwd drift."""

    timestamp: str | None
    tool_name: str
    cwd: str
    project_root: str
    command: str
    drift_type: str
    outside_project_root: bool
    source_table: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionCwdDriftRow:
    """Working-directory drift metrics for one Claude session."""

    session_id: str
    severity: str
    risk_reason: str
    command_count: int
    cwd_change_count: int
    outside_project_root_count: int
    repeated_cwd_change: bool
    distinct_cwds: tuple[str, ...]
    distinct_project_roots: tuple[str, ...]
    commands: tuple[ClaudeSessionCwdDriftCommand, ...]
    source_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["distinct_cwds"] = list(self.distinct_cwds)
        payload["distinct_project_roots"] = list(self.distinct_project_roots)
        payload["commands"] = [command.to_dict() for command in self.commands]
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeSessionCwdDriftReport:
    """Claude session cwd drift report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    risk_summary: dict[str, int]
    rows: tuple[ClaudeSessionCwdDriftRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_cwd_drift",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "risk_summary": dict(sorted(self.risk_summary.items())),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


@dataclass(frozen=True)
class _CwdEvent:
    session_id: str
    timestamp: str | None
    tool_name: str
    command: str | None
    cwd: str | None
    project_root: str | None
    source_table: str
    ordinal: int


def build_claude_session_cwd_drift_report(
    db_or_rows: Any,
    *,
    project_root: str,
    limit: int = DEFAULT_LIMIT,
    min_cwd_changes: int = DEFAULT_MIN_CWD_CHANGES,
    now: datetime | None = None,
) -> ClaudeSessionCwdDriftReport:
    """Build a deterministic report of cwd drift in Claude command sessions."""
    if not str(project_root or "").strip():
        raise ValueError("project_root is required")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_cwd_changes <= 0:
        raise ValueError("min_cwd_changes must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    configured_root = _normalize_path(project_root, None)
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

    events, malformed_metadata_count = load_cwd_events(raw_rows, configured_root=configured_root)
    drift_rows = detect_cwd_drift(
        events,
        configured_root=configured_root,
        min_cwd_changes=min_cwd_changes,
    )
    reported = tuple(drift_rows[:limit])

    return ClaudeSessionCwdDriftReport(
        generated_at=generated_at.isoformat(),
        filters={
            "limit": limit,
            "min_cwd_changes": min_cwd_changes,
            "project_root": configured_root,
        },
        totals={
            "command_event_count": len(events),
            "drift_session_count": len(drift_rows),
            "malformed_metadata_count": malformed_metadata_count,
            "reported_count": len(reported),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
        },
        risk_summary={
            "high": sum(1 for row in drift_rows if row.severity == "high"),
            "medium": sum(1 for row in drift_rows if row.severity == "medium"),
            "outside_project_root_commands": sum(
                row.outside_project_root_count for row in drift_rows
            ),
            "repeated_cwd_change_sessions": sum(
                1 for row in drift_rows if row.repeated_cwd_change
            ),
        },
        rows=reported,
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def load_cwd_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    configured_root: str,
) -> tuple[list[_CwdEvent], int]:
    """Normalize parsed Claude rows into command/tool cwd events."""
    events: list[_CwdEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        tool_name = _tool_name(row, metadata)
        command = _command(row, metadata)
        if not _is_command_event(tool_name, command):
            continue
        project_root = _project_root(row, metadata) or configured_root
        cwd = _cwd(row, metadata) or project_root
        events.append(
            _CwdEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                timestamp=_first_text(row, TIMESTAMP_COLUMNS)
                or _first_text(metadata, TIMESTAMP_COLUMNS),
                tool_name=tool_name,
                command=command,
                cwd=_normalize_path(cwd, project_root),
                project_root=_normalize_path(project_root, configured_root),
                source_table=str(row.get("_source_table") or "rows"),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def detect_cwd_drift(
    events: Iterable[_CwdEvent],
    *,
    configured_root: str,
    min_cwd_changes: int = DEFAULT_MIN_CWD_CHANGES,
) -> list[ClaudeSessionCwdDriftRow]:
    """Detect per-session cwd drift metrics."""
    sessions: dict[str, list[_CwdEvent]] = {}
    for event in events:
        sessions.setdefault(event.session_id, []).append(event)

    rows: list[ClaudeSessionCwdDriftRow] = []
    for session_id, session_events in sorted(sessions.items()):
        ordered = sorted(session_events, key=_event_sort_key)
        cwds = tuple(dict.fromkeys(event.cwd for event in ordered if event.cwd))
        roots = tuple(dict.fromkeys(event.project_root for event in ordered if event.project_root))
        cwd_change_count = sum(
            1
            for previous, current in zip(ordered, ordered[1:])
            if previous.cwd and current.cwd and previous.cwd != current.cwd
        )
        repeated = cwd_change_count >= min_cwd_changes
        evidence: list[ClaudeSessionCwdDriftCommand] = []
        for event in ordered:
            if not event.cwd:
                continue
            outside = not _is_relative_to(event.cwd, configured_root)
            drift_type: str | None = None
            if outside:
                drift_type = "outside_project_root"
            elif repeated and event.cwd != configured_root:
                drift_type = "subdirectory_change"
            if not drift_type:
                continue
            evidence.append(
                ClaudeSessionCwdDriftCommand(
                    timestamp=event.timestamp,
                    tool_name=event.tool_name,
                    cwd=event.cwd,
                    project_root=event.project_root or configured_root,
                    command=_summary(event.command or event.tool_name),
                    drift_type=drift_type,
                    outside_project_root=outside,
                    source_table=event.source_table,
                )
            )
        outside_count = sum(1 for command in evidence if command.outside_project_root)
        if not outside_count and not repeated:
            continue
        rows.append(
            ClaudeSessionCwdDriftRow(
                session_id=session_id,
                severity="high" if outside_count else "medium",
                risk_reason=(
                    "command_cwd_outside_project_root"
                    if outside_count
                    else "repeated_cwd_changes_within_project"
                ),
                command_count=len(ordered),
                cwd_change_count=cwd_change_count,
                outside_project_root_count=outside_count,
                repeated_cwd_change=repeated,
                distinct_cwds=tuple(sorted(cwds)),
                distinct_project_roots=tuple(sorted(roots)),
                commands=tuple(evidence[:10]),
                source_tables=tuple(sorted({event.source_table for event in ordered})),
            )
        )
    return sorted(rows, key=_row_sort_key)


def format_claude_session_cwd_drift_json(report: ClaudeSessionCwdDriftReport) -> str:
    """Serialize a cwd drift report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_cwd_drift_text(report: ClaudeSessionCwdDriftReport) -> str:
    """Render a compact cwd drift report."""
    totals = report.totals
    risk = report.risk_summary
    lines = [
        "Claude Session Working Directory Drift",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"project_root={report.filters['project_root']} "
            f"min_cwd_changes={report.filters['min_cwd_changes']} "
            f"limit={report.filters['limit']}"
        ),
        (
            "Totals: "
            f"rows={totals['rows_scanned']} commands={totals['command_event_count']} "
            f"sessions={totals['session_count']} drift_sessions={totals['drift_session_count']} "
            f"outside_commands={risk['outside_project_root_commands']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        for table, columns in sorted(report.missing_columns.items()):
            lines.append(f"Missing columns for {table}: " + ", ".join(columns))
    if not report.rows:
        lines.extend(["", "No cwd drift detected."])
        return "\n".join(lines)
    lines.extend(["", "Sessions:"])
    for row in report.rows:
        lines.append(
            f"- session={row.session_id} severity={row.severity} "
            f"cwd_changes={row.cwd_change_count} outside={row.outside_project_root_count}"
        )
        for command in row.commands[:3]:
            lines.append(
                f"  {command.timestamp or '-'} {command.drift_type} "
                f"cwd={command.cwd} command={command.command}"
            )
    return "\n".join(lines)


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


def _cwd(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    for source in (row, metadata):
        text = _first_text(source, CWD_COLUMNS)
        if text:
            return text
    for path in (
        ("tool_input", "cwd"),
        ("input", "cwd"),
        ("tool", "input", "cwd"),
        ("tool_use", "input", "cwd"),
    ):
        text = _nested_text(metadata, path)
        if text:
            return text
    return None


def _project_root(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    for source in (row, metadata):
        text = _first_text(source, PROJECT_ROOT_COLUMNS)
        if text:
            return text
    for path in (
        ("tool_input", "project_root"),
        ("input", "project_root"),
        ("tool", "input", "project_root"),
        ("tool_use", "input", "project_root"),
    ):
        text = _nested_text(metadata, path)
        if text:
            return text
    return None


def _is_command_event(tool_name: str, command: str | None) -> bool:
    return bool(command) or tool_name in COMMAND_LIKE_TOOLS


def _normalize_path(value: str, base: str | None) -> str:
    raw = os.path.expanduser(str(value).strip())
    path = Path(raw)
    if not path.is_absolute() and base:
        path = Path(base) / path
    return str(path.resolve(strict=False))


def _is_relative_to(path: str, root: str) -> bool:
    try:
        Path(path).resolve(strict=False).relative_to(Path(root).resolve(strict=False))
    except ValueError:
        return False
    return True


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
        "command": COMMAND_COLUMNS,
        "cwd": CWD_COLUMNS + PROJECT_ROOT_COLUMNS,
        "metadata": METADATA_COLUMNS,
        "session": SESSION_COLUMNS,
        "timestamp": TIMESTAMP_COLUMNS,
        "tool": TOOL_COLUMNS,
        "text": TEXT_COLUMNS,
    }
    return tuple(
        name
        for name, candidates in sorted(groups.items())
        if not _first_existing(columns, candidates)
    )


def _row_sort_key(row: ClaudeSessionCwdDriftRow) -> tuple[int, int, str]:
    severity_rank = {"high": 0, "medium": 1}.get(row.severity, 2)
    return (severity_rank, -row.outside_project_root_count, row.session_id)


def _event_sort_key(event: _CwdEvent) -> tuple[str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.ordinal)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _summary(value: str, *, limit: int = 240) -> str:
    text = _clean(value)
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


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
