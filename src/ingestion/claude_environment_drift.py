"""Report Claude session environment drift across projects."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping

from ingestion.claude_session_approval_decision_audit import (
    COMMAND_COLUMNS,
    METADATA_COLUMNS,
    PROJECT_COLUMNS,
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


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100

# Package manager and runtime inference patterns
PACKAGE_MANAGER_PATTERNS = {
    "uv": re.compile(r"\buv\s+(run|pip|venv|sync|lock|add|remove|tree|tool)\b"),
    "pip": re.compile(r"\bpip\s+(install|uninstall|freeze|list|show)\b"),
    "npm": re.compile(r"\bnpm\s+(install|run|start|test|build|ci)\b"),
    "pnpm": re.compile(r"\bpnpm\s+(install|run|start|test|build|add|remove)\b"),
    "yarn": re.compile(r"\byarn\s+(install|add|remove|run|start|test|build)\b"),
}

PYTHON_INVOCATION_PATTERNS = {
    "uv_run": re.compile(r"\buv\s+run\b"),
    "python": re.compile(r"\bpython\s+"),
    "python3": re.compile(r"\bpython3\s+"),
}

NODE_INVOCATION_PATTERNS = {
    "node": re.compile(r"\bnode\s+"),
}


@dataclass(frozen=True)
class ClaudeEnvironmentDriftCommand:
    """One command that contributed to environment drift."""

    timestamp: str | None
    tool_name: str
    command: str
    package_manager: str | None
    python_invocation: str | None
    node_invocation: str | None
    source_table: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeEnvironmentDriftRow:
    """Environment drift metrics for one project path."""

    project_path: str
    session_count: int
    command_count: int
    package_managers: tuple[str, ...]
    python_invocations: tuple[str, ...]
    node_invocations: tuple[str, ...]
    severity: str
    risk_reason: str
    commands: tuple[ClaudeEnvironmentDriftCommand, ...]
    source_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["package_managers"] = list(self.package_managers)
        payload["python_invocations"] = list(self.python_invocations)
        payload["node_invocations"] = list(self.node_invocations)
        payload["commands"] = [command.to_dict() for command in self.commands]
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeEnvironmentDriftReport:
    """Claude session environment drift report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    risk_summary: dict[str, int]
    rows: tuple[ClaudeEnvironmentDriftRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_environment_drift",
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
class _CommandEvent:
    session_id: str
    project_path: str | None
    timestamp: str | None
    tool_name: str
    command: str
    source_table: str
    ordinal: int


def build_claude_environment_drift_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeEnvironmentDriftReport:
    """Build a deterministic report of environment drift across Claude sessions."""
    if days <= 0:
        raise ValueError("days must be positive")
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
            for row in _load_rows(conn, table, schema[table], days=days, now=generated_at)
        ]

    events, malformed_metadata_count = load_command_events(raw_rows)
    drift_rows = detect_environment_drift(events)
    reported = tuple(drift_rows[:limit])

    return ClaudeEnvironmentDriftReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "limit": limit,
        },
        totals={
            "command_event_count": len(events),
            "drift_project_count": len(drift_rows),
            "malformed_metadata_count": malformed_metadata_count,
            "reported_count": len(reported),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
        },
        risk_summary={
            "high": sum(1 for row in drift_rows if row.severity == "high"),
            "medium": sum(1 for row in drift_rows if row.severity == "medium"),
            "conflicting_package_managers": sum(
                1 for row in drift_rows if len(row.package_managers) > 1
            ),
            "conflicting_python_invocations": sum(
                1 for row in drift_rows if len(row.python_invocations) > 1
            ),
        },
        rows=reported,
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def load_command_events(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[_CommandEvent], int]:
    """Normalize parsed Claude rows into command events."""
    events: list[_CommandEvent] = []
    malformed_metadata_count = 0
    for ordinal, row in enumerate(rows):
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        tool_name = _tool_name(row, metadata)
        command = _command(row, metadata)
        if not command:
            continue
        events.append(
            _CommandEvent(
                session_id=(
                    _first_text(row, SESSION_COLUMNS)
                    or _first_text(metadata, SESSION_COLUMNS)
                    or "unknown-session"
                ),
                project_path=(
                    _first_text(row, PROJECT_COLUMNS) or _first_text(metadata, PROJECT_COLUMNS)
                ),
                timestamp=_first_text(row, TIMESTAMP_COLUMNS)
                or _first_text(metadata, TIMESTAMP_COLUMNS),
                tool_name=tool_name,
                command=command,
                source_table=str(row.get("_source_table") or "rows"),
                ordinal=ordinal,
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def detect_environment_drift(
    events: Iterable[_CommandEvent],
) -> list[ClaudeEnvironmentDriftRow]:
    """Detect per-project environment drift metrics."""
    projects: dict[str, list[_CommandEvent]] = {}
    for event in events:
        project_path = event.project_path or "unknown-project"
        projects.setdefault(project_path, []).append(event)

    rows: list[ClaudeEnvironmentDriftRow] = []
    for project_path, project_events in sorted(projects.items()):
        ordered = sorted(project_events, key=_event_sort_key)
        sessions = {event.session_id for event in ordered}

        # Analyze commands for package managers and runtime invocations
        package_managers: set[str] = set()
        python_invocations: set[str] = set()
        node_invocations: set[str] = set()
        evidence: list[ClaudeEnvironmentDriftCommand] = []

        for event in ordered:
            pm = _detect_package_manager(event.command)
            py_invoc = _detect_python_invocation(event.command)
            node_invoc = _detect_node_invocation(event.command)

            if pm:
                package_managers.add(pm)
            if py_invoc:
                python_invocations.add(py_invoc)
            if node_invoc:
                node_invocations.add(node_invoc)

            # Record commands that contribute to drift
            if pm or py_invoc or node_invoc:
                evidence.append(
                    ClaudeEnvironmentDriftCommand(
                        timestamp=event.timestamp,
                        tool_name=event.tool_name,
                        command=_summary(event.command),
                        package_manager=pm,
                        python_invocation=py_invoc,
                        node_invocation=node_invoc,
                        source_table=event.source_table,
                    )
                )

        # Only report if there's conflicting usage
        has_package_manager_drift = len(package_managers) > 1
        has_python_invocation_drift = len(python_invocations) > 1
        has_node_invocation_drift = len(node_invocations) > 1

        if not (has_package_manager_drift or has_python_invocation_drift or has_node_invocation_drift):
            continue

        # Determine severity
        if has_package_manager_drift:
            severity = "high"
            risk_reason = "conflicting_package_managers"
        elif has_python_invocation_drift:
            severity = "medium"
            risk_reason = "conflicting_python_invocations"
        else:
            severity = "medium"
            risk_reason = "conflicting_node_invocations"

        rows.append(
            ClaudeEnvironmentDriftRow(
                project_path=project_path,
                session_count=len(sessions),
                command_count=len(ordered),
                package_managers=tuple(sorted(package_managers)),
                python_invocations=tuple(sorted(python_invocations)),
                node_invocations=tuple(sorted(node_invocations)),
                severity=severity,
                risk_reason=risk_reason,
                commands=tuple(evidence[:20]),  # Limit to first 20 for brevity
                source_tables=tuple(sorted({event.source_table for event in ordered})),
            )
        )

    return sorted(rows, key=_row_sort_key)


def format_claude_environment_drift_json(report: ClaudeEnvironmentDriftReport) -> str:
    """Serialize an environment drift report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_environment_drift_text(report: ClaudeEnvironmentDriftReport) -> str:
    """Render a compact environment drift report."""
    totals = report.totals
    risk = report.risk_summary
    lines = [
        "Claude Session Environment Drift",
        f"Generated: {report.generated_at}",
        f"Filters: days={report.filters['days']} limit={report.filters['limit']}",
        (
            "Totals: "
            f"rows={totals['rows_scanned']} commands={totals['command_event_count']} "
            f"sessions={totals['session_count']} drift_projects={totals['drift_project_count']}"
        ),
        (
            "Risk: "
            f"high={risk['high']} medium={risk['medium']} "
            f"conflicting_package_managers={risk['conflicting_package_managers']} "
            f"conflicting_python_invocations={risk['conflicting_python_invocations']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        for table, columns in sorted(report.missing_columns.items()):
            lines.append(f"Missing columns for {table}: " + ", ".join(columns))
    if not report.rows:
        lines.extend(["", "No environment drift detected."])
        return "\n".join(lines)
    lines.extend(["", "Projects:"])
    for row in report.rows:
        lines.append(
            f"- project={row.project_path} severity={row.severity} "
            f"sessions={row.session_count} commands={row.command_count}"
        )
        if row.package_managers:
            lines.append(f"  package_managers: {', '.join(row.package_managers)}")
        if row.python_invocations:
            lines.append(f"  python_invocations: {', '.join(row.python_invocations)}")
        if row.node_invocations:
            lines.append(f"  node_invocations: {', '.join(row.node_invocations)}")
        for command in row.commands[:3]:
            parts = [f"  {command.timestamp or '-'} {command.tool_name}"]
            if command.package_manager:
                parts.append(f"pm={command.package_manager}")
            if command.python_invocation:
                parts.append(f"py={command.python_invocation}")
            if command.node_invocation:
                parts.append(f"node={command.node_invocation}")
            parts.append(f"cmd={command.command}")
            lines.append(" ".join(parts))
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


def _detect_package_manager(command: str) -> str | None:
    """Detect package manager from command text."""
    for manager, pattern in PACKAGE_MANAGER_PATTERNS.items():
        if pattern.search(command):
            return manager
    return None


def _detect_python_invocation(command: str) -> str | None:
    """Detect Python invocation style from command text."""
    # Check for uv run first - it takes precedence over bare python/python3
    if PYTHON_INVOCATION_PATTERNS["uv_run"].search(command):
        return "uv_run"
    # Only check for bare python/python3 if no uv run
    for invocation in ("python", "python3"):
        if PYTHON_INVOCATION_PATTERNS[invocation].search(command):
            return invocation
    return None


def _detect_node_invocation(command: str) -> str | None:
    """Detect Node invocation from command text."""
    for invocation, pattern in NODE_INVOCATION_PATTERNS.items():
        if pattern.search(command):
            return invocation
    return None


def _load_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    *,
    days: int,
    now: datetime,
) -> list[dict[str, Any]]:
    select_columns = ", ".join(_quote_identifier(column) for column in sorted(columns))
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    where = []
    params: list[Any] = []
    if timestamp_column:
        cutoff = now - timedelta(days=days)
        where.append(f"datetime({_quote_identifier(timestamp_column)}) >= datetime(?)")
        params.append(cutoff.isoformat())
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    order_sql = f"{_quote_identifier(timestamp_column) if timestamp_column else 'rowid'} ASC, rowid ASC"
    cursor = conn.execute(
        f"SELECT {select_columns} FROM {_quote_identifier(table)} {where_clause} ORDER BY {order_sql}",
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


def _missing_optional_columns(columns: set[str]) -> tuple[str, ...]:
    groups = {
        "command": COMMAND_COLUMNS,
        "metadata": METADATA_COLUMNS,
        "project": PROJECT_COLUMNS,
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


def _row_sort_key(row: ClaudeEnvironmentDriftRow) -> tuple[int, int, int, str]:
    severity_rank = {"high": 0, "medium": 1}.get(row.severity, 2)
    return (
        severity_rank,
        -len(row.package_managers),
        -len(row.python_invocations),
        row.project_path,
    )


def _event_sort_key(event: _CommandEvent) -> tuple[str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.ordinal)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _summary(value: str, *, limit: int = 200) -> str:
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
