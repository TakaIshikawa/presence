"""Taxonomize failed Claude Code tool invocations by tool and error class."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Iterable, Mapping

from ingestion.claude_command_failure_summary import normalize_command_prefix
from ingestion.claude_command_retry_recovery import (
    SOURCE_TABLE_CANDIDATES,
    _command,
    _connection,
    _ensure_utc,
    _error_text,
    _event_status,
    _is_row_iterable,
    _metadata,
    _parse_datetime,
    _schema,
    _tool_name,
    load_claude_command_event_rows,
)


DEFAULT_DAYS = 14
DEFAULT_REPRESENTATIVE_LIMIT = 3

ERROR_CLASS_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("timeout", re.compile(r"\b(timed?\s*out|timeout|deadline exceeded)\b", re.IGNORECASE)),
    (
        "permission_denied",
        re.compile(r"\b(permission denied|access denied|operation not permitted|eacces|eperm)\b", re.IGNORECASE),
    ),
    (
        "missing_file",
        re.compile(
            r"\b(no such file|file not found|cannot find|not found|enoent|missing file)\b",
            re.IGNORECASE,
        ),
    ),
    (
        "parse_error",
        re.compile(r"\b(parse error|jsondecodeerror|syntaxerror|invalid json|yaml error)\b", re.IGNORECASE),
    ),
    (
        "command_failed",
        re.compile(r"\b(command failed|exit code [1-9]\d*|non[- ]?zero|exited with code)\b", re.IGNORECASE),
    ),
)


@dataclass(frozen=True)
class ClaudeToolErrorTaxonomyEvent:
    """One normalized failed Claude tool invocation."""

    session_id: str
    project_path: str | None
    timestamp: str | None
    tool_name: str
    error_class: str
    command_prefix: str
    error_snippet: str
    source_table: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeToolErrorTaxonomyRow:
    """One grouped Claude tool error taxonomy row."""

    tool_name: str
    error_class: str
    command_prefix: str
    failure_count: int
    session_count: int
    first_seen_at: str | None
    last_seen_at: str | None
    representative_session_ids: tuple[str, ...]
    error_snippets: tuple[str, ...]
    source_tables: tuple[str, ...]
    taxonomy_id: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["error_snippets"] = list(self.error_snippets)
        payload["representative_session_ids"] = list(self.representative_session_ids)
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeToolErrorTaxonomyReport:
    """Claude tool error taxonomy report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeToolErrorTaxonomyRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_tool_error_taxonomy",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "rows": [row.to_dict() for row in self.rows],
            "source_tables": list(self.source_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_tool_error_taxonomy_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    tool: str | None = None,
    now: datetime | None = None,
) -> ClaudeToolErrorTaxonomyReport:
    """Build a deterministic taxonomy of failed Claude tool invocations."""
    if days <= 0:
        raise ValueError("days must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    normalized_tool = normalize_tool_filter(tool)
    source_tables: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()

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

    events, malformed_metadata_count = load_claude_tool_error_events(
        raw_rows,
        tool=normalized_tool,
    )
    rows = group_tool_error_taxonomy_events(events)

    return ClaudeToolErrorTaxonomyReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "tool": normalized_tool,
        },
        totals={
            "error_event_count": len(events),
            "malformed_metadata_count": malformed_metadata_count,
            "reported_group_count": len(rows),
            "rows_scanned": len(raw_rows),
            "session_count": len({event.session_id for event in events}),
        },
        rows=tuple(rows),
        source_tables=source_tables,
        missing_tables=missing_tables,
    )


def load_claude_tool_error_events(
    rows: Iterable[Mapping[str, Any]],
    *,
    tool: str | None = None,
) -> tuple[list[ClaudeToolErrorTaxonomyEvent], int]:
    """Normalize raw Claude rows into failed tool invocation events."""
    events: list[ClaudeToolErrorTaxonomyEvent] = []
    malformed_metadata_count = 0
    for row in rows:
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        status = _event_status(row, metadata)
        if status != "failed":
            continue
        tool_name = _tool_name(row, metadata)
        if tool and tool_name != tool:
            continue
        error_text = _error_text(row, metadata) or status or "tool failure"
        command = _command(row, metadata)
        command_prefix = normalize_command_prefix(command) if command else ""
        session_id = (
            _first_text(row, ("session_id", "sessionId"))
            or _first_text(metadata, ("session_id", "sessionId"))
            or "unknown-session"
        )
        project_path = (
            _first_text(row, ("project_path", "cwd", "working_directory"))
            or _first_text(metadata, ("project_path", "cwd", "working_directory"))
        )
        timestamp = (
            _first_text(row, ("timestamp", "created_at", "event_time", "event_at"))
            or _first_text(metadata, ("timestamp", "created_at", "event_time", "event_at"))
        )
        events.append(
            ClaudeToolErrorTaxonomyEvent(
                session_id=session_id,
                project_path=project_path,
                timestamp=timestamp,
                tool_name=tool_name,
                error_class=classify_error_text(error_text),
                command_prefix=command_prefix,
                error_snippet=_snippet(error_text),
                source_table=str(row.get("_source_table") or "unknown"),
            )
        )
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def group_tool_error_taxonomy_events(
    events: Iterable[ClaudeToolErrorTaxonomyEvent],
) -> list[ClaudeToolErrorTaxonomyRow]:
    """Group failed tool invocations into deterministic taxonomy rows."""
    grouped: dict[tuple[str, str, str], list[ClaudeToolErrorTaxonomyEvent]] = {}
    for event in events:
        key = (event.tool_name, event.error_class, event.command_prefix)
        grouped.setdefault(key, []).append(event)

    rows = [_taxonomy_row(key, tuple(sorted(group, key=_event_sort_key))) for key, group in grouped.items()]
    return sorted(rows, key=_row_sort_key)


def classify_error_text(error_text: Any) -> str:
    """Classify common Claude tool failure text into a stable error class."""
    text = str(error_text or "")
    for error_class, pattern in ERROR_CLASS_PATTERNS:
        if pattern.search(text):
            return error_class
    return "unknown"


def normalize_tool_filter(tool: str | None) -> str | None:
    """Normalize an optional CLI/API tool filter the same way events do."""
    if tool is None:
        return None
    text = re.sub(r"[^a-z0-9_.-]+", "_", str(tool).lower()).strip("_")
    return text or None


def format_claude_tool_error_taxonomy_json(report: ClaudeToolErrorTaxonomyReport) -> str:
    """Serialize a Claude tool error taxonomy report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _taxonomy_row(
    key: tuple[str, str, str],
    events: tuple[ClaudeToolErrorTaxonomyEvent, ...],
) -> ClaudeToolErrorTaxonomyRow:
    tool_name, error_class, command_prefix = key
    digest = hashlib.sha256(
        f"{tool_name}:{error_class}:{command_prefix}".encode("utf-8")
    ).hexdigest()[:12]
    sessions = tuple(dict.fromkeys(event.session_id for event in events))
    snippets = tuple(dict.fromkeys(event.error_snippet for event in events if event.error_snippet))
    return ClaudeToolErrorTaxonomyRow(
        tool_name=tool_name,
        error_class=error_class,
        command_prefix=command_prefix,
        failure_count=len(events),
        session_count=len(set(sessions)),
        first_seen_at=events[0].timestamp,
        last_seen_at=events[-1].timestamp,
        representative_session_ids=tuple(sorted(sessions)[:DEFAULT_REPRESENTATIVE_LIMIT]),
        error_snippets=snippets[:DEFAULT_REPRESENTATIVE_LIMIT],
        source_tables=tuple(sorted({event.source_table for event in events})),
        taxonomy_id=f"claude_tool_error_taxonomy_{digest}",
    )


def _event_sort_key(event: ClaudeToolErrorTaxonomyEvent) -> tuple[str, str, str, str, str]:
    return (
        _timestamp_sort(event.timestamp),
        event.session_id,
        event.tool_name,
        event.error_class,
        event.command_prefix,
    )


def _row_sort_key(row: ClaudeToolErrorTaxonomyRow) -> tuple[int, str, str, str]:
    return (-row.failure_count, row.tool_name, row.error_class, row.command_prefix)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


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
