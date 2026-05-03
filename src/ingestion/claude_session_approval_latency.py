"""Report Claude Code approval request-to-decision latency."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping

from ingestion.claude_session_approval_decision_audit import (
    APPROVED_DECISIONS,
    BLOCKED_DECISIONS,
    APPROVAL_RE,
    APPROVAL_TOOL_NAMES,
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


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 50
DEFAULT_SLOW_THRESHOLD_SECONDS = 300


@dataclass(frozen=True)
class ClaudeSessionApprovalLatencyRow:
    """One approval request paired with its later decision."""

    session_id: str
    project_path: str | None
    request_at: str | None
    decision_at: str | None
    elapsed_seconds: int | None
    approval_decision: str
    is_slow: bool
    request_text: str
    decision_text: str
    source_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["source_tables"] = list(self.source_tables)
        return payload


@dataclass(frozen=True)
class ClaudeSessionApprovalLatencyReport:
    """Claude session approval latency report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    rows: tuple[ClaudeSessionApprovalLatencyRow, ...]
    source_tables: tuple[str, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_approval_latency",
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
class _ApprovalEvent:
    session_id: str
    project_path: str | None
    timestamp: str | None
    tool_name: str
    status: str | None
    text: str | None
    decision: str | None
    source_table: str
    ordinal: int


def build_claude_session_approval_latency_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    slow_threshold_seconds: int = DEFAULT_SLOW_THRESHOLD_SECONDS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionApprovalLatencyReport:
    """Build a deterministic approval latency report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if slow_threshold_seconds <= 0:
        raise ValueError("slow_threshold_seconds must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}

    if _looks_like_rows(db_or_rows):
        raw_rows = [_mapping(row) for row in db_or_rows]
        raw_rows = _filter_rows(raw_rows, cutoff=cutoff)
        source_tables = tuple(
            sorted({str(row.get("_source_table") or "rows") for row in raw_rows})
        ) or ("rows",)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        source_tables = tuple(table for table in SOURCE_TABLE_CANDIDATES if table in schema)
        missing_tables = () if source_tables else SOURCE_TABLE_CANDIDATES
        loadable_tables = []
        for table in source_tables:
            table_missing = _missing_required_columns(schema[table])
            if table_missing:
                missing_columns[table] = table_missing
            else:
                loadable_tables.append(table)
        raw_rows = [
            row
            for table in loadable_tables
            for row in _load_rows(conn, table, schema[table], cutoff=cutoff)
        ]

    events, malformed_metadata_count = _load_approval_events(raw_rows)
    paired_rows, missing_decision_count = pair_approval_requests(
        events,
        slow_threshold_seconds=slow_threshold_seconds,
    )
    reported = tuple(paired_rows[:limit])

    return ClaudeSessionApprovalLatencyReport(
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "limit": limit,
            "lookback_end": generated_at.isoformat(),
            "lookback_start": cutoff.isoformat(),
            "slow_threshold_seconds": slow_threshold_seconds,
        },
        totals={
            "approved_count": sum(1 for row in paired_rows if row.approval_decision == "approved"),
            "denied_count": sum(1 for row in paired_rows if row.approval_decision == "denied"),
            "malformed_metadata_count": malformed_metadata_count,
            "missing_decision_count": missing_decision_count,
            "paired_count": len(paired_rows),
            "rows_scanned": len(raw_rows),
            "slow_count": sum(1 for row in paired_rows if row.is_slow),
        },
        rows=reported,
        source_tables=source_tables,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def pair_approval_requests(
    events: Iterable[_ApprovalEvent],
    *,
    slow_threshold_seconds: int = DEFAULT_SLOW_THRESHOLD_SECONDS,
) -> tuple[list[ClaudeSessionApprovalLatencyRow], int]:
    """Pair approval requests with the next decision event in the same session."""
    sessions: dict[str, list[_ApprovalEvent]] = {}
    for event in events:
        sessions.setdefault(event.session_id, []).append(event)

    rows: list[ClaudeSessionApprovalLatencyRow] = []
    missing_decision_count = 0
    for session_id, session_events in sorted(sessions.items()):
        ordered = sorted(session_events, key=_event_sort_key)
        used_decisions: set[int] = set()
        for index, event in enumerate(ordered):
            if not _is_approval_request(event):
                continue
            decision_index, decision = _next_decision(
                ordered[index + 1 :],
                used_decisions,
                base_index=index + 1,
            )
            if decision is None:
                missing_decision_count += 1
                continue
            used_decisions.add(decision_index + index + 1)
            rows.append(
                _latency_row(
                    session_id,
                    event,
                    decision,
                    slow_threshold_seconds=slow_threshold_seconds,
                )
            )
    return sorted(rows, key=_row_sort_key), missing_decision_count


def format_claude_session_approval_latency_json(
    report: ClaudeSessionApprovalLatencyReport,
) -> str:
    """Serialize an approval latency report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_approval_latency_text(
    report: ClaudeSessionApprovalLatencyReport,
) -> str:
    """Render a concise approval latency report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Session Approval Latency",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"slow_threshold_seconds={filters['slow_threshold_seconds']}"
        ),
        (
            "Totals: "
            f"rows={totals['rows_scanned']} paired={totals['paired_count']} "
            f"missing_decisions={totals['missing_decision_count']} "
            f"slow={totals['slow_count']} approved={totals['approved_count']} "
            f"denied={totals['denied_count']} "
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
        lines.extend(["", "No approval latency pairs found."])
        return "\n".join(lines)

    lines.extend(["", "Approvals:"])
    for row in report.rows:
        elapsed = "-" if row.elapsed_seconds is None else f"{row.elapsed_seconds}s"
        slow = " slow" if row.is_slow else ""
        lines.append(
            f"- session={row.session_id} decision={row.approval_decision} "
            f"elapsed={elapsed}{slow} request_at={row.request_at or '-'} "
            f"decision_at={row.decision_at or '-'}"
        )
        if row.request_text:
            lines.append(f"  request={row.request_text}")
    return "\n".join(lines)


def _load_approval_events(rows: Iterable[Mapping[str, Any]]) -> tuple[list[_ApprovalEvent], int]:
    events: list[_ApprovalEvent] = []
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
        status = _decision_text(row, metadata) or _first_text(row, STATUS_COLUMNS)
        text = _event_text(row, metadata)
        tool_name = _tool_name(row, metadata)
        event = _ApprovalEvent(
            session_id=session_id,
            project_path=_first_text(row, PROJECT_COLUMNS) or _first_text(metadata, PROJECT_COLUMNS),
            timestamp=_first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS),
            tool_name=tool_name,
            status=status,
            text=text,
            decision=_approval_decision(tool_name, status, text),
            source_table=str(row.get("_source_table") or "rows"),
            ordinal=ordinal,
        )
        if _is_approval_signal(event):
            events.append(event)
    return sorted(events, key=_event_sort_key), malformed_metadata_count


def _latency_row(
    session_id: str,
    request: _ApprovalEvent,
    decision: _ApprovalEvent,
    *,
    slow_threshold_seconds: int,
) -> ClaudeSessionApprovalLatencyRow:
    elapsed_seconds = _elapsed_seconds(request.timestamp, decision.timestamp)
    decision_bucket = decision.decision or "unknown"
    if decision_bucket in BLOCKED_DECISIONS:
        decision_bucket = "denied"
    return ClaudeSessionApprovalLatencyRow(
        session_id=session_id,
        project_path=request.project_path or decision.project_path,
        request_at=request.timestamp,
        decision_at=decision.timestamp,
        elapsed_seconds=elapsed_seconds,
        approval_decision=decision_bucket,
        is_slow=elapsed_seconds is not None and elapsed_seconds >= slow_threshold_seconds,
        request_text=_summary(request.text or request.status or request.tool_name),
        decision_text=_summary(decision.text or decision.status or decision.tool_name),
        source_tables=tuple(sorted({request.source_table, decision.source_table})),
    )


def _next_decision(
    candidates: Iterable[_ApprovalEvent],
    used_decisions: set[int],
    *,
    base_index: int,
) -> tuple[int, _ApprovalEvent | None]:
    for offset, candidate in enumerate(candidates):
        if base_index + offset in used_decisions:
            continue
        if _is_approval_decision(candidate):
            return offset, candidate
    return -1, None


def _is_approval_signal(event: _ApprovalEvent) -> bool:
    return event.tool_name in APPROVAL_TOOL_NAMES or bool(
        APPROVAL_RE.search(" ".join(part for part in (event.status, event.text) if part))
    )


def _is_approval_request(event: _ApprovalEvent) -> bool:
    if event.decision:
        return False
    text = " ".join(part for part in (event.status, event.text, event.tool_name) if part).lower()
    return bool(
        re.search(r"\b(request|requested|pending|prompt|permission|approval|approve|allow)\b", text)
    )


def _is_approval_decision(event: _ApprovalEvent) -> bool:
    return event.decision in {"approved", "denied"} or event.decision in BLOCKED_DECISIONS


def _approval_decision(tool_name: str, status: str | None, text: str | None) -> str | None:
    if tool_name not in APPROVAL_TOOL_NAMES and not APPROVAL_RE.search(text or ""):
        return None
    status_text = (status or "").lower()
    if re.search(r"\b(request|requested|pending|prompt|ask|asking)\b", status_text):
        return None
    status_decision = _decision_from_text(status_text)
    if status_decision:
        return status_decision
    text_decision = _decision_from_text(text or "")
    if text_decision:
        return text_decision
    return None


def _decision_from_text(value: str) -> str | None:
    text = value.lower()
    for decision in APPROVED_DECISIONS:
        if re.search(rf"\b{re.escape(decision)}\b", text):
            return "approved"
    for decision in sorted(BLOCKED_DECISIONS, key=len, reverse=True):
        if re.search(rf"\b{re.escape(decision)}\b", text):
            return "denied"
    return None


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


def _elapsed_seconds(start: str | None, end: str | None) -> int | None:
    start_at = _parse_datetime(start)
    end_at = _parse_datetime(end)
    if not start_at or not end_at:
        return None
    elapsed = int((end_at - start_at).total_seconds())
    return elapsed if elapsed >= 0 else None


def _load_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    select_columns = ", ".join(_quote_identifier(column) for column in sorted(columns))
    timestamp_column = _first_existing(columns, TIMESTAMP_COLUMNS)
    where_sql = ""
    params: list[Any] = []
    if timestamp_column:
        where_sql = f"WHERE {_quote_identifier(timestamp_column)} >= ?"
        params.append(cutoff.isoformat())
    order_sql = f"{_quote_identifier(timestamp_column) if timestamp_column else 'rowid'} ASC, rowid ASC"
    cursor = conn.execute(
        f"SELECT {select_columns} FROM {_quote_identifier(table)} {where_sql} ORDER BY {order_sql}",
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


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for row in rows:
        metadata, _malformed = _metadata(row)
        timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
        parsed = _parse_datetime(timestamp)
        if parsed is None or parsed >= cutoff:
            filtered.append(row)
    return filtered


def _missing_required_columns(columns: set[str]) -> tuple[str, ...]:
    groups = {
        "approval_signal": TOOL_COLUMNS + STATUS_COLUMNS + TEXT_COLUMNS + METADATA_COLUMNS + COMMAND_COLUMNS,
        "session": SESSION_COLUMNS,
        "timestamp": TIMESTAMP_COLUMNS,
    }
    return tuple(
        name
        for name, candidates in sorted(groups.items())
        if not _first_existing(columns, candidates)
    )


def _event_sort_key(event: _ApprovalEvent) -> tuple[str, str, int]:
    return (_timestamp_sort(event.timestamp), event.session_id, event.ordinal)


def _row_sort_key(row: ClaudeSessionApprovalLatencyRow) -> tuple[str, str, str]:
    return (_timestamp_sort(row.request_at), row.session_id, row.request_text)


def _timestamp_sort(value: Any) -> str:
    parsed = _parse_datetime(value)
    return parsed.isoformat() if parsed else str(value or "")


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    return dict(row)


def _looks_like_rows(value: Any) -> bool:
    return not hasattr(value, "execute") and not hasattr(value, "conn") and not isinstance(
        value,
        (str, bytes),
    )
