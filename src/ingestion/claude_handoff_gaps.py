"""Detect Claude sessions that ended without a usable handoff."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 20
DEFAULT_EXCERPT_CHARS = 180

EVENT_TABLE_CANDIDATES = (
    "claude_session_events",
    "claude_tool_events",
    "claude_events",
)
TEXT_TABLE = "claude_messages"
SESSION_COLUMNS = ("session_id", "sessionId")
TIMESTAMP_COLUMNS = ("timestamp", "created_at", "event_time", "event_at")
ROLE_COLUMNS = ("role", "message_role", "speaker", "type")
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
    "error",
    "error_message",
    "stderr",
    "output",
    "result",
)

GAP_TYPES = (
    "missing_next_step",
    "unresolved_blocker",
    "dangling_todo",
    "ended_after_error",
)

NEXT_STEP_RE = re.compile(
    r"\b(next steps?|follow[- ]?ups?|handoff|remaining work|recommendation|"
    r"continue by|pick up)\b",
    re.IGNORECASE,
)
TODO_RE = re.compile(
    r"\b(todo|fixme|follow[- ]?up|remaining|still need(?:s|ed)?|need to|"
    r"needs to|left to|not yet)\b",
    re.IGNORECASE,
)
BLOCKER_RE = re.compile(
    r"\b(blocked|blocker|cannot|can't|unable|stuck|waiting on|needs human|"
    r"manual intervention|permission denied|credentials?|missing dependency|"
    r"failing tests?)\b",
    re.IGNORECASE,
)
RESOLUTION_RE = re.compile(
    r"\b(resolved|fixed|unblocked|completed|done|passing|validated|verified)\b",
    re.IGNORECASE,
)
FAILURE_WORD_RE = re.compile(
    r"\b(error|failed|failure|exception|traceback|timeout|timed out|denied|"
    r"non[- ]?zero|exit code|no such file)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ClaudeHandoffGapExample:
    """Representative session for one handoff gap type."""

    session_id: str
    project_path: str | None
    last_timestamp: str | None
    final_role: str
    message_count: int
    gap_types: tuple[str, ...]
    final_excerpt: str
    error_excerpt: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["gap_types"] = list(self.gap_types)
        return payload


@dataclass(frozen=True)
class ClaudeHandoffGapGroup:
    """Sessions grouped by a detected handoff gap type."""

    gap_type: str
    count: int
    examples: tuple[ClaudeHandoffGapExample, ...]
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["examples"] = [example.to_dict() for example in self.examples]
        return payload


@dataclass(frozen=True)
class ClaudeHandoffGapReport:
    """Claude handoff gap report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    groups: tuple[ClaudeHandoffGapGroup, ...]
    source_table: str | None = None
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_handoff_gaps",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "groups": [group.to_dict() for group in self.groups],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "source_table": self.source_table,
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_handoff_gaps_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeHandoffGapReport:
    """Build a deterministic report of recent Claude sessions needing follow-up."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
    }
    source_table = "rows"
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}

    if _looks_like_rows(db_or_rows):
        rows = [_normalize_row(_mapping(row), source_table="rows") for row in db_or_rows]
        rows = _filter_rows(rows, cutoff=cutoff)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        source_table = _source_table(schema)
        if source_table is None:
            missing_tables = (*EVENT_TABLE_CANDIDATES, TEXT_TABLE)
            rows = []
        else:
            missing = _missing_required_columns(schema[source_table], source_table)
            if missing:
                missing_columns = {source_table: missing}
                rows = []
            else:
                rows = _load_rows(
                    conn,
                    source_table,
                    schema[source_table],
                    cutoff=cutoff,
                )

    examples = _detect_session_gaps(rows)
    groups = _group_examples(examples, limit=limit)
    flagged_sessions = {example.session_id for example in examples}
    return ClaudeHandoffGapReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "gap_count": sum(group.count for group in groups),
            "groups": len(groups),
            "rows_scanned": len(rows),
            "sessions_flagged": len(flagged_sessions),
            "sessions_scanned": len(_session_keys(rows)),
        },
        groups=tuple(groups),
        source_table=source_table if rows or not missing_tables else None,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_handoff_gaps_json(report: ClaudeHandoffGapReport) -> str:
    """Serialize a handoff-gap report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_handoff_gaps_text(report: ClaudeHandoffGapReport) -> str:
    """Render a concise human-readable handoff-gap report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Handoff Gaps",
        f"Generated: {report.generated_at}",
        (
            "Lookback: "
            f"days={filters['days']} start={filters['lookback_start']} "
            f"end={filters['lookback_end']} limit={filters['limit']}"
        ),
        (
            "Totals: "
            f"sessions={totals['sessions_scanned']} "
            f"flagged={totals['sessions_flagged']} "
            f"gaps={totals['gap_count']} rows={totals['rows_scanned']}"
        ),
    ]
    if report.source_table:
        lines.append(f"Source table: {report.source_table}")
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in (report.missing_columns or {}).items()
        if columns
    ]
    if missing_columns:
        lines.append("Missing columns: " + "; ".join(missing_columns))

    lines.extend(["", "Gap types:"])
    if not report.groups:
        lines.append("- none")
    for group in report.groups:
        lines.append(
            f"- {group.gap_type} count={group.count} "
            f"recommendation={group.recommendation}"
        )
        for example in group.examples:
            lines.append(
                f"  example session={example.session_id} "
                f"role={example.final_role} latest={example.last_timestamp or '-'} "
                f"gaps={','.join(example.gap_types)}"
            )
            lines.append(f"    final: {example.final_excerpt}")
            if example.error_excerpt:
                lines.append(f"    error: {example.error_excerpt}")
    return "\n".join(lines)


def _detect_session_gaps(rows: Iterable[dict[str, Any]]) -> list[ClaudeHandoffGapExample]:
    grouped: dict[tuple[str, str | None], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        session_id = _optional_text(row.get("session_id")) or "unknown-session"
        grouped[(session_id, _optional_text(row.get("project_path")))].append(row)

    examples: list[ClaudeHandoffGapExample] = []
    for (session_id, project_path), session_rows in grouped.items():
        ordered = sorted(session_rows, key=_row_sort_key)
        if not ordered:
            continue
        final = _last_conversation_row(ordered) or ordered[-1]
        tail = ordered[-5:]
        final_text = _optional_text(final.get("text")) or ""
        combined_tail = "\n".join(_optional_text(row.get("text")) or "" for row in tail)
        gap_types = _gap_types(final, final_text, combined_tail, tail)
        if not gap_types:
            continue
        latest_error = _latest_error(tail)
        examples.append(
            ClaudeHandoffGapExample(
                session_id=session_id,
                project_path=project_path,
                last_timestamp=_optional_text(final.get("timestamp")),
                final_role=_role(final),
                message_count=len(ordered),
                gap_types=tuple(gap_types),
                final_excerpt=_excerpt(final_text or "(no final text)"),
                error_excerpt=_excerpt(latest_error) if latest_error else None,
            )
        )
    examples.sort(key=_example_sort_key)
    return examples


def _gap_types(
    final: Mapping[str, Any],
    final_text: str,
    combined_tail: str,
    tail: list[dict[str, Any]],
) -> list[str]:
    detected: list[str] = []
    if _role(final) != "assistant" or not NEXT_STEP_RE.search(final_text):
        detected.append("missing_next_step")
    if BLOCKER_RE.search(combined_tail) and not RESOLUTION_RE.search(final_text):
        detected.append("unresolved_blocker")
    if TODO_RE.search(final_text) or TODO_RE.search(combined_tail) and not RESOLUTION_RE.search(final_text):
        detected.append("dangling_todo")
    if _latest_error(tail) and not RESOLUTION_RE.search(final_text):
        detected.append("ended_after_error")
    return [gap_type for gap_type in GAP_TYPES if gap_type in set(detected)]


def _group_examples(
    examples: list[ClaudeHandoffGapExample],
    *,
    limit: int,
) -> list[ClaudeHandoffGapGroup]:
    groups: list[ClaudeHandoffGapGroup] = []
    for gap_type in GAP_TYPES:
        grouped_examples = [
            example for example in examples if gap_type in example.gap_types
        ]
        if not grouped_examples:
            continue
        groups.append(
            ClaudeHandoffGapGroup(
                gap_type=gap_type,
                count=len(grouped_examples),
                examples=tuple(grouped_examples[:limit]),
                recommendation=_recommendation(gap_type),
            )
        )
    return groups


def _recommendation(gap_type: str) -> str:
    return {
        "missing_next_step": "add_explicit_handoff_next_step",
        "unresolved_blocker": "triage_blocker_before_synthesis",
        "dangling_todo": "resolve_or_promote_todo",
        "ended_after_error": "verify_tool_error_recovery",
    }.get(gap_type, "review_session")


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


def _source_table(schema: dict[str, set[str]]) -> str | None:
    for table in EVENT_TABLE_CANDIDATES:
        if table in schema:
            return table
    return TEXT_TABLE if TEXT_TABLE in schema else None


def _missing_required_columns(columns: set[str], table: str) -> tuple[str, ...]:
    expected = {
        "session_id": SESSION_COLUMNS,
        "timestamp": TIMESTAMP_COLUMNS,
        "text": ("prompt_text",) if table == TEXT_TABLE else TEXT_COLUMNS,
    }
    return tuple(
        name
        for name, variants in expected.items()
        if not any(column in columns for column in variants)
    )


def _load_rows(
    conn: sqlite3.Connection,
    table: str,
    columns: set[str],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    timestamp_column = _first_column(columns, TIMESTAMP_COLUMNS)
    select_columns = ", ".join(sorted(columns))
    params: list[Any] = []
    where = ""
    if timestamp_column:
        where = f"WHERE {timestamp_column} >= ?"
        params.append(cutoff.isoformat())
    cursor = conn.execute(
        f"SELECT {select_columns} FROM {table} {where} ORDER BY "
        f"{timestamp_column or 'rowid'} ASC, rowid ASC",
        params,
    )
    column_names = [description[0] for description in cursor.description]
    return [
        _normalize_row(
            dict(row)
            if isinstance(row, Mapping)
            else dict(zip(column_names, row, strict=False)),
            source_table=table,
        )
        for row in cursor.fetchall()
    ]


def _normalize_row(row: Mapping[str, Any], *, source_table: str) -> dict[str, Any]:
    metadata, malformed = _metadata(row)
    text = _text(row, metadata)
    role = (
        _first_text(row, ROLE_COLUMNS)
        or _first_text(metadata, ROLE_COLUMNS)
        or _nested_text(metadata, ("message", "role"))
        or ("user" if source_table == TEXT_TABLE else None)
    )
    tool_name = (
        _first_text(row, TOOL_COLUMNS)
        or _first_text(metadata, TOOL_COLUMNS)
        or _nested_text(metadata, ("tool", "name"))
        or _nested_text(metadata, ("tool_use", "name"))
    )
    status = _first_text(row, STATUS_COLUMNS) or _first_text(metadata, STATUS_COLUMNS)
    return {
        "id": row.get("id") or row.get("rowid"),
        "session_id": _first_text(row, SESSION_COLUMNS)
        or _first_text(metadata, SESSION_COLUMNS),
        "project_path": _optional_text(row.get("project_path"))
        or _optional_text(metadata.get("project_path")),
        "timestamp": _first_text(row, TIMESTAMP_COLUMNS)
        or _first_text(metadata, TIMESTAMP_COLUMNS),
        "role": _normalize_role(role, tool_name=tool_name),
        "tool_name": _optional_text(tool_name),
        "status": _optional_text(status),
        "text": text,
        "is_error": _is_failure(status, text, metadata),
        "malformed_metadata": malformed,
    }


def _metadata(row: Mapping[str, Any]) -> tuple[Mapping[str, Any], bool]:
    for column in METADATA_COLUMNS:
        value = row.get(column)
        if value in (None, ""):
            continue
        if isinstance(value, Mapping):
            return value, False
        if isinstance(value, str):
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError:
                return {}, True
            return decoded if isinstance(decoded, Mapping) else {}, False
    return {}, False


def _text(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    parts = []
    for source in (row, metadata):
        for column in TEXT_COLUMNS:
            value = source.get(column)
            if isinstance(value, str) and value.strip():
                parts.append(value)
            elif isinstance(value, Mapping):
                nested = _first_text(value, TEXT_COLUMNS)
                if nested:
                    parts.append(nested)
    for keys in (
        ("message", "content"),
        ("message", "text"),
        ("error", "message"),
        ("result", "error"),
        ("response", "content"),
    ):
        nested = _nested_text(metadata, keys)
        if nested:
            parts.append(nested)
    return "\n".join(dict.fromkeys(parts)) or None


def _is_failure(
    status: str | None,
    text: str | None,
    metadata: Mapping[str, Any],
) -> bool:
    status_text = (status or "").lower()
    if status_text in {"error", "failed", "failure", "exception"}:
        return True
    if bool(metadata.get("is_error") or metadata.get("failed")):
        return True
    return bool(text and FAILURE_WORD_RE.search(text))


def _last_conversation_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    for row in reversed(rows):
        if _role(row) in {"assistant", "user"} and _optional_text(row.get("text")):
            return row
    return None


def _latest_error(rows: list[dict[str, Any]]) -> str | None:
    for row in reversed(rows):
        if row.get("is_error"):
            return _optional_text(row.get("text")) or _optional_text(row.get("status"))
    return None


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    filtered = []
    for row in rows:
        timestamp = _parse_datetime(row.get("timestamp"))
        if timestamp is not None and timestamp < cutoff:
            continue
        filtered.append(row)
    return filtered


def _session_keys(rows: Iterable[Mapping[str, Any]]) -> set[tuple[str, str | None]]:
    return {
        (
            _optional_text(row.get("session_id")) or "unknown-session",
            _optional_text(row.get("project_path")),
        )
        for row in rows
    }


def _row_sort_key(row: Mapping[str, Any]) -> tuple[str, int, str]:
    timestamp = _timestamp_sort(_optional_text(row.get("timestamp")))
    try:
        row_id = int(row.get("id") or 0)
    except (TypeError, ValueError):
        row_id = 0
    return (timestamp, row_id, _optional_text(row.get("text")) or "")


def _example_sort_key(example: ClaudeHandoffGapExample) -> tuple[str, str]:
    return (_reverse_text(example.last_timestamp), example.session_id)


def _reverse_text(value: Any) -> str:
    return "".join(chr(0x10FFFF - ord(char)) for char in str(value or ""))


def _first_column(columns: set[str], candidates: tuple[str, ...]) -> str | None:
    for column in candidates:
        if column in columns:
            return column
    return None


def _first_text(source: Mapping[str, Any], candidates: tuple[str, ...]) -> str | None:
    for key in candidates:
        value = source.get(key)
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


def _normalize_role(value: Any, *, tool_name: Any = None) -> str:
    if tool_name:
        return "tool"
    text = str(value or "").strip().lower()
    if "assistant" in text:
        return "assistant"
    if "user" in text or "human" in text:
        return "user"
    if "tool" in text:
        return "tool"
    return text or "unknown"


def _role(row: Mapping[str, Any]) -> str:
    return _normalize_role(row.get("role"), tool_name=row.get("tool_name"))


def _excerpt(text: str, max_chars: int = DEFAULT_EXCERPT_CHARS) -> str:
    compact = " ".join(str(text).split())
    if len(compact) <= max_chars:
        return compact
    return compact[: max(0, max_chars - 3)].rstrip() + "..."


def _timestamp_sort(value: str | None) -> str:
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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row)


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(
        value,
        (str, bytes, sqlite3.Connection),
    ) and not hasattr(value, "conn")
