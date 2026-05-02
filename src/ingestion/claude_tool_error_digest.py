"""Digest repeated Claude Code tool failures from ingested session events."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 14
DEFAULT_THRESHOLD = 2
DEFAULT_LIMIT = 20

EVENT_TABLE_CANDIDATES = (
    "claude_session_events",
    "claude_tool_events",
    "claude_events",
)
SESSION_COLUMNS = ("session_id", "sessionId")
TIMESTAMP_COLUMNS = ("timestamp", "created_at", "event_time", "event_at")
TOOL_COLUMNS = ("tool_name", "tool", "toolName", "name")
STATUS_COLUMNS = ("status", "outcome")
ERROR_COLUMNS = (
    "error",
    "error_message",
    "message",
    "stderr",
    "output",
    "result",
    "content",
)
METADATA_COLUMNS = ("metadata", "raw_metadata", "event_json", "payload")
FAILURE_WORD_RE = re.compile(
    r"\b(error|failed|failure|exception|traceback|timeout|timed out|denied|"
    r"non[- ]?zero|exit code)\b",
    re.IGNORECASE,
)


def build_claude_tool_error_digest(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    threshold: int = DEFAULT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return repeated Claude tool failures grouped by tool and error signature."""

    if days <= 0:
        raise ValueError("days must be positive")
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "threshold": threshold,
    }

    schema_gaps = {"missing_tables": [], "missing_columns": {}}
    if _looks_like_rows(db_or_rows):
        table_name = "rows"
        rows = [_mapping(row) for row in db_or_rows]
        rows = _filter_rows(rows, cutoff=cutoff)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        table_name = _event_table(schema)
        if table_name is None:
            schema_gaps["missing_tables"] = list(EVENT_TABLE_CANDIDATES)
            return _report(
                generated_at=generated_at,
                filters=filters,
                source_table=None,
                rows_scanned=0,
                malformed_metadata_count=0,
                groups=[],
                schema_gaps=schema_gaps,
            )
        missing_columns = _missing_optional_columns(schema[table_name])
        if missing_columns:
            schema_gaps["missing_columns"] = {table_name: missing_columns}
        rows = _load_rows(conn, table_name, schema[table_name], cutoff=cutoff)

    groups, malformed_metadata_count, failure_rows = _group_failures(rows)
    groups = [group for group in groups if group["count"] >= threshold]
    groups.sort(key=_group_sort_key)

    return _report(
        generated_at=generated_at,
        filters=filters,
        source_table=table_name,
        rows_scanned=len(rows),
        malformed_metadata_count=malformed_metadata_count,
        groups=groups[:limit],
        schema_gaps=schema_gaps,
        failure_rows=failure_rows,
    )


def format_claude_tool_error_digest_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_claude_tool_error_digest_text(report: dict[str, Any]) -> str:
    """Render a compact operator-facing error pattern digest."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Claude Tool Error Digest",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} threshold={filters['threshold']} "
            f"limit={filters['limit']}"
        ),
        (
            "Totals: "
            f"rows={totals['rows_scanned']} failures={totals['failure_rows']} "
            f"groups={totals['groups']} malformed_metadata="
            f"{totals['malformed_metadata_count']}"
        ),
    ]
    if report.get("source_table"):
        lines.append(f"Source table: {report['source_table']}")
    gaps = report.get("schema_gaps") or {}
    if gaps.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(gaps["missing_tables"]))
    if gaps.get("missing_columns"):
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(gaps["missing_columns"].items())
        ]
        lines.append("Missing optional columns: " + "; ".join(missing))
    if not report["groups"]:
        lines.append("No repeated tool error groups matched the threshold.")
        return "\n".join(lines)

    lines.extend(["", "Top error groups:"])
    for group in report["groups"]:
        sessions = ", ".join(group["session_ids"]) or "-"
        lines.append(
            f"- {group['tool_name']} count={group['count']} "
            f"latest={group['latest_at'] or '-'} action={group['suggested_next_action']}"
        )
        lines.append(f"  signature={group['signature']}")
        lines.append(f"  sessions={sessions}")
        for example in group["examples"]:
            lines.append(
                f"  example session={example['session_id']} at={example['timestamp'] or '-'} "
                f"{example['error_excerpt']}"
            )
    return "\n".join(lines)


def _report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    source_table: str | None,
    rows_scanned: int,
    malformed_metadata_count: int,
    groups: list[dict[str, Any]],
    schema_gaps: dict[str, Any],
    failure_rows: int = 0,
) -> dict[str, Any]:
    return {
        "artifact_type": "claude_tool_error_digest",
        "filters": filters,
        "generated_at": generated_at.isoformat(),
        "groups": groups,
        "schema_gaps": {
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(schema_gaps.get("missing_columns", {}).items())
            },
            "missing_tables": list(schema_gaps.get("missing_tables", [])),
        },
        "source_table": source_table,
        "totals": {
            "failure_rows": failure_rows,
            "groups": len(groups),
            "malformed_metadata_count": malformed_metadata_count,
            "rows_scanned": rows_scanned,
        },
    }


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


def _event_table(schema: dict[str, set[str]]) -> str | None:
    for table in EVENT_TABLE_CANDIDATES:
        if table in schema:
            return table
    return None


def _missing_optional_columns(columns: set[str]) -> list[str]:
    expected_groups = {
        "session_id": SESSION_COLUMNS,
        "timestamp": TIMESTAMP_COLUMNS,
        "tool_name": TOOL_COLUMNS,
        "metadata": METADATA_COLUMNS,
    }
    return [
        name
        for name, variants in expected_groups.items()
        if not any(column in columns for column in variants)
    ]


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
        dict(row)
        if isinstance(row, Mapping)
        else dict(zip(column_names, row, strict=False))
        for row in cursor.fetchall()
    ]


def _group_failures(rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    malformed_metadata_count = 0
    failure_rows = 0
    for row in rows:
        metadata, malformed = _metadata(row)
        if malformed:
            malformed_metadata_count += 1
        event = _failure_event(row, metadata)
        if event is None:
            continue
        failure_rows += 1
        grouped[(event["tool_name"], event["signature"])].append(event)

    groups = [
        _group_payload(tool_name, signature, events)
        for (tool_name, signature), events in grouped.items()
    ]
    return groups, malformed_metadata_count, failure_rows


def _failure_event(
    row: dict[str, Any],
    metadata: Mapping[str, Any],
) -> dict[str, Any] | None:
    status = _first_text(row, STATUS_COLUMNS) or _first_text(metadata, STATUS_COLUMNS)
    error_text = _error_text(row, metadata)
    if not _is_failure(status, error_text, metadata):
        return None
    tool_name = _normalize_tool_name(
        _first_text(row, TOOL_COLUMNS)
        or _first_text(metadata, TOOL_COLUMNS)
        or _nested_text(metadata, ("tool", "name"))
        or _nested_text(metadata, ("tool_use", "name"))
        or "unknown"
    )
    signature = _normalize_signature(error_text or status or "tool failure")
    timestamp = _first_text(row, TIMESTAMP_COLUMNS) or _first_text(metadata, TIMESTAMP_COLUMNS)
    session_id = (
        _first_text(row, SESSION_COLUMNS)
        or _first_text(metadata, SESSION_COLUMNS)
        or "unknown-session"
    )
    return {
        "error_excerpt": _excerpt(error_text or status or "tool failure"),
        "session_id": session_id,
        "signature": signature,
        "timestamp": timestamp,
        "timestamp_sort": _timestamp_sort(timestamp),
        "tool_name": tool_name,
    }


def _group_payload(
    tool_name: str,
    signature: str,
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    ordered = sorted(
        events,
        key=lambda item: (item["timestamp_sort"], item["session_id"], item["error_excerpt"]),
    )
    first = ordered[0]
    latest = ordered[-1]
    session_ids = sorted({event["session_id"] for event in ordered})
    signature_id = hashlib.sha256(f"{tool_name}:{signature}".encode("utf-8")).hexdigest()[:12]
    return {
        "count": len(events),
        "examples": [
            {
                "error_excerpt": event["error_excerpt"],
                "session_id": event["session_id"],
                "timestamp": event["timestamp"],
            }
            for event in ordered[:3]
        ],
        "first_seen_at": first["timestamp"],
        "latest_at": latest["timestamp"],
        "session_ids": session_ids[:5],
        "signature": signature,
        "signature_id": f"claude_tool_error_{signature_id}",
        "suggested_next_action": _suggested_next_action(tool_name, signature),
        "tool_name": tool_name,
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


def _error_text(row: Mapping[str, Any], metadata: Mapping[str, Any]) -> str | None:
    parts = []
    for source in (row, metadata):
        for column in ERROR_COLUMNS:
            value = source.get(column)
            if isinstance(value, str) and value.strip():
                parts.append(value)
            elif isinstance(value, Mapping):
                nested = _first_text(value, ERROR_COLUMNS)
                if nested:
                    parts.append(nested)
    nested = (
        _nested_text(metadata, ("error", "message"))
        or _nested_text(metadata, ("result", "error"))
        or _nested_text(metadata, ("response", "error"))
    )
    if nested:
        parts.append(nested)
    return "\n".join(dict.fromkeys(parts)) or None


def _is_failure(
    status: str | None,
    error_text: str | None,
    metadata: Mapping[str, Any],
) -> bool:
    status_text = (status or "").lower()
    if status_text in {"error", "failed", "failure", "exception"}:
        return True
    if bool(metadata.get("is_error") or metadata.get("failed")):
        return True
    return bool(error_text and FAILURE_WORD_RE.search(error_text))


def _normalize_signature(text: str) -> str:
    value = " ".join(str(text).split()).lower()
    value = re.sub(r"`{1,3}", " ", value)
    value = re.sub(r"https?://\S+", "<url>", value)
    value = re.sub(r"(/[^\s:]+)+", "<path>", value)
    value = re.sub(r"\b[\w.-]+(?:/[\w.-]+)+\b", "<path>", value)
    value = re.sub(r"\b[0-9a-f]{8}-[0-9a-f-]{13,}\b", "<uuid>", value)
    value = re.sub(r"\b[a-f0-9]{7,40}\b", "<hash>", value)
    value = re.sub(r"\b\d+\b", "<num>", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip(" -:,.")[:240]


def _normalize_tool_name(value: Any) -> str:
    text = str(value or "unknown").strip().lower()
    return re.sub(r"[^a-z0-9_.-]+", "_", text).strip("_") or "unknown"


def _suggested_next_action(tool_name: str, signature: str) -> str:
    combined = f"{tool_name} {signature}"
    if "permission denied" in combined or "eacces" in combined:
        return "fix_permissions"
    if "timeout" in combined or "timed out" in combined:
        return "raise_timeout"
    if "not found" in combined or "no such file" in combined or "missing" in combined:
        return "repair_missing_dependency_or_path"
    if "exit code" in combined or "non-zero" in combined:
        return "repair_command"
    if "json" in combined or "parse" in combined:
        return "inspect_tool_input"
    return "triage_tool_failure"


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    filtered = []
    for row in rows:
        timestamp = _first_text(row, TIMESTAMP_COLUMNS)
        parsed = _parse_datetime(timestamp)
        if parsed is not None and parsed < cutoff:
            continue
        filtered.append(row)
    return filtered


def _group_sort_key(group: dict[str, Any]) -> tuple[int, str, str, str]:
    return (
        -int(group["count"]),
        _reverse_text(group["latest_at"]),
        str(group["tool_name"]),
        str(group["signature"]),
    )


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


def _excerpt(text: str, width: int = 180) -> str:
    compact = " ".join(str(text).split())
    if len(compact) <= width:
        return compact
    return compact[: max(0, width - 3)].rstrip() + "..."


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


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row)


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(
        value,
        (str, bytes, sqlite3.Connection),
    ) and not hasattr(value, "conn")
