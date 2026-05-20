"""Report Claude Code session logs that look partially ingested."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_LIMIT = 50

REASON_CODES = (
    "missing_parsed_messages",
    "missing_project_path_metadata",
    "invalid_or_absent_session_timestamp",
    "source_log_without_content_artifact",
)


def build_claude_session_ingestion_gaps_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    project_path: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a SQLite-backed report of skipped or malformed Claude sessions."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=lookback_days)
    normalized_project = _optional_text(project_path)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    schema_gaps = _schema_gaps(schema)

    metadata_rows = _load_metadata_rows(conn, schema, lookback_start)
    message_rows = _load_message_rows(conn, schema, lookback_start)
    content_rows = _load_content_rows(conn, schema)
    findings = _findings(metadata_rows, message_rows, content_rows)
    if normalized_project:
        findings = [
            row
            for row in findings
            if (row["project_path"] or "").lower() == normalized_project.lower()
        ]
    findings.sort(key=_finding_sort_key)
    shown = findings[:limit]
    counts = Counter(reason for row in findings for reason in row["reason_codes"])
    return {
        "artifact_type": "claude_session_ingestion_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "limit": limit,
            "lookback_days": lookback_days,
            "lookback_start": lookback_start.isoformat(),
            "project_path": normalized_project,
        },
        "summary": {
            "content_artifact_count": len(content_rows),
            "finding_count": len(findings),
            "metadata_session_count": len(metadata_rows),
            "message_session_count": len(_group_messages(message_rows)),
            "shown_finding_count": len(shown),
            "by_reason": dict(sorted(counts.items())),
        },
        "findings": shown,
        "schema_gaps": schema_gaps,
    }


def format_claude_session_ingestion_gaps_json(report: dict[str, Any]) -> str:
    """Serialize report JSON deterministically."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_claude_session_ingestion_gaps_text(report: dict[str, Any]) -> str:
    """Render a concise text report."""
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Claude Session Ingestion Gaps",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"lookback_days={filters['lookback_days']} limit={filters['limit']} "
            f"project_path={filters['project_path'] or '-'}"
        ),
        (
            "Totals: "
            f"metadata_sessions={summary['metadata_session_count']} "
            f"message_sessions={summary['message_session_count']} "
            f"content_artifacts={summary['content_artifact_count']} "
            f"findings={summary['finding_count']} shown={summary['shown_finding_count']}"
        ),
    ]
    gaps = report.get("schema_gaps") or {}
    if gaps.get("missing_tables"):
        lines.append("Missing optional tables: " + ", ".join(gaps["missing_tables"]))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in sorted((gaps.get("missing_columns") or {}).items())
        if columns
    ]
    if missing_columns:
        lines.append("Missing columns: " + "; ".join(missing_columns))

    if not report["findings"]:
        lines.append("No Claude session ingestion gaps found.")
        return "\n".join(lines)

    lines.extend(["", "Findings:"])
    for row in report["findings"]:
        lines.append(
            f"- session={row['session_id']} project={row['project_path'] or '-'} "
            f"source_log={row['source_log'] or '-'} messages={row['message_count']} "
            f"first={row['first_message_at'] or '-'} last={row['last_message_at'] or '-'} "
            f"reasons={','.join(row['reason_codes'])}"
        )
    return "\n".join(lines)


def _findings(
    metadata_rows: list[dict[str, Any]],
    message_rows: list[dict[str, Any]],
    content_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    messages_by_session = _group_messages(message_rows)
    metadata_by_session: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in metadata_rows:
        metadata_by_session[_session_id(row)].append(row)
    artifact_refs = _artifact_refs(content_rows)
    sessions = set(metadata_by_session) | set(messages_by_session)
    findings: list[dict[str, Any]] = []

    for session_id in sessions:
        metadata = metadata_by_session.get(session_id, [])
        messages = messages_by_session.get(session_id, [])
        source_logs = sorted({_optional_text(row.get("source_log")) for row in metadata if _optional_text(row.get("source_log"))})
        project_path = _first_text(
            [row.get("project_path") for row in messages]
            + [row.get("project_path") for row in metadata]
        )
        message_times = [_parse_dt(row.get("timestamp")) for row in messages]
        valid_message_times = sorted(value for value in message_times if value is not None)
        metadata_times = [
            _parse_dt(_first(row, "session_timestamp", "timestamp", "started_at", "updated_at", "ingested_at"))
            for row in metadata
        ]
        valid_metadata_times = sorted(value for value in metadata_times if value is not None)

        reasons: list[str] = []
        if metadata and not messages:
            reasons.append("missing_parsed_messages")
        if not project_path:
            reasons.append("missing_project_path_metadata")
        if (messages and len(valid_message_times) != len(messages)) or (
            metadata and len(valid_metadata_times) != len(metadata)
        ) or not (valid_message_times or valid_metadata_times):
            reasons.append("invalid_or_absent_session_timestamp")
        if source_logs and not _has_content_artifact(session_id, messages, source_logs, artifact_refs):
            reasons.append("source_log_without_content_artifact")
        if not reasons:
            continue
        findings.append(
            {
                "session_id": session_id,
                "project_path": project_path,
                "source_log": source_logs[0] if source_logs else None,
                "source_logs": source_logs,
                "message_count": len(messages),
                "first_message_at": _iso(valid_message_times[0] if valid_message_times else None),
                "last_message_at": _iso(valid_message_times[-1] if valid_message_times else None),
                "metadata_count": len(metadata),
                "reason_codes": [reason for reason in REASON_CODES if reason in reasons],
            }
        )
    return findings


def _load_metadata_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    lookback_start: datetime,
) -> list[dict[str, Any]]:
    table = _metadata_table(schema)
    if not table:
        return []
    columns = schema[table]
    selected = [
        _alias_expr(columns, ("session_id", "sessionId", "id"), "session_id", table),
        _alias_expr(columns, ("source_log", "log_path", "file_path", "path"), "source_log", table),
        _alias_expr(columns, ("project_path", "cwd", "workspace_path"), "project_path", table),
        _alias_expr(columns, ("session_timestamp", "timestamp", "started_at", "created_at"), "session_timestamp", table),
        _alias_expr(columns, ("ingested_at", "updated_at", "created_at"), "ingested_at", table),
    ]
    order_col = next((column for column in ("ingested_at", "updated_at", "created_at", "session_timestamp", "timestamp", "id") if column in columns), None)
    rows = [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    recent = []
    for row in rows:
        reference = _parse_dt(_first(row, "ingested_at", "session_timestamp"))
        if reference is None or reference >= lookback_start:
            recent.append(row)
    recent.sort(key=lambda row: (_optional_text(row.get("session_id")) or "", _optional_text(row.get("source_log")) or "", row.get(order_col or "ingested_at") or ""))
    return recent


def _load_message_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    lookback_start: datetime,
) -> list[dict[str, Any]]:
    if "claude_messages" not in schema:
        return []
    columns = schema["claude_messages"]
    selected = [
        _column_expr(columns, "id", "NULL", alias="id"),
        _alias_expr(columns, ("session_id", "sessionId"), "session_id", "claude_messages"),
        _alias_expr(columns, ("message_uuid", "uuid"), "message_uuid", "claude_messages"),
        _column_expr(columns, "project_path", "NULL", alias="project_path"),
        _column_expr(columns, "timestamp", "NULL", alias="timestamp"),
    ]
    rows = [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM claude_messages").fetchall()]
    recent = []
    for row in rows:
        timestamp = _parse_dt(row.get("timestamp"))
        if timestamp is None or timestamp >= lookback_start:
            recent.append(row)
    recent.sort(key=lambda row: (row.get("timestamp") or "", _optional_text(row.get("session_id")) or "", _optional_text(row.get("message_uuid")) or ""))
    return recent


def _load_content_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    columns = schema["generated_content"]
    selected = [
        _column_expr(columns, "id", "NULL", alias="id"),
        _column_expr(columns, "source_messages", "NULL", alias="source_messages"),
        _column_expr(columns, "source_metadata", "NULL", alias="source_metadata"),
        _column_expr(columns, "metadata", "NULL", alias="metadata"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content ORDER BY id ASC").fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    missing_tables = []
    missing_columns: dict[str, list[str]] = {}
    if "claude_messages" not in schema:
        missing_tables.append("claude_messages")
    else:
        missing = sorted({"session_id", "timestamp"} - schema["claude_messages"])
        if missing:
            missing_columns["claude_messages"] = missing
    if not _metadata_table(schema):
        missing_tables.append("ingestion_metadata")
    else:
        table = _metadata_table(schema)
        assert table is not None
        if not any(column in schema[table] for column in ("source_log", "log_path", "file_path", "path")):
            missing_columns[table] = ["source_log"]
    if "generated_content" not in schema:
        missing_tables.append("generated_content")
    elif "source_messages" not in schema["generated_content"]:
        missing_columns["generated_content"] = ["source_messages"]
    return {"missing_tables": missing_tables, "missing_columns": missing_columns}


def _metadata_table(schema: dict[str, set[str]]) -> str | None:
    for table in ("ingestion_metadata", "claude_session_ingestion_metadata", "claude_log_ingestion_metadata"):
        if table in schema:
            return table
    return None


def _group_messages(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_session_id(row)].append(row)
    return grouped


def _artifact_refs(rows: list[dict[str, Any]]) -> set[str]:
    refs: set[str] = set()
    for row in rows:
        for key in ("source_messages", "source_metadata", "metadata"):
            refs.update(_json_refs(row.get(key)))
    return refs


def _json_refs(value: Any) -> set[str]:
    if value in (None, ""):
        return set()
    payload = value
    if isinstance(value, str):
        try:
            payload = json.loads(value)
        except json.JSONDecodeError:
            return {value}
    refs: set[str] = set()
    if isinstance(payload, list):
        for item in payload:
            refs.update(_json_refs(item))
    elif isinstance(payload, dict):
        for key, item in payload.items():
            if key in {"message_uuid", "session_id", "source_log", "log_path", "file_path", "path"}:
                text = _optional_text(item)
                if text:
                    refs.add(text)
            else:
                refs.update(_json_refs(item))
    else:
        text = _optional_text(payload)
        if text:
            refs.add(text)
    return refs


def _has_content_artifact(
    session_id: str,
    messages: list[dict[str, Any]],
    source_logs: list[str],
    artifact_refs: set[str],
) -> bool:
    if session_id in artifact_refs:
        return True
    if any(log in artifact_refs for log in source_logs):
        return True
    return any(_optional_text(message.get("message_uuid")) in artifact_refs for message in messages)


def _finding_sort_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    first_time = row.get("first_message_at") or ""
    return (first_time, row.get("project_path") or "", row.get("session_id") or "", ",".join(row.get("reason_codes") or ()))


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _column_expr(columns: set[str], column: str, default: str, *, alias: str | None = None) -> str:
    return f"{column if column in columns else default} AS {alias or column}"


def _alias_expr(columns: set[str], names: tuple[str, ...], alias: str, table: str) -> str:
    del table
    for name in names:
        if name in columns:
            return f"{name} AS {alias}"
    return f"NULL AS {alias}"


def _session_id(row: Mapping[str, Any]) -> str:
    return _optional_text(_first(row, "session_id", "sessionId")) or "unknown-session"


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value is not None:
            return value
    return None


def _first_text(values: list[Any]) -> str | None:
    for value in values:
        text = _optional_text(value)
        if text:
            return text
    return None


def _optional_text(value: Any) -> str | None:
    text = "" if value is None else str(value).strip()
    return text or None


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
