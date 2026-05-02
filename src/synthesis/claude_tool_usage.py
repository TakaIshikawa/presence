"""Summarize Claude Code tool and command usage by session."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 25

TOOL_TOKENS = ("bash", "edit", "read", "write", "rg", "pytest", "git")

ERROR_INDICATOR_RE = re.compile(
    r"\b(error|failed|failure|exception|traceback|timed out|timeout|nonzero|non-zero|"
    r"exit code|permission denied|missing dependency|could not|cannot)\b",
    re.IGNORECASE,
)
INTERRUPTION_INDICATOR_RE = re.compile(
    r"\b(aborted|cancelled|canceled|interrupted|stopped|halted|user cancelled|"
    r"tool call was aborted|remaining work|todo|follow-up|follow up)\b",
    re.IGNORECASE,
)


def build_claude_tool_usage_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    project_path: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return per-session and aggregate Claude Code tool usage counts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    project_path = _optional_text(project_path)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
        "project_path": project_path,
        "project_path_filter_applied": False,
    }

    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    if _looks_like_rows(db_or_rows):
        rows = [_mapping(row) for row in db_or_rows]
        rows = _filter_rows(rows, cutoff=cutoff, project_path=project_path)
        filters["project_path_filter_applied"] = bool(project_path)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        if "claude_messages" not in schema:
            missing_tables.append("claude_messages")
            rows = []
        else:
            columns = schema["claude_messages"]
            missing_columns = _missing_columns(columns)
            rows = _load_rows(
                conn,
                columns,
                cutoff=cutoff,
                project_path=project_path,
            )
            filters["project_path_filter_applied"] = bool(
                project_path and "project_path" in columns
            )

    sessions = _summarize_sessions(rows)
    sessions.sort(key=_session_sort_key)
    aggregate_tool_counts = _aggregate_tool_counts(sessions)
    high_cooccurrence = [
        session
        for session in sessions
        if session["error_indicator_count"] or session["interruption_indicator_count"]
    ]
    high_cooccurrence.sort(key=_cooccurrence_sort_key)

    totals = {
        "message_count": sum(session["message_count"] for session in sessions),
        "session_count": len(sessions),
        "tool_mention_count": sum(session["tool_mention_count"] for session in sessions),
        "error_indicator_count": sum(
            session["error_indicator_count"] for session in sessions
        ),
        "interruption_indicator_count": sum(
            session["interruption_indicator_count"] for session in sessions
        ),
        "cooccurrence_session_count": len(high_cooccurrence),
    }
    return {
        "artifact_type": "claude_tool_usage",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": totals,
        "aggregate_tool_counts": aggregate_tool_counts,
        "sessions": sessions[:limit],
        "high_error_interruption_sessions": high_cooccurrence[:limit],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "has_tool_usage": bool(aggregate_tool_counts),
    }


def format_claude_tool_usage_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_claude_tool_usage_text(report: dict[str, Any]) -> str:
    """Render a deterministic command-line summary."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Claude Tool Usage",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"project_path={filters['project_path'] or '-'} "
            f"project_filter_applied={filters['project_path_filter_applied']}"
        ),
        (
            "Totals: "
            f"sessions={totals['session_count']} messages={totals['message_count']} "
            f"tool_mentions={totals['tool_mention_count']} "
            f"errors={totals['error_indicator_count']} "
            f"interruptions={totals['interruption_indicator_count']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in report.get("missing_columns", {}).items()
        if columns
    ]
    if missing_columns:
        lines.append("Missing optional columns: " + "; ".join(missing_columns))

    lines.extend(["", "Aggregate tool counts:"])
    if report["aggregate_tool_counts"]:
        for tool, count in report["aggregate_tool_counts"].items():
            lines.append(f"- {tool}: {count}")
    else:
        lines.append("- none")

    lines.extend(["", "Sessions:"])
    if report["sessions"]:
        for session in report["sessions"]:
            lines.append(
                f"- session={session['session_id']} project={session['project_path'] or '-'} "
                f"messages={session['message_count']} tools={session['tool_mention_count']} "
                f"errors={session['error_indicator_count']} "
                f"interruptions={session['interruption_indicator_count']} "
                f"top={_format_top_tools(session['tool_counts'])}"
            )
    else:
        lines.append("- none")

    lines.extend(["", "High error/interruption co-occurrence:"])
    if report["high_error_interruption_sessions"]:
        for session in report["high_error_interruption_sessions"]:
            lines.append(
                f"- session={session['session_id']} project={session['project_path'] or '-'} "
                f"score={session['cooccurrence_score']} tools={session['tool_mention_count']} "
                f"errors={session['error_indicator_count']} "
                f"interruptions={session['interruption_indicator_count']}"
            )
    else:
        lines.append("- none")
    return "\n".join(lines)


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


def _missing_columns(columns: set[str]) -> dict[str, list[str]]:
    optional = ("project_path", "response_text")
    missing = [column for column in optional if column not in columns]
    return {"claude_messages": missing} if missing else {}


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    project_path: str | None,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id"),
        _column_expr(columns, "session_id"),
        _column_expr(columns, "message_uuid"),
        _column_expr(columns, "project_path"),
        _column_expr(columns, "timestamp"),
        _column_expr(columns, "prompt_text"),
        _column_expr(columns, "response_text"),
    ]
    where = ["timestamp >= ?"]
    params: list[Any] = [cutoff.isoformat()]
    if project_path and "project_path" in columns:
        where.append("project_path = ?")
        params.append(project_path)
    cursor = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM claude_messages
            WHERE {' AND '.join(where)}
            ORDER BY timestamp ASC, id ASC""",
        params,
    )
    column_names = [description[0] for description in cursor.description]
    return [
        dict(row)
        if isinstance(row, Mapping)
        else dict(zip(column_names, row, strict=False))
        for row in cursor.fetchall()
    ]


def _column_expr(columns: set[str], column: str, default: str = "NULL") -> str:
    return column if column in columns else f"{default} AS {column}"


def _filter_rows(
    rows: list[dict[str, Any]],
    *,
    cutoff: datetime,
    project_path: str | None,
) -> list[dict[str, Any]]:
    filtered = []
    for row in rows:
        timestamp = _parse_datetime(row.get("timestamp"))
        if timestamp is not None and timestamp < cutoff:
            continue
        if project_path and _optional_text(row.get("project_path")) != project_path:
            continue
        filtered.append(row)
    return filtered


def _summarize_sessions(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str | None], dict[str, Any]] = {}
    for row in rows:
        session_id = str(row.get("session_id") or "unknown-session")
        project_path = _optional_text(row.get("project_path"))
        key = (session_id, project_path)
        if key not in grouped:
            grouped[key] = {
                "session_id": session_id,
                "project_path": project_path,
                "message_count": 0,
                "tool_counts": Counter(),
                "error_indicator_count": 0,
                "interruption_indicator_count": 0,
                "first_timestamp": None,
                "last_timestamp": None,
            }
        item = grouped[key]
        text = _row_text(row)
        tool_counts = _tool_counts(text)
        item["message_count"] += 1
        item["tool_counts"].update(tool_counts)
        item["error_indicator_count"] += _indicator_count(ERROR_INDICATOR_RE, text)
        item["interruption_indicator_count"] += _indicator_count(
            INTERRUPTION_INDICATOR_RE,
            text,
        )
        timestamp = _optional_text(row.get("timestamp"))
        if timestamp:
            item["first_timestamp"] = min(
                item["first_timestamp"] or timestamp,
                timestamp,
            )
            item["last_timestamp"] = max(item["last_timestamp"] or timestamp, timestamp)

    sessions: list[dict[str, Any]] = []
    for item in grouped.values():
        tool_counts = dict(sorted(item["tool_counts"].items()))
        tool_mentions = sum(tool_counts.values())
        cooccurrence_score = tool_mentions * (
            item["error_indicator_count"] + item["interruption_indicator_count"]
        )
        sessions.append(
            {
                "session_id": item["session_id"],
                "project_path": item["project_path"],
                "message_count": item["message_count"],
                "tool_mention_count": tool_mentions,
                "tool_counts": tool_counts,
                "error_indicator_count": item["error_indicator_count"],
                "interruption_indicator_count": item["interruption_indicator_count"],
                "cooccurrence_score": cooccurrence_score,
                "first_timestamp": item["first_timestamp"],
                "last_timestamp": item["last_timestamp"],
            }
        )
    return sessions


def _aggregate_tool_counts(sessions: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for session in sessions:
        counts.update(session["tool_counts"])
    return dict(sorted(counts.items()))


def _tool_counts(text: str) -> Counter[str]:
    lowered = text.lower()
    counts: Counter[str] = Counter()
    for token in TOOL_TOKENS:
        counts[token] = len(re.findall(rf"(?<![a-z0-9_]){re.escape(token)}(?![a-z0-9_])", lowered))
    return Counter({key: value for key, value in counts.items() if value})


def _indicator_count(pattern: re.Pattern[str], text: str) -> int:
    return len(pattern.findall(text))


def _row_text(row: Mapping[str, Any]) -> str:
    parts = []
    for key in ("prompt_text", "response_text", "transcript", "content", "text", "message"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value)
        elif isinstance(value, Mapping):
            content = value.get("content")
            if isinstance(content, str) and content.strip():
                parts.append(content)
    return "\n".join(parts)


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row)


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(
        value,
        (str, bytes, sqlite3.Connection),
    ) and not hasattr(value, "conn")


def _session_sort_key(session: dict[str, Any]) -> tuple[int, int, str, str]:
    return (
        -int(session["tool_mention_count"]),
        -int(session["message_count"]),
        str(session["session_id"]),
        str(session["project_path"] or ""),
    )


def _cooccurrence_sort_key(session: dict[str, Any]) -> tuple[int, int, str, str]:
    return (
        -int(session["cooccurrence_score"]),
        -int(session["tool_mention_count"]),
        str(session["session_id"]),
        str(session["project_path"] or ""),
    )


def _format_top_tools(tool_counts: dict[str, int]) -> str:
    if not tool_counts:
        return "none"
    pairs = sorted(tool_counts.items(), key=lambda item: (-item[1], item[0]))
    return ", ".join(f"{tool}:{count}" for tool, count in pairs[:3])


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
