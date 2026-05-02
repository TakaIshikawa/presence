"""Detect repeated Claude Code user prompts in recent sessions."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 20
DEFAULT_THRESHOLD = 2
DEFAULT_EXAMPLE_LIMIT = 3

TABLE = "claude_messages"
REQUIRED_COLUMNS = ("session_id", "timestamp", "prompt_text")
OPTIONAL_COLUMNS = ("id", "message_uuid", "project_path")

STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "can",
        "could",
        "for",
        "i",
        "in",
        "is",
        "it",
        "just",
        "me",
        "my",
        "now",
        "of",
        "on",
        "or",
        "please",
        "the",
        "this",
        "to",
        "you",
    }
)

_TOKEN_RE = re.compile(r"[a-z0-9]+(?:[-_'][a-z0-9]+)?")
_MARKDOWN_PREFIX_RE = re.compile(r"^\s*(?:[-*+>]|\d+[.)])\s+")
_PATH_RE = re.compile(
    r"(?<![\w./-])(?:\.?/|/)?(?:[\w@%+=:,.-]+/)+[\w@%+=:,.-]+(?:\.[a-z0-9]{1,12})?(?::\d+)?",
    re.IGNORECASE,
)
_UUID_RE = re.compile(r"\b[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\b", re.IGNORECASE)
_HEX_RE = re.compile(r"\b[0-9a-f]{7,40}\b", re.IGNORECASE)


def build_claude_prompt_repetition_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    threshold: int = DEFAULT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    project_path: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic report of repeated normalized prompts by session."""
    if days <= 0:
        raise ValueError("days must be positive")
    if threshold <= 0:
        raise ValueError("threshold must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    project_filter = _optional_text(project_path)
    filters = {
        "days": days,
        "limit": limit,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "project_path": project_filter,
        "project_path_filter_applied": False,
        "threshold": threshold,
    }
    schema_gaps: dict[str, Any] = {"missing_columns": {}, "missing_tables": []}

    if _looks_like_rows(db_or_rows):
        source_table = "rows"
        rows = [_mapping(row) for row in db_or_rows]
        rows = _filter_rows(rows, cutoff=cutoff, project_path=project_filter)
        filters["project_path_filter_applied"] = bool(project_filter)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        if TABLE not in schema:
            schema_gaps["missing_tables"] = [TABLE]
            rows = []
            source_table = None
        else:
            source_table = TABLE
            columns = schema[TABLE]
            missing = _missing_columns(columns)
            if missing:
                schema_gaps["missing_columns"] = {TABLE: missing}
            rows = _load_rows(conn, columns, cutoff=cutoff, project_path=project_filter)
            filters["project_path_filter_applied"] = bool(project_filter and "project_path" in columns)

    sessions_scanned = len({str(row.get("session_id") or "unknown-session") for row in rows})
    sessions = _group_repeated_prompts(rows, threshold=threshold)
    sessions.sort(key=_session_sort_key)
    limited_sessions = sessions[:limit]

    return {
        "artifact_type": "claude_prompt_repetition",
        "filters": filters,
        "generated_at": generated_at.isoformat(),
        "schema_gaps": {
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(schema_gaps.get("missing_columns", {}).items())
            },
            "missing_tables": list(schema_gaps.get("missing_tables", [])),
        },
        "sessions": limited_sessions,
        "source_table": source_table,
        "totals": {
            "messages_scanned": len(rows),
            "repeated_prompt_groups": sum(len(session["repeated_prompts"]) for session in sessions),
            "repeated_prompt_instances": sum(
                int(item["count"])
                for session in sessions
                for item in session["repeated_prompts"]
            ),
            "sessions_flagged": len(sessions),
            "sessions_scanned": sessions_scanned,
        },
    }


def format_claude_prompt_repetition_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_claude_prompt_repetition_text(report: dict[str, Any]) -> str:
    """Render a concise human-readable prompt repetition report."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Claude Prompt Repetition",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} threshold={filters['threshold']} "
            f"limit={filters['limit']} project_path={filters['project_path'] or '-'} "
            f"project_filter_applied={filters['project_path_filter_applied']}"
        ),
        (
            "Totals: "
            f"sessions={totals['sessions_scanned']} flagged={totals['sessions_flagged']} "
            f"messages={totals['messages_scanned']} repeated_groups={totals['repeated_prompt_groups']}"
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
        lines.append("Missing columns: " + "; ".join(missing))

    if not report["sessions"]:
        lines.extend(["", "No Claude prompt repetition found."])
        return "\n".join(lines)

    lines.extend(["", "Repeated prompts:"])
    for session in report["sessions"]:
        lines.append(
            f"- session={session['session_id']} project={session['project_path'] or '-'} "
            f"groups={len(session['repeated_prompts'])} repeated_messages={session['repeated_prompt_count']} "
            f"first={session['first_timestamp'] or '-'} latest={session['latest_timestamp'] or '-'} "
            f"action={session['suggested_action']}"
        )
        for item in session["repeated_prompts"]:
            lines.append(
                f"  - count={item['count']} first={item['first_timestamp'] or '-'} "
                f"latest={item['latest_timestamp'] or '-'} signature={item['signature']}"
            )
            for example in item["examples"]:
                lines.append(f"    example: {example['excerpt']}")
    return "\n".join(lines)


def normalize_prompt_text(text: str) -> str:
    """Return the deterministic prompt signature used for repeat grouping."""
    compact = " ".join(
        _MARKDOWN_PREFIX_RE.sub("", line).strip()
        for line in str(text).replace("\r\n", "\n").splitlines()
        if line.strip()
    ).lower()
    compact = _UUID_RE.sub(" uuid ", compact)
    compact = _HEX_RE.sub(" hash ", compact)
    compact = _PATH_RE.sub(" path ", compact)

    tokens: list[str] = []
    for match in _TOKEN_RE.finditer(compact):
        token = match.group(0).strip("'_-")
        if len(token) < 2 or token in STOPWORDS:
            continue
        if token.endswith("ies") and len(token) > 4:
            token = token[:-3] + "y"
        elif token.endswith("s") and len(token) > 3:
            token = token[:-1]
        if token and token not in STOPWORDS:
            tokens.append(token)
    return " ".join(tokens)


def _group_repeated_prompts(
    rows: Iterable[Mapping[str, Any]],
    *,
    threshold: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str | None], dict[str, list[dict[str, Any]]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for row in rows:
        prompt_text = _optional_text(row.get("prompt_text"))
        if not prompt_text:
            continue
        signature = normalize_prompt_text(prompt_text)
        if not signature:
            continue
        session_id = str(row.get("session_id") or "unknown-session")
        project_path = _optional_text(row.get("project_path"))
        grouped[(session_id, project_path)][signature].append({**dict(row), "prompt_text": prompt_text})

    sessions: list[dict[str, Any]] = []
    for (session_id, project_path), by_signature in grouped.items():
        repeated = []
        for signature, signature_rows in by_signature.items():
            if len(signature_rows) < threshold:
                continue
            ordered = sorted(signature_rows, key=_row_sort_key)
            repeated.append(
                {
                    "count": len(ordered),
                    "examples": [
                        {
                            "excerpt": _excerpt(str(row["prompt_text"])),
                            "message_uuid": _optional_text(row.get("message_uuid")),
                            "timestamp": _optional_text(row.get("timestamp")),
                        }
                        for row in ordered[:DEFAULT_EXAMPLE_LIMIT]
                    ],
                    "first_timestamp": _optional_text(ordered[0].get("timestamp")),
                    "latest_timestamp": _optional_text(ordered[-1].get("timestamp")),
                    "message_uuids": [
                        uuid
                        for uuid in (_optional_text(row.get("message_uuid")) for row in ordered)
                        if uuid
                    ],
                    "signature": signature,
                    "suggested_action": _suggested_action(ordered),
                }
            )
        if not repeated:
            continue
        repeated.sort(key=_repeat_sort_key)
        first_timestamp = min(
            (item["first_timestamp"] for item in repeated if item["first_timestamp"]),
            default=None,
        )
        latest_timestamp = max(
            (item["latest_timestamp"] for item in repeated if item["latest_timestamp"]),
            default=None,
        )
        repeated_count = sum(int(item["count"]) for item in repeated)
        sessions.append(
            {
                "first_timestamp": first_timestamp,
                "latest_timestamp": latest_timestamp,
                "project_path": project_path,
                "repeated_prompt_count": repeated_count,
                "repeated_prompts": repeated,
                "session_id": session_id,
                "suggested_action": (
                    "investigate_loop"
                    if any(item["suggested_action"] == "investigate_loop" for item in repeated)
                    else "consolidate_session_notes"
                ),
            }
        )
    return sessions


def _suggested_action(rows: list[Mapping[str, Any]]) -> str:
    timestamps = [_parse_datetime(row.get("timestamp")) for row in rows]
    parsed = [timestamp for timestamp in timestamps if timestamp is not None]
    if len(rows) >= 4:
        return "investigate_loop"
    if len(parsed) >= 2 and max(parsed) - min(parsed) <= timedelta(minutes=30):
        return "investigate_loop"
    return "consolidate_session_notes"


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


def _missing_columns(columns: set[str]) -> tuple[str, ...]:
    return tuple(
        column
        for column in (*REQUIRED_COLUMNS, *OPTIONAL_COLUMNS)
        if column not in columns
    )


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    project_path: str | None,
) -> list[dict[str, Any]]:
    select_columns = [
        _column_expr(columns, "id", "rowid"),
        _column_expr(columns, "session_id"),
        _column_expr(columns, "message_uuid"),
        _column_expr(columns, "project_path"),
        _column_expr(columns, "timestamp"),
        _column_expr(columns, "prompt_text"),
    ]
    where = []
    params: list[Any] = []
    if "timestamp" in columns:
        where.append("timestamp >= ?")
        params.append(cutoff.isoformat())
    if project_path and "project_path" in columns:
        where.append("project_path = ?")
        params.append(project_path)
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order_sql = "timestamp ASC, id ASC" if {"timestamp", "id"}.issubset(columns) else "rowid ASC"
    cursor = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM claude_messages
            {where_sql}
            ORDER BY {order_sql}""",
        params,
    )
    column_names = [description[0] for description in cursor.description]
    return [
        dict(row)
        if isinstance(row, Mapping)
        else dict(zip(column_names, row, strict=False))
        for row in cursor.fetchall()
    ]


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    return column if column in columns else f"{fallback} AS {column}"


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


def _row_sort_key(row: Mapping[str, Any]) -> tuple[str, int, str]:
    timestamp = _optional_text(row.get("timestamp")) or ""
    row_id = row.get("id")
    try:
        numeric_id = int(row_id)
    except (TypeError, ValueError):
        numeric_id = 0
    return (timestamp, numeric_id, str(row.get("message_uuid") or ""))


def _repeat_sort_key(item: Mapping[str, Any]) -> tuple[int, str, str]:
    return (-int(item["count"]), str(item.get("first_timestamp") or ""), str(item["signature"]))


def _session_sort_key(session: Mapping[str, Any]) -> tuple[int, str, str, str]:
    return (
        -int(session["repeated_prompt_count"]),
        str(session.get("first_timestamp") or ""),
        str(session["session_id"]),
        str(session.get("project_path") or ""),
    )


def _excerpt(text: str, max_chars: int = 180) -> str:
    compact = " ".join(text.split())
    if len(compact) <= max_chars:
        return compact
    return compact[: max(0, max_chars - 3)].rstrip() + "..."


def _mapping(row: Any) -> dict[str, Any]:
    return dict(row) if isinstance(row, Mapping) else dict(row)


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(
        value,
        (str, bytes, sqlite3.Connection),
    ) and not hasattr(value, "conn")


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
