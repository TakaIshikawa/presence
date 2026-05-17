"""Measure how often Claude session artifacts become generated content."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100


def build_claude_session_content_yield_report(
    session_rows: list[dict[str, Any]],
    content_rows: list[dict[str, Any]] | None = None,
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    content_rows = content_rows or []
    records = []
    by_project: dict[str, Counter[str]] = defaultdict(Counter)
    for row in session_rows:
        session_id = _text(_first(row, "session_id", "id")) or "unknown"
        cwd = _text(_first(row, "cwd", "working_directory")) or None
        project = _text(_first(row, "project", "project_name")) or _project_from_cwd(cwd)
        session_at = _parse_ts(_first(row, "session_at", "timestamp", "created_at", "started_at"))
        age_days = round((generated_at - session_at).total_seconds() / 86400, 2) if session_at else None
        matches = sorted(_matched_content_ids(session_id, cwd, content_rows))
        status = "converted" if matches else "unconverted"
        by_project[project or "unknown"][status] += 1
        records.append(
            {
                "session_id": session_id,
                "cwd": cwd,
                "project": project,
                "session_at": session_at.isoformat() if session_at else None,
                "age_days": age_days,
                "age_bucket": _age_bucket(age_days),
                "conversion_status": status,
                "matched_content_ids": matches,
            }
        )
    records.sort(key=lambda item: (item["conversion_status"], -(item["age_days"] or 0), item["session_id"]))
    total = len(records)
    converted = sum(1 for item in records if item["conversion_status"] == "converted")
    return {
        "artifact_type": "claude_session_content_yield",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "totals": {
            "session_count": total,
            "converted": converted,
            "unconverted": total - converted,
            "conversion_rate": round(converted / total, 4) if total else 0.0,
            "by_project": [
                {
                    "project": project,
                    "session_count": counts["converted"] + counts["unconverted"],
                    "converted": counts["converted"],
                    "unconverted": counts["unconverted"],
                }
                for project, counts in sorted(by_project.items())
            ],
        },
        "sessions": records[:limit],
        "empty_state": {"is_empty": not records, "message": "No Claude session rows found." if not records else None},
    }


def build_claude_session_content_yield_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_claude_session_content_yield_report(_load_sessions(conn, schema), _load_content(conn, schema), **kwargs)


def format_claude_session_content_yield_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_claude_session_content_yield_text(report: dict[str, Any]) -> str:
    lines = [
        "Claude Session Content Yield",
        f"Generated: {report['generated_at']}",
        (
            f"Totals: sessions={report['totals']['session_count']} converted={report['totals']['converted']} "
            f"unconverted={report['totals']['unconverted']} conversion_rate={report['totals']['conversion_rate']:.2f}"
        ),
    ]
    if not report["sessions"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "session_id | project | age_bucket | status | matched_content_ids"])
    for row in report["sessions"]:
        lines.append(f"{row['session_id']} | {row['project'] or '-'} | {row['age_bucket']} | {row['conversion_status']} | {', '.join(row['matched_content_ids']) or '-'}")
    return "\n".join(lines)


format_claude_session_content_yield_table = format_claude_session_content_yield_text


def _load_sessions(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("claude_sessions", "claude_session_events", "claude_messages"):
        if table not in schema:
            continue
        cols = schema[table]
        selected = [
            _col(cols, "session_id", "id", default="NULL") + " AS session_id",
            _col(cols, "cwd", "working_directory", default="NULL") + " AS cwd",
            _col(cols, "project", "project_name", default="NULL") + " AS project",
            _col(cols, "timestamp", "created_at", "started_at", default="NULL") + " AS session_at",
        ]
        rows = [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
        by_id: dict[str, dict[str, Any]] = {}
        for row in rows:
            sid = _text(row.get("session_id")) or "unknown"
            by_id.setdefault(sid, row)
        return list(by_id.values())
    return []


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    cols = schema["generated_content"]
    selected = [
        _col(cols, "id", "content_id", default="NULL") + " AS content_id",
        _col(cols, "content", "body", "text", "metadata", default="NULL") + " AS content",
        _col(cols, "metadata", "raw_metadata", default="NULL") + " AS metadata",
        _col(cols, "source_session_ids", "claude_session_ids", default="NULL") + " AS source_session_ids",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content").fetchall()]


def _matched_content_ids(session_id: str, cwd: str | None, content_rows: list[dict[str, Any]]) -> set[str]:
    matched = set()
    for row in content_rows:
        haystack = " ".join(_text(_first(row, "content", "body", "text", "metadata")).split())
        explicit = _items(_first(row, "source_session_ids", "claude_session_ids"))
        if session_id in explicit or (session_id != "unknown" and re.search(rf"\b{re.escape(session_id)}\b", haystack)) or (cwd and cwd in haystack):
            matched.add(_text(_first(row, "content_id", "id")) or "unknown")
    return matched


def _items(value: Any) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    text = _text(value)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list):
        return [_text(item) for item in parsed if _text(item)]
    return [part.strip() for part in re.split(r"[,;\n]+", text) if part.strip()]


def _age_bucket(age_days: float | None) -> str:
    if age_days is None:
        return "unknown"
    if age_days <= 7:
        return "0_7_days"
    if age_days <= 30:
        return "8_30_days"
    return "over_30_days"


def _project_from_cwd(cwd: str | None) -> str | None:
    if not cwd:
        return None
    return cwd.rstrip("/").split("/")[-1] or None


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _parse_ts(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
