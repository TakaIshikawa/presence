"""Report downstream artifact latency for Claude Code sessions."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 25
STATUSES = ("no_artifact", "commit_only", "content_only", "content_and_commit")

_STATUS_SORT_ORDER = {
    "no_artifact": 0,
    "commit_only": 1,
    "content_only": 2,
    "content_and_commit": 3,
}


@dataclass(frozen=True)
class ClaudeSessionArtifactLatency:
    """Artifact latency for one Claude session and project path."""

    session_id: str
    project_path: str | None
    message_count: int
    first_message_at: str | None
    last_message_at: str | None
    first_commit_at: str | None
    first_commit_sha: str | None
    first_commit_latency_seconds: int | None
    first_generated_content_at: str | None
    first_generated_content_id: int | None
    first_generated_content_latency_seconds: int | None
    first_artifact_at: str | None
    first_artifact_latency_seconds: int | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionArtifactLatencyReport:
    """Claude session artifact latency report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    sessions: tuple[ClaudeSessionArtifactLatency, ...]
    warnings: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_artifact_latency",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "sessions": [session.to_dict() for session in self.sessions],
            "totals": dict(sorted(self.totals.items())),
            "warnings": list(self.warnings),
        }


def build_claude_session_artifact_latency_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    project_path: str | None = None,
    status: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionArtifactLatencyReport:
    """Build a report showing how quickly Claude sessions produce artifacts."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    status = _optional_text(status)
    if status is not None and status not in STATUSES:
        raise ValueError(f"status must be one of: {', '.join(STATUSES)}")

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
        "status": status,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, tuple[str, ...]] = {}
    warnings: list[str] = []

    if "claude_messages" not in schema:
        missing_tables.append("claude_messages")
        return ClaudeSessionArtifactLatencyReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_totals([]),
            sessions=(),
            missing_tables=tuple(missing_tables),
            missing_columns=missing_columns,
        )

    message_columns = schema["claude_messages"]
    required_missing = tuple(
        column for column in ("session_id", "timestamp") if column not in message_columns
    )
    optional_missing = tuple(
        column for column in ("id", "message_uuid", "project_path") if column not in message_columns
    )
    if required_missing or optional_missing:
        missing_columns["claude_messages"] = required_missing + optional_missing
    messages = _load_messages(
        conn,
        message_columns,
        cutoff=cutoff,
        project_path=project_path,
        warnings=warnings,
    )
    filters["project_path_filter_applied"] = bool(project_path and "project_path" in message_columns)

    keys_by_message_id: dict[int, set[tuple[str, str | None]]] = defaultdict(set)
    keys_by_message_uuid: dict[str, set[tuple[str, str | None]]] = defaultdict(set)
    for message in messages:
        key = _session_key(message)
        message_id = _int_or_none(message.get("id"))
        if message_id is not None:
            keys_by_message_id[message_id].add(key)
        message_uuid = _optional_text(message.get("message_uuid"))
        if message_uuid:
            keys_by_message_uuid[message_uuid].add(key)

    first_commits = _first_linked_commits(
        conn,
        schema,
        keys_by_message_id,
        missing_tables,
        missing_columns,
    )
    first_content = _first_generated_content(
        conn,
        schema,
        keys_by_message_uuid,
        missing_tables,
        missing_columns,
        warnings,
    )

    sessions = _build_sessions(messages, first_commits, first_content)
    sessions.sort(key=_session_sort_key)
    if status:
        sessions = [session for session in sessions if session.status == status]

    return ClaudeSessionArtifactLatencyReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(sessions),
        sessions=tuple(sessions[:limit]),
        warnings=tuple(warnings),
        missing_tables=tuple(dict.fromkeys(missing_tables)),
        missing_columns=missing_columns,
    )


def format_claude_session_artifact_latency_json(
    report: ClaudeSessionArtifactLatencyReport,
) -> str:
    """Serialize a Claude session artifact latency report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_artifact_latency_text(
    report: ClaudeSessionArtifactLatencyReport,
) -> str:
    """Render a concise human-readable Claude session artifact latency report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Session Artifact Latency",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"project_path={filters['project_path'] or '-'} "
            f"project_filter_applied={filters['project_path_filter_applied']} "
            f"status={filters['status'] or '-'}"
        ),
        (
            "Totals: "
            f"sessions={totals['sessions_scanned']} "
            f"messages={totals['messages_scanned']} "
            f"no_artifact={totals['no_artifact']} "
            f"commit_only={totals['commit_only']} "
            f"content_only={totals['content_only']} "
            f"content_and_commit={totals['content_and_commit']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing optional tables: " + ", ".join(report.missing_tables))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in (report.missing_columns or {}).items()
        if columns
    ]
    if missing_columns:
        lines.append("Missing columns: " + "; ".join(missing_columns))
    if report.warnings:
        lines.append("Warnings:")
        lines.extend(f"  - {warning}" for warning in report.warnings)

    lines.extend(["", "Sessions:"])
    if not report.sessions:
        lines.append("- none")
    for session in report.sessions:
        lines.append(
            f"- session={session.session_id} project={session.project_path or '-'} "
            f"status={session.status} messages={session.message_count} "
            f"first_message={session.first_message_at or '-'} "
            f"last_message={session.last_message_at or '-'} "
            f"first_commit={session.first_commit_at or '-'} "
            f"commit_latency_s={_display_int(session.first_commit_latency_seconds)} "
            f"first_content={session.first_generated_content_at or '-'} "
            f"content_latency_s={_display_int(session.first_generated_content_latency_seconds)} "
            f"first_artifact_latency_s={_display_int(session.first_artifact_latency_seconds)}"
        )
    return "\n".join(lines)


def _build_sessions(
    messages: Iterable[Mapping[str, Any]],
    first_commits: Mapping[tuple[str, str | None], Mapping[str, Any]],
    first_content: Mapping[tuple[str, str | None], Mapping[str, Any]],
) -> list[ClaudeSessionArtifactLatency]:
    grouped: dict[tuple[str, str | None], list[Mapping[str, Any]]] = defaultdict(list)
    for message in messages:
        grouped[_session_key(message)].append(message)

    sessions: list[ClaudeSessionArtifactLatency] = []
    for key, rows in grouped.items():
        message_times = sorted(
            _parse_datetime(row.get("timestamp")) for row in rows if row.get("timestamp")
        )
        first_message = message_times[0] if message_times else None
        last_message = message_times[-1] if message_times else None
        commit = first_commits.get(key)
        content = first_content.get(key)
        commit_at = _parse_datetime(commit.get("timestamp")) if commit else None
        content_at = _parse_datetime(content.get("created_at")) if content else None
        artifact_times = [value for value in (commit_at, content_at) if value is not None]
        first_artifact_at = min(artifact_times) if artifact_times else None
        sessions.append(
            ClaudeSessionArtifactLatency(
                session_id=key[0],
                project_path=key[1],
                message_count=len(rows),
                first_message_at=_iso(first_message),
                last_message_at=_iso(last_message),
                first_commit_at=_iso(commit_at),
                first_commit_sha=_optional_text(commit.get("commit_sha")) if commit else None,
                first_commit_latency_seconds=_latency_seconds(first_message, commit_at),
                first_generated_content_at=_iso(content_at),
                first_generated_content_id=_int_or_none(content.get("id")) if content else None,
                first_generated_content_latency_seconds=_latency_seconds(first_message, content_at),
                first_artifact_at=_iso(first_artifact_at),
                first_artifact_latency_seconds=_latency_seconds(first_message, first_artifact_at),
                status=_classify(commit_at is not None, content_at is not None),
            )
        )
    return sessions


def _classify(has_commit: bool, has_content: bool) -> str:
    if has_commit and has_content:
        return "content_and_commit"
    if has_commit:
        return "commit_only"
    if has_content:
        return "content_only"
    return "no_artifact"


def _totals(sessions: Iterable[ClaudeSessionArtifactLatency]) -> dict[str, int]:
    session_list = list(sessions)
    totals = {
        "commit_only": 0,
        "content_and_commit": 0,
        "content_only": 0,
        "messages_scanned": sum(session.message_count for session in session_list),
        "no_artifact": 0,
        "sessions_scanned": len(session_list),
    }
    for session in session_list:
        totals[session.status] += 1
    return totals


def _load_messages(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
    project_path: str | None,
    warnings: list[str],
) -> list[dict[str, Any]]:
    if "timestamp" not in columns:
        warnings.append("claude_messages is missing timestamp; no sessions loaded")
        return []
    selected = [
        _column_expr(columns, "id"),
        _column_expr(columns, "session_id"),
        _column_expr(columns, "message_uuid"),
        _column_expr(columns, "project_path"),
        _column_expr(columns, "timestamp"),
    ]
    where = ["timestamp >= ?"]
    params: list[Any] = [_db_time(cutoff)]
    if project_path and "project_path" in columns:
        where.append("project_path = ?")
        params.append(project_path)
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
              FROM claude_messages
             WHERE {' AND '.join(where)}
             ORDER BY timestamp ASC, id ASC""",
        tuple(params),
    ).fetchall()
    return [dict(row) for row in rows]


def _first_linked_commits(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    keys_by_message_id: Mapping[int, set[tuple[str, str | None]]],
    missing_tables: list[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[tuple[str, str | None], dict[str, Any]]:
    if not keys_by_message_id:
        return {}
    if "commit_prompt_links" not in schema or "github_commits" not in schema:
        for table in ("commit_prompt_links", "github_commits"):
            if table not in schema:
                missing_tables.append(table)
        return {}
    required = {
        "commit_prompt_links": ("commit_id", "message_id"),
        "github_commits": ("id", "commit_sha", "timestamp"),
    }
    for table, columns in required.items():
        missing = tuple(column for column in columns if column not in schema[table])
        if missing:
            missing_columns[table] = missing
    if any(table in missing_columns for table in required):
        return {}

    message_ids = sorted(keys_by_message_id)
    placeholders = ", ".join("?" for _ in message_ids)
    rows = conn.execute(
        f"""SELECT DISTINCT
                   cpl.message_id,
                   gc.id,
                   gc.commit_sha,
                   gc.timestamp
              FROM commit_prompt_links cpl
              JOIN github_commits gc ON gc.id = cpl.commit_id
             WHERE cpl.message_id IN ({placeholders})
             ORDER BY gc.timestamp ASC, gc.id ASC""",
        tuple(message_ids),
    ).fetchall()
    first_by_session: dict[tuple[str, str | None], dict[str, Any]] = {}
    for row in rows:
        message_id = _int_or_none(row["message_id"])
        if message_id is None:
            continue
        candidate = dict(row)
        for key in keys_by_message_id.get(message_id, set()):
            existing = first_by_session.get(key)
            if existing is None or _artifact_sort_key(candidate, "timestamp") < _artifact_sort_key(
                existing, "timestamp"
            ):
                first_by_session[key] = candidate
    return first_by_session


def _first_generated_content(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    keys_by_message_uuid: Mapping[str, set[tuple[str, str | None]]],
    missing_tables: list[str],
    missing_columns: dict[str, tuple[str, ...]],
    warnings: list[str],
) -> dict[tuple[str, str | None], dict[str, Any]]:
    if not keys_by_message_uuid:
        return {}
    if "generated_content" not in schema:
        missing_tables.append("generated_content")
        return {}
    columns = schema["generated_content"]
    missing = tuple(column for column in ("id", "source_messages", "created_at") if column not in columns)
    if missing:
        missing_columns["generated_content"] = missing
        return {}

    rows = conn.execute(
        """SELECT id, source_messages, created_at
             FROM generated_content
            ORDER BY created_at ASC, id ASC"""
    ).fetchall()
    first_by_session: dict[tuple[str, str | None], dict[str, Any]] = {}
    for row in rows:
        content_id = _int_or_none(row["id"])
        if content_id is None:
            continue
        candidate = dict(row)
        for value in _json_list(row["source_messages"], "source_messages", content_id, warnings):
            for key in keys_by_message_uuid.get(str(value), set()):
                existing = first_by_session.get(key)
                if existing is None or _artifact_sort_key(candidate, "created_at") < _artifact_sort_key(
                    existing, "created_at"
                ):
                    first_by_session[key] = candidate
    return first_by_session


def _json_list(value: Any, field: str, row_id: int, warnings: list[str]) -> list[Any]:
    if value in (None, ""):
        return []
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        detail = exc.msg if isinstance(exc, json.JSONDecodeError) else str(exc)
        warnings.append(f"generated_content {row_id} has malformed {field}: {detail}")
        return []
    if not isinstance(parsed, list):
        warnings.append(
            f"generated_content {row_id} has non-list {field}: {type(parsed).__name__}"
        )
        return []
    return parsed


def _session_key(row: Mapping[str, Any]) -> tuple[str, str | None]:
    return (
        str(row.get("session_id") or "unknown-session"),
        _optional_text(row.get("project_path")),
    )


def _session_sort_key(session: ClaudeSessionArtifactLatency) -> tuple[int, int, str, str]:
    return (
        _STATUS_SORT_ORDER.get(session.status, 99),
        session.first_artifact_latency_seconds
        if session.first_artifact_latency_seconds is not None
        else 10**12,
        session.session_id,
        session.project_path or "",
    )


def _artifact_sort_key(row: Mapping[str, Any], timestamp_column: str) -> tuple[str, int]:
    return (
        _optional_text(row.get(timestamp_column)) or "",
        _int_or_none(row.get("id")) or 0,
    )


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {
            str(col["name"] if isinstance(col, sqlite3.Row) else col[1])
            for col in conn.execute(f"PRAGMA table_info({table})").fetchall()
        }
    return schema


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _db_time(value: datetime) -> str:
    return _ensure_utc(value).isoformat()


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    text = _optional_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _latency_seconds(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return int((end - start).total_seconds())


def _display_int(value: int | None) -> str:
    return str(value) if value is not None else "-"


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
