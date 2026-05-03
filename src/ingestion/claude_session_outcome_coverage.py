"""Report downstream artifact coverage for Claude Code sessions."""

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
STATUSES = ("no_commit", "commit_no_content", "content_generated", "idea_only")

_STATUS_SORT_ORDER = {
    "no_commit": 0,
    "commit_no_content": 1,
    "idea_only": 2,
    "content_generated": 3,
}


@dataclass(frozen=True)
class ClaudeSessionOutcome:
    """Outcome coverage for one Claude session and project path."""

    session_id: str
    project_path: str | None
    message_count: int
    first_seen_at: str | None
    last_seen_at: str | None
    linked_commit_count: int
    generated_content_count: int
    idea_count: int
    status: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeSessionOutcomeCoverageReport:
    """Claude session downstream artifact coverage report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    sessions: tuple[ClaudeSessionOutcome, ...]
    warnings: tuple[str, ...] = ()
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_outcome_coverage",
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


def build_claude_session_outcome_coverage_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    project_path: str | None = None,
    status: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionOutcomeCoverageReport:
    """Build a report showing whether Claude sessions produced downstream artifacts."""
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
        return ClaudeSessionOutcomeCoverageReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            totals=_totals([]),
            sessions=(),
            warnings=(),
            missing_tables=tuple(missing_tables),
            missing_columns=missing_columns,
        )

    message_columns = schema["claude_messages"]
    missing_message_columns = tuple(
        column for column in ("session_id", "timestamp") if column not in message_columns
    )
    optional_message_columns = tuple(
        column for column in ("id", "message_uuid", "project_path") if column not in message_columns
    )
    if missing_message_columns or optional_message_columns:
        missing_columns["claude_messages"] = missing_message_columns + optional_message_columns
    messages = _load_messages(
        conn,
        message_columns,
        cutoff=cutoff,
        project_path=project_path,
        warnings=warnings,
    )
    filters["project_path_filter_applied"] = bool(project_path and "project_path" in message_columns)

    session_keys_by_message_id: dict[int, set[tuple[str, str | None]]] = defaultdict(set)
    session_keys_by_message_uuid: dict[str, set[tuple[str, str | None]]] = defaultdict(set)
    session_keys_by_session_id: dict[str, set[tuple[str, str | None]]] = defaultdict(set)
    for message in messages:
        key = _session_key(message)
        message_id = _int_or_none(message.get("id"))
        if message_id is not None:
            session_keys_by_message_id[message_id].add(key)
        message_uuid = _optional_text(message.get("message_uuid"))
        if message_uuid:
            session_keys_by_message_uuid[message_uuid].add(key)
        session_keys_by_session_id[key[0]].add(key)

    commit_counts = _linked_commit_counts(conn, schema, session_keys_by_message_id, missing_tables, missing_columns)
    content_counts = _generated_content_counts(
        conn,
        schema,
        session_keys_by_message_uuid,
        missing_tables,
        missing_columns,
        warnings,
    )
    idea_counts = _content_idea_counts(
        conn,
        schema,
        session_keys_by_message_uuid,
        session_keys_by_session_id,
        missing_tables,
        missing_columns,
        warnings,
    )

    sessions = _build_sessions(messages, commit_counts, content_counts, idea_counts)
    sessions.sort(key=_session_sort_key)
    if status:
        sessions = [session for session in sessions if session.status == status]

    return ClaudeSessionOutcomeCoverageReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals=_totals(sessions),
        sessions=tuple(sessions[:limit]),
        warnings=tuple(warnings),
        missing_tables=tuple(dict.fromkeys(missing_tables)),
        missing_columns=missing_columns,
    )


def format_claude_session_outcome_coverage_json(
    report: ClaudeSessionOutcomeCoverageReport,
) -> str:
    """Serialize a Claude session outcome coverage report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_outcome_coverage_text(
    report: ClaudeSessionOutcomeCoverageReport,
) -> str:
    """Render a concise human-readable Claude session outcome coverage report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Session Outcome Coverage",
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
            f"no_commit={totals['no_commit']} "
            f"commit_no_content={totals['commit_no_content']} "
            f"content_generated={totals['content_generated']} "
            f"idea_only={totals['idea_only']}"
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
            f"first={session.first_seen_at or '-'} last={session.last_seen_at or '-'} "
            f"commits={session.linked_commit_count} "
            f"generated_content={session.generated_content_count} "
            f"ideas={session.idea_count}"
        )
    return "\n".join(lines)


def _build_sessions(
    messages: Iterable[Mapping[str, Any]],
    commit_counts: Mapping[tuple[str, str | None], int],
    content_counts: Mapping[tuple[str, str | None], int],
    idea_counts: Mapping[tuple[str, str | None], int],
) -> list[ClaudeSessionOutcome]:
    grouped: dict[tuple[str, str | None], list[Mapping[str, Any]]] = defaultdict(list)
    for message in messages:
        grouped[_session_key(message)].append(message)

    sessions: list[ClaudeSessionOutcome] = []
    for key, rows in grouped.items():
        timestamps = sorted(_optional_text(row.get("timestamp")) for row in rows if row.get("timestamp"))
        linked_commit_count = int(commit_counts.get(key, 0))
        generated_content_count = int(content_counts.get(key, 0))
        idea_count = int(idea_counts.get(key, 0))
        sessions.append(
            ClaudeSessionOutcome(
                session_id=key[0],
                project_path=key[1],
                message_count=len(rows),
                first_seen_at=timestamps[0] if timestamps else None,
                last_seen_at=timestamps[-1] if timestamps else None,
                linked_commit_count=linked_commit_count,
                generated_content_count=generated_content_count,
                idea_count=idea_count,
                status=_classify(linked_commit_count, generated_content_count, idea_count),
            )
        )
    return sessions


def _classify(linked_commit_count: int, generated_content_count: int, idea_count: int) -> str:
    if generated_content_count > 0:
        return "content_generated"
    if linked_commit_count > 0:
        return "commit_no_content"
    if idea_count > 0:
        return "idea_only"
    return "no_commit"


def _totals(sessions: Iterable[ClaudeSessionOutcome]) -> dict[str, int]:
    session_list = list(sessions)
    totals = {
        "commit_no_content": 0,
        "content_generated": 0,
        "idea_only": 0,
        "messages_scanned": sum(session.message_count for session in session_list),
        "no_commit": 0,
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


def _linked_commit_counts(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    session_keys_by_message_id: Mapping[int, set[tuple[str, str | None]]],
    missing_tables: list[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> dict[tuple[str, str | None], int]:
    if not session_keys_by_message_id:
        return {}
    if "commit_prompt_links" not in schema or "github_commits" not in schema:
        for table in ("commit_prompt_links", "github_commits"):
            if table not in schema:
                missing_tables.append(table)
        return {}
    required = {
        "commit_prompt_links": ("commit_id", "message_id"),
        "github_commits": ("id",),
    }
    for table, columns in required.items():
        missing = tuple(column for column in columns if column not in schema[table])
        if missing:
            missing_columns[table] = missing
    if any(table in missing_columns for table in required):
        return {}

    message_ids = sorted(session_keys_by_message_id)
    placeholders = ", ".join("?" for _ in message_ids)
    rows = conn.execute(
        f"""SELECT DISTINCT cpl.message_id, gc.id AS commit_id
              FROM commit_prompt_links cpl
              JOIN github_commits gc ON gc.id = cpl.commit_id
             WHERE cpl.message_id IN ({placeholders})
             ORDER BY cpl.message_id ASC, gc.id ASC""",
        tuple(message_ids),
    ).fetchall()
    commits_by_session: dict[tuple[str, str | None], set[int]] = defaultdict(set)
    for row in rows:
        message_id = _int_or_none(row["message_id"])
        commit_id = _int_or_none(row["commit_id"])
        if message_id is None or commit_id is None:
            continue
        for key in session_keys_by_message_id.get(message_id, set()):
            commits_by_session[key].add(commit_id)
    return {key: len(commit_ids) for key, commit_ids in commits_by_session.items()}


def _generated_content_counts(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    session_keys_by_message_uuid: Mapping[str, set[tuple[str, str | None]]],
    missing_tables: list[str],
    missing_columns: dict[str, tuple[str, ...]],
    warnings: list[str],
) -> dict[tuple[str, str | None], int]:
    if not session_keys_by_message_uuid:
        return {}
    if "generated_content" not in schema:
        missing_tables.append("generated_content")
        return {}
    columns = schema["generated_content"]
    missing = tuple(column for column in ("id", "source_messages") if column not in columns)
    if missing:
        missing_columns["generated_content"] = missing
        return {}

    rows = conn.execute(
        "SELECT id, source_messages FROM generated_content ORDER BY id ASC"
    ).fetchall()
    content_by_session: dict[tuple[str, str | None], set[int]] = defaultdict(set)
    for row in rows:
        content_id = _int_or_none(row["id"])
        if content_id is None:
            continue
        for value in _json_list(row["source_messages"], "source_messages", content_id, warnings):
            for key in session_keys_by_message_uuid.get(str(value), set()):
                content_by_session[key].add(content_id)
    return {key: len(content_ids) for key, content_ids in content_by_session.items()}


def _content_idea_counts(
    conn: sqlite3.Connection,
    schema: Mapping[str, set[str]],
    session_keys_by_message_uuid: Mapping[str, set[tuple[str, str | None]]],
    session_keys_by_session_id: Mapping[str, set[tuple[str, str | None]]],
    missing_tables: list[str],
    missing_columns: dict[str, tuple[str, ...]],
    warnings: list[str],
) -> dict[tuple[str, str | None], int]:
    if "content_ideas" not in schema:
        missing_tables.append("content_ideas")
        return {}
    if not session_keys_by_message_uuid and not session_keys_by_session_id:
        return {}
    columns = schema["content_ideas"]
    missing = tuple(column for column in ("id", "source_metadata") if column not in columns)
    if missing:
        missing_columns["content_ideas"] = missing
        return {}

    rows = conn.execute(
        "SELECT id, source_metadata FROM content_ideas ORDER BY id ASC"
    ).fetchall()
    ideas_by_session: dict[tuple[str, str | None], set[int]] = defaultdict(set)
    for row in rows:
        idea_id = _int_or_none(row["id"])
        if idea_id is None:
            continue
        metadata = _json_object(row["source_metadata"], "source_metadata", idea_id, warnings)
        if not metadata:
            continue
        refs = {str(value) for value in _walk_json_scalars(metadata) if value not in (None, "")}
        for ref in refs:
            for key in session_keys_by_message_uuid.get(ref, set()):
                ideas_by_session[key].add(idea_id)
            for key in session_keys_by_session_id.get(ref, set()):
                ideas_by_session[key].add(idea_id)
    return {key: len(idea_ids) for key, idea_ids in ideas_by_session.items()}


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


def _json_object(value: Any, field: str, row_id: int, warnings: list[str]) -> dict[str, Any] | None:
    if value in (None, ""):
        return None
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError) as exc:
        detail = exc.msg if isinstance(exc, json.JSONDecodeError) else str(exc)
        warnings.append(f"content_ideas {row_id} has malformed {field}: {detail}")
        return None
    if not isinstance(parsed, dict):
        warnings.append(
            f"content_ideas {row_id} has non-object {field}: {type(parsed).__name__}"
        )
        return None
    return parsed


def _walk_json_scalars(value: Any) -> Iterable[Any]:
    if isinstance(value, dict):
        for nested in value.values():
            yield from _walk_json_scalars(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_json_scalars(nested)
    else:
        yield value


def _session_key(row: Mapping[str, Any]) -> tuple[str, str | None]:
    return (
        str(row.get("session_id") or "unknown-session"),
        _optional_text(row.get("project_path")),
    )


def _session_sort_key(session: ClaudeSessionOutcome) -> tuple[int, str, str, str]:
    return (
        _STATUS_SORT_ORDER.get(session.status, 99),
        session.last_seen_at or "",
        session.session_id,
        session.project_path or "",
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
