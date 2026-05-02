"""Detect Claude sessions with vague prompts that lack local context anchors."""

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
DEFAULT_LIMIT = 25
DEFAULT_EXCERPT_CHARS = 180

_REQUIRED_COLUMNS = ("session_id", "timestamp", "prompt_text")
_OPTIONAL_COLUMNS = ("id", "message_uuid", "project_path")

_VAGUE_REFERENCE_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("that_error", re.compile(r"\bthat\s+(?:same\s+)?error\b", re.IGNORECASE)),
    (
        "previous_failure",
        re.compile(r"\b(?:the\s+)?previous\s+(?:failure|error|issue|problem)\b", re.IGNORECASE),
    ),
    ("same_issue", re.compile(r"\bsame\s+(?:issue|problem|failure|error)\b", re.IGNORECASE)),
    ("failing_test", re.compile(r"\b(?:the\s+)?failing\s+test\b", re.IGNORECASE)),
    ("that_failure", re.compile(r"\bthat\s+(?:failure|issue|problem|traceback)\b", re.IGNORECASE)),
    ("it_still_fails", re.compile(r"\b(?:it|this|that)\s+still\s+fails?\b", re.IGNORECASE)),
    ("fix_it", re.compile(r"\b(?:fix|debug|investigate)\s+(?:it|that|this)\b", re.IGNORECASE)),
)

_FILE_PATH_RE = re.compile(
    r"(?:^|\s)(?:[\w./-]+/[\w./-]+\.(?:py|js|ts|tsx|jsx|go|rs|rb|java|md|sql|yml|yaml|toml|json)|[\w.-]+\.(?:py|js|ts|tsx|jsx|go|rs|rb|java|md|sql|yml|yaml|toml|json)(?::\d+)?)"
)
_COMMAND_RE = re.compile(
    r"(?:`[^`]*(?:pytest|uv|npm|pnpm|yarn|python|ruff|mypy|git|make)[^`]*`|\b(?:uv\s+run\s+pytest|pytest|npm\s+test|pnpm\s+test|yarn\s+test|python\s+-m\s+pytest|ruff\s+check|mypy|git\s+\w+|make\s+\w+)\b)",
    re.IGNORECASE,
)
_TRACEBACK_RE = re.compile(
    r"\b(?:Traceback \(most recent call last\)|AssertionError|TypeError|ValueError|KeyError|sqlite3\.\w+|Error:|Exception:)\b"
)
_TEST_IDENTIFIER_RE = re.compile(
    r"\b(?:test_[a-zA-Z0-9_]+|[A-Za-z_][\w.]*::test_[A-Za-z0-9_]+)\b"
)


@dataclass(frozen=True)
class ClaudeUnresolvedReferencePoint:
    """One prompt that uses unresolved contextual language."""

    message_uuid: str | None
    timestamp: str | None
    excerpt: str
    vague_references: tuple[str, ...]
    anchor_evidence: tuple[str, ...]
    unresolved_score: float
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["anchor_evidence"] = list(self.anchor_evidence)
        payload["vague_references"] = list(self.vague_references)
        return payload


@dataclass(frozen=True)
class ClaudeUnresolvedReferenceSession:
    """Unresolved-reference findings for one Claude session."""

    session_id: str
    project_path: str | None
    first_timestamp: str | None
    last_timestamp: str | None
    message_count: int
    vague_reference_count: int
    unresolved_reference_count: int
    vague_reference_density: float
    max_unresolved_score: float
    findings: tuple[ClaudeUnresolvedReferencePoint, ...]
    recommendation: str

    @property
    def has_unresolved_references(self) -> bool:
        return bool(self.findings)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["findings"] = [finding.to_dict() for finding in self.findings]
        payload["has_unresolved_references"] = self.has_unresolved_references
        return payload


@dataclass(frozen=True)
class ClaudeUnresolvedReferencesReport:
    """Claude unresolved-reference report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    sessions: tuple[ClaudeUnresolvedReferenceSession, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_unresolved_references",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "sessions": [session.to_dict() for session in self.sessions],
            "totals": dict(sorted(self.totals.items())),
        }


def build_claude_session_unresolved_reference_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    project_path: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeUnresolvedReferencesReport:
    """Build a deterministic report for vague Claude prompts without local anchors."""
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

    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}
    if _looks_like_rows(db_or_rows):
        rows = [_mapping(row) for row in db_or_rows]
        rows = _filter_rows(rows, cutoff=cutoff, project_path=project_path)
        filters["project_path_filter_applied"] = bool(project_path)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        if "claude_messages" not in schema:
            missing_tables = ("claude_messages",)
            rows = []
        else:
            columns = schema["claude_messages"]
            missing_columns = _missing_columns(columns)
            rows = _load_rows(conn, columns, cutoff=cutoff, project_path=project_path)
            filters["project_path_filter_applied"] = bool(
                project_path and "project_path" in columns
            )

    all_sessions = _analyze_sessions(rows)
    flagged_sessions = [session for session in all_sessions if session.has_unresolved_references]
    flagged_sessions.sort(key=_session_sort_key)
    return ClaudeUnresolvedReferencesReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "messages_scanned": sum(session.message_count for session in all_sessions),
            "sessions_flagged": len(flagged_sessions),
            "sessions_scanned": len(all_sessions),
            "unresolved_references": sum(
                session.unresolved_reference_count for session in flagged_sessions
            ),
            "vague_references": sum(session.vague_reference_count for session in all_sessions),
        },
        sessions=tuple(flagged_sessions[:limit]),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_unresolved_references_json(
    report: ClaudeUnresolvedReferencesReport,
) -> str:
    """Serialize an unresolved-reference report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_unresolved_references_text(
    report: ClaudeUnresolvedReferencesReport,
) -> str:
    """Render a concise human-readable unresolved-reference report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Unresolved References",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"project_path={filters['project_path'] or '-'} "
            f"project_filter_applied={filters['project_path_filter_applied']}"
        ),
        (
            "Totals: "
            f"sessions={totals['sessions_scanned']} "
            f"flagged={totals['sessions_flagged']} "
            f"messages={totals['messages_scanned']} "
            f"vague_references={totals['vague_references']} "
            f"unresolved_references={totals['unresolved_references']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    missing_columns = [
        f"{table}({', '.join(columns)})"
        for table, columns in (report.missing_columns or {}).items()
        if columns
    ]
    if missing_columns:
        lines.append("Missing columns: " + "; ".join(missing_columns))

    lines.extend(["", "Sessions:"])
    if not report.sessions:
        lines.append("- none")
    for session in report.sessions:
        lines.append(
            f"- session={session.session_id} project={session.project_path or '-'} "
            f"messages={session.message_count} "
            f"density={session.vague_reference_density:.3f} "
            f"max_score={session.max_unresolved_score:.3f} "
            f"findings={len(session.findings)} "
            f"recommendation={session.recommendation}"
        )
        for finding in session.findings:
            refs = ", ".join(finding.vague_references) or "-"
            anchors = ", ".join(finding.anchor_evidence) or "none"
            lines.append(
                f"  - {finding.timestamp or '-'} score={finding.unresolved_score:.3f} "
                f"recommendation={finding.recommendation} refs={refs} anchors={anchors}"
            )
            lines.append(f"    excerpt: {finding.excerpt}")
    return "\n".join(lines)


def _analyze_sessions(rows: Iterable[Mapping[str, Any]]) -> list[ClaudeUnresolvedReferenceSession]:
    grouped: dict[tuple[str, str | None], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        prompt_text = _optional_text(row.get("prompt_text"))
        if not prompt_text:
            continue
        session_id = str(row.get("session_id") or "unknown-session")
        project_path = _optional_text(row.get("project_path"))
        grouped[(session_id, project_path)].append({**dict(row), "prompt_text": prompt_text})

    sessions: list[ClaudeUnresolvedReferenceSession] = []
    for (session_id, project_path), session_rows in grouped.items():
        ordered = sorted(session_rows, key=_row_sort_key)
        findings: list[ClaudeUnresolvedReferencePoint] = []
        vague_reference_count = 0
        vague_prompt_count = 0
        max_unresolved_score = 0.0
        for index, row in enumerate(ordered):
            prompt_text = row["prompt_text"]
            vague_refs = _vague_references(prompt_text)
            if not vague_refs:
                continue
            vague_reference_count += len(vague_refs)
            vague_prompt_count += 1
            if index == 0:
                continue
            local_anchor_evidence = _local_anchor_evidence(ordered, index)
            current_anchor_evidence = _anchor_evidence(prompt_text)
            if local_anchor_evidence:
                continue
            unresolved_score = _unresolved_score(
                vague_reference_count=len(vague_refs),
                message_count=len(ordered),
                current_anchor_count=len(current_anchor_evidence),
            )
            max_unresolved_score = max(max_unresolved_score, unresolved_score)
            findings.append(
                ClaudeUnresolvedReferencePoint(
                    message_uuid=_optional_text(row.get("message_uuid")),
                    timestamp=_optional_text(row.get("timestamp")),
                    excerpt=_excerpt(prompt_text),
                    vague_references=vague_refs,
                    anchor_evidence=(),
                    unresolved_score=unresolved_score,
                    recommendation="add_context_anchor",
                )
            )

        message_count = len(ordered)
        sessions.append(
            ClaudeUnresolvedReferenceSession(
                session_id=session_id,
                project_path=project_path,
                first_timestamp=_optional_text(ordered[0].get("timestamp")),
                last_timestamp=_optional_text(ordered[-1].get("timestamp")),
                message_count=message_count,
                vague_reference_count=vague_reference_count,
                unresolved_reference_count=len(findings),
                vague_reference_density=round(vague_prompt_count / message_count, 6),
                max_unresolved_score=round(max_unresolved_score, 6),
                findings=tuple(findings),
                recommendation="add_context_anchor" if findings else "preserve_context",
            )
        )
    return sessions


def _vague_references(text: str) -> tuple[str, ...]:
    names = [
        name
        for name, pattern in _VAGUE_REFERENCE_PATTERNS
        if pattern.search(text)
    ]
    return tuple(sorted(names))


def _local_anchor_evidence(rows: list[dict[str, Any]], index: int) -> tuple[str, ...]:
    evidence: set[str] = set()
    for nearby in rows[max(0, index - 1) : index + 1]:
        evidence.update(_anchor_evidence(nearby["prompt_text"]))
    return tuple(sorted(evidence))


def _anchor_evidence(text: str) -> tuple[str, ...]:
    evidence = []
    if _FILE_PATH_RE.search(text):
        evidence.append("file_path")
    if _COMMAND_RE.search(text):
        evidence.append("command")
    if _TRACEBACK_RE.search(text):
        evidence.append("traceback")
    if _TEST_IDENTIFIER_RE.search(text):
        evidence.append("test_identifier")
    return tuple(evidence)


def _unresolved_score(
    *,
    vague_reference_count: int,
    message_count: int,
    current_anchor_count: int,
) -> float:
    base = 0.65
    vague_bonus = min(0.2, 0.05 * vague_reference_count)
    density_bonus = min(0.15, vague_reference_count / max(message_count, 1))
    anchor_penalty = min(0.25, current_anchor_count * 0.08)
    return round(max(0.0, min(1.0, base + vague_bonus + density_bonus - anchor_penalty)), 6)


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


def _missing_columns(columns: set[str]) -> dict[str, tuple[str, ...]]:
    missing = tuple(
        column
        for column in (*_REQUIRED_COLUMNS, *_OPTIONAL_COLUMNS)
        if column not in columns
    )
    return {"claude_messages": missing} if missing else {}


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


def _session_sort_key(
    session: ClaudeUnresolvedReferenceSession,
) -> tuple[float, float, str, str]:
    return (
        -session.max_unresolved_score,
        -session.vague_reference_density,
        session.first_timestamp or "",
        session.session_id,
    )


def _excerpt(text: str, max_chars: int = DEFAULT_EXCERPT_CHARS) -> str:
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
