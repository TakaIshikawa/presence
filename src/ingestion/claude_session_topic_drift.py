"""Detect substantial topic drift across prompts in Claude sessions."""

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
DEFAULT_THRESHOLD = 0.72
DEFAULT_EXCERPT_CHARS = 140

STOPWORDS = frozenset(
    {
        "a",
        "about",
        "above",
        "add",
        "after",
        "again",
        "all",
        "also",
        "am",
        "an",
        "and",
        "any",
        "are",
        "as",
        "at",
        "be",
        "because",
        "been",
        "before",
        "being",
        "by",
        "can",
        "could",
        "do",
        "does",
        "done",
        "for",
        "from",
        "get",
        "had",
        "has",
        "have",
        "help",
        "how",
        "i",
        "if",
        "in",
        "into",
        "is",
        "it",
        "its",
        "just",
        "let",
        "make",
        "me",
        "my",
        "need",
        "needs",
        "new",
        "now",
        "of",
        "on",
        "or",
        "our",
        "please",
        "run",
        "should",
        "so",
        "that",
        "the",
        "their",
        "then",
        "there",
        "this",
        "to",
        "up",
        "use",
        "using",
        "we",
        "what",
        "when",
        "where",
        "which",
        "with",
        "work",
        "would",
        "you",
        "your",
    }
)

_TOKEN_RE = re.compile(r"[a-z0-9]+(?:[-_'][a-z0-9]+)?")
_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
_REQUIRED_COLUMNS = ("session_id", "timestamp", "prompt_text")
_OPTIONAL_COLUMNS = ("id", "message_uuid", "project_path")


@dataclass(frozen=True)
class ClaudeSessionDriftPoint:
    """One adjacent-prompt topic drift point."""

    from_message_uuid: str | None
    to_message_uuid: str | None
    from_timestamp: str | None
    to_timestamp: str | None
    from_excerpt: str
    to_excerpt: str
    from_keywords: tuple[str, ...]
    to_keywords: tuple[str, ...]
    drift_score: float
    recommendation: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["from_keywords"] = list(self.from_keywords)
        payload["to_keywords"] = list(self.to_keywords)
        return payload


@dataclass(frozen=True)
class ClaudeSessionTopicDriftSession:
    """Topic drift findings for one Claude session."""

    session_id: str
    project_path: str | None
    first_timestamp: str | None
    last_timestamp: str | None
    message_count: int
    max_drift_score: float
    drift_points: tuple[ClaudeSessionDriftPoint, ...]
    recommendation: str

    @property
    def has_drift(self) -> bool:
        return bool(self.drift_points)

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["drift_points"] = [point.to_dict() for point in self.drift_points]
        payload["has_drift"] = self.has_drift
        return payload


@dataclass(frozen=True)
class ClaudeSessionTopicDriftReport:
    """Claude session topic drift report."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    sessions: tuple[ClaudeSessionTopicDriftSession, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "claude_session_topic_drift",
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


def build_claude_session_topic_drift_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    project_path: str | None = None,
    threshold: float = DEFAULT_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> ClaudeSessionTopicDriftReport:
    """Build a deterministic topic-drift report for recent Claude messages."""
    if days <= 0:
        raise ValueError("days must be positive")
    if threshold <= 0 or threshold > 1:
        raise ValueError("threshold must be greater than 0 and at most 1")
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
        "threshold": threshold,
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
            rows = _load_rows(
                conn,
                columns,
                cutoff=cutoff,
                project_path=project_path,
            )
            filters["project_path_filter_applied"] = bool(
                project_path and "project_path" in columns
            )

    sessions = _analyze_sessions(rows, threshold=threshold)
    sessions.sort(key=_session_sort_key)
    return ClaudeSessionTopicDriftReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={
            "messages_scanned": sum(session.message_count for session in sessions),
            "sessions_flagged": sum(1 for session in sessions if session.has_drift),
            "sessions_scanned": len(sessions),
        },
        sessions=tuple(sessions[:limit]),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_claude_session_topic_drift_json(
    report: ClaudeSessionTopicDriftReport,
) -> str:
    """Serialize a topic-drift report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_session_topic_drift_text(
    report: ClaudeSessionTopicDriftReport,
) -> str:
    """Render a concise human-readable topic-drift report."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Claude Session Topic Drift",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"days={filters['days']} limit={filters['limit']} "
            f"threshold={filters['threshold']:.2f} "
            f"project_path={filters['project_path'] or '-'} "
            f"project_filter_applied={filters['project_path_filter_applied']}"
        ),
        (
            "Totals: "
            f"sessions={totals['sessions_scanned']} "
            f"flagged={totals['sessions_flagged']} "
            f"messages={totals['messages_scanned']}"
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
            f"max_drift={session.max_drift_score:.3f} "
            f"drift_points={len(session.drift_points)} "
            f"recommendation={session.recommendation}"
        )
        for point in session.drift_points:
            lines.append(
                f"  - {point.from_timestamp or '-'} -> {point.to_timestamp or '-'} "
                f"score={point.drift_score:.3f} "
                f"recommendation={point.recommendation}"
            )
            lines.append(f"    from: {point.from_excerpt}")
            lines.append(f"    to: {point.to_excerpt}")
    return "\n".join(lines)


def _analyze_sessions(
    rows: Iterable[Mapping[str, Any]],
    *,
    threshold: float,
) -> list[ClaudeSessionTopicDriftSession]:
    grouped: dict[tuple[str, str | None], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        prompt_text = _optional_text(row.get("prompt_text"))
        if not prompt_text:
            continue
        session_id = str(row.get("session_id") or "unknown-session")
        project_path = _optional_text(row.get("project_path"))
        grouped[(session_id, project_path)].append({**dict(row), "prompt_text": prompt_text})

    sessions: list[ClaudeSessionTopicDriftSession] = []
    for (session_id, project_path), session_rows in grouped.items():
        ordered = sorted(session_rows, key=_row_sort_key)
        drift_points: list[ClaudeSessionDriftPoint] = []
        max_drift_score = 0.0
        for previous, current in zip(ordered, ordered[1:]):
            previous_keywords = tokenize_prompt_keywords(previous["prompt_text"])
            current_keywords = tokenize_prompt_keywords(current["prompt_text"])
            drift_score = jaccard_distance(previous_keywords, current_keywords)
            max_drift_score = max(max_drift_score, drift_score)
            if drift_score >= threshold:
                drift_points.append(
                    ClaudeSessionDriftPoint(
                        from_message_uuid=_optional_text(previous.get("message_uuid")),
                        to_message_uuid=_optional_text(current.get("message_uuid")),
                        from_timestamp=_optional_text(previous.get("timestamp")),
                        to_timestamp=_optional_text(current.get("timestamp")),
                        from_excerpt=_excerpt(previous["prompt_text"]),
                        to_excerpt=_excerpt(current["prompt_text"]),
                        from_keywords=tuple(sorted(previous_keywords)),
                        to_keywords=tuple(sorted(current_keywords)),
                        drift_score=round(drift_score, 6),
                        recommendation="split_session_summary",
                    )
                )
        sessions.append(
            ClaudeSessionTopicDriftSession(
                session_id=session_id,
                project_path=project_path,
                first_timestamp=_optional_text(ordered[0].get("timestamp")),
                last_timestamp=_optional_text(ordered[-1].get("timestamp")),
                message_count=len(ordered),
                max_drift_score=round(max_drift_score, 6),
                drift_points=tuple(drift_points),
                recommendation=(
                    "split_session_summary" if drift_points else "preserve_single_session"
                ),
            )
        )
    return sessions


def tokenize_prompt_keywords(text: str) -> frozenset[str]:
    """Tokenize prompt text into deterministic normalized keywords."""
    keywords = set()
    for match in _TOKEN_RE.finditer(text.lower()):
        token = _NON_ALNUM_RE.sub("", match.group(0))
        if len(token) < 3 or token in STOPWORDS or token.isdigit():
            continue
        if token.endswith("ies") and len(token) > 4:
            token = token[:-3] + "y"
        elif token.endswith("s") and len(token) > 4:
            token = token[:-1]
        if token and token not in STOPWORDS:
            keywords.add(token)
    return frozenset(keywords)


def jaccard_distance(left: set[str] | frozenset[str], right: set[str] | frozenset[str]) -> float:
    """Return Jaccard distance between two keyword sets."""
    if not left and not right:
        return 0.0
    union = left | right
    if not union:
        return 0.0
    return 1.0 - (len(left & right) / len(union))


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
    session: ClaudeSessionTopicDriftSession,
) -> tuple[float, str, str, str]:
    return (
        -session.max_drift_score,
        session.first_timestamp or "",
        session.session_id,
        session.project_path or "",
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
