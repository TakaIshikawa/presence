"""Mine durable technical decisions from recent Claude Code messages."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_DAYS = 14
DEFAULT_MIN_CONFIDENCE = 0.7
EXCERPT_CHARS = 280

TEXT_COLUMNS = ("prompt_text", "response_text", "content", "text", "message", "body")
REQUIRED_COLUMNS = ("session_id", "timestamp", "prompt_text")
OPTIONAL_COLUMNS = ("id", "message_uuid", "project_path", "response_text")

DECISION_RE = re.compile(
    r"\b(decided|decision is|we chose|chose to|picked|settled on)\b",
    re.IGNORECASE,
)
TRADEOFF_RE = re.compile(
    r"\b(because|trade[- ]?off|instead of|rather than|so that|to avoid)\b",
    re.IGNORECASE,
)
DEFER_RE = re.compile(r"\b(defer|deferred|postpone|later|follow[- ]?up)\b", re.IGNORECASE)
REJECT_RE = re.compile(
    r"\b(rejected|skip|skipped|avoid|avoided|not use|won't use|instead)\b",
    re.IGNORECASE,
)
TECHNICAL_RE = re.compile(
    r"\b(api|cli|schema|database|sqlite|test|pytest|module|script|function|class|"
    r"json|text|report|pipeline|session|message|regex|heuristic|formatter|"
    r"migration|table|column|validation|builder|runner|cache|config)\b",
    re.IGNORECASE,
)
UNCERTAIN_RE = re.compile(r"\b(maybe|possibly|might|could|consider|unclear|not sure)\b", re.I)


@dataclass(frozen=True)
class ClaudeDecision:
    decision_id: str
    session_id: str
    timestamp: str | None
    project_path: str | None
    excerpt: str
    decision_type: str
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ClaudeDecisionSession:
    session_id: str
    project_path: str | None
    decisions: tuple[ClaudeDecision, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision_count": len(self.decisions),
            "decisions": [decision.to_dict() for decision in self.decisions],
            "project_path": self.project_path,
            "session_id": self.session_id,
        }


@dataclass(frozen=True)
class ClaudeDecisionReport:
    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    source_table: str | None
    schema_gaps: dict[str, Any]
    totals: dict[str, int]
    sessions: tuple[ClaudeDecisionSession, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "filters": self.filters,
            "generated_at": self.generated_at,
            "schema_gaps": self.schema_gaps,
            "sessions": [session.to_dict() for session in self.sessions],
            "source_table": self.source_table,
            "totals": self.totals,
        }


def build_claude_decision_miner_report(
    db_or_rows: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
) -> ClaudeDecisionReport:
    """Return recent decision-like Claude messages grouped by session."""
    if days <= 0:
        raise ValueError("days must be positive")
    _validate_confidence(min_confidence)

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "lookback_end": generated_at.isoformat(),
        "lookback_start": cutoff.isoformat(),
        "min_confidence": min_confidence,
    }
    schema_gaps: dict[str, Any] = {"missing_columns": {}, "missing_tables": []}

    if _looks_like_rows(db_or_rows):
        source_table = "rows"
        rows = [_mapping(row) for row in db_or_rows]
        rows = _filter_rows(rows, cutoff=cutoff)
    else:
        conn = _connection(db_or_rows)
        schema = _schema(conn)
        source_table = "claude_messages" if "claude_messages" in schema else None
        if source_table is None:
            schema_gaps["missing_tables"] = ["claude_messages"]
            return _report(
                generated_at=generated_at,
                filters=filters,
                source_table=None,
                schema_gaps=schema_gaps,
                rows_scanned=0,
                empty_rows=0,
                malformed_rows=0,
                decisions=[],
            )
        columns = schema[source_table]
        missing = _missing_columns(columns)
        if missing:
            schema_gaps["missing_columns"] = {source_table: missing}
        rows = _load_rows(conn, columns, cutoff=cutoff)

    decisions, empty_rows, malformed_rows = _extract_decisions_from_rows(
        rows,
        min_confidence=min_confidence,
    )
    return _report(
        generated_at=generated_at,
        filters=filters,
        source_table=source_table,
        schema_gaps=schema_gaps,
        rows_scanned=len(rows),
        empty_rows=empty_rows,
        malformed_rows=malformed_rows,
        decisions=decisions,
    )


def format_claude_decision_miner_json(report: ClaudeDecisionReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_claude_decision_miner_text(report: ClaudeDecisionReport) -> str:
    """Render a deterministic command-line decision digest."""
    data = report.to_dict()
    filters = data["filters"]
    totals = data["totals"]
    lines = [
        "Claude Decision Miner",
        f"Generated: {data['generated_at']}",
        (
            "Filters: "
            f"days={filters['days']} min_confidence={filters['min_confidence']:.2f}"
        ),
        (
            "Totals: "
            f"sessions={totals['session_count']} decisions={totals['decision_count']} "
            f"rows={totals['rows_scanned']} empty_rows={totals['empty_rows']} "
            f"malformed_rows={totals['malformed_rows']}"
        ),
    ]
    if data.get("source_table"):
        lines.append(f"Source table: {data['source_table']}")
    gaps = data.get("schema_gaps") or {}
    if gaps.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(gaps["missing_tables"]))
    if gaps.get("missing_columns"):
        missing = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(gaps["missing_columns"].items())
        ]
        lines.append("Missing required columns: " + "; ".join(missing))

    if not data["sessions"]:
        lines.append("No Claude decisions matched the confidence threshold.")
        return "\n".join(lines)

    lines.extend(["", "Decisions by session:"])
    for session in data["sessions"]:
        lines.append(
            f"- session={session['session_id']} project={session['project_path'] or '-'} "
            f"decisions={session['decision_count']}"
        )
        for decision in session["decisions"]:
            lines.append(
                f"  - {decision['timestamp'] or '-'} {decision['decision_type']} "
                f"confidence={decision['confidence']:.2f} {decision['excerpt']}"
            )
    return "\n".join(lines)


def _report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    source_table: str | None,
    schema_gaps: dict[str, Any],
    rows_scanned: int,
    empty_rows: int,
    malformed_rows: int,
    decisions: list[ClaudeDecision],
) -> ClaudeDecisionReport:
    sessions = _group_by_session(decisions)
    return ClaudeDecisionReport(
        artifact_type="claude_decision_miner",
        generated_at=generated_at.isoformat(),
        filters=filters,
        source_table=source_table,
        schema_gaps={
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(schema_gaps.get("missing_columns", {}).items())
            },
            "missing_tables": list(schema_gaps.get("missing_tables", [])),
        },
        totals={
            "decision_count": len(decisions),
            "empty_rows": empty_rows,
            "malformed_rows": malformed_rows,
            "rows_scanned": rows_scanned,
            "session_count": len(sessions),
        },
        sessions=tuple(sessions),
    )


def _extract_decisions_from_rows(
    rows: Iterable[dict[str, Any]],
    *,
    min_confidence: float,
) -> tuple[list[ClaudeDecision], int, int]:
    best: dict[str, ClaudeDecision] = {}
    empty_rows = 0
    malformed_rows = 0
    for row in rows:
        if not isinstance(row, Mapping):
            malformed_rows += 1
            continue
        text = _row_text(row)
        if not text:
            empty_rows += 1
            continue
        metadata = _row_metadata(row)
        if not metadata["timestamp"] or not metadata["session_id"]:
            malformed_rows += 1
        for excerpt in _candidate_excerpts(text):
            decision = _decision_from_excerpt(excerpt, metadata)
            if decision is None or decision.confidence < min_confidence:
                continue
            existing = best.get(decision.decision_id)
            if existing is None or decision.confidence > existing.confidence:
                best[decision.decision_id] = decision
    return sorted(best.values(), key=_decision_sort_key), empty_rows, malformed_rows


def _decision_from_excerpt(
    excerpt: str,
    metadata: dict[str, Any],
) -> ClaudeDecision | None:
    decision_type = _decision_type(excerpt)
    if decision_type is None:
        return None
    confidence = _confidence(excerpt, decision_type)
    if confidence <= 0:
        return None
    session_id = metadata["session_id"] or "unknown-session"
    timestamp = metadata["timestamp"]
    project_path = metadata["project_path"]
    clean_excerpt = _shorten(_clean_text(excerpt), EXCERPT_CHARS)
    decision_id = _decision_id(session_id, decision_type, clean_excerpt)
    return ClaudeDecision(
        decision_id=decision_id,
        session_id=session_id,
        timestamp=timestamp,
        project_path=project_path,
        excerpt=clean_excerpt,
        decision_type=decision_type,
        confidence=confidence,
    )


def _decision_type(text: str) -> str | None:
    if REJECT_RE.search(text):
        return "rejected_alternative"
    if DEFER_RE.search(text):
        return "deferred_alternative"
    if DECISION_RE.search(text):
        return "technical_decision"
    if TRADEOFF_RE.search(text) and TECHNICAL_RE.search(text):
        return "tradeoff"
    return None


def _confidence(text: str, decision_type: str) -> float:
    score = {
        "technical_decision": 0.74,
        "tradeoff": 0.68,
        "deferred_alternative": 0.72,
        "rejected_alternative": 0.72,
    }[decision_type]
    if DECISION_RE.search(text):
        score += 0.1
    if TRADEOFF_RE.search(text):
        score += 0.08
    if TECHNICAL_RE.search(text):
        score += 0.06
    if REJECT_RE.search(text) and decision_type != "rejected_alternative":
        score += 0.03
    if UNCERTAIN_RE.search(text):
        score -= 0.16
    if text.strip().endswith("?"):
        score -= 0.3
    return round(max(0.0, min(score, 0.98)), 2)


def _candidate_excerpts(text: str) -> list[str]:
    lines = [_clean_text(line.strip(" \t-*[]")) for line in str(text).splitlines()]
    candidates: list[str] = []
    for line in lines:
        if not line:
            continue
        for sentence in re.split(r"(?<=[.!?])\s+", line):
            sentence = _clean_text(sentence)
            if sentence and _has_decision_signal(sentence):
                candidates.append(sentence)
    return candidates


def _has_decision_signal(text: str) -> bool:
    return bool(
        DECISION_RE.search(text)
        or REJECT_RE.search(text)
        or DEFER_RE.search(text)
        or (TRADEOFF_RE.search(text) and TECHNICAL_RE.search(text))
    )


def _group_by_session(decisions: list[ClaudeDecision]) -> list[ClaudeDecisionSession]:
    grouped: dict[str, list[ClaudeDecision]] = {}
    for decision in decisions:
        grouped.setdefault(decision.session_id, []).append(decision)
    sessions = []
    for session_id, session_decisions in grouped.items():
        ordered = tuple(sorted(session_decisions, key=_decision_sort_key))
        project_path = next(
            (decision.project_path for decision in ordered if decision.project_path),
            None,
        )
        sessions.append(
            ClaudeDecisionSession(
                session_id=session_id,
                project_path=project_path,
                decisions=ordered,
            )
        )
    return sorted(sessions, key=_session_sort_key)


def _load_rows(
    conn: sqlite3.Connection,
    columns: set[str],
    *,
    cutoff: datetime,
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
    where = ""
    params: list[Any] = []
    if "timestamp" in columns:
        where = "WHERE timestamp >= ?"
        params.append(cutoff.isoformat())
    order = "timestamp ASC, id ASC" if {"timestamp", "id"}.issubset(columns) else "rowid ASC"
    cursor = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM claude_messages
            {where}
            ORDER BY {order}""",
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


def _filter_rows(rows: list[dict[str, Any]], *, cutoff: datetime) -> list[dict[str, Any]]:
    filtered = []
    for row in rows:
        timestamp = _parse_datetime(row.get("timestamp"))
        if timestamp is not None and timestamp < cutoff:
            continue
        filtered.append(row)
    return filtered


def _row_text(row: Mapping[str, Any]) -> str:
    parts = []
    for key in TEXT_COLUMNS:
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            parts.append(value)
    return "\n".join(dict.fromkeys(parts))


def _row_metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "project_path": _optional_text(row.get("project_path")),
        "session_id": _optional_text(row.get("session_id") or row.get("sessionId")),
        "timestamp": _optional_text(row.get("timestamp")),
    }


def _missing_columns(columns: set[str]) -> list[str]:
    return [column for column in REQUIRED_COLUMNS if column not in columns]


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


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, (list, tuple))


def _mapping(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    try:
        return dict(row)
    except (TypeError, ValueError):
        return {}


def _validate_confidence(value: float) -> None:
    if value <= 0 or value > 1:
        raise ValueError("min_confidence must be between 0 and 1")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        text = str(value).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value)).strip()


def _shorten(value: str, limit: int) -> str:
    text = _clean_text(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _decision_id(session_id: str, decision_type: str, excerpt: str) -> str:
    normalized = re.sub(r"\W+", " ", excerpt.lower()).strip()
    digest = hashlib.sha256(
        f"{session_id}|{decision_type}|{normalized}".encode("utf-8")
    ).hexdigest()[:16]
    return f"claude_decision_{digest}"


def _decision_sort_key(decision: ClaudeDecision) -> tuple[str, str, str, str]:
    return (
        str(decision.timestamp or ""),
        decision.session_id,
        str(_decision_type_priority(decision.decision_type)),
        decision.excerpt,
    )


def _session_sort_key(session: ClaudeDecisionSession) -> tuple[str, str]:
    first_timestamp = session.decisions[0].timestamp if session.decisions else ""
    return (str(first_timestamp or ""), session.session_id)


def _decision_type_priority(decision_type: str) -> int:
    return {
        "technical_decision": 0,
        "tradeoff": 1,
        "rejected_alternative": 2,
        "deferred_alternative": 3,
    }.get(decision_type, 99)
