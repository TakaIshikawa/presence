"""Plan concise closing actions for inbound reply conversations."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal


DEFAULT_MAX_THREAD_AGE_HOURS = 72
DEFAULT_MIN_EXCHANGE_COUNT = 4
EVIDENCE_CHARS = 160

CloserAction = Literal[
    "close_with_thanks",
    "answer_remaining_question",
    "no_action",
    "escalate",
]

ACTION_ORDER: tuple[CloserAction, ...] = (
    "answer_remaining_question",
    "close_with_thanks",
    "escalate",
    "no_action",
)

_QUESTION_RE = re.compile(r"\?")
_TOKEN_RE = re.compile(r"[a-z0-9']+")
_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_HANDLE_RE = re.compile(r"@\w+")

_QUESTION_OPENERS = {
    "can",
    "could",
    "do",
    "does",
    "did",
    "how",
    "is",
    "are",
    "what",
    "when",
    "where",
    "which",
    "who",
    "why",
    "will",
    "would",
    "should",
}

_ASK_PHRASES = (
    "any advice",
    "any idea",
    "can you",
    "could you",
    "do you recommend",
    "help me",
    "how should",
    "i need help",
    "please explain",
    "please help",
    "what should",
    "would love your take",
)

_THANKS_PHRASES = (
    "appreciate it",
    "helpful",
    "makes sense",
    "perfect thanks",
    "thank you",
    "thanks",
    "that helps",
    "got it",
)

_RESOLVED_STATUSES = {"approved", "posted", "sent", "done", "resolved"}
_CLOSED_STATUSES = _RESOLVED_STATUSES | {"dismissed", "expired", "closed"}


@dataclass(frozen=True)
class ReplyConversationCloserRecommendation:
    """One operator recommendation for a reply conversation thread."""

    thread_id: str
    action: CloserAction
    reason_codes: tuple[str, ...]
    evidence_snippets: tuple[str, ...]
    exchange_count: int
    age_hours: float
    latest_at: str | None
    platform: str
    author: str | None
    reply_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["reason_codes"] = list(self.reason_codes)
        data["evidence_snippets"] = list(self.evidence_snippets)
        data["reply_ids"] = list(self.reply_ids)
        return data


@dataclass(frozen=True)
class ReplyConversationCloserReport:
    """Read-only conversation closer plan."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    recommendations: tuple[ReplyConversationCloserRecommendation, ...]
    availability: dict[str, bool]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "recommendations": [item.to_dict() for item in self.recommendations],
            "availability": dict(sorted(self.availability.items())),
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_reply_conversation_closer_report(
    db_or_conn: Any,
    *,
    max_thread_age_hours: int = DEFAULT_MAX_THREAD_AGE_HOURS,
    min_exchange_count: int = DEFAULT_MIN_EXCHANGE_COUNT,
    now: datetime | None = None,
) -> ReplyConversationCloserReport:
    """Inspect reply history and recommend whether to close, answer, stop, or escalate."""

    if max_thread_age_hours <= 0:
        raise ValueError("max_thread_age_hours must be positive")
    if min_exchange_count <= 0:
        raise ValueError("min_exchange_count must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables: set[str] = set()
    missing_columns: dict[str, tuple[str, ...]] = {}

    rows = _reply_rows(conn, schema, missing_tables, missing_columns)
    threads = _group_threads(rows)
    recommendations = tuple(
        sorted(
            (
                _classify_thread(
                    thread_id,
                    thread_rows,
                    max_thread_age_hours=max_thread_age_hours,
                    min_exchange_count=min_exchange_count,
                    now=generated_at,
                )
                for thread_id, thread_rows in threads.items()
            ),
            key=lambda item: (
                ACTION_ORDER.index(item.action),
                item.latest_at or "",
                item.thread_id,
            ),
        )
    )
    by_action = {action: 0 for action in ACTION_ORDER}
    for item in recommendations:
        by_action[item.action] += 1

    return ReplyConversationCloserReport(
        generated_at=generated_at.isoformat(),
        filters={
            "max_thread_age_hours": max_thread_age_hours,
            "min_exchange_count": min_exchange_count,
        },
        totals={
            "thread_count": len(recommendations),
            **by_action,
        },
        recommendations=recommendations,
        availability={"reply_queue": "reply_queue" in schema},
        missing_tables=tuple(sorted(missing_tables)),
        missing_columns=missing_columns,
    )


def format_reply_conversation_closer_json(report: ReplyConversationCloserReport) -> str:
    """Serialize a conversation closer report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_reply_conversation_closer_text(report: ReplyConversationCloserReport) -> str:
    """Render a compact operator view for conversation closing decisions."""
    lines = [
        "Reply Conversation Closer",
        f"Generated: {report.generated_at}",
        f"Max thread age hours: {report.filters['max_thread_age_hours']}",
        f"Minimum exchanges: {report.filters['min_exchange_count']}",
        (
            f"Threads: {report.totals['thread_count']} "
            f"(answer_remaining_question={report.totals['answer_remaining_question']}, "
            f"close_with_thanks={report.totals['close_with_thanks']}, "
            f"escalate={report.totals['escalate']}, "
            f"no_action={report.totals['no_action']})"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = ", ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if not report.recommendations:
        lines.append("No reply conversation threads found.")
        return "\n".join(lines)

    lines.append("Recommendations:")
    for item in report.recommendations:
        author = f"@{item.author}" if item.author else "@unknown"
        lines.append(
            f"- {item.action} {item.thread_id} {item.platform} {author} "
            f"exchanges={item.exchange_count} age_hours={item.age_hours:.1f} "
            f"reasons={','.join(item.reason_codes)}"
        )
        for snippet in item.evidence_snippets:
            lines.append(f"  evidence: {snippet}")
    return "\n".join(lines)


def _classify_thread(
    thread_id: str,
    rows: list[dict[str, Any]],
    *,
    max_thread_age_hours: int,
    min_exchange_count: int,
    now: datetime,
) -> ReplyConversationCloserRecommendation:
    sorted_rows = sorted(rows, key=_row_sort_key)
    latest = sorted_rows[-1]
    latest_at = _latest_timestamp(latest)
    age_hours = _age_hours(latest_at, now)
    unresolved_questions = [row for row in sorted_rows if _is_unresolved_question(row)]
    resolved_questions = [row for row in sorted_rows if _is_resolved_question(row)]
    exchange_count = len(sorted_rows)
    reasons: list[str] = []

    if unresolved_questions and exchange_count >= min_exchange_count:
        action: CloserAction = "escalate"
        reasons.extend(["unresolved_direct_ask", "repeated_back_and_forth"])
        evidence_rows = [unresolved_questions[-1], latest]
    elif age_hours > max_thread_age_hours:
        action = "no_action"
        reasons.append("stale_thread")
        if unresolved_questions:
            reasons.append("unresolved_direct_ask")
        evidence_rows = [latest]
    elif unresolved_questions:
        action = "answer_remaining_question"
        reasons.append("unresolved_direct_ask")
        evidence_rows = [unresolved_questions[-1]]
    elif exchange_count >= min_exchange_count:
        action = "escalate"
        reasons.append("repeated_back_and_forth")
        evidence_rows = sorted_rows[-2:]
    elif resolved_questions and _is_thanks_like(latest):
        action = "close_with_thanks"
        reasons.append("resolved_question")
        evidence_rows = [resolved_questions[-1], latest]
    else:
        action = "no_action"
        reasons.append("low_closing_value")
        evidence_rows = [latest]

    return ReplyConversationCloserRecommendation(
        thread_id=thread_id,
        action=action,
        reason_codes=tuple(dict.fromkeys(reasons)),
        evidence_snippets=tuple(
            _snippet(row.get("inbound_text") or row.get("draft_text"))
            for row in evidence_rows
        ),
        exchange_count=exchange_count,
        age_hours=round(age_hours, 2),
        latest_at=latest_at.isoformat() if latest_at else None,
        platform=str(latest.get("platform") or "x"),
        author=_normalize_handle(latest.get("inbound_author_handle")),
        reply_ids=tuple(
            int(row["id"]) for row in sorted_rows if row.get("id") is not None
        ),
    )


def _reply_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    missing_tables: set[str],
    missing_columns: dict[str, tuple[str, ...]],
) -> list[dict[str, Any]]:
    if "reply_queue" not in schema:
        missing_tables.add("reply_queue")
        return []
    required = ("id", "inbound_text")
    missing = tuple(column for column in required if column not in schema["reply_queue"])
    if missing:
        missing_columns["reply_queue"] = missing
        return []
    return _fetch_dicts(
        conn,
        "SELECT * FROM reply_queue ORDER BY " + _order_clause(schema["reply_queue"]),
    )


def _group_threads(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    threads: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        key = _thread_key(row)
        threads.setdefault(key, []).append(row)
    return dict(sorted(threads.items()))


def _thread_key(row: dict[str, Any]) -> str:
    platform = str(row.get("platform") or "x").strip() or "x"
    author = (
        str(row.get("inbound_author_id") or "").strip()
        or _normalize_handle(row.get("inbound_author_handle"))
        or "unknown"
    )
    target = (
        str(row.get("our_platform_id") or "").strip()
        or str(row.get("our_tweet_id") or "").strip()
        or str(row.get("conversation_id") or "").strip()
        or str(row.get("thread_id") or "").strip()
        or str(row.get("inbound_tweet_id") or row.get("id") or "unknown")
    )
    return f"{platform}:{author}:{target}"


def _is_unresolved_question(row: dict[str, Any]) -> bool:
    if _is_closed(row):
        return False
    return _is_question_like(row)


def _is_resolved_question(row: dict[str, Any]) -> bool:
    return _is_question_like(row) and _is_resolved(row)


def _is_question_like(row: dict[str, Any]) -> bool:
    text = str(row.get("inbound_text") or "")
    normalized = _normalize(text)
    intent = str(row.get("intent") or "").strip().lower()
    if intent in {"question", "bug_report", "support"}:
        return True
    if _QUESTION_RE.search(text):
        return True
    tokens = _TOKEN_RE.findall(normalized)
    if tokens and tokens[0] in _QUESTION_OPENERS:
        return True
    return any(phrase in normalized for phrase in _ASK_PHRASES)


def _is_thanks_like(row: dict[str, Any]) -> bool:
    normalized = _normalize(str(row.get("inbound_text") or ""))
    if str(row.get("intent") or "").strip().lower() == "appreciation":
        return True
    return any(phrase in normalized for phrase in _THANKS_PHRASES)


def _is_closed(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status in _CLOSED_STATUSES:
        return True
    return any(row.get(column) for column in ("posted_at", "posted_tweet_id", "posted_platform_id"))


def _is_resolved(row: dict[str, Any]) -> bool:
    status = str(row.get("status") or "").strip().lower()
    if status in _RESOLVED_STATUSES:
        return True
    return any(row.get(column) for column in ("posted_at", "posted_tweet_id", "posted_platform_id"))


def _row_sort_key(row: dict[str, Any]) -> tuple[str, int]:
    timestamp = _latest_timestamp(row)
    return (timestamp.isoformat() if timestamp else "", _int(row.get("id")))


def _latest_timestamp(row: dict[str, Any]) -> datetime | None:
    timestamps = [
        parsed
        for column in ("posted_at", "reviewed_at", "detected_at", "created_at")
        if (parsed := _parse_timestamp(row.get(column))) is not None
    ]
    return max(timestamps) if timestamps else None


def _age_hours(timestamp: datetime | None, now: datetime) -> float:
    if timestamp is None:
        return 0.0
    return max((now - timestamp).total_seconds() / 3600, 0.0)


def _order_clause(columns: set[str]) -> str:
    parts = []
    if "detected_at" in columns:
        parts.append("datetime(detected_at) ASC")
    if "id" in columns:
        parts.append("id ASC")
    return ", ".join(parts) or "rowid ASC"


def _fetch_dicts(
    conn: sqlite3.Connection,
    sql: str,
    params: tuple[Any, ...] = (),
) -> list[dict[str, Any]]:
    cursor = conn.execute(sql, params)
    names = [description[0] for description in cursor.description or ()]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    if isinstance(db_or_conn, sqlite3.Connection):
        return db_or_conn
    conn = getattr(db_or_conn, "conn", None)
    if conn is None:
        raise TypeError("db_or_conn must be a sqlite3 connection or Database-like object")
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _normalize(text: str) -> str:
    value = _URL_RE.sub(" ", text.lower())
    value = _HANDLE_RE.sub(" ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _normalize_handle(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip().lstrip("@").lower()
    return normalized or None


def _snippet(text: Any, width: int = EVIDENCE_CHARS) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= width:
        return value
    return value[: width - 3].rstrip() + "..."


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0
