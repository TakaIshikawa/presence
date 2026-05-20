"""Report reply follow-ups due soon without enough drafting context."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping, Sequence


DEFAULT_DAYS_AHEAD = 2
DEFAULT_MAX_CONTEXT_AGE_HOURS = 24.0
DEFAULT_LIMIT = 100
ARTIFACT_TYPE = "reply_followup_context_readiness"
TERMINAL_STATUSES = {"done", "completed", "sent", "resolved", "dismissed", "cancelled", "canceled", "expired"}
READINESS_REASONS = (
    "source_mention_unlinked",
    "missing_target_tweet_metadata",
    "missing_relationship_context",
    "missing_prior_reply_linkage",
    "stale_context",
)
CONTEXT_TIMESTAMP_KEYS = {
    "context_at",
    "context_timestamp",
    "context_updated_at",
    "context_refreshed_at",
    "retrieved_at",
    "refreshed_at",
    "fetched_at",
    "updated_at",
    "generated_at",
    "created_at",
}
RELATIONSHIP_KEYS = {
    "relationship_strength",
    "strength",
    "engagement_stage",
    "stage",
    "dunbar_tier",
    "tier",
    "relationship_notes",
    "relationship_summary",
    "notes",
    "display_name",
    "bio",
    "recent_interactions",
}
TARGET_METADATA_KEYS = {
    "inbound_url",
    "target_url",
    "tweet_url",
    "inbound_tweet_id",
    "mention_id",
    "platform",
    "parent_post_text",
    "conversation_id",
}


def build_reply_followup_context_readiness_report(
    rows: list[dict[str, Any]],
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    max_context_age_hours: float = DEFAULT_MAX_CONTEXT_AGE_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    missing_optional_tables: list[str] | None = None,
) -> dict[str, Any]:
    if days_ahead < 0:
        raise ValueError("days_ahead must be non-negative")
    if max_context_age_hours <= 0:
        raise ValueError("max_context_age_hours must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    due_until = generated_at + timedelta(days=days_ahead)
    findings: list[dict[str, Any]] = []
    scanned = 0
    for row in rows:
        status = _clean(row.get("status"), "pending").lower()
        due_at = _parse_dt(row.get("due_at"))
        if status in TERMINAL_STATUSES or due_at is None or due_at > due_until:
            continue
        scanned += 1
        context_age = _context_age_hours(row, generated_at)
        urgency = _urgency(due_at, generated_at)
        for reason in _readiness_reasons(row, context_age, max_context_age_hours):
            findings.append(_finding(row, reason, urgency, context_age, due_at))

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days_ahead": days_ahead,
            "due_until": due_until.isoformat(),
            "max_context_age_hours": max_context_age_hours,
            "limit": limit,
        },
        "summary": {
            "followups_scanned": scanned,
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_readiness_reason": _counts_by_reason(findings),
            "by_urgency_bucket": dict(sorted(Counter(item["urgency_bucket"] for item in findings).items())),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items())},
        "missing_optional_tables": sorted(missing_optional_tables or []),
        "empty_state": {
            "is_empty": not findings,
            "message": "No reply follow-up context readiness gaps found." if not findings else None,
        },
    }


def build_reply_followup_context_readiness_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"reply_followup_reminders": {"id", "due_at"}}
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and not columns.issubset(schema[table])
    }
    if missing_tables or missing_columns:
        return build_reply_followup_context_readiness_report(
            [],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )
    rows = _load_rows(conn, schema)
    optional = [] if "reply_queue" in schema else ["reply_queue"]
    if "people" not in schema:
        optional.append("people")
    return build_reply_followup_context_readiness_report(
        rows,
        missing_optional_tables=optional,
        **kwargs,
    )


def format_reply_followup_context_readiness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_followup_context_readiness_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    filters = report["filters"]
    lines = [
        "Reply Follow-up Context Readiness",
        f"Generated: {report['generated_at']}",
        f"Filters: days_ahead={filters['days_ahead']} max_context_age_hours={filters['max_context_age_hours']:g} limit={filters['limit']}",
        f"Totals: scanned={summary['followups_scanned']} findings={summary['finding_count']} shown={summary['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if report["missing_optional_tables"]:
        lines.append("Missing optional tables: " + ", ".join(report["missing_optional_tables"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "followup_id | reply_id | mention_id | due_at | urgency | readiness_reason | context_age_hours | target_url | platform"])
    for item in report["findings"]:
        lines.append(
            f"{item['followup_id']} | {_display(item['reply_id'])} | {_display(item['mention_id'])} | "
            f"{item['due_at']} | {item['urgency_bucket']} | {item['readiness_reason']} | "
            f"{_display(item['context_age_hours'])} | {_display(item['target_url'])} | {_display(item['platform'])}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    r = schema["reply_followup_reminders"]
    q = schema.get("reply_queue", set())
    p = schema.get("people", set())
    join_q = ""
    if q and "id" in q:
        if {"source_reply_id", "source_id"}.issubset(r):
            join_q = "LEFT JOIN reply_queue q ON q.id = COALESCE(r.source_reply_id, r.source_id)"
        elif "source_reply_id" in r:
            join_q = "LEFT JOIN reply_queue q ON q.id = r.source_reply_id"
        elif {"source_type", "source_id"}.issubset(r):
            join_q = "LEFT JOIN reply_queue q ON q.id = r.source_id AND lower(COALESCE(r.source_type, '')) = 'reply_queue'"
        elif "source_id" in r:
            join_q = "LEFT JOIN reply_queue q ON q.id = r.source_id"
    source_exists = "q.id IS NOT NULL" if join_q else "0"
    join_p = ""
    if p and join_q:
        handle = _qcol(q, "inbound_author_handle", "q", _qcol(r, "target_handle", "r", "NULL"))
        author_id = _qcol(q, "inbound_author_id", "q", "NULL")
        clauses = []
        if {"x_handle", "handle", "username"}.intersection(p):
            person_handle = _coalesce([_qcol(p, col, "p", "NULL") for col in ("x_handle", "handle", "username") if col in p])
            clauses.append(f"LOWER(REPLACE({person_handle}, '@', '')) = LOWER(REPLACE({handle}, '@', ''))")
        if {"x_user_id", "user_id", "id"}.intersection(p):
            person_id = _coalesce([_qcol(p, col, "p", "NULL") for col in ("x_user_id", "user_id", "id") if col in p])
            clauses.append(f"{person_id} = {author_id}")
        if clauses:
            join_p = "LEFT JOIN people p ON " + " OR ".join(f"({clause})" for clause in clauses)
    person_columns = p if join_p else set()

    select = [
        "r.id AS followup_id",
        _qcol(r, "status", "r", "'pending'") + " AS status",
        _qcol(r, "due_at", "r", "NULL") + " AS due_at",
        _qcol(r, "source_type", "r", "NULL") + " AS source_type",
        _qcol(r, "source_id", "r", "NULL") + " AS source_id",
        _qcol(r, "source_reply_id", "r", "NULL") + " AS source_reply_id",
        _qcol(r, "source_mention_id", "r", "NULL") + " AS source_mention_id",
        _qcol(r, "target_handle", "r", "NULL") + " AS target_handle",
        f"{source_exists} AS source_exists",
        _qcol(q, "id", "q", "NULL") + " AS reply_id",
        _qcol(q, "inbound_tweet_id", "q", "NULL") + " AS mention_id",
        _qcol(q, "inbound_url", "q", "NULL") + " AS target_url",
        _qcol(q, "platform", "q", "NULL") + " AS platform",
        _qcol(q, "platform_metadata", "q", "NULL") + " AS platform_metadata",
        _qcol(q, "relationship_context", "q", "NULL") + " AS relationship_context",
        _qcol(person_columns, "display_name", "p", "NULL") + " AS person_display_name",
        _qcol(person_columns, "bio", "p", "NULL") + " AS person_bio",
        _qcol(person_columns, "relationship_strength", "p", "NULL") + " AS person_relationship_strength",
        _qcol(person_columns, "relationship_notes", "p", "NULL") + " AS person_relationship_notes",
        _qcol(person_columns, "notes", "p", "NULL") + " AS person_notes",
    ]
    query = f"SELECT {', '.join(select)} FROM reply_followup_reminders r {join_q} {join_p} ORDER BY datetime(r.due_at) ASC, r.id ASC"
    return [dict(row) for row in conn.execute(query).fetchall()]


def _readiness_reasons(row: dict[str, Any], context_age_hours: float | None, max_context_age_hours: float) -> list[str]:
    reasons: list[str] = []
    if _source_should_exist(row) and not _truthy(row.get("source_exists")):
        reasons.append("source_mention_unlinked")
    if not _has_target_metadata(row):
        reasons.append("missing_target_tweet_metadata")
    if not _has_relationship_context(row):
        reasons.append("missing_relationship_context")
    if _int_or_none(row.get("reply_id")) is None and _int_or_none(row.get("source_reply_id")) is None:
        reasons.append("missing_prior_reply_linkage")
    if context_age_hours is not None and context_age_hours > max_context_age_hours:
        reasons.append("stale_context")
    return [reason for reason in READINESS_REASONS if reason in reasons]


def _finding(row: dict[str, Any], reason: str, urgency: str, context_age_hours: float | None, due_at: datetime) -> dict[str, Any]:
    return {
        "followup_id": _int_or_none(row.get("followup_id")),
        "reply_id": _int_or_none(row.get("reply_id") or row.get("source_reply_id")),
        "mention_id": _clean(row.get("mention_id") or row.get("source_mention_id")),
        "due_at": due_at.isoformat(),
        "readiness_reason": reason,
        "urgency_bucket": urgency,
        "context_age_hours": round(context_age_hours, 2) if context_age_hours is not None else None,
        "target_url": _clean(row.get("target_url")),
        "platform": _clean(row.get("platform")),
    }


def _source_should_exist(row: dict[str, Any]) -> bool:
    source_type = _clean(row.get("source_type")).lower()
    return source_type in {"", "reply_queue", "mention", "tweet", "inbound_mention"} or _int_or_none(row.get("source_reply_id")) is not None


def _has_target_metadata(row: dict[str, Any]) -> bool:
    if _clean(row.get("target_url")) and _clean(row.get("platform")) and _clean(row.get("mention_id")):
        return True
    metadata = _parse_json_object(row.get("platform_metadata"))
    return any(_present(metadata.get(key)) for key in TARGET_METADATA_KEYS)


def _has_relationship_context(row: dict[str, Any]) -> bool:
    context = _parse_json_object(row.get("relationship_context"))
    if any(_present(context.get(key)) for key in RELATIONSHIP_KEYS):
        return True
    return any(_present(row.get(key)) for key in ("person_display_name", "person_bio", "person_relationship_strength", "person_relationship_notes", "person_notes"))


def _context_age_hours(row: dict[str, Any], now: datetime) -> float | None:
    timestamps: list[datetime] = []
    for column in ("platform_metadata", "relationship_context"):
        _collect_context_timestamps(_parse_json_object(row.get(column)), timestamps)
    if not timestamps:
        return None
    return max((now - max(timestamps)).total_seconds() / 3600, 0.0)


def _collect_context_timestamps(value: Any, timestamps: list[datetime]) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key) in CONTEXT_TIMESTAMP_KEYS:
                parsed = _parse_dt(child)
                if parsed is not None:
                    timestamps.append(parsed)
            _collect_context_timestamps(child, timestamps)
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for item in value:
            _collect_context_timestamps(item, timestamps)


def _urgency(due_at: datetime, now: datetime) -> str:
    if due_at < now:
        return "overdue"
    if due_at.date() == now.date():
        return "due_today"
    return "due_soon"


def _counts_by_reason(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(item["readiness_reason"] for item in findings)
    return {reason: counts[reason] for reason in READINESS_REASONS}


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (item["due_at"], READINESS_REASONS.index(item["readiness_reason"]), item["followup_id"] or 0)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _qcol(columns: set[str], column: str, alias: str, default: str) -> str:
    return f"{alias}.{_quote_identifier(column)}" if column in columns else default


def _coalesce(expressions: list[str]) -> str:
    if not expressions:
        return "NULL"
    if len(expressions) == 1:
        return expressions[0]
    return "COALESCE(" + ", ".join(expressions) + ")"


def _parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return _utc(value)
    text = str(value).strip().replace("Z", "+00:00")
    for candidate in (text, text.replace(" ", "T", 1)):
        try:
            return _utc(datetime.fromisoformat(candidate))
        except ValueError:
            continue
    return None


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _clean(value: Any, default: str = "") -> str:
    text = "" if value is None else str(value).strip()
    return text or default


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return bool(value)
    return True


def _truthy(value: Any) -> bool:
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes"}


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
