"""Report reply drafts with stale relationship or thread context."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_STALE_HOURS = 72.0
DEFAULT_OLD_HOURS = 24.0


def build_reply_context_staleness_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    stale_hours: float = DEFAULT_STALE_HOURS,
    old_hours: float = DEFAULT_OLD_HOURS,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return reply draft rows scored by context age at draft creation time."""
    if stale_hours <= 0:
        raise ValueError("stale_hours must be positive")
    if old_hours <= 0:
        raise ValueError("old_hours must be positive")
    if old_hours >= stale_hours:
        raise ValueError("old_hours must be less than stale_hours")

    generated_at = _utc(now or datetime.now(timezone.utc))
    findings = [
        inspect_reply_context_staleness(row, stale_hours=stale_hours, old_hours=old_hours)
        for row in rows
    ]
    findings.sort(key=_sort_key)
    return {
        "artifact_type": "reply_context_staleness",
        "generated_at": generated_at.isoformat(),
        "filters": {"old_hours": old_hours, "stale_hours": stale_hours, "status": "pending"},
        "summary": {
            "draft_count": len(findings),
            "missing_context_count": sum(1 for row in findings if row["risk_label"] == "missing_context"),
            "stale_context_count": sum(1 for row in findings if row["risk_label"] == "stale_context"),
            "old_context_count": sum(1 for row in findings if row["risk_label"] == "old_context"),
            "fresh_context_count": sum(1 for row in findings if row["risk_label"] == "fresh_context"),
        },
        "rows": findings,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def inspect_reply_context_staleness(
    row: Mapping[str, Any],
    *,
    stale_hours: float = DEFAULT_STALE_HOURS,
    old_hours: float = DEFAULT_OLD_HOURS,
) -> dict[str, Any]:
    """Score one reply draft's context timestamp relative to draft creation."""
    data = _row_dict(row)
    draft_ts = _parse_dt(_first(data, "draft_created_at", "created_at", "updated_at"))
    context_ts = _parse_dt(
        _first(
            data,
            "relationship_context_updated_at",
            "thread_context_updated_at",
            "context_updated_at",
            "conversation_context_updated_at",
        )
    )
    age_hours: float | None = None
    if draft_ts is not None and context_ts is not None:
        age_hours = round(max((draft_ts - context_ts).total_seconds() / 3600.0, 0.0), 2)

    bucket, risk = _bucket(age_hours, draft_ts=draft_ts, context_ts=context_ts, old_hours=old_hours, stale_hours=stale_hours)
    return {
        "draft_id": _int_or_none(_first(data, "draft_id", "id", "reply_queue_id")),
        "mention_id": _text(_first(data, "mention_id", "inbound_tweet_id", "inbound_id")),
        "platform": _text(data.get("platform")) or "x",
        "draft_timestamp": _iso(draft_ts),
        "context_timestamp": _iso(context_ts),
        "context_age_hours": age_hours,
        "age_bucket": bucket,
        "risk_label": risk,
    }


def build_reply_context_staleness_report_from_db(
    db_or_conn: Any,
    *,
    stale_hours: float = DEFAULT_STALE_HOURS,
    old_hours: float = DEFAULT_OLD_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load pending reply drafts from SQLite and build the staleness report."""
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_reply_rows(conn, schema) if not gaps["missing_tables"] else []
    return build_reply_context_staleness_report(
        rows,
        stale_hours=stale_hours,
        old_hours=old_hours,
        now=now,
        schema_gaps=gaps,
    )


def format_reply_context_staleness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_context_staleness_table(report: dict[str, Any]) -> str:
    lines = [
        "Reply Context Staleness",
        f"Generated: {report['generated_at']}",
        (
            "Totals: "
            f"drafts={report['summary']['draft_count']} "
            f"missing={report['summary']['missing_context_count']} "
            f"stale={report['summary']['stale_context_count']} "
            f"old={report['summary']['old_context_count']} "
            f"fresh={report['summary']['fresh_context_count']}"
        ),
    ]
    if not report["rows"]:
        lines.extend(["", "No reply draft context staleness found."])
        return "\n".join(lines)
    lines.extend(["", "draft  mention          draft_time                 context_time               age_h   bucket         risk"])
    for row in report["rows"]:
        lines.append(
            f"{str(row['draft_id'] or '-'):<6} "
            f"{(row['mention_id'] or '-')[:15]:<15} "
            f"{(row['draft_timestamp'] or '-'):<26} "
            f"{(row['context_timestamp'] or '-'):<26} "
            f"{_fmt_num(row['context_age_hours']):<7} "
            f"{row['age_bucket']:<14} "
            f"{row['risk_label']}"
        )
    return "\n".join(lines)


def _load_reply_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("reply_queue", set())
    if not columns:
        return []
    query = "SELECT * FROM reply_queue"
    if "status" in columns:
        query += " WHERE LOWER(COALESCE(status, 'pending')) = 'pending'"
    order = "created_at" if "created_at" in columns else ("id" if "id" in columns else "rowid")
    query += f" ORDER BY {order} ASC"
    return [dict(row) for row in conn.execute(query).fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "reply_queue" not in schema:
        return {"missing_tables": ["reply_queue"], "missing_columns": {}}
    columns = schema["reply_queue"]
    missing = sorted({"id", "created_at"} - columns)
    return {"missing_tables": [], "missing_columns": {"reply_queue": missing} if missing else {}}


def _bucket(
    age_hours: float | None,
    *,
    draft_ts: datetime | None,
    context_ts: datetime | None,
    old_hours: float,
    stale_hours: float,
) -> tuple[str, str]:
    if context_ts is None:
        return "missing", "missing_context"
    if draft_ts is None:
        return "unknown_draft_time", "missing_draft_timestamp"
    if age_hours is None:
        return "unknown", "missing_context"
    if age_hours >= stale_hours:
        return "stale", "stale_context"
    if age_hours >= old_hours:
        return "old", "old_context"
    return "fresh", "fresh_context"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _row_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return dict(row)


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return _utc(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except ValueError:
        try:
            return _utc(datetime.strptime(text, "%Y-%m-%d %H:%M:%S"))
        except ValueError:
            return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fmt_num(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value):.2f}".rstrip("0").rstrip(".")


def _sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    risk_rank = {
        "missing_context": 0,
        "missing_draft_timestamp": 1,
        "stale_context": 2,
        "old_context": 3,
        "fresh_context": 4,
    }
    return (
        risk_rank.get(row["risk_label"], 9),
        -(row["context_age_hours"] or 0),
        row["platform"],
        row["draft_id"] or 0,
    )
