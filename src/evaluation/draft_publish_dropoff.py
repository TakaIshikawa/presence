"""Show where generated content drops off before publication."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 14
DEFAULT_LIMIT = 100
STAGES = ("draft", "queued", "failed", "published", "stale_unpublished")


def build_draft_publish_dropoff_report(
    rows: list[dict[str, Any]],
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    records = []
    counts = Counter({stage: 0 for stage in STAGES})
    for row in rows:
        created_at = _parse_ts(_first(row, "created_at", "generated_at", "drafted_at"))
        published_at = _parse_ts(_first(row, "published_at", "sent_at"))
        age_days = round((generated_at - created_at).total_seconds() / 86400, 2) if created_at else None
        stage = _stage(row, age_days, stale_days, published_at)
        counts[stage] += 1
        records.append(
            {
                "content_id": _text(_first(row, "content_id", "id")) or "unknown",
                "format": _text(_first(row, "format", "content_type", "type")) or None,
                "created_at": created_at.isoformat() if created_at else None,
                "age_days": age_days,
                "publish_stage": stage,
                "last_error": _text(_first(row, "last_error", "error_message", "failure_reason")) or None,
            }
        )
    records.sort(key=lambda item: (STAGES.index(item["publish_stage"]), -(item["age_days"] or 0), item["content_id"]))
    generated = len(records)
    return {
        "artifact_type": "draft_publish_dropoff",
        "generated_at": generated_at.isoformat(),
        "filters": {"stale_days": stale_days, "limit": limit},
        "totals": {
            "generated": generated,
            "queued": counts["queued"],
            "failed": counts["failed"],
            "published": counts["published"],
            "draft": counts["draft"],
            "stale_unpublished": counts["stale_unpublished"],
            "publish_rate": round(counts["published"] / generated, 4) if generated else 0.0,
        },
        "contents": records[:limit],
        "empty_state": {"is_empty": not records, "message": "No generated content rows found." if not records else None},
    }


def build_draft_publish_dropoff_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    return build_draft_publish_dropoff_report(_load_rows(conn, _schema(conn)), **kwargs)


def format_draft_publish_dropoff_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_draft_publish_dropoff_text(report: dict[str, Any]) -> str:
    lines = [
        "Draft Publish Dropoff",
        f"Generated: {report['generated_at']}",
        f"Stale threshold: {report['filters']['stale_days']} days",
        (
            f"Totals: generated={report['totals']['generated']} published={report['totals']['published']} "
            f"queued={report['totals']['queued']} failed={report['totals']['failed']} stale={report['totals']['stale_unpublished']} "
            f"publish_rate={report['totals']['publish_rate']:.2f}"
        ),
    ]
    if not report["contents"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "content_id | format | age_days | stage | last_error"])
    for row in report["contents"]:
        lines.append(f"{row['content_id']} | {row['format'] or '-'} | {row['age_days'] if row['age_days'] is not None else '-'} | {row['publish_stage']} | {row['last_error'] or '-'}")
    return "\n".join(lines)


format_draft_publish_dropoff_table = format_draft_publish_dropoff_text


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    gc = schema["generated_content"]
    selected = [
        _col(gc, "id", "content_id", default="NULL") + " AS content_id",
        _col(gc, "content_type", "format", "type", default="NULL") + " AS format",
        _col(gc, "created_at", "generated_at", "drafted_at", default="NULL") + " AS created_at",
        _col(gc, "status", "state", default="NULL") + " AS status",
    ]
    rows = [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM generated_content").fetchall()]
    publications = _publication_by_content(conn, schema)
    queue = _queue_by_content(conn, schema)
    for row in rows:
        row.update(publications.get(row["content_id"], {}))
        row.update(queue.get(row["content_id"], {}))
    return rows


def _publication_by_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[Any, dict[str, Any]]:
    if "content_publications" not in schema:
        return {}
    cols = schema["content_publications"]
    selected = [
        _col(cols, "content_id", default="NULL") + " AS content_id",
        _col(cols, "published_at", "sent_at", default="NULL") + " AS published_at",
        _col(cols, "status", "outcome", "state", default="NULL") + " AS publication_status",
        _col(cols, "last_error", "error_message", "failure_reason", default="NULL") + " AS last_error",
    ]
    return {row["content_id"]: dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM content_publications").fetchall()}


def _queue_by_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[Any, dict[str, Any]]:
    if "publish_queue" not in schema:
        return {}
    cols = schema["publish_queue"]
    selected = [
        _col(cols, "content_id", default="NULL") + " AS content_id",
        _col(cols, "status", "state", default="queued") + " AS queue_status",
        _col(cols, "last_error", "error_message", "failure_reason", default="NULL") + " AS last_error",
    ]
    return {row["content_id"]: dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM publish_queue").fetchall()}


def _stage(row: dict[str, Any], age_days: float | None, stale_days: int, published_at: datetime | None) -> str:
    status = _text(_first(row, "publication_status", "queue_status", "status")).lower()
    if published_at or status in {"published", "sent", "success", "succeeded"}:
        return "published"
    if status in {"failed", "failure", "error", "dead_letter"} or _text(_first(row, "last_error", "error_message")):
        return "failed"
    if status in {"queued", "pending", "scheduled", "retry", "retrying"}:
        return "queued"
    if age_days is not None and age_days >= stale_days:
        return "stale_unpublished"
    return "draft"


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
