"""Summarize ages of generated drafts waiting for review."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


BUCKETS = (
    ("0-24h", 0, 24),
    ("1-3d", 24, 72),
    ("3-7d", 72, 168),
    ("7d+", 168, None),
)


def build_draft_review_age_distribution_report(
    db_or_conn: Any,
    *,
    oldest_limit: int = 10,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Bucket pending draft review rows by content type, source kind, and age."""
    if oldest_limit <= 0:
        raise ValueError("oldest_limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    drafts = _load_drafts(conn, schema)
    pending = [draft for draft in drafts if _is_pending(draft)]

    grouped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for draft in pending:
        age_hours = _age_hours(generated_at, _parse_ts(draft.get("created_at")))
        bucket = _bucket(age_hours)
        key = (draft["content_type"], draft["source_kind"], bucket)
        row = grouped.setdefault(
            key,
            {
                "content_type": draft["content_type"],
                "source_kind": draft["source_kind"],
                "bucket": bucket,
                "item_count": 0,
                "oldest_item_age_hours": 0.0,
            },
        )
        row["item_count"] += 1
        row["oldest_item_age_hours"] = max(row["oldest_item_age_hours"], age_hours)

    rows = sorted(grouped.values(), key=lambda row: (row["content_type"], row["source_kind"], _bucket_index(row["bucket"])))
    oldest = sorted(
        (
            {
                "content_id": draft.get("content_id"),
                "content_type": draft["content_type"],
                "source_kind": draft["source_kind"],
                "age_hours": _age_hours(generated_at, _parse_ts(draft.get("created_at"))),
                "created_at": draft.get("created_at"),
                "title": draft.get("title"),
            }
            for draft in pending
        ),
        key=lambda item: (-item["age_hours"], item["content_type"], item["content_id"] or 0),
    )[:oldest_limit]
    return {
        "artifact_type": "draft_review_age_distribution",
        "filters": {"oldest_limit": oldest_limit},
        "generated_at": generated_at.isoformat(),
        "rows": rows,
        "oldest_items": oldest,
        "schema_gaps": {"missing_tables": ["generated_content"] if "generated_content" not in schema else []},
        "summary": {
            "pending_count": len(pending),
            "bucket_count": len(rows),
            "oldest_age_hours": max((item["age_hours"] for item in oldest), default=0.0),
        },
    }


def format_draft_review_age_distribution_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_draft_review_age_distribution_text(report: dict[str, Any]) -> str:
    """Render a table for terminal review."""
    summary = report["summary"]
    lines = [
        "Draft Review Age Distribution",
        f"Generated: {report['generated_at']}",
        f"Summary: pending={summary['pending_count']} buckets={summary['bucket_count']} oldest_hours={summary['oldest_age_hours']:.1f}",
    ]
    if not report["rows"]:
        lines.extend(["", "No pending draft review items found."])
        return "\n".join(lines)
    lines.extend(["", "Buckets:", "content_type        source_kind        bucket  count  oldest_hours"])
    for row in report["rows"]:
        lines.append(
            f"{row['content_type']:<19} {row['source_kind']:<18} "
            f"{row['bucket']:<7} {row['item_count']:<6} {row['oldest_item_age_hours']:.1f}"
        )
    if report["oldest_items"]:
        lines.extend(["", "Oldest pending items:"])
        for item in report["oldest_items"]:
            lines.append(f"- content_id={item['content_id']} age_hours={item['age_hours']:.1f} title={item['title'] or '-'}")
    return "\n".join(lines)


def _load_drafts(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    cols = schema["generated_content"]
    if "id" not in cols:
        return []
    type_col = _first(cols, ("content_type", "type", "format"))
    created_col = _first(cols, ("created_at", "generated_at", "updated_at"))
    status_col = _first(cols, ("review_status", "status", "state"))
    published_col = _first(cols, ("published", "is_published"))
    source_kind_col = _first(cols, ("source_kind", "source_type", "origin"))
    title_col = _first(cols, ("title", "headline"))
    text_col = _first(cols, ("content", "body", "text"))
    rows = conn.execute(
        f"""SELECT id AS content_id,
                   {type_col if type_col else "'unknown'"} AS content_type,
                   {created_col if created_col else "NULL"} AS created_at,
                   {status_col if status_col else "NULL"} AS status,
                   {published_col if published_col else "0"} AS published,
                   {source_kind_col if source_kind_col else "NULL"} AS source_kind,
                   {title_col if title_col else "NULL"} AS title,
                   {text_col if text_col else "NULL"} AS content_text
            FROM generated_content"""
    ).fetchall()
    return [_normalize(dict(row)) for row in rows]


def _normalize(row: dict[str, Any]) -> dict[str, Any]:
    row["content_type"] = _clean(row.get("content_type")) or "unknown"
    row["source_kind"] = _clean(row.get("source_kind")) or _infer_source_kind(row.get("content_text"))
    row["title"] = _clean(row.get("title")) or _title(row.get("content_text"))
    return row


def _is_pending(row: dict[str, Any]) -> bool:
    status = (_clean(row.get("status")) or "pending_review").lower()
    if status in {"approved", "published", "rejected", "dismissed", "archived"}:
        return False
    return not bool(row.get("published"))


def _bucket(age_hours: float) -> str:
    for label, start, end in BUCKETS:
        if age_hours >= start and (end is None or age_hours < end):
            return label
    return "0-24h"


def _bucket_index(label: str) -> int:
    return next((index for index, bucket in enumerate(BUCKETS) if bucket[0] == label), 99)


def _age_hours(now: datetime, then: datetime | None) -> float:
    if then is None:
        return 0.0
    return round(max((now - then).total_seconds(), 0) / 3600, 2)


def _infer_source_kind(text: Any) -> str:
    value = str(text or "").lower()
    if "github" in value or "commit" in value:
        return "github"
    if "newsletter" in value:
        return "newsletter"
    return "unknown"


def _title(text: Any) -> str | None:
    for line in str(text or "").splitlines():
        if line.startswith("# "):
            return line[2:].strip() or None
        if line.lower().startswith("title:"):
            return line.split(":", 1)[1].strip() or None
    return None


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _clean(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _first(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
