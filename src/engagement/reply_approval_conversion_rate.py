"""Report reply draft approval and publication conversion rates."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
OUTCOMES = ("drafted", "approved", "rejected", "revised", "published")


def build_reply_approval_conversion_rate_report(
    rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    groups: dict[tuple[str, str, str], Counter[str]] = defaultdict(Counter)
    for row in rows:
        metadata = _json_obj(row.get("metadata"))
        key = (
            _text(row.get("platform") or metadata.get("platform")) or "unknown",
            _text(row.get("author_id") or row.get("relationship_id") or metadata.get("author_id")) or "unknown",
            _text(row.get("draft_reason") or metadata.get("draft_reason") or metadata.get("reason")) or "unknown",
        )
        groups[key]["drafted"] += 1
        status = _text(row.get("review_status") or row.get("status") or metadata.get("review_status")).lower()
        if status in {"approved", "rejected", "revised"}:
            groups[key][status] += 1
        if _truthy(row.get("published") or metadata.get("published")) or status == "published" or row.get("published_at"):
            groups[key]["published"] += 1
        if status == "approved" and row.get("published_at"):
            groups[key]["approved"] += 0
    findings = []
    totals = Counter()
    for (platform, author, reason), counts in groups.items():
        totals.update(counts)
        drafted = counts["drafted"]
        findings.append(
            {
                "platform": platform,
                "author_id": author,
                "draft_reason": reason,
                "drafted": drafted,
                "approved": counts["approved"],
                "rejected": counts["rejected"],
                "revised": counts["revised"],
                "published": counts["published"],
                "approval_rate": round(counts["approved"] / drafted, 4) if drafted else 0.0,
                "publish_rate": round(counts["published"] / drafted, 4) if drafted else 0.0,
            }
        )
    findings.sort(key=lambda item: (-item["drafted"], item["platform"], item["author_id"], item["draft_reason"]))
    summary = {key: totals[key] for key in OUTCOMES}
    summary["approval_rate"] = round(totals["approved"] / totals["drafted"], 4) if totals["drafted"] else 0.0
    summary["publish_rate"] = round(totals["published"] / totals["drafted"], 4) if totals["drafted"] else 0.0
    return {
        "artifact_type": "reply_approval_conversion_rate",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "summary": summary,
        "findings": findings[:limit],
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_reply_approval_conversion_rate_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    gaps = _schema_gaps(schema)
    rows = _load_rows(conn, schema) if not gaps["missing_tables"] else []
    return build_reply_approval_conversion_rate_report(rows, schema_gaps=gaps, **kwargs)


def format_reply_approval_conversion_rate_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_approval_conversion_rate_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Reply Approval Conversion Rate",
        f"Generated: {report['generated_at']}",
        f"Totals: drafted={summary['drafted']} approved={summary['approved']} rejected={summary['rejected']} revised={summary['revised']} published={summary['published']} approval_rate={summary['approval_rate']} publish_rate={summary['publish_rate']}",
    ]
    if not report["findings"]:
        lines.extend(["", "No reply approval conversion rows found."])
        return "\n".join(lines)
    lines.extend(["", "Groups:"])
    for item in report["findings"]:
        lines.append(
            f"  - platform={item['platform']} author={item['author_id']} reason={item['draft_reason']} drafted={item['drafted']} approved={item['approved']} published={item['published']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "reply_drafts" if "reply_drafts" in schema else "draft_replies" if "draft_replies" in schema else ""
    if not table:
        return []
    columns = schema[table]
    select = [
        _select(columns, ("id",), "id"),
        _select(columns, ("platform",), "platform"),
        _select(columns, ("author_id", "relationship_id"), "author_id"),
        _select(columns, ("draft_reason", "reason"), "draft_reason"),
        _select(columns, ("review_status", "status"), "review_status"),
        _select(columns, ("published",), "published"),
        _select(columns, ("published_at",), "published_at"),
        _select(columns, ("metadata",), "metadata"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table}").fetchall()]


def _schema_gaps(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "reply_drafts" not in schema and "draft_replies" not in schema:
        return {"missing_tables": ["reply_drafts|draft_replies"], "missing_columns": {}}
    return {"missing_tables": [], "missing_columns": {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _select(columns: set[str], candidates: tuple[str, ...], alias: str) -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate if candidate == alias else f"{candidate} AS {alias}"
    return f"NULL AS {alias}"


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(str(value))
    except (TypeError, ValueError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "published"}


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
