"""Summarize relationship context coverage for proactive actions."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_LIMIT = 25
CONTEXT_CLASSIFICATIONS = (
    "has_context",
    "missing_context",
    "malformed_context",
    "posted_without_context",
)
POSTED_STATUSES = {"posted", "published", "sent", "completed"}
REQUIRED_COLUMNS = {
    "id",
    "action_type",
    "status",
    "discovery_source",
    "target_author_handle",
    "relationship_context",
    "created_at",
}


def build_proactive_relationship_coverage_report(
    db_or_conn: Any,
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return relationship context coverage for recent proactive actions."""

    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    filters = {
        "lookback_days": lookback_days,
        "lookback_start": cutoff.isoformat(),
        "lookback_end": generated_at.isoformat(),
        "limit": limit,
    }

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "proactive_actions" not in schema:
        return _empty_report(generated_at, filters, missing_tables=["proactive_actions"])
    missing = sorted(REQUIRED_COLUMNS - schema["proactive_actions"])
    if missing:
        return _empty_report(
            generated_at,
            filters,
            missing_columns={"proactive_actions": missing},
        )

    rows = _rows(conn, cutoff, generated_at)
    items = [_item(row) for row in rows]
    items.sort(key=_sort_key)

    return {
        "artifact_type": "proactive_relationship_coverage",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items),
        "groups": _groups(items),
        "items": items[:limit],
        "missing_tables": [],
        "missing_columns": {},
    }


def format_proactive_relationship_coverage_json(report: dict[str, Any]) -> str:
    """Serialize the report as stable JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_proactive_relationship_coverage_text(report: dict[str, Any]) -> str:
    """Render a compact coverage report."""

    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Proactive Relationship Coverage",
        f"Generated: {report['generated_at']}",
        f"Filters: lookback_days={filters['lookback_days']} limit={filters['limit']}",
        (
            "Coverage: "
            + " ".join(
                f"{name}={totals['by_context_classification'][name]}"
                for name in CONTEXT_CLASSIFICATIONS
            )
            + f" total={totals['total']}"
        ),
        (
            f"Malformed JSON: {totals['malformed_context_count']} "
            f"posted_missing_context={totals['posted_without_context_count']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report["missing_columns"].items())
            )
        )
    if not report["items"]:
        lines.append("No proactive actions matched the lookback window.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Items:")
    for item in report["items"]:
        handle = item["target_author_handle"] or "-"
        lines.append(
            f"- #{item['id']} {item['action_type']} @{handle} "
            f"status={item['status']} source={item['discovery_source'] or '-'} "
            f"context={item['context_classification']}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in tables}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    return {
        "artifact_type": "proactive_relationship_coverage",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "total": 0,
            "by_context_classification": {name: 0 for name in CONTEXT_CLASSIFICATIONS},
            "malformed_context_count": 0,
            "posted_without_context_count": 0,
            "by_action_type": {},
            "by_status": {},
            "by_discovery_source": {},
            "by_target_author_handle": {},
        },
        "groups": [],
        "items": [],
        "missing_tables": missing_tables or [],
        "missing_columns": missing_columns or {},
    }


def _rows(
    conn: sqlite3.Connection,
    cutoff: datetime,
    generated_at: datetime,
) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """SELECT id, action_type, status, discovery_source, target_author_handle,
                  relationship_context, created_at, reviewed_at, posted_at
           FROM proactive_actions
           WHERE datetime(created_at) >= datetime(?)
             AND datetime(created_at) <= datetime(?)
           ORDER BY datetime(created_at) DESC, id DESC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    )
    return [dict(row) for row in cursor.fetchall()]


def _item(row: dict[str, Any]) -> dict[str, Any]:
    status = _clean(row.get("status")) or "pending"
    base = _context_state(row.get("relationship_context"))
    classification = (
        "posted_without_context"
        if status in POSTED_STATUSES and base in {"missing_context", "malformed_context"}
        else base
    )
    return {
        "id": int(row["id"]),
        "action_type": _clean(row.get("action_type")) or "unknown",
        "status": status,
        "discovery_source": _clean(row.get("discovery_source")),
        "target_author_handle": row.get("target_author_handle"),
        "context_classification": classification,
        "relationship_context_parse_status": base,
        "created_at": row.get("created_at"),
        "reviewed_at": row.get("reviewed_at"),
        "posted_at": row.get("posted_at"),
    }


def _context_state(value: Any) -> str:
    if value is None:
        return "missing_context"
    if isinstance(value, (dict, list)):
        return "has_context" if bool(value) else "missing_context"
    text = str(value).strip()
    if not text:
        return "missing_context"
    try:
        parsed = json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return "malformed_context"
    if parsed in (None, "", [], {}):
        return "missing_context"
    return "has_context"


def _totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    classifications = Counter(item["context_classification"] for item in items)
    return {
        "total": len(items),
        "by_context_classification": {
            name: classifications.get(name, 0) for name in CONTEXT_CLASSIFICATIONS
        },
        "malformed_context_count": sum(
            1 for item in items if item["relationship_context_parse_status"] == "malformed_context"
        ),
        "posted_without_context_count": classifications.get("posted_without_context", 0),
        "by_action_type": _counter(items, "action_type"),
        "by_status": _counter(items, "status"),
        "by_discovery_source": _counter(items, "discovery_source"),
        "by_target_author_handle": _counter(items, "target_author_handle"),
    }


def _groups(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], Counter[str]] = {}
    for item in items:
        key = (
            item["action_type"],
            item["status"],
            item["discovery_source"] or "(none)",
            item["target_author_handle"] or "(none)",
        )
        grouped.setdefault(key, Counter())[item["context_classification"]] += 1
    groups = []
    for key, counts in sorted(grouped.items()):
        groups.append(
            {
                "action_type": key[0],
                "status": key[1],
                "discovery_source": key[2],
                "target_author_handle": key[3],
                "count": sum(counts.values()),
                "by_context_classification": {
                    name: counts.get(name, 0) for name in CONTEXT_CLASSIFICATIONS
                },
            }
        )
    return groups


def _counter(items: list[dict[str, Any]], field: str) -> dict[str, int]:
    counts = Counter(str(item.get(field) or "(none)") for item in items)
    return dict(sorted(counts.items()))


def _sort_key(item: dict[str, Any]) -> tuple[int, str, int]:
    rank = {
        "posted_without_context": 0,
        "malformed_context": 1,
        "missing_context": 2,
        "has_context": 3,
    }
    return (rank[item["context_classification"]], item.get("created_at") or "", item["id"])


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
