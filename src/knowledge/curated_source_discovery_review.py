"""Audit discovered curated source candidates that need review."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 30
DEFAULT_LIMIT = 50
REVIEW_STATUSES = ("needs_review", "reviewed", "stale_candidate", "paused", "rejected")
REQUIRED_COLUMNS = {
    "id",
    "source_type",
    "identifier",
    "status",
    "discovery_source",
    "license",
    "relevance_score",
    "reviewed_at",
    "created_at",
}


def build_curated_source_discovery_review_report(
    db_or_conn: Any,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    include_config: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return discovered curated source candidates grouped by review state."""

    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {
        "stale_days": stale_days,
        "limit": limit,
        "include_config": include_config,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "curated_sources" not in schema:
        return _empty_report(generated_at, filters, missing_tables=["curated_sources"])
    missing = sorted(REQUIRED_COLUMNS - schema["curated_sources"])
    if missing:
        return _empty_report(generated_at, filters, missing_columns={"curated_sources": missing})

    rows = _rows(conn, include_config=include_config)
    items = [_item(row, stale_days=stale_days, now=generated_at) for row in rows]
    items.sort(key=_sort_key)
    return {
        "artifact_type": "curated_source_discovery_review",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items),
        "items": items[:limit],
        "missing_tables": [],
        "missing_columns": {},
    }


def format_curated_source_discovery_review_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_curated_source_discovery_review_text(report: dict[str, Any]) -> str:
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Curated Source Discovery Review",
        f"Generated: {report['generated_at']}",
        f"Filters: stale_days={filters['stale_days']} limit={filters['limit']}",
        (
            "Review counts: "
            + " ".join(f"{name}={totals['by_status'][name]}" for name in REVIEW_STATUSES)
            + f" total={totals['total']}"
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
        lines.append("No curated source candidates matched.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Items:")
    for item in report["items"]:
        lines.append(
            f"- #{item['source_id']} {item['source_type']} {item['identifier']} "
            f"status={item['status']} review={item['review_status']} "
            f"source={item['discovery_source'] or '-'} score={item['relevance_score']}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    *,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    return {
        "artifact_type": "curated_source_discovery_review",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "total": 0,
            "by_source_type": {},
            "by_status": {name: 0 for name in REVIEW_STATUSES},
            "by_discovery_source": {},
            "by_license": {},
        },
        "items": [],
        "missing_tables": missing_tables or [],
        "missing_columns": missing_columns or {},
    }


def _rows(conn: sqlite3.Connection, *, include_config: bool) -> list[dict[str, Any]]:
    filters = ["(discovery_source IS NOT NULL AND discovery_source != '' OR status = 'candidate')"]
    if not include_config:
        filters.append("(discovery_source IS NULL OR discovery_source != 'config')")
    cursor = conn.execute(
        f"""SELECT id, source_type, identifier, status, discovery_source, license,
                  relevance_score, reviewed_at, created_at
           FROM curated_sources
           WHERE {' AND '.join(filters)}
           ORDER BY datetime(created_at) ASC, id ASC"""
    )
    return [dict(row) for row in cursor.fetchall()]


def _item(row: dict[str, Any], *, stale_days: int, now: datetime) -> dict[str, Any]:
    status = _clean(row.get("status")) or "active"
    created_at = _parse(row.get("created_at")) or now
    age_days = max(0, int((now - created_at).total_seconds() // 86400))
    if status == "rejected":
        review_status = "rejected"
    elif status == "paused":
        review_status = "paused"
    elif row.get("reviewed_at") or status == "active":
        review_status = "reviewed"
    elif status == "candidate" and age_days >= stale_days:
        review_status = "stale_candidate"
    else:
        review_status = "needs_review"
    return {
        "source_id": int(row["id"]),
        "source_type": row.get("source_type"),
        "identifier": row.get("identifier"),
        "status": status,
        "discovery_source": row.get("discovery_source"),
        "license": row.get("license"),
        "relevance_score": row.get("relevance_score"),
        "reviewed_at": row.get("reviewed_at"),
        "created_at": row.get("created_at"),
        "age_days": age_days,
        "age_bucket": _age_bucket(age_days),
        "review_status": review_status,
    }


def _totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "total": len(items),
        "by_source_type": _counter(items, "source_type"),
        "by_status": {name: Counter(item["review_status"] for item in items).get(name, 0) for name in REVIEW_STATUSES},
        "by_discovery_source": _counter(items, "discovery_source"),
        "by_license": _counter(items, "license"),
    }


def _counter(items: list[dict[str, Any]], field: str) -> dict[str, int]:
    return dict(sorted(Counter(str(item.get(field) or "(none)") for item in items).items()))


def _age_bucket(days: int) -> str:
    if days <= 7:
        return "0-7d"
    if days <= 30:
        return "8-30d"
    if days <= 90:
        return "31-90d"
    return "91d+"


def _sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
    rank = {"stale_candidate": 0, "needs_review": 1, "paused": 2, "rejected": 3, "reviewed": 4}
    return (rank[item["review_status"]], -item["age_days"], item["source_id"])


def _parse(value: Any) -> datetime | None:
    if value is None:
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    return text or None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
