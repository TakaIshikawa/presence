"""Identify open GitHub activity rows with stale update timestamps."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 14
DEFAULT_LIMIT = 50
STALE_STATUSES = ("fresh", "stale", "missing_updated_at")
OPEN_STATES = {"open", "opened", ""}
REQUIRED_COLUMNS = {
    "id",
    "repo_name",
    "activity_type",
    "number",
    "title",
    "state",
    "updated_at",
    "ingested_at",
    "labels",
}


def build_github_activity_stale_updates_report(
    db_or_conn: Any,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return open GitHub activities classified by update freshness."""

    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    filters = {"stale_days": stale_days, "limit": limit}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "github_activity" not in schema:
        return _empty_report(generated_at, filters, missing_tables=["github_activity"])
    missing = sorted(REQUIRED_COLUMNS - schema["github_activity"])
    if missing:
        return _empty_report(generated_at, filters, missing_columns={"github_activity": missing})

    rows = _rows(conn)
    items = [_item(row, stale_days=stale_days, now=generated_at) for row in rows]
    items.sort(key=_sort_key)
    return {
        "artifact_type": "github_activity_stale_updates",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(items),
        "items": items[:limit],
        "missing_tables": [],
        "missing_columns": {},
    }


def format_github_activity_stale_updates_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_github_activity_stale_updates_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    filters = report["filters"]
    lines = [
        "GitHub Activity Stale Updates",
        f"Generated: {report['generated_at']}",
        f"Filters: stale_days={filters['stale_days']} limit={filters['limit']}",
        (
            "Freshness counts: "
            + " ".join(f"{name}={totals['by_freshness'][name]}" for name in STALE_STATUSES)
            + f" malformed_labels={totals['malformed_labels_count']}"
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
        lines.append("No open GitHub activities matched.")
        return "\n".join(lines)
    lines.append("")
    lines.append("Items:")
    for item in report["items"]:
        lines.append(
            f"- #{item['activity_id']} {item['repo']} {item['activity_type']} "
            f"{item['number']} state={item['state'] or '-'} "
            f"freshness={item['freshness_status']} age={item['age_days']}"
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
        "artifact_type": "github_activity_stale_updates",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "total": 0,
            "by_freshness": {name: 0 for name in STALE_STATUSES},
            "by_repo_name": {},
            "by_activity_type": {},
            "by_state": {},
            "by_label": {},
            "malformed_labels_count": 0,
        },
        "items": [],
        "missing_tables": missing_tables or [],
        "missing_columns": missing_columns or {},
    }


def _rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    cursor = conn.execute(
        """SELECT id, repo_name, activity_type, number, title, state, updated_at,
                  ingested_at, labels
           FROM github_activity
           WHERE LOWER(COALESCE(state, '')) IN ('open', 'opened', '')
           ORDER BY datetime(COALESCE(updated_at, ingested_at)) ASC, id ASC"""
    )
    return [dict(row) for row in cursor.fetchall()]


def _item(row: dict[str, Any], *, stale_days: int, now: datetime) -> dict[str, Any]:
    updated_at = _parse(row.get("updated_at"))
    ingested_at = _parse(row.get("ingested_at"))
    reference = max([value for value in (now, ingested_at) if value is not None])
    labels, labels_malformed = _parse_labels(row.get("labels"))
    if updated_at is None:
        status = "missing_updated_at"
        age_days = None
    else:
        age_days = max(0, int((reference - updated_at).total_seconds() // 86400))
        status = "stale" if age_days >= stale_days else "fresh"
    return {
        "activity_id": int(row["id"]),
        "repo": row.get("repo_name"),
        "repo_name": row.get("repo_name"),
        "activity_type": row.get("activity_type"),
        "number": row.get("number"),
        "title": row.get("title"),
        "state": row.get("state"),
        "updated_at": row.get("updated_at"),
        "ingested_at": row.get("ingested_at"),
        "age_days": age_days,
        "freshness_status": status,
        "labels": labels,
        "labels_malformed": labels_malformed,
    }


def _parse_labels(value: Any) -> tuple[list[str], bool]:
    if value in (None, ""):
        return [], False
    if isinstance(value, list):
        return [str(item) for item in value], False
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return [], True
    if not isinstance(parsed, list):
        return [], True
    labels: list[str] = []
    for item in parsed:
        if isinstance(item, dict):
            label = item.get("name")
        else:
            label = item
        if label is not None:
            labels.append(str(label))
    return labels, False


def _totals(items: list[dict[str, Any]]) -> dict[str, Any]:
    by_freshness = Counter(item["freshness_status"] for item in items)
    label_counts: Counter[str] = Counter()
    for item in items:
        for label in item["labels"]:
            label_counts[label] += 1
    return {
        "total": len(items),
        "by_freshness": {name: by_freshness.get(name, 0) for name in STALE_STATUSES},
        "by_repo_name": _counter(items, "repo_name"),
        "by_activity_type": _counter(items, "activity_type"),
        "by_state": _counter(items, "state"),
        "by_label": dict(sorted(label_counts.items())),
        "malformed_labels_count": sum(1 for item in items if item["labels_malformed"]),
    }


def _counter(items: list[dict[str, Any]], field: str) -> dict[str, int]:
    return dict(sorted(Counter(str(item.get(field) or "(none)") for item in items).items()))


def _sort_key(item: dict[str, Any]) -> tuple[int, int, int]:
    rank = {"missing_updated_at": 0, "stale": 1, "fresh": 2}
    age = item["age_days"] if item["age_days"] is not None else 10**9
    return (rank[item["freshness_status"]], -age, item["activity_id"])


def _parse(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
