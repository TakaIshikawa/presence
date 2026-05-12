"""Summarize pending reply review workload by reviewer or queue metadata."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 100
PENDING_STATUSES = {"pending", "drafted", "review", "needs_review"}


def build_reply_reviewer_workload_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    include_low_priority: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a pending reply review workload report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_rows(conn, schema, cutoff, include_low_priority)
    items = [_item(row, generated_at) for row in rows]

    groups: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    age_buckets: Counter[str] = Counter()
    quality_bands: Counter[str] = Counter()
    by_platform: Counter[str] = Counter()
    by_status: Counter[str] = Counter()
    by_priority: Counter[str] = Counter()

    for item in items:
        age_buckets[item["age_bucket"]] += 1
        quality_bands[item["quality_band"]] += 1
        by_platform[item["platform"]] += 1
        by_status[item["status"]] += 1
        by_priority[item["priority"]] += 1
        key = (
            item["workload_owner"],
            item["platform"],
            item["priority"],
            item["status"],
            item["age_bucket"],
            item["quality_band"],
        )
        group = groups.setdefault(
            key,
            {
                "workload_owner": key[0],
                "platform": key[1],
                "priority": key[2],
                "status": key[3],
                "age_bucket": key[4],
                "quality_band": key[5],
                "reply_count": 0,
                "representative_reply_ids": [],
            },
        )
        group["reply_count"] += 1
        if len(group["representative_reply_ids"]) < 5:
            group["representative_reply_ids"].append(item["reply_id"])

    workload_groups = sorted(
        groups.values(),
        key=lambda group: (-group["reply_count"], group["workload_owner"], group["platform"], group["priority"]),
    )
    items.sort(key=lambda item: (-item["age_hours"], item["reply_id"]))
    return {
        "artifact_type": "reply_reviewer_workload",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "include_low_priority": include_low_priority},
        "totals": {
            "pending_reply_count": len(items),
            "workload_group_count": len(workload_groups),
            "age_buckets": dict(sorted(age_buckets.items(), key=lambda pair: _age_sort(pair[0]))),
            "quality_bands": dict(sorted(quality_bands.items(), key=lambda pair: _quality_sort(pair[0]))),
            "by_platform": dict(sorted(by_platform.items())),
            "by_status": dict(sorted(by_status.items())),
            "by_priority": dict(sorted(by_priority.items())),
        },
        "workload_groups": workload_groups,
        "representative_replies": items[:limit],
        "missing_tables": [] if "reply_queue" in schema else ["reply_queue"],
        "missing_columns": _missing_columns(schema),
    }


def format_reply_reviewer_workload_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_reviewer_workload_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Reply Reviewer Workload",
        f"Generated: {report['generated_at']}",
        (
            f"Window: {report['filters']['days']} days limit={report['filters']['limit']} "
            f"include_low_priority={report['filters']['include_low_priority']}"
        ),
        f"Totals: pending={totals['pending_reply_count']} groups={totals['workload_group_count']}",
        "By platform: " + ", ".join(f"{key}={value}" for key, value in totals["by_platform"].items()),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["workload_groups"]:
        lines.extend(["", "No pending reply review workload found."])
        return "\n".join(lines)
    lines.extend(["", "Workload groups:"])
    for group in report["workload_groups"]:
        ids = ", ".join(str(reply_id) for reply_id in group["representative_reply_ids"])
        lines.append(
            f"  - owner={group['workload_owner']} platform={group['platform']} "
            f"priority={group['priority']} age={group['age_bucket']} "
            f"quality={group['quality_band']} count={group['reply_count']} ids={ids}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    include_low_priority: bool,
) -> list[dict[str, Any]]:
    columns = schema.get("reply_queue")
    if columns is None or "id" not in columns:
        return []
    select = [
        "rq.id AS reply_id",
        _expr(columns, "platform", "rq", "platform"),
        _expr(columns, "priority", "rq", "priority"),
        _expr(columns, "quality_score", "rq", "quality_score"),
        _expr(columns, "status", "rq", "status"),
        _expr(columns, "detected_at", "rq", "detected_at"),
        _expr(columns, "draft_text", "rq", "draft_text"),
        _expr(columns, "reviewer", "rq", "reviewer"),
        _expr(columns, "owner", "rq", "owner"),
        _expr(columns, "assignee", "rq", "assignee"),
        _expr(columns, "metadata", "rq", "metadata"),
    ]
    where = []
    params: list[Any] = []
    if "status" in columns:
        where.append("LOWER(COALESCE(rq.status, 'pending')) IN ('pending', 'drafted', 'review', 'needs_review')")
    if "detected_at" in columns:
        where.append("rq.detected_at >= ?")
        params.append(cutoff.isoformat())
    if "draft_text" in columns:
        where.append("COALESCE(TRIM(rq.draft_text), '') != ''")
    if "priority" in columns and not include_low_priority:
        where.append("LOWER(COALESCE(rq.priority, 'normal')) != 'low'")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM reply_queue rq
            {where_sql}
            ORDER BY {('rq.detected_at ASC,' if 'detected_at' in columns else '')} rq.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _item(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    detected = _parse_dt(row.get("detected_at")) or now
    age_hours = int((now - detected).total_seconds() // 3600)
    platform = _clean(row.get("platform")).lower() or "unknown"
    priority = _clean(row.get("priority")).lower() or "normal"
    status = _clean(row.get("status")).lower() or "pending"
    quality_score = _float(row.get("quality_score"))
    owner = _owner(row)
    if owner == "unassigned":
        owner = f"{platform}:{priority}"
    return {
        "reply_id": int(row["reply_id"]),
        "workload_owner": owner,
        "platform": platform,
        "priority": priority,
        "status": status,
        "detected_at": row.get("detected_at"),
        "age_hours": age_hours,
        "age_bucket": _age_bucket(age_hours),
        "quality_score": quality_score,
        "quality_band": _quality_band(quality_score),
    }


def _owner(row: dict[str, Any]) -> str:
    for column in ("reviewer", "owner", "assignee"):
        value = _clean(row.get(column))
        if value:
            return value
    metadata = _json_obj(row.get("metadata"))
    for key in ("reviewer", "owner", "assignee"):
        value = _clean(metadata.get(key))
        if value:
            return value
    return "unassigned"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    optional = {"reply_queue": {"platform", "priority", "quality_score", "status", "detected_at", "draft_text"}}
    missing: dict[str, list[str]] = {}
    for table, columns in optional.items():
        if table in schema:
            gaps = sorted(columns - schema[table])
            if gaps:
                missing[table] = gaps
    return missing


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _json_obj(value: Any) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _age_bucket(age_hours: int) -> str:
    if age_hours < 24:
        return "0-1d"
    if age_hours < 72:
        return "1-3d"
    if age_hours < 168:
        return "3-7d"
    return "7d+"


def _age_sort(bucket: str) -> int:
    return {"0-1d": 0, "1-3d": 1, "3-7d": 2, "7d+": 3}.get(bucket, 4)


def _quality_band(value: float | None) -> str:
    if value is None:
        return "unscored"
    if value >= 8:
        return "high"
    if value >= 5:
        return "medium"
    return "low"


def _quality_sort(bucket: str) -> int:
    return {"low": 0, "medium": 1, "high": 2, "unscored": 3}.get(bucket, 4)


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
