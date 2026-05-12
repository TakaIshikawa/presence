"""Report whether generated-content feedback has been resolved."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_LIMIT = 100
REPRESENTATIVE_LIMIT = 5
FEEDBACK_TYPES = {"reject", "revise", "prefer"}


def build_content_feedback_resolution_throughput_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only resolution report for content_feedback rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = [
        table
        for table in ("content_feedback", "generated_content")
        if table not in schema
    ]
    missing_columns = _missing_columns(schema)
    rows = _load_rows(conn, schema, cutoff=cutoff, limit=limit)

    tag_counts: Counter[str] = Counter()
    group_counts: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    oldest_unresolved: list[dict[str, Any]] = []
    malformed_tags = 0
    resolved_count = 0

    for row in rows:
        tags, malformed = _tags(row.get("tags"))
        malformed_tags += int(malformed)
        if not tags:
            tags = ["untagged"]
        for tag in tags:
            tag_counts[tag] += 1
        age_bucket = _age_bucket(row.get("created_at"), generated_at)
        state = _resolution_state(row)
        resolved = state == "resolved"
        resolved_count += int(resolved)
        for tag in tags:
            key = (
                _clean(row.get("feedback_type")) or "unknown",
                tag,
                age_bucket,
                state,
            )
            group = group_counts.setdefault(
                key,
                {
                    "feedback_type": key[0],
                    "tag": key[1],
                    "age_bucket": key[2],
                    "resolution_state": key[3],
                    "feedback_count": 0,
                    "resolved_count": 0,
                    "unresolved_count": 0,
                    "representative_feedback_ids": [],
                },
            )
            group["feedback_count"] += 1
            group["resolved_count"] += int(resolved)
            group["unresolved_count"] += int(not resolved)
            if len(group["representative_feedback_ids"]) < REPRESENTATIVE_LIMIT:
                group["representative_feedback_ids"].append(row["feedback_id"])
        if not resolved:
            oldest_unresolved.append(_unresolved_item(row, tags, age_bucket))

    groups = list(group_counts.values())
    for group in groups:
        group["resolution_rate"] = _rate(group["resolved_count"], group["feedback_count"])
    groups.sort(
        key=lambda item: (
            item["resolution_state"] != "unresolved",
            -item["unresolved_count"],
            -item["feedback_count"],
            item["feedback_type"],
            item["tag"],
            _age_sort(item["age_bucket"]),
        )
    )
    oldest_unresolved.sort(key=lambda item: (item["created_at"] or "", item["feedback_id"]))

    return {
        "artifact_type": "content_feedback_resolution_throughput",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "days": days,
            "limit": limit,
            "lookback_start": cutoff.isoformat(),
            "lookback_end": generated_at.isoformat(),
        },
        "totals": {
            "feedback_count": len(rows),
            "resolved_count": resolved_count,
            "unresolved_count": len(rows) - resolved_count,
            "resolution_rate": _rate(resolved_count, len(rows)),
            "malformed_tag_rows": malformed_tags,
            "feedback_type_counts": dict(
                sorted(Counter(_clean(row.get("feedback_type")) or "unknown" for row in rows).items())
            ),
        },
        "tag_counts": dict(sorted(tag_counts.items())),
        "groups": groups,
        "oldest_unresolved": oldest_unresolved[:limit],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_content_feedback_resolution_throughput_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_feedback_resolution_throughput_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Content Feedback Resolution Throughput",
        f"Generated: {report['generated_at']}",
        f"Window: {report['filters']['days']} days limit={report['filters']['limit']}",
        (
            "Totals: "
            f"feedback={totals['feedback_count']} "
            f"resolved={totals['resolved_count']} "
            f"unresolved={totals['unresolved_count']} "
            f"resolution_rate={totals['resolution_rate']:.2f}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["groups"]:
        lines.extend(["", "No content feedback found."])
        return "\n".join(lines)

    lines.extend(["", "Unresolved feedback by type and tag:"])
    unresolved_groups = [group for group in report["groups"] if group["resolution_state"] == "unresolved"]
    if not unresolved_groups:
        lines.append("  none")
    for group in unresolved_groups:
        ids = ", ".join(str(item) for item in group["representative_feedback_ids"])
        lines.append(
            f"  - type={group['feedback_type']} tag={group['tag']} "
            f"age={group['age_bucket']} unresolved={group['unresolved_count']} ids={ids}"
        )
    if report["oldest_unresolved"]:
        lines.append("")
        lines.append("Oldest unresolved:")
        for item in report["oldest_unresolved"][:REPRESENTATIVE_LIMIT]:
            lines.append(
                f"  - feedback_id={item['feedback_id']} content_id={item['content_id']} "
                f"type={item['feedback_type']} tags={','.join(item['tags'])} "
                f"created_at={item['created_at'] or '-'}"
            )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    limit: int,
) -> list[dict[str, Any]]:
    if "content_feedback" not in schema:
        return []
    cf = schema["content_feedback"]
    gc = schema.get("generated_content", set())
    select = [
        "cf.id AS feedback_id",
        _expr(cf, "content_id", "cf", "content_id"),
        _expr(cf, "feedback_type", "cf", "feedback_type"),
        _expr(cf, "replacement_text", "cf", "replacement_text"),
        _expr(cf, "tags", "cf", "tags"),
        _expr(cf, "created_at", "cf", "created_at"),
        _expr(gc, "curation_quality", "gc", "curation_quality"),
        _expr(gc, "published", "gc", "published"),
        _expr(gc, "published_at", "gc", "published_at"),
    ]
    join = (
        "LEFT JOIN generated_content gc ON gc.id = cf.content_id"
        if "generated_content" in schema and "content_id" in cf and "id" in gc
        else "LEFT JOIN (SELECT NULL AS id) gc ON 0"
    )
    where = []
    params: list[Any] = []
    if "created_at" in cf:
        where.append("cf.created_at >= ?")
        params.append(cutoff.isoformat())
    if "feedback_type" in cf:
        where.append("cf.feedback_type IN ('reject', 'revise', 'prefer')")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    order = "cf.created_at ASC, cf.id ASC" if "created_at" in cf else "cf.id ASC"
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM content_feedback cf
            {join}
            {where_sql}
            ORDER BY {order}
            LIMIT ?""",
        (*params, limit),
    ).fetchall()
    return [dict(row) for row in rows]


def _resolution_state(row: dict[str, Any]) -> str:
    if _clean(row.get("replacement_text")):
        return "resolved"
    if _clean(row.get("curation_quality")):
        return "resolved"
    published = row.get("published")
    if str(published) in {"1", "-1"} or _clean(row.get("published_at")):
        return "resolved"
    return "unresolved"


def _unresolved_item(row: dict[str, Any], tags: list[str], age_bucket: str) -> dict[str, Any]:
    return {
        "feedback_id": row.get("feedback_id"),
        "content_id": row.get("content_id"),
        "feedback_type": _clean(row.get("feedback_type")) or "unknown",
        "tags": tags,
        "age_bucket": age_bucket,
        "created_at": row.get("created_at"),
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    required = {
        "content_feedback": {"id", "content_id", "feedback_type", "created_at"},
        "generated_content": {"id", "curation_quality", "published"},
    }
    missing: dict[str, list[str]] = {}
    for table, columns in required.items():
        if table in schema:
            gaps = sorted(columns - schema[table])
            if gaps:
                missing[table] = gaps
    return missing


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _tags(value: Any) -> tuple[list[str], bool]:
    if value in (None, ""):
        return [], False
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return ["malformed"], True
    if not isinstance(parsed, list):
        return ["malformed"], True
    tags = sorted({_clean(item) for item in parsed if _clean(item)})
    return (tags or ["untagged"], False)


def _age_bucket(value: Any, now: datetime) -> str:
    parsed = _parse_dt(value)
    if parsed is None:
        return "unknown"
    days = (now - parsed).total_seconds() / 86400
    if days < 1:
        return "0-1d"
    if days < 3:
        return "1-3d"
    if days < 7:
        return "3-7d"
    if days < 14:
        return "7-14d"
    return "14d+"


def _age_sort(bucket: str) -> int:
    return {"0-1d": 0, "1-3d": 1, "3-7d": 2, "7-14d": 3, "14d+": 4}.get(bucket, 5)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
