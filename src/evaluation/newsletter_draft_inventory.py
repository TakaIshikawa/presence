"""Inventory generated newsletter drafts and their source coverage."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 14
DEFAULT_LIMIT = 100
NEWSLETTER_TYPES = {"newsletter", "newsletter_issue", "newsletter_draft", "email_newsletter"}
SOURCE_FIELDS = ("source_commits", "source_messages", "source_activity_ids")


def build_newsletter_draft_inventory_report(
    db_or_conn: Any,
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return newsletter-like generated content classified by draft state."""
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    newsletter_variant_ids = _newsletter_variant_ids(conn, schema)
    rows = _load_rows(conn, schema, newsletter_variant_ids)

    items = []
    malformed_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    stale_buckets: Counter[str] = Counter()
    source_coverage_counts: Counter[str] = Counter()
    representatives: dict[str, list[int]] = {}

    for row in rows:
        item, malformed = _item(row, generated_at, stale_days, newsletter_variant_ids)
        for field in malformed:
            malformed_counts[field] += 1
        status_counts[item["inventory_status"]] += 1
        stale_buckets[item["age_bucket"]] += 1
        for field in SOURCE_FIELDS:
            source_coverage_counts[f"{field}_with_sources"] += int(item["source_counts"][field] > 0)
            source_coverage_counts[f"{field}_missing"] += int(item["source_counts"][field] == 0)
        representatives.setdefault(item["inventory_status"], [])
        if len(representatives[item["inventory_status"]]) < 5:
            representatives[item["inventory_status"]].append(item["content_id"])
        items.append(item)

    items.sort(key=lambda item: (_status_sort(item["inventory_status"]), -item["age_days"], item["content_id"]))
    return {
        "artifact_type": "newsletter_draft_inventory",
        "generated_at": generated_at.isoformat(),
        "filters": {"stale_days": stale_days, "limit": limit},
        "totals": {
            "newsletter_count": len(items),
            "status_counts": dict(sorted(status_counts.items())),
            "source_coverage_counts": dict(sorted(source_coverage_counts.items())),
            "stale_age_buckets": dict(sorted(stale_buckets.items(), key=lambda pair: _age_sort(pair[0]))),
            "malformed_source_fields": dict(sorted(malformed_counts.items())),
        },
        "representative_content_ids": dict(sorted(representatives.items())),
        "items": items[:limit],
        "missing_tables": [table for table in ("generated_content",) if table not in schema],
        "missing_optional_table_metadata": [] if "content_variants" in schema else ["content_variants"],
        "missing_columns": _missing_columns(schema),
    }


def format_newsletter_draft_inventory_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_draft_inventory_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Draft Inventory",
        f"Generated: {report['generated_at']}",
        f"Stale after: {report['filters']['stale_days']} days limit={report['filters']['limit']}",
        "Status counts: "
        + ", ".join(f"{status}={count}" for status, count in report["totals"]["status_counts"].items()),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_optional_table_metadata"]:
        lines.append("Missing optional table metadata: " + ", ".join(report["missing_optional_table_metadata"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["items"]:
        lines.extend(["", "No newsletter drafts found."])
        return "\n".join(lines)
    lines.extend(["", "Newsletter items:"])
    for item in report["items"]:
        lines.append(
            f"  - content_id={item['content_id']} status={item['inventory_status']} "
            f"type={item['content_type']} age={item['age_bucket']} "
            f"sources={item['total_source_count']}"
        )
    return "\n".join(lines)


def _newsletter_variant_ids(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> set[int]:
    columns = schema.get("content_variants")
    if columns is None or not {"content_id", "platform"}.issubset(columns):
        return set()
    rows = conn.execute(
        "SELECT DISTINCT content_id FROM content_variants WHERE LOWER(COALESCE(platform, '')) = 'newsletter'"
    ).fetchall()
    return {int(row["content_id"]) for row in rows if row["content_id"] is not None}


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    variant_ids: set[int],
) -> list[dict[str, Any]]:
    columns = schema.get("generated_content")
    if columns is None or "id" not in columns:
        return []
    where = []
    params: list[Any] = []
    if "content_type" in columns:
        placeholders = ", ".join("?" for _ in NEWSLETTER_TYPES)
        where.append(f"LOWER(COALESCE(content_type, '')) IN ({placeholders})")
        params.extend(sorted(NEWSLETTER_TYPES))
    if variant_ids:
        placeholders = ", ".join("?" for _ in variant_ids)
        where.append(f"id IN ({placeholders})")
        params.extend(sorted(variant_ids))
    if not where:
        return []
    select = [
        "gc.id AS content_id",
        _expr(columns, "content_type", "gc", "content_type"),
        _expr(columns, "created_at", "gc", "created_at"),
        _expr(columns, "eval_score", "gc", "eval_score"),
        _expr(columns, "published", "gc", "published"),
        _expr(columns, "published_at", "gc", "published_at"),
        *[_expr(columns, field, "gc", field) for field in SOURCE_FIELDS],
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM generated_content gc
            WHERE {' OR '.join(f'({part})' for part in where)}
            ORDER BY gc.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _item(
    row: dict[str, Any],
    now: datetime,
    stale_days: int,
    variant_ids: set[int],
) -> tuple[dict[str, Any], list[str]]:
    counts: dict[str, int] = {}
    malformed: list[str] = []
    for field in SOURCE_FIELDS:
        values, bad = _json_list(row.get(field))
        counts[field] = len(values)
        if bad:
            malformed.append(field)
    total_sources = sum(counts.values())
    created = _parse_dt(row.get("created_at"))
    age_days = int((now - created).total_seconds() // 86400) if created else 0
    status = _classify(row, total_sources, malformed, age_days, stale_days)
    return (
        {
            "content_id": int(row["content_id"]),
            "content_type": _clean(row.get("content_type")) or "unknown",
            "inventory_status": status,
            "created_at": row.get("created_at"),
            "age_days": age_days,
            "age_bucket": _age_bucket(age_days, created is None),
            "source_counts": counts,
            "total_source_count": total_sources,
            "malformed_source_fields": malformed,
            "newsletter_variant": int(row["content_id"]) in variant_ids,
            "eval_score": _float(row.get("eval_score")),
            "published": _int(row.get("published")),
        },
        malformed,
    )


def _classify(
    row: dict[str, Any],
    total_sources: int,
    malformed: list[str],
    age_days: int,
    stale_days: int,
) -> str:
    published = _int(row.get("published"))
    if published == -1:
        return "abandoned"
    if published == 1 or _clean(row.get("published_at")):
        return "sent"
    if total_sources == 0 or malformed:
        return "needs_sources"
    if _float(row.get("eval_score")) is None:
        return "needs_eval"
    if age_days >= stale_days:
        return "stale_draft"
    return "draft_ready"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    required = {"generated_content": {"id", "content_type", *SOURCE_FIELDS}}
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


def _json_list(value: Any) -> tuple[list[Any], bool]:
    if value in (None, ""):
        return [], False
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
    except (TypeError, json.JSONDecodeError):
        return [], True
    return (parsed, False) if isinstance(parsed, list) else ([], True)


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _age_bucket(age_days: int, unknown: bool = False) -> str:
    if unknown:
        return "unknown"
    if age_days < 3:
        return "0-3d"
    if age_days < 7:
        return "3-7d"
    if age_days < 14:
        return "7-14d"
    return "14d+"


def _age_sort(bucket: str) -> int:
    return {"0-3d": 0, "3-7d": 1, "7-14d": 2, "14d+": 3}.get(bucket, 4)


def _status_sort(status: str) -> int:
    return {
        "needs_sources": 0,
        "needs_eval": 1,
        "stale_draft": 2,
        "draft_ready": 3,
        "sent": 4,
        "abandoned": 5,
    }.get(status, 9)


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
