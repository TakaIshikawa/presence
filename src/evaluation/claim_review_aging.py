"""Aging report for generated content with unsupported claim-review signals."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 20
DEFAULT_MAX_AGE_BUCKET = "31_plus_days"
AGE_BUCKETS = (
    ("0_7_days", 0, 7),
    ("8_14_days", 8, 14),
    ("15_30_days", 15, 30),
    ("31_plus_days", 31, None),
)


def build_claim_review_aging_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    max_age_bucket: str = DEFAULT_MAX_AGE_BUCKET,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic report for content needing claim-review attention."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if max_age_bucket not in {bucket[0] for bucket in AGE_BUCKETS}:
        raise ValueError(f"invalid max_age_bucket: {max_age_bucket}")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_rows(conn, schema, cutoff, generated_at)
    rows = [row for row in rows if row["age_bucket_index"] <= _bucket_index(max_age_bucket)]
    for row in rows:
        del row["age_bucket_index"]
    totals = _totals(rows)
    rows = sorted(
        rows,
        key=lambda row: (
            -row["risk_score"],
            -row["age_days"],
            -row["unsupported_claim_count"],
            row["content_id"],
        ),
    )[:limit]
    return {
        "artifact_type": "claim_review_aging",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "max_age_bucket": max_age_bucket},
        "totals": totals,
        "items": rows,
        "missing_tables": [] if "generated_content" in schema else ["generated_content"],
    }


def format_claim_review_aging_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_claim_review_aging_text(report: dict[str, Any]) -> str:
    lines = [
        "Claim Review Aging",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={report['filters']['days']} limit={report['filters']['limit']} "
            f"max_age_bucket={report['filters']['max_age_bucket']}"
        ),
        (
            f"Totals: scanned={report['totals']['rows_scanned']} "
            f"flagged={report['totals']['flagged_items']}"
        ),
        "Age buckets: " + _fmt_counts(report["totals"]["age_buckets"]),
    ]
    if not report["items"]:
        lines.extend(["", "No claim-review aging items found."])
        return "\n".join(lines)
    lines.extend(["", "Items:"])
    for item in report["items"]:
        lines.append(
            f"- content_id={item['content_id']} risk={item['risk_score']} "
            f"age={item['age_days']} bucket={item['age_bucket']} "
            f"unsupported={item['unsupported_claim_count']} "
            f"type={item['content_type']} published={item['publish_status']}"
        )
    return "\n".join(lines)


def _load_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    cols = schema["generated_content"]
    claim_cols = schema.get("content_claim_checks", set())
    join = "LEFT JOIN content_claim_checks cc ON cc.content_id = gc.id" if claim_cols else ""
    unsupported_expr = "cc.unsupported_count" if "unsupported_count" in claim_cols else "NULL"
    supported_expr = "cc.supported_count" if "supported_count" in claim_cols else "NULL"
    metadata_expr = "gc.metadata" if "metadata" in cols else "NULL"
    stats_expr = "gc.stats" if "stats" in cols else "NULL"
    created_expr = "gc.created_at" if "created_at" in cols else "NULL"
    published_expr = "gc.published" if "published" in cols else "NULL"
    published_at_expr = "gc.published_at" if "published_at" in cols else "NULL"
    rows = conn.execute(
        f"""SELECT gc.id, gc.content_type, {created_expr} AS created_at,
                  {published_expr} AS published, {published_at_expr} AS published_at,
                  {metadata_expr} AS metadata, {stats_expr} AS stats,
                  {unsupported_expr} AS unsupported_count,
                  {supported_expr} AS supported_count
           FROM generated_content gc
           {join}
           ORDER BY gc.id ASC"""
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        created = _parse_dt(row["created_at"]) or now
        if created < cutoff or created > now:
            continue
        unsupported = _unsupported_count(row)
        if unsupported <= 0 and not _has_claim_metadata(row):
            continue
        age_days = int((now - created).total_seconds() // 86400)
        bucket, index = _age_bucket(age_days)
        publish_status = _publish_status(row["published"], row["published_at"])
        risk = unsupported * 20 + age_days + (10 if publish_status == "published" else 0)
        out.append(
            {
                "content_id": int(row["id"]),
                "content_type": row["content_type"] or "unknown",
                "created_at": created.isoformat(),
                "age_days": age_days,
                "age_bucket": bucket,
                "age_bucket_index": index,
                "unsupported_claim_count": unsupported,
                "supported_claim_count": _int(row["supported_count"]) or 0,
                "publish_status": publish_status,
                "risk_score": risk,
            }
        )
    return out


def _unsupported_count(row: sqlite3.Row) -> int:
    direct = _int(row["unsupported_count"])
    metadata = _json_obj(row["metadata"])
    stats = _json_obj(row["stats"])
    values = [
        direct,
        _dig_int(metadata, "unsupported_claim_count"),
        _dig_int(metadata, "unsupported_count"),
        _dig_int(metadata.get("claim_check"), "unsupported_count"),
        _dig_int(stats, "unsupported_claim_count"),
        _dig_int(stats.get("claim_check"), "unsupported_count"),
    ]
    return max([value for value in values if value is not None] or [0])


def _has_claim_metadata(row: sqlite3.Row) -> bool:
    metadata = _json_obj(row["metadata"])
    stats = _json_obj(row["stats"])
    return bool(metadata.get("claim_check") or stats.get("claim_check"))


def _totals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    age_buckets = {bucket[0]: 0 for bucket in AGE_BUCKETS}
    content_types: Counter[str] = Counter()
    publish_statuses: Counter[str] = Counter()
    unsupported_buckets: Counter[str] = Counter()
    for row in rows:
        age_buckets[row["age_bucket"]] += 1
        content_types[row["content_type"]] += 1
        publish_statuses[row["publish_status"]] += 1
        unsupported_buckets[_unsupported_bucket(row["unsupported_claim_count"])] += 1
    return {
        "rows_scanned": len(rows),
        "flagged_items": len(rows),
        "age_buckets": age_buckets,
        "by_content_type": dict(sorted(content_types.items())),
        "by_publish_status": dict(sorted(publish_statuses.items())),
        "unsupported_claim_buckets": dict(sorted(unsupported_buckets.items())),
    }


def _age_bucket(age_days: int) -> tuple[str, int]:
    for index, (name, start, end) in enumerate(AGE_BUCKETS):
        if age_days >= start and (end is None or age_days <= end):
            return name, index
    return AGE_BUCKETS[-1][0], len(AGE_BUCKETS) - 1


def _bucket_index(name: str) -> int:
    return [bucket[0] for bucket in AGE_BUCKETS].index(name)


def _unsupported_bucket(count: int) -> str:
    if count <= 0:
        return "0"
    if count == 1:
        return "1"
    if count <= 3:
        return "2_3"
    return "4_plus"


def _publish_status(published: Any, published_at: Any) -> str:
    value = _int(published)
    if value == 1 or published_at:
        return "published"
    if value == -1:
        return "abandoned"
    return "draft"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _dig_int(value: Any, key: str) -> int | None:
    return _int(value.get(key)) if isinstance(value, dict) else None


def _int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fmt_counts(counts: dict[str, int]) -> str:
    return ",".join(f"{key}={value}" for key, value in sorted(counts.items()))
