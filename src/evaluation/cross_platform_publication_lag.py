"""Report lag between platform publications for the same generated content."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_THRESHOLD_HOURS = 1.0
DEFAULT_LIMIT = 100


def build_cross_platform_publication_lag_report(
    db_or_conn: Any,
    *,
    threshold_hours: float = DEFAULT_THRESHOLD_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Compare per-platform publication state for each content item."""
    if threshold_hours < 0:
        raise ValueError("threshold_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_publications(conn, schema)
    content_meta = _load_content(conn, schema)
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        if row.get("content_id") is None:
            continue
        grouped.setdefault(int(row["content_id"]), []).append(row)

    lagging_items: list[dict[str, Any]] = []
    pair_counts: Counter[str] = Counter()
    by_platform: Counter[str] = Counter()
    by_status: Counter[str] = Counter()

    for content_id, pubs in grouped.items():
        published = [
            row
            for row in pubs
            if _status(row.get("status")) == "published" and _parse_dt(row.get("published_at"))
        ]
        if not published:
            continue
        first = min(published, key=lambda row: (_parse_dt(row.get("published_at")) or generated_at, row.get("id") or 0))
        first_at = _parse_dt(first.get("published_at")) or generated_at
        first_platform = _platform(first.get("platform"))
        for row in pubs:
            platform = _platform(row.get("platform"))
            if platform == first_platform:
                continue
            status = _status(row.get("status"))
            published_at = _parse_dt(row.get("published_at"))
            lag_hours = (
                (published_at - first_at).total_seconds() / 3600
                if status == "published" and published_at is not None
                else (generated_at - first_at).total_seconds() / 3600
            )
            if lag_hours < threshold_hours:
                continue
            if status == "published" and published_at is not None:
                continue
            item = {
                "content_id": content_id,
                "publication_id": row.get("id"),
                "first_platform": first_platform,
                "first_published_at": first_at.isoformat(),
                "lagging_platform": platform,
                "lagging_status": status,
                "lag_hours": round(lag_hours, 2),
                "error_category": _clean(row.get("error_category")) or "none",
                "error": row.get("error"),
                "updated_at": row.get("updated_at"),
                "content_type": content_meta.get(content_id, {}).get("content_type"),
                "content_published": content_meta.get(content_id, {}).get("published"),
            }
            lagging_items.append(item)
            pair_counts[f"{first_platform}->{platform}:{status}"] += 1
            by_platform[platform] += 1
            by_status[status] += 1

    lagging_items.sort(key=lambda item: (-item["lag_hours"], item["content_id"], item["lagging_platform"]))
    limited = lagging_items[:limit]
    return {
        "artifact_type": "cross_platform_publication_lag",
        "generated_at": generated_at.isoformat(),
        "filters": {"threshold_hours": threshold_hours, "limit": limit},
        "totals": {
            "publication_rows_scanned": len(rows),
            "content_items_scanned": len(grouped),
            "lagging_count": len(lagging_items),
        },
        "lagging_items": limited,
        "by_platform": dict(sorted(by_platform.items())),
        "by_status": dict(sorted(by_status.items())),
        "by_platform_pair_status": dict(sorted(pair_counts.items())),
        "missing_tables": [table for table in ("content_publications", "generated_content") if table not in schema],
        "missing_columns": _missing_columns(schema),
    }


def format_cross_platform_publication_lag_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_cross_platform_publication_lag_text(report: dict[str, Any]) -> str:
    lines = [
        "Cross-Platform Publication Lag",
        f"Generated: {report['generated_at']}",
        f"Threshold: {report['filters']['threshold_hours']}h limit={report['filters']['limit']}",
        (
            "Totals: "
            f"rows={report['totals']['publication_rows_scanned']} "
            f"content={report['totals']['content_items_scanned']} "
            f"lagging={report['totals']['lagging_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["lagging_items"]:
        lines.extend(["", "No cross-platform publication lag found."])
        return "\n".join(lines)
    lines.extend(["", "Lagging items:"])
    for item in report["lagging_items"]:
        lines.append(
            f"  - content_id={item['content_id']} first={item['first_platform']} "
            f"lagging={item['lagging_platform']} status={item['lagging_status']} "
            f"lag_hours={item['lag_hours']:.2f} error_category={item['error_category']}"
        )
    return "\n".join(lines)


def _load_publications(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("content_publications")
    if columns is None or not {"content_id", "platform", "status"}.issubset(columns):
        return []
    select = [
        "cp.id AS id" if "id" in columns else "NULL AS id",
        "cp.content_id AS content_id",
        "cp.platform AS platform",
        "cp.status AS status",
        _expr(columns, "published_at", "cp", "published_at"),
        _expr(columns, "updated_at", "cp", "updated_at"),
        _expr(columns, "error", "cp", "error"),
        _expr(columns, "error_category", "cp", "error_category"),
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(select)}
            FROM content_publications cp
            ORDER BY cp.content_id ASC, cp.platform ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, dict[str, Any]]:
    columns = schema.get("generated_content")
    if columns is None or "id" not in columns:
        return {}
    rows = conn.execute(
        f"""SELECT gc.id AS id,
                   {_expr(columns, "content_type", "gc", "content_type")},
                   {_expr(columns, "published", "gc", "published")}
            FROM generated_content gc"""
    ).fetchall()
    return {int(row["id"]): dict(row) for row in rows}


def _missing_columns(schema: dict[str, set[str]]) -> dict[str, list[str]]:
    required = {
        "content_publications": {"content_id", "platform", "status", "published_at"},
        "generated_content": {"id"},
    }
    missing: dict[str, list[str]] = {}
    for table, columns in required.items():
        if table in schema:
            gaps = sorted(columns - schema[table])
            if gaps:
                missing[table] = gaps
    return missing


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
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _platform(value: Any) -> str:
    return _clean(value).lower() or "unknown"


def _status(value: Any) -> str:
    return _clean(value).lower() or "unknown"


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _expr(columns: set[str], column: str, alias: str, output: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"NULL AS {output}"


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
