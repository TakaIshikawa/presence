"""Rank knowledge rows that need embedding backfill or refresh."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LIMIT = 100
DEFAULT_STALE_DAYS = 90
PRIORITY_SOURCE_TYPES = {
    "curated_article": 30,
    "curated_newsletter": 25,
    "curated_x": 20,
    "own_conversation": 15,
    "own_post": 10,
}


def build_knowledge_embedding_backfill_priority_report(
    knowledge_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    stale_days: int = DEFAULT_STALE_DAYS,
    now: datetime | None = None,
    missing_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a deterministic priority report from already-loaded knowledge rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    stale_before = generated_at - timedelta(days=stale_days)
    candidates = []
    for row in knowledge_rows:
        reasons: list[str] = []
        score = 0
        if not _filled(row.get("embedding")):
            reasons.append("missing_embedding")
            score += 100
        if not _clean(row.get("embedding_model")):
            reasons.append("missing_embedding_model")
            score += 50

        reference_at = _parse_dt(row.get("updated_at")) or _parse_dt(row.get("created_at"))
        age_days = _age_days(reference_at, generated_at)
        if reference_at is not None and reference_at < stale_before:
            reasons.append("stale_knowledge")
            score += 40 + min(age_days or 0, stale_days)

        source_type = _clean(row.get("source_type")) or "unknown"
        source_type_score = PRIORITY_SOURCE_TYPES.get(source_type, 0)
        if source_type_score:
            reasons.append("priority_source_type")
            score += source_type_score

        if not reasons:
            continue
        candidates.append(
            {
                "knowledge_id": _value(row, "knowledge_id", "id"),
                "source_type": source_type,
                "source_id": row.get("source_id"),
                "source_url": row.get("source_url"),
                "embedding_model": _clean(row.get("embedding_model")) or None,
                "updated_at": _iso_or_none(reference_at),
                "age_days": age_days,
                "priority_score": score,
                "reasons": reasons,
                "content_preview": _snippet(row.get("content") or row.get("insight")),
            }
        )

    candidates.sort(key=_priority_sort_key)
    shown = candidates[:limit]
    reason_counts: Counter[str] = Counter()
    for item in candidates:
        reason_counts.update(item["reasons"])
    return {
        "artifact_type": "knowledge_embedding_backfill_priority",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "limit": limit,
            "stale_days": stale_days,
            "stale_before": stale_before.isoformat(),
            "priority_source_types": list(PRIORITY_SOURCE_TYPES),
        },
        "totals": {
            "rows_scanned": len(knowledge_rows),
            "priority_count": len(candidates),
            "shown_count": len(shown),
            "counts_by_reason": dict(sorted(reason_counts.items())),
            "counts_by_source_type": dict(sorted(Counter(item["source_type"] for item in candidates).items())),
        },
        "prioritized_items": shown,
        "missing_schema": missing_schema or {"missing_tables": [], "missing_columns": {}},
    }


def build_knowledge_embedding_backfill_priority_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_schema = _missing_schema(schema)
    if "knowledge" not in schema:
        return build_knowledge_embedding_backfill_priority_report([], missing_schema=missing_schema, **kwargs)
    rows = _load_knowledge_rows(conn, schema["knowledge"])
    return build_knowledge_embedding_backfill_priority_report(rows, missing_schema=missing_schema, **kwargs)


def format_knowledge_embedding_backfill_priority_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_embedding_backfill_priority_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Knowledge Embedding Backfill Priority",
        f"Generated: {report['generated_at']}",
        f"Stale after: {report['filters']['stale_days']} days",
        (
            "Totals: "
            f"rows={totals['rows_scanned']} "
            f"prioritized={totals['priority_count']} "
            f"shown={totals['shown_count']}"
        ),
    ]
    missing = report["missing_schema"]
    if missing["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(missing["missing_tables"]))
    if missing["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(missing["missing_columns"]))
    if not report["prioritized_items"]:
        lines.extend(["", "No knowledge embedding backfill priorities found."])
        return "\n".join(lines)

    lines.extend(["", "Prioritized items:"])
    for item in report["prioritized_items"]:
        lines.append(
            f"  - knowledge_id={item['knowledge_id'] or '-'} type={item['source_type']} "
            f"score={item['priority_score']} reasons={','.join(item['reasons'])} "
            f"age_days={item['age_days'] if item['age_days'] is not None else '-'} "
            f"model={item['embedding_model'] or '-'}"
        )
    return "\n".join(lines)


def _load_knowledge_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    select = [
        _expr(columns, "id", "k", "knowledge_id", default="k.rowid"),
        _expr(columns, "source_type", "k", "source_type", default="'unknown'"),
        _expr(columns, "source_id", "k", "source_id", default="NULL"),
        _expr(columns, "source_url", "k", "source_url", default="NULL"),
        _expr(columns, "content", "k", "content", default="NULL"),
        _expr(columns, "insight", "k", "insight", default="NULL"),
        _expr(columns, "embedding", "k", "embedding", default="NULL"),
        _expr(columns, "embedding_model", "k", "embedding_model", default="NULL"),
        _expr(columns, "updated_at", "k", "updated_at", default="NULL"),
        _expr(columns, "created_at", "k", "created_at", default="NULL"),
    ]
    order = "k.id ASC" if "id" in columns else "k.rowid ASC"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM knowledge k ORDER BY {order}").fetchall()]


def _missing_schema(schema: dict[str, set[str]]) -> dict[str, Any]:
    if "knowledge" not in schema:
        return {"missing_tables": ["knowledge"], "missing_columns": {}}
    optional = {"embedding", "embedding_model", "updated_at", "created_at"}
    missing = sorted(optional - schema["knowledge"])
    return {"missing_tables": [], "missing_columns": {"knowledge": missing} if missing else {}}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    return {table: {column[1] for column in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _expr(columns: set[str], column: str, alias: str, output: str, *, default: str) -> str:
    return f"{alias}.{column} AS {output}" if column in columns else f"{default} AS {output}"


def _priority_sort_key(item: dict[str, Any]) -> tuple[int, int, str, str]:
    age = item["age_days"] if item["age_days"] is not None else -1
    return (-int(item["priority_score"]), -int(age), item["source_type"], str(item["knowledge_id"] or ""))


def _parse_dt(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _utc(parsed)


def _age_days(value: datetime | None, now: datetime) -> int | None:
    if value is None:
        return None
    return int((_utc(now) - _utc(value)).total_seconds() // 86400)


def _iso_or_none(value: datetime | None) -> str | None:
    return None if value is None else _utc(value).isoformat()


def _filled(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bool(value)
    if isinstance(value, (list, tuple)):
        return bool(value)
    text = str(value).strip()
    return bool(text and text not in {"[]", "{}", "null"})


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row:
            return row[key]
    return None


def _snippet(value: Any, limit: int = 120) -> str:
    text = " ".join(_clean(value).split())
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def _format_missing(missing_columns: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}={','.join(columns)}" for table, columns in sorted(missing_columns.items()))


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
