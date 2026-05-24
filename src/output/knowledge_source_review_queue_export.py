"""Export a deterministic knowledge source human review queue."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from evaluation._batch_report_utils import connection, csv_rows, dump_json, first_table, number, parse_time, pick, schema, text, utc_now

FIELDS = ["source_id", "url", "title", "review_priority", "reasons", "license", "confidence", "last_seen_at", "reuse_count"]
DEFAULT_LIMIT = 100


def build_knowledge_source_review_queue_export_from_db(db_or_conn: Any, *, min_priority: int = 1, reason: str | None = None, limit: int = DEFAULT_LIMIT, now: datetime | None = None) -> dict[str, Any]:
    if min_priority < 0 or limit <= 0:
        raise ValueError("min_priority must be non-negative and limit must be positive")
    conn = connection(db_or_conn)
    generated_at = utc_now(now)
    sch = schema(conn)
    table = first_table(sch, ("knowledge_sources", "knowledge_items", "curated_sources"))
    missing_tables = [] if table else ["knowledge_sources|knowledge_items|curated_sources"]
    rows = _load(conn, table, sch[table]) if table else []
    out = []
    for row in rows:
        reasons, score = _score(row, generated_at)
        if score < min_priority or (reason and reason not in reasons):
            continue
        out.append({**row, "review_priority": score, "reasons": reasons})
    out.sort(key=lambda r: (-r["review_priority"], str(r["source_id"])))
    return {"artifact_type": "knowledge_source_review_queue_export", "filters": {"min_priority": min_priority, "reason": reason, "limit": limit}, "rows": out[:limit], "missing_tables": missing_tables, "empty_state": {"is_empty": not out, "message": "No knowledge sources need review." if not out and not missing_tables else None}}


def build_knowledge_source_review_queue_export(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return build_knowledge_source_review_queue_export_from_db(*args, **kwargs)


def format_knowledge_source_review_queue_export_json(export: dict[str, Any]) -> str:
    return dump_json(export)


def format_knowledge_source_review_queue_export_csv(export: dict[str, Any]) -> str:
    return csv_rows(export["rows"], FIELDS)


def _load(conn, table: str, cols: set[str]) -> list[dict[str, Any]]:
    select = [f"{pick(cols, 'id', 'source_id', default='rowid')} AS source_id", f"{pick(cols, 'url', 'source_url', default='NULL')} AS url", f"{pick(cols, 'title', default='NULL')} AS title", f"{pick(cols, 'license', 'license_type', default='NULL')} AS license", f"{pick(cols, 'confidence', 'confidence_score', default='NULL')} AS confidence", f"{pick(cols, 'last_seen_at', 'crawled_at', 'updated_at', default='NULL')} AS last_seen_at", f"{pick(cols, 'reuse_count', 'usage_count', default='0')} AS reuse_count", f"{pick(cols, 'extraction_error', 'error', default='NULL')} AS extraction_error"]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY source_id ASC")]


def _score(row: dict[str, Any], now: datetime) -> tuple[list[str], int]:
    reasons = []
    score = 0
    if not text(row.get("license")):
        reasons.append("missing_license")
        score += 3
    if row.get("confidence") is not None and number(row.get("confidence")) < 0.5:
        reasons.append("low_confidence")
        score += 2
    seen = parse_time(row.get("last_seen_at"))
    if not seen or now - seen > timedelta(days=90):
        reasons.append("stale_crawl")
        score += 2
    if text(row.get("extraction_error")):
        reasons.append("extraction_error")
        score += 3
    if int(number(row.get("reuse_count"))) >= 10:
        reasons.append("high_reuse")
        score += 1
    return reasons, score
