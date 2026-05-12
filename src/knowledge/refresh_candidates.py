"""Find approved knowledge rows that should be recrawled or re-embedded."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_LIMIT = 20
DEFAULT_MIN_PRIORITY = 20


def build_knowledge_refresh_candidates_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    min_priority: int = DEFAULT_MIN_PRIORITY,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return approved knowledge items whose freshness signals need attention."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if min_priority < 0:
        raise ValueError("min_priority must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    threshold = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    rows = _load_rows(conn, schema)
    link_ages = _last_link_ages(conn, schema)
    candidates = [_candidate(row, link_ages.get(int(row["id"])), threshold, generated_at) for row in rows]
    candidates = [item for item in candidates if item["priority_score"] >= min_priority]
    candidates.sort(key=lambda item: (-item["priority_score"], item["knowledge_id"]))
    return {
        "artifact_type": "knowledge_refresh_candidates",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "limit": limit, "min_priority": min_priority},
        "totals": {
            "rows_scanned": len(rows),
            "candidate_count": len(candidates),
            "reason_counts": dict(sorted(Counter(code for item in candidates for code in item["reason_codes"]).items())),
        },
        "candidates": candidates[:limit],
        "missing_tables": [] if "knowledge" in schema else ["knowledge"],
    }


def format_knowledge_refresh_candidates_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_knowledge_refresh_candidates_text(report: dict[str, Any]) -> str:
    lines = [
        "Knowledge Refresh Candidates",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={report['filters']['days']} limit={report['filters']['limit']} "
            f"min_priority={report['filters']['min_priority']}"
        ),
        f"Totals: scanned={report['totals']['rows_scanned']} candidates={report['totals']['candidate_count']}",
    ]
    if not report["candidates"]:
        lines.extend(["", "No knowledge refresh candidates found."])
        return "\n".join(lines)
    lines.extend(["", "Candidates:"])
    for item in report["candidates"]:
        lines.append(
            f"- knowledge_id={item['knowledge_id']} priority={item['priority_score']} "
            f"reasons={','.join(item['reason_codes'])} source={item['source_type']}:{item['source_url'] or item['source_id'] or '-'}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[sqlite3.Row]:
    if "knowledge" not in schema:
        return []
    cols = schema["knowledge"]
    expr = lambda col: f"k.{col}" if col in cols else f"NULL AS {col}"
    approved_filter = "COALESCE(k.approved, 0) = 1" if "approved" in cols else "1 = 1"
    return list(
        conn.execute(
            f"""SELECT k.id, {expr('source_type')}, {expr('source_id')},
                      {expr('source_url')}, {expr('metadata')},
                      {expr('embedding')}, {expr('ingested_at')},
                      {expr('created_at')}
               FROM knowledge k
               WHERE {approved_filter}
               ORDER BY k.id ASC"""
        )
    )


def _candidate(row: sqlite3.Row, last_link_at: datetime | None, threshold: datetime, now: datetime) -> dict[str, Any]:
    metadata = _json_obj(row["metadata"])
    reason_codes: list[str] = []
    priority = 0
    ingested = _parse_dt(row["ingested_at"]) or _parse_dt(row["created_at"])
    embedding_at = _parse_dt(metadata.get("embedding_generated_at") or metadata.get("embedded_at"))
    link_metadata_at = _parse_dt(metadata.get("link_metadata_refreshed_at") or metadata.get("source_metadata_refreshed_at"))

    if not metadata:
        reason_codes.append("missing_metadata")
        priority += 30
    if ingested is None or ingested < threshold:
        reason_codes.append("stale_ingested_at")
        priority += 25 + _age_bonus(ingested, now)
    if row["embedding"] is None:
        reason_codes.append("missing_embedding")
        priority += 35
    elif embedding_at is None or embedding_at < threshold:
        reason_codes.append("stale_embedding_metadata")
        priority += 25 + _age_bonus(embedding_at, now)
    if link_metadata_at is None or link_metadata_at < threshold:
        reason_codes.append("stale_link_metadata")
        priority += 20 + _age_bonus(link_metadata_at, now)
    if last_link_at is None:
        reason_codes.append("never_cited")
        priority += 15
    elif last_link_at < threshold:
        reason_codes.append("stale_last_citation")
        priority += 15 + _age_bonus(last_link_at, now)

    return {
        "knowledge_id": int(row["id"]),
        "source_type": row["source_type"],
        "source_id": row["source_id"],
        "source_url": row["source_url"],
        "reason_codes": reason_codes,
        "priority_score": priority,
        "ingested_at": ingested.isoformat() if ingested else None,
        "embedding_metadata_at": embedding_at.isoformat() if embedding_at else None,
        "link_metadata_at": link_metadata_at.isoformat() if link_metadata_at else None,
        "last_cited_at": last_link_at.isoformat() if last_link_at else None,
    }


def _last_link_ages(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[int, datetime]:
    if "content_knowledge_links" not in schema:
        return {}
    rows = conn.execute(
        "SELECT knowledge_id, MAX(created_at) AS last_cited_at FROM content_knowledge_links GROUP BY knowledge_id"
    ).fetchall()
    out: dict[int, datetime] = {}
    for row in rows:
        parsed = _parse_dt(row["last_cited_at"])
        if parsed:
            out[int(row["knowledge_id"])] = parsed
    return out


def _age_bonus(value: datetime | None, now: datetime) -> int:
    if value is None:
        return 10
    return min(25, int((now - value).total_seconds() // 86400 // 30))


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
