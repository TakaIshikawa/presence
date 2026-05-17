"""Rank knowledge sources worth recrawling by expected ROI."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALENESS_WEIGHT = 1.0
DEFAULT_USAGE_WEIGHT = 2.0
DEFAULT_FAILURE_WEIGHT = 5.0


def build_source_recrawl_roi_report(
    db_or_conn: Any,
    *,
    staleness_weight: float = DEFAULT_STALENESS_WEIGHT,
    usage_weight: float = DEFAULT_USAGE_WEIGHT,
    failure_weight: float = DEFAULT_FAILURE_WEIGHT,
    now: datetime | None = None,
) -> dict[str, Any]:
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "staleness_weight": staleness_weight,
        "usage_weight": usage_weight,
        "failure_weight": failure_weight,
    }
    if "knowledge_sources" not in schema:
        return _report(generated_at, filters, [], missing_tables=["knowledge_sources"])
    rows = [dict(row) for row in conn.execute("SELECT * FROM knowledge_sources").fetchall()]
    candidates = [_candidate(row, generated_at, staleness_weight, usage_weight, failure_weight) for row in rows]
    candidates.sort(key=lambda item: (-item["roi_score"], item["source_id"], item["url"]))
    return _report(generated_at, filters, candidates)


def format_source_recrawl_roi_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_recrawl_roi_text(report: dict[str, Any]) -> str:
    lines = [
        "Knowledge Source Recrawl ROI",
        f"Generated: {report['generated_at']}",
        f"Totals: candidates={report['totals']['candidate_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if not report["candidates"]:
        lines.append("No knowledge sources found.")
        return "\n".join(lines)
    for item in report["candidates"]:
        lines.append(
            f"  - score={item['roi_score']:.2f} source={item['source_id']} "
            f"stale={item['staleness_days']} usage={item['usage_count']} failures={item['failure_count']}"
        )
    return "\n".join(lines)


def _candidate(row: dict[str, Any], now: datetime, staleness_weight: float, usage_weight: float, failure_weight: float) -> dict[str, Any]:
    last_crawled = _parse_dt(row.get("last_crawled_at") or row.get("last_success_at"))
    staleness_days = int((now - last_crawled).total_seconds() // 86400) if last_crawled else 999
    usage_count = _int(row.get("usage_count") or row.get("citation_count"))
    failure_count = _int(row.get("failure_count") or row.get("consecutive_failures"))
    success_rate = _float(row.get("success_rate"), 1.0)
    failure_penalty = failure_count * failure_weight + ((1 - success_rate) * failure_weight)
    roi_score = (staleness_days * staleness_weight) + (usage_count * usage_weight) - failure_penalty
    return {
        "source_id": _clean(row.get("id") or row.get("source_id")),
        "url": _clean(row.get("url") or row.get("identifier")),
        "staleness_days": staleness_days,
        "usage_count": usage_count,
        "failure_count": failure_count,
        "success_rate": success_rate,
        "failure_penalty": round(failure_penalty, 4),
        "roi_score": round(roi_score, 4),
    }


def _report(generated_at: datetime, filters: dict[str, Any], candidates: list[dict[str, Any]], *, missing_tables: list[str] | None = None) -> dict[str, Any]:
    return {
        "artifact_type": "source_recrawl_roi",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"candidate_count": len(candidates)},
        "candidates": candidates,
        "missing_tables": missing_tables or [],
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    return {table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
