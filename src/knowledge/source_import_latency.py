"""Measure latency from source discovery to knowledge ingestion readiness."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import math
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MAX_LATENCY_HOURS = 24


def build_source_import_latency_report(
    rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    max_latency_hours: int = DEFAULT_MAX_LATENCY_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if max_latency_hours < 0:
        raise ValueError("max_latency_hours must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    items = []
    for row in rows:
        discovered_at = _parse_datetime(_first(row, "discovered_at", "curated_at", "created_at"))
        if discovered_at is not None and discovered_at < cutoff:
            continue
        items.append(_latency_item(row, generated_at))
    completed = [item["latency_hours"] for item in items if item["status"] == "completed" and item["latency_hours"] is not None]
    flagged = [item for item in items if item["status"] != "completed" or (item["latency_hours"] or 0) > max_latency_hours]
    flagged.sort(key=lambda item: (_status_rank(item["status"]), -(item["latency_hours"] or 0), item["source_id"]))
    return {
        "artifact_type": "source_import_latency",
        "generated_at": generated_at.isoformat(),
        "filters": {"lookback_days": lookback_days, "max_latency_hours": max_latency_hours, "cutoff": cutoff.isoformat()},
        "totals": {
            "rows_scanned": len(rows),
            "source_count": len(items),
            "flagged_count": len(flagged),
            "latency_percentiles": _percentiles(completed),
        },
        "flagged_sources": flagged,
        "empty_state": {"is_empty": not items, "message": "No knowledge sources found in the lookback window." if not items else None},
    }


def build_source_import_latency_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_source_import_latency_report(_load_rows(conn, schema), **kwargs)


def format_source_import_latency_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_source_import_latency_text(report: dict[str, Any]) -> str:
    lines = [
        "Knowledge Source Import Latency",
        f"Generated: {report['generated_at']}",
        f"Lookback days: {report['filters']['lookback_days']} max latency hours: {report['filters']['max_latency_hours']}",
        f"Totals: sources={report['totals']['source_count']} flagged={report['totals']['flagged_count']}",
    ]
    if not report["flagged_sources"]:
        lines.append(report["empty_state"]["message"] or "No slow or incomplete imports found.")
        return "\n".join(lines)
    lines.extend(["", "Flagged sources:"])
    for item in report["flagged_sources"]:
        lines.append(
            f"- {item['source_id']} status={item['status']} latency_hours={item['latency_hours']} reason={item['reason']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = "knowledge_sources" if "knowledge_sources" in schema else "sources" if "sources" in schema else None
    if table is None:
        return []
    columns = schema[table]
    selected = [
        _col(columns, "id", "source_id", "url") + " AS source_id",
        _col(columns, "discovered_at", "curated_at", "created_at", default="NULL") + " AS discovered_at",
        _col(columns, "ingested_at", "imported_at", default="NULL") + " AS ingested_at",
        _col(columns, "embedded_at", "embeddings_ready_at", default="NULL") + " AS embedded_at",
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]


def _latency_item(row: dict[str, Any], now: datetime) -> dict[str, Any]:
    discovered_at = _parse_datetime(_first(row, "discovered_at", "curated_at", "created_at"))
    ingested_at = _parse_datetime(_first(row, "ingested_at", "imported_at"))
    embedded_at = _parse_datetime(_first(row, "embedded_at", "embeddings_ready_at"))
    completion_at = embedded_at or ingested_at
    status = _status(ingested_at, embedded_at)
    end = completion_at if status == "completed" else now
    latency_hours = None if discovered_at is None else int((end - discovered_at).total_seconds() // 3600)
    return {
        "source_id": _text(_first(row, "source_id", "id", "url")) or "unknown",
        "status": status,
        "discovered_at": discovered_at.isoformat() if discovered_at else None,
        "ingested_at": ingested_at.isoformat() if ingested_at else None,
        "embedded_at": embedded_at.isoformat() if embedded_at else None,
        "latency_hours": latency_hours,
        "reason": _reason(status, latency_hours),
    }


def _status(ingested_at: datetime | None, embedded_at: datetime | None) -> str:
    if ingested_at is None:
        return "pending_ingestion"
    if embedded_at is None:
        return "ingested_without_embeddings"
    return "completed"


def _reason(status: str, latency_hours: int | None) -> str:
    if status == "pending_ingestion":
        return "Source has not completed ingestion."
    if status == "ingested_without_embeddings":
        return "Source ingestion completed but embeddings are not ready."
    return f"Completed import took {latency_hours} hours."


def _percentiles(values: list[int]) -> dict[str, int | None]:
    if not values:
        return {"p50": None, "p90": None, "p95": None}
    sorted_values = sorted(values)
    return {name: _percentile(sorted_values, rank) for name, rank in (("p50", 0.5), ("p90", 0.9), ("p95", 0.95))}


def _percentile(sorted_values: list[int], rank: float) -> int:
    index = min(len(sorted_values) - 1, math.ceil((len(sorted_values) - 1) * rank))
    return sorted_values[index]


def _status_rank(status: str) -> int:
    return {"pending_ingestion": 0, "ingested_without_embeddings": 1, "completed": 2}.get(status, 3)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _col(columns: set[str], *names: str, default: str = "NULL") -> str:
    for name in names:
        if name in columns:
            return name
    return default


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] is not None:
            return row[key]
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _parse_datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
