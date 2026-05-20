"""Report curated sources with no recent ingested knowledge."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_STALE_DAYS = 30
DEFAULT_LIMIT = 100


def build_curated_source_ingestion_silence_report(
    curated_sources: list[dict[str, Any]],
    knowledge_rows: list[dict[str, Any]],
    *,
    stale_days: int = DEFAULT_STALE_DAYS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_schema: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    stale_before = generated_at - timedelta(days=stale_days)
    knowledge_index = [_knowledge_match(row) for row in knowledge_rows]
    silent = []
    active_count = 0
    for source in curated_sources:
        source_match = _source_match(source)
        matches = [
            item
            for item in knowledge_index
            if _matches(source_match, item)
        ]
        latest = max((_parse_dt(item["ingested_at"]) for item in matches), default=None)
        days_since = _age_days(latest, generated_at)
        reasons: list[str] = []
        if not matches:
            reasons.append("no_matching_knowledge")
        elif latest is None:
            reasons.append("missing_ingestion_timestamp")
        elif latest < stale_before:
            reasons.append("stale_ingestion")

        item = {
            "source_id": _first(source, "source_id", "id"),
            "source": _clean(_first(source, "source", "source_name", "name", "domain")) or source_match["url"] or "unknown",
            "source_url": source_match["url"] or None,
            "author": source_match["author"] or None,
            "last_ingested_at": _iso(latest),
            "days_since_last_ingestion": days_since,
            "matched_knowledge_count": len(matches),
            "reason_codes": reasons,
        }
        if reasons:
            silent.append(item)
        else:
            active_count += 1

    silent.sort(key=lambda item: (-(item["days_since_last_ingestion"] or 10**9), item["source"]))
    reason_counts = Counter(reason for item in silent for reason in item["reason_codes"])
    return {
        "artifact_type": "curated_source_ingestion_silence",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "stale_days": stale_days,
            "stale_before": stale_before.isoformat(),
            "limit": limit,
        },
        "summary": {
            "curated_sources_count": len(curated_sources),
            "knowledge_rows_count": len(knowledge_rows),
            "silent_sources_count": len(silent),
            "active_sources_count": active_count,
            "reason_counts": dict(sorted(reason_counts.items())),
        },
        "silent_sources": silent[:limit],
        "active_sources_count": active_count,
        "missing_schema": missing_schema or {"missing_tables": [], "missing_columns": {}},
    }


def build_curated_source_ingestion_silence_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_schema = _missing_schema(schema)
    if "curated_sources" not in schema or "knowledge" not in schema:
        return build_curated_source_ingestion_silence_report([], [], missing_schema=missing_schema, **kwargs)
    return build_curated_source_ingestion_silence_report(
        _load_rows(conn, "curated_sources", schema["curated_sources"], "cs"),
        _load_rows(conn, "knowledge", schema["knowledge"], "k"),
        missing_schema=missing_schema,
        **kwargs,
    )


def format_curated_source_ingestion_silence_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_curated_source_ingestion_silence_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Curated Source Ingestion Silence",
        f"Generated: {report['generated_at']}",
        f"Stale after: {report['thresholds']['stale_days']} days",
        f"Totals: sources={summary['curated_sources_count']} silent={summary['silent_sources_count']} active={summary['active_sources_count']}",
    ]
    missing = report["missing_schema"]
    if missing["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(missing["missing_tables"]))
    if report["silent_sources"]:
        lines.extend(["", "Silent sources:"])
        for item in report["silent_sources"]:
            lines.append(
                f"  - source_id={item['source_id'] or '-'} source={item['source']} "
                f"days={item['days_since_last_ingestion'] if item['days_since_last_ingestion'] is not None else '-'} "
                f"reasons={','.join(item['reason_codes'])}"
            )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str], alias: str) -> list[dict[str, Any]]:
    wanted = [
        "id", "source_id", "source", "source_name", "name", "domain", "source_url", "url", "feed_url",
        "author", "author_name", "created_at", "updated_at", "ingested_at", "fetched_at", "published_at",
    ]
    select = [
        f"{alias}.{column} AS {column}"
        for column in wanted
        if column in columns
    ]
    if "rowid" not in columns:
        select.append(f"{alias}.rowid AS _rowid")
    order = "id" if "id" in columns else "rowid"
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(select)} FROM {table} {alias} ORDER BY {alias}.{order}").fetchall()]


def _missing_schema(schema: dict[str, set[str]]) -> dict[str, Any]:
    missing_tables = [table for table in ("curated_sources", "knowledge") if table not in schema]
    return {"missing_tables": missing_tables, "missing_columns": {}}


def _source_match(row: dict[str, Any]) -> dict[str, str]:
    return {
        "id": _clean(_first(row, "id", "source_id")),
        "source": _norm(_first(row, "source", "source_name", "name", "domain")),
        "url": _norm_url(_first(row, "source_url", "url", "feed_url")),
        "author": _norm(_first(row, "author", "author_name")),
    }


def _knowledge_match(row: dict[str, Any]) -> dict[str, Any]:
    match = _source_match(row)
    match["source_id"] = _clean(_first(row, "source_id", "id"))
    match["ingested_at"] = _first(row, "ingested_at", "fetched_at", "updated_at", "created_at", "published_at")
    return match


def _matches(source: dict[str, str], knowledge: dict[str, Any]) -> bool:
    if source["id"] and source["id"] == knowledge.get("source_id"):
        return True
    for key in ("url", "source", "author"):
        if source.get(key) and source.get(key) == knowledge.get(key):
            return True
    return False


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")]
    return {table: {column[1] for column in conn.execute(f"PRAGMA table_info({table})")} for table in tables}


def _parse_dt(value: Any) -> datetime | None:
    text = _clean(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age_days(value: datetime | None, now: datetime) -> int | None:
    return None if value is None else int((_utc(now) - _utc(value)).total_seconds() // 86400)


def _iso(value: datetime | None) -> str | None:
    return None if value is None else _utc(value).isoformat()


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _norm(value: Any) -> str:
    return " ".join(_clean(value).lower().split())


def _norm_url(value: Any) -> str:
    text = _clean(value).lower().removesuffix("/")
    if text.startswith("http://"):
        text = "https://" + text[7:]
    return text
