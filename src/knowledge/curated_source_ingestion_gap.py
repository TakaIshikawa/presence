"""Report curated sources with missing or stale ingested content."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_STALE_DAYS = 14


def build_curated_source_ingestion_gap_report(
    db_or_conn: Any,
    *,
    expected_sources: Iterable[Mapping[str, Any]] | None = None,
    stale_days: int = DEFAULT_STALE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Compare configured curated sources against recently ingested knowledge rows."""
    if stale_days <= 0:
        raise ValueError("stale_days must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=stale_days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    sources = _configured_sources(conn, schema)
    for source in expected_sources or ():
        normalized = _source(source)
        sources.setdefault((normalized["source_type"], normalized["identifier"].lower()), normalized)
    knowledge_rows = _knowledge_rows(conn, schema)

    rows = []
    for source in sources.values():
        matches = [_row for _row in knowledge_rows if _matches(source, _row)]
        last_ingested_at = max((_parse_ts(row.get("ingested_at")) for row in matches), default=None)
        recent_count = sum(1 for row in matches if (_parse_ts(row.get("ingested_at")) or datetime.min.replace(tzinfo=timezone.utc)) >= cutoff)
        status = "missing" if not matches else "fresh" if recent_count else "stale"
        age_days = _age_days(generated_at, last_ingested_at)
        rows.append(
            {
                "source_type": source["source_type"],
                "identifier": source["identifier"],
                "name": source.get("name"),
                "last_ingested_at": last_ingested_at.isoformat() if last_ingested_at else None,
                "last_seen_age_days": age_days,
                "ingested_item_count": len(matches),
                "recent_item_count": recent_count,
                "status": status,
            }
        )
    rows.sort(key=lambda row: ({"missing": 0, "stale": 1, "fresh": 2}[row["status"]], -(row["last_seen_age_days"] or 10**9), row["source_type"], row["identifier"]))
    return {
        "artifact_type": "curated_source_ingestion_gap",
        "filters": {"stale_days": stale_days},
        "generated_at": generated_at.isoformat(),
        "rows": rows,
        "schema_gaps": {
            "missing_tables": [name for name in ("curated_sources", "knowledge") if name not in schema],
        },
        "summary": {
            "total_sources": len(rows),
            "fresh_count": sum(1 for row in rows if row["status"] == "fresh"),
            "stale_count": sum(1 for row in rows if row["status"] == "stale"),
            "missing_count": sum(1 for row in rows if row["status"] == "missing"),
        },
    }


def format_curated_source_ingestion_gap_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_curated_source_ingestion_gap_text(report: dict[str, Any]) -> str:
    """Render a table for terminal review."""
    summary = report["summary"]
    lines = [
        "Curated Source Ingestion Gap",
        f"Generated: {report['generated_at']}",
        (
            f"Summary: total={summary['total_sources']} fresh={summary['fresh_count']} "
            f"stale={summary['stale_count']} missing={summary['missing_count']} "
            f"stale_days={report['filters']['stale_days']}"
        ),
    ]
    if not report["rows"]:
        lines.extend(["", "No curated sources found."])
        return "\n".join(lines)
    lines.extend(["", "Sources:", "status   count  age_days  source_type       identifier"])
    for row in report["rows"]:
        age = "-" if row["last_seen_age_days"] is None else f"{row['last_seen_age_days']:.1f}"
        lines.append(
            f"{row['status']:<8} {row['ingested_item_count']:<6} {age:<9} "
            f"{row['source_type']:<17} {row['identifier']}"
        )
    return "\n".join(lines)


def _configured_sources(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> dict[tuple[str, str], dict[str, Any]]:
    columns = schema.get("curated_sources")
    if not columns or not {"source_type", "identifier"}.issubset(columns):
        return {}
    name_col = "name" if "name" in columns else "NULL"
    status_filter = "WHERE LOWER(COALESCE(status, 'active')) IN ('active', 'approved', 'candidate')" if "status" in columns else ""
    rows = conn.execute(
        f"""SELECT source_type, identifier, {name_col} AS name
            FROM curated_sources
            {status_filter}
            ORDER BY source_type, identifier"""
    ).fetchall()
    return {
        (source["source_type"], source["identifier"].lower()): source
        for source in (_source(dict(row)) for row in rows)
        if source["identifier"]
    }


def _knowledge_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    columns = schema.get("knowledge")
    if not columns:
        return []
    source_type = "source_type" if "source_type" in columns else "'unknown'"
    source_id = "source_id" if "source_id" in columns else "NULL"
    source_url = "source_url" if "source_url" in columns else "NULL"
    author = "author" if "author" in columns else "NULL"
    ingested = next((name for name in ("ingested_at", "created_at", "published_at", "timestamp") if name in columns), None)
    if not ingested:
        return []
    rows = conn.execute(
        f"""SELECT {source_type} AS source_type,
                   {source_id} AS source_id,
                   {source_url} AS source_url,
                   {author} AS author,
                   {ingested} AS ingested_at
            FROM knowledge"""
    ).fetchall()
    return [dict(row) for row in rows]


def _matches(source: Mapping[str, Any], row: Mapping[str, Any]) -> bool:
    identifier = str(source.get("identifier") or "").lower().lstrip("@")
    if not identifier:
        return False
    haystack = " ".join(
        str(row.get(key) or "").lower().lstrip("@")
        for key in ("source_type", "source_id", "source_url", "author")
    )
    return identifier in haystack


def _source(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_type": str(row.get("source_type") or "unknown").strip() or "unknown",
        "identifier": str(row.get("identifier") or row.get("handle") or row.get("source") or "").strip(),
        "name": _clean(row.get("name")),
    }


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _age_days(now: datetime, then: datetime | None) -> float | None:
    return None if then is None else round((now - then).total_seconds() / 86400, 2)


def _clean(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
