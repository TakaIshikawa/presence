"""Report newsletter segment source freshness."""

from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable, Mapping


DEFAULT_FRESH_HOURS = 72.0
DEFAULT_AGING_HOURS = 336.0


def build_newsletter_segment_source_freshness_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    fresh_hours: float = DEFAULT_FRESH_HOURS,
    aging_hours: float = DEFAULT_AGING_HOURS,
    now: datetime | None = None,
    schema_gaps: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return per-newsletter-segment source freshness rows."""
    if fresh_hours < 0:
        raise ValueError("fresh_hours must be non-negative")
    if aging_hours <= fresh_hours:
        raise ValueError("aging_hours must be greater than fresh_hours")
    generated_at = _utc(now or datetime.now(timezone.utc))
    report_rows = [
        _segment_row(row, now=generated_at, fresh_hours=fresh_hours, aging_hours=aging_hours)
        for row in rows
    ]
    report_rows.sort(key=_sort_key)
    return {
        "artifact_type": "newsletter_segment_source_freshness",
        "generated_at": generated_at.isoformat(),
        "filters": {"fresh_hours": fresh_hours, "aging_hours": aging_hours},
        "summary": {
            "segment_count": len(report_rows),
            "missing_source_count": sum(1 for row in report_rows if row["freshness_bucket"] == "missing_source"),
            "stale_segment_count": sum(1 for row in report_rows if row["freshness_bucket"] == "stale"),
            "aging_segment_count": sum(1 for row in report_rows if row["freshness_bucket"] == "aging"),
            "fresh_segment_count": sum(1 for row in report_rows if row["freshness_bucket"] == "fresh"),
        },
        "rows": report_rows,
        "schema_gaps": schema_gaps or {"missing_tables": [], "missing_columns": {}},
    }


def build_newsletter_segment_source_freshness_report_from_db(
    db_or_conn: Any,
    *,
    fresh_hours: float = DEFAULT_FRESH_HOURS,
    aging_hours: float = DEFAULT_AGING_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Load segment rows from SQLite and build the freshness report."""
    conn = db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn
    schema = _schema(conn)
    rows = _load_segment_rows(conn, schema)
    gaps = {"missing_tables": [], "missing_columns": {}}
    if "newsletter_sends" not in schema and "newsletter_segments" not in schema:
        gaps["missing_tables"] = ["newsletter_sends"]
    return build_newsletter_segment_source_freshness_report(
        rows,
        fresh_hours=fresh_hours,
        aging_hours=aging_hours,
        now=now,
        schema_gaps=gaps,
    )


def format_newsletter_segment_source_freshness_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_segment_source_freshness_table(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Segment Source Freshness",
        f"Generated: {report['generated_at']}",
        (
            "Totals: "
            f"segments={report['summary']['segment_count']} "
            f"missing={report['summary']['missing_source_count']} "
            f"stale={report['summary']['stale_segment_count']} "
            f"aging={report['summary']['aging_segment_count']} "
            f"fresh={report['summary']['fresh_segment_count']}"
        ),
    ]
    if not report["rows"]:
        lines.extend(["", "No newsletter segments found."])
        return "\n".join(lines)
    lines.extend(["", "newsletter  segment          sources  newest_h  oldest_h  bucket          risk"])
    for row in report["rows"]:
        lines.append(
            f"{(row['newsletter_id'] or '-')[:10]:<10} "
            f"{(row['segment_id'] or '-')[:16]:<16} "
            f"{row['source_count']:<7} "
            f"{_fmt(row['newest_source_age_hours']):<9} "
            f"{_fmt(row['oldest_source_age_hours']):<9} "
            f"{row['freshness_bucket']:<15} "
            f"{row['risk_label']}"
        )
    return "\n".join(lines)


def _segment_row(
    row: Mapping[str, Any],
    *,
    now: datetime,
    fresh_hours: float,
    aging_hours: float,
) -> dict[str, Any]:
    data = _row_dict(row)
    source_timestamps = _source_timestamps(data)
    ages = [round(max((now - stamp).total_seconds() / 3600.0, 0.0), 2) for stamp in source_timestamps]
    newest = min(ages) if ages else None
    oldest = max(ages) if ages else None
    bucket, risk = _bucket(source_count=len(source_timestamps), oldest_age=oldest, fresh_hours=fresh_hours, aging_hours=aging_hours)
    return {
        "newsletter_id": _text(_first(data, "newsletter_id", "issue_id", "newsletter_send_id", "send_id")),
        "segment_id": _text(_first(data, "segment_id", "id", "section_id", "name", "title")),
        "segment_title": _text(_first(data, "segment_title", "title", "name")),
        "source_count": len(source_timestamps),
        "newest_source_age_hours": newest,
        "oldest_source_age_hours": oldest,
        "freshness_bucket": bucket,
        "risk_label": risk,
        "source_ids": _source_ids(data),
    }


def _load_segment_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    if "newsletter_segments" in schema:
        return [dict(row) for row in conn.execute("SELECT * FROM newsletter_segments").fetchall()]
    if "newsletter_sends" not in schema:
        return []
    rows: list[dict[str, Any]] = []
    columns = schema["newsletter_sends"]
    selected = [column for column in ("id", "issue_id", "metadata", "source_content_ids", "sent_at") if column in columns]
    if not selected:
        return []
    for send in conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_sends ORDER BY id ASC").fetchall():
        data = dict(send)
        metadata = _json_obj(data.get("metadata"))
        segments = metadata.get("segments") or metadata.get("sections") or []
        for index, segment in enumerate(segments, start=1):
            if not isinstance(segment, dict):
                continue
            merged = dict(segment)
            merged.setdefault("newsletter_id", data.get("issue_id") or data.get("id"))
            merged.setdefault("newsletter_send_id", data.get("id"))
            merged.setdefault("segment_id", segment.get("id") or segment.get("key") or f"segment-{index}")
            rows.append(merged)
        if not segments and data.get("source_content_ids"):
            rows.append(
                {
                    "newsletter_id": data.get("issue_id") or data.get("id"),
                    "newsletter_send_id": data.get("id"),
                    "segment_id": "issue",
                    "source_content_ids": data.get("source_content_ids"),
                }
            )
    return rows


def _source_timestamps(row: Mapping[str, Any]) -> list[datetime]:
    stamps: list[datetime] = []
    for source in _sources(row):
        if isinstance(source, dict):
            parsed = _parse_dt(_first(source, "published_at", "created_at", "source_timestamp", "timestamp", "date"))
            if parsed is not None:
                stamps.append(parsed)
        else:
            parsed = _parse_dt(source)
            if parsed is not None:
                stamps.append(parsed)
    return stamps


def _source_ids(row: Mapping[str, Any]) -> list[str]:
    ids: list[str] = []
    for source in _sources(row):
        if isinstance(source, dict):
            value = _first(source, "source_id", "id", "url", "canonical_id")
            if value not in (None, ""):
                ids.append(str(value))
        elif source not in (None, "") and _parse_dt(source) is None:
            ids.append(str(source))
    return sorted(set(ids))


def _sources(row: Mapping[str, Any]) -> list[Any]:
    for key in ("sources", "linked_sources", "source_payloads"):
        parsed = _json(row.get(key))
        if isinstance(parsed, list):
            return parsed
    for key in ("source_timestamps", "source_dates"):
        parsed = _json(row.get(key))
        if isinstance(parsed, list):
            return [{"published_at": item} for item in parsed]
    parsed_ids = _json(row.get("source_content_ids"))
    return parsed_ids if isinstance(parsed_ids, list) else []


def _bucket(
    *,
    source_count: int,
    oldest_age: float | None,
    fresh_hours: float,
    aging_hours: float,
) -> tuple[str, str]:
    if source_count == 0:
        return "missing_source", "missing_source"
    if oldest_age is None:
        return "missing_timestamp", "unknown_source_age"
    if oldest_age > aging_hours:
        return "stale", "stale_sources"
    if oldest_age > fresh_hours:
        return "aging", "aging_sources"
    return "fresh", "fresh_sources"


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _row_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return dict(row)


def _json_obj(value: Any) -> dict[str, Any]:
    parsed = _json(value)
    return parsed if isinstance(parsed, dict) else {}


def _json(value: Any) -> Any:
    if value in (None, ""):
        return None
    if isinstance(value, (list, dict)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return None


def _first(row: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value):.2f}".rstrip("0").rstrip(".")


def _sort_key(row: dict[str, Any]) -> tuple[Any, ...]:
    bucket_rank = {"missing_source": 0, "missing_timestamp": 1, "stale": 2, "aging": 3, "fresh": 4}
    return (
        bucket_rank.get(row["freshness_bucket"], 9),
        -(row["oldest_source_age_hours"] or 0),
        row["newsletter_id"] or "",
        row["segment_id"] or "",
    )
