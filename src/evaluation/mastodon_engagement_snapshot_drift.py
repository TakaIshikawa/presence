"""Report drift in Mastodon engagement snapshots."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, parse_time, schema, text_format, to_float, to_int, valid_json_object, utc


ARTIFACT_TYPE = "mastodon_engagement_snapshot_drift"
DEFAULT_MAX_GAP_HOURS = 48
DEFAULT_LIMIT = 100
REASONS = ("count_drop", "duplicate_fetched_at", "invalid_raw_metrics", "stale_gap")


def build_mastodon_engagement_snapshot_drift_report(rows: list[dict[str, Any]], *, max_gap_hours: int = DEFAULT_MAX_GAP_HOURS, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if max_gap_hours < 0:
        raise ValueError("max_gap_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    groups: dict[tuple[Any, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (row.get("content_id"), clean(row.get("post_id") or row.get("mastodon_url") or row.get("id")))
        groups.setdefault(key, []).append(row)
        if not valid_json_object(row.get("raw_metrics")):
            findings.append({"reason": "invalid_raw_metrics", "id": row.get("id"), "content_id": row.get("content_id"), "detail": "raw_metrics is missing or malformed"})
    for (_content_id, _identity), items in groups.items():
        items.sort(key=lambda r: (parse_time(r.get("fetched_at")) or generated_at, to_int(r.get("id")) or 0))
        seen: set[str] = set()
        prev: dict[str, Any] | None = None
        for row in items:
            fetched = clean(row.get("fetched_at"))
            if fetched in seen:
                findings.append({"reason": "duplicate_fetched_at", "id": row.get("id"), "content_id": row.get("content_id"), "fetched_at": fetched, "detail": "duplicate fetched_at for platform identity"})
            seen.add(fetched)
            if prev is not None:
                current_total = sum(to_int(row.get(k)) or 0 for k in ("favourite_count", "boost_count", "reply_count"))
                previous_total = sum(to_int(prev.get(k)) or 0 for k in ("favourite_count", "boost_count", "reply_count"))
                if current_total < previous_total:
                    findings.append({"reason": "count_drop", "id": row.get("id"), "content_id": row.get("content_id"), "previous_id": prev.get("id"), "detail": f"engagement count dropped from {previous_total} to {current_total}"})
                prev_at = parse_time(prev.get("fetched_at"))
                current_at = parse_time(row.get("fetched_at"))
                if max_gap_hours and prev_at and current_at and (current_at - prev_at).total_seconds() > max_gap_hours * 3600:
                    findings.append({"reason": "stale_gap", "id": row.get("id"), "content_id": row.get("content_id"), "previous_id": prev.get("id"), "gap_hours": round((current_at - prev_at).total_seconds() / 3600, 2), "detail": "snapshot gap exceeds max age"})
            prev = row
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("content_id")), str(f.get("id"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"max_gap_hours": max_gap_hours, "limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_mastodon_engagement_snapshot_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"mastodon_engagement": {"content_id", "fetched_at"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_mastodon_engagement_snapshot_drift_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    cols = db_schema["mastodon_engagement"]
    select = ["rowid AS id", "content_id", col(cols, "mastodon_url", "mastodon_engagement") + " AS mastodon_url", col(cols, "post_id", "mastodon_engagement") + " AS post_id", col(cols, "favourite_count", "mastodon_engagement", "0") + " AS favourite_count", col(cols, "boost_count", "mastodon_engagement", "0") + " AS boost_count", col(cols, "reply_count", "mastodon_engagement", "0") + " AS reply_count", col(cols, "raw_metrics", "mastodon_engagement") + " AS raw_metrics", "fetched_at"]
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM mastodon_engagement ORDER BY content_id, fetched_at, rowid").fetchall()]
    return build_mastodon_engagement_snapshot_drift_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_mastodon_engagement_snapshot_drift_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_mastodon_engagement_snapshot_drift_text(report: dict[str, Any]) -> str:
    return text_format("Mastodon Engagement Snapshot Drift", report)
