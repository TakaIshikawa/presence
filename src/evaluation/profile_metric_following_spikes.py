"""Report profile metric following spikes and ratio collapses."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from ._report_helpers import base_report, clean, col, connection, json_format, missing, parse_time, schema, text_format, to_int, utc


ARTIFACT_TYPE = "profile_metric_following_spikes"
DEFAULT_FOLLOWING_DELTA_THRESHOLD = 100
DEFAULT_RATIO_THRESHOLD = 0.5
DEFAULT_LIMIT = 100
REASONS = ("following_spike", "following_drop", "ratio_collapse", "duplicate_fetched_at", "tweet_count_decrease")


def _ratio(row: dict[str, Any]) -> float | None:
    followers = to_int(row.get("follower_count"))
    following = to_int(row.get("following_count"))
    if followers is None or following in (None, 0):
        return None
    return followers / following


def build_profile_metric_following_spikes_report(rows: list[dict[str, Any]], *, following_delta_threshold: int = DEFAULT_FOLLOWING_DELTA_THRESHOLD, ratio_threshold: float = DEFAULT_RATIO_THRESHOLD, limit: int = DEFAULT_LIMIT, now: datetime | None = None, missing_tables: list[str] | None = None, missing_columns: dict[str, list[str]] | None = None) -> dict[str, Any]:
    if following_delta_threshold < 0:
        raise ValueError("following_delta_threshold must be non-negative")
    if ratio_threshold < 0:
        raise ValueError("ratio_threshold must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = utc(now)
    findings: list[dict[str, Any]] = []
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(clean(row.get("platform"), "x"), []).append(row)
    for platform, items in groups.items():
        items.sort(key=lambda row: (parse_time(row.get("fetched_at")) or generated_at, to_int(row.get("id")) or 0))
        seen: set[str] = set()
        prev: dict[str, Any] | None = None
        for row in items:
            fetched = clean(row.get("fetched_at"))
            if fetched in seen:
                findings.append({"reason": "duplicate_fetched_at", "id": row.get("id"), "platform": platform, "detail": "duplicate fetched_at for platform"})
            seen.add(fetched)
            if prev is not None:
                delta = (to_int(row.get("following_count")) or 0) - (to_int(prev.get("following_count")) or 0)
                base = {"id": row.get("id"), "previous_id": prev.get("id"), "platform": platform}
                if delta >= following_delta_threshold:
                    findings.append({**base, "reason": "following_spike", "following_delta": delta, "detail": "following_count increased sharply"})
                if delta <= -following_delta_threshold:
                    findings.append({**base, "reason": "following_drop", "following_delta": delta, "detail": "following_count decreased sharply"})
                prev_ratio, current_ratio = _ratio(prev), _ratio(row)
                if prev_ratio is not None and current_ratio is not None and current_ratio <= ratio_threshold and current_ratio < prev_ratio:
                    findings.append({**base, "reason": "ratio_collapse", "previous_ratio": round(prev_ratio, 4), "current_ratio": round(current_ratio, 4), "detail": "follower/following ratio collapsed below threshold"})
                if (to_int(row.get("tweet_count")) or 0) < (to_int(prev.get("tweet_count")) or 0):
                    findings.append({**base, "reason": "tweet_count_decrease", "detail": "tweet_count decreased between snapshots"})
            prev = row
    findings.sort(key=lambda f: (REASONS.index(f["reason"]), str(f.get("platform")), str(f.get("id"))))
    return base_report(artifact_type=ARTIFACT_TYPE, generated_at=generated_at, filters={"following_delta_threshold": following_delta_threshold, "ratio_threshold": ratio_threshold, "limit": limit}, rows_count=len(rows), findings=findings, reasons=REASONS, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def build_profile_metric_following_spikes_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    required = {"profile_metrics": {"following_count", "follower_count", "tweet_count", "fetched_at"}}
    missing_tables, missing_columns = missing(db_schema, required)
    if missing_tables or missing_columns:
        return build_profile_metric_following_spikes_report([], missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)
    pm = db_schema["profile_metrics"]
    select = ["rowid AS id", col(pm, "platform", "profile_metrics", "'x'") + " AS platform", "follower_count", "following_count", "tweet_count", "fetched_at"]
    rows = [dict(r) for r in conn.execute(f"SELECT {', '.join(select)} FROM profile_metrics ORDER BY platform, fetched_at, rowid").fetchall()]
    return build_profile_metric_following_spikes_report(rows, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_profile_metric_following_spikes_json(report: dict[str, Any]) -> str:
    return json_format(report)


def format_profile_metric_following_spikes_text(report: dict[str, Any]) -> str:
    return text_format("Profile Metric Following Spikes", report)
