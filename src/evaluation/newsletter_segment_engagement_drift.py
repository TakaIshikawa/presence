"""Compare newsletter engagement by subscriber segment."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_BASELINE_DAYS = 90
DEFAULT_MIN_DELTA_PCT = 10.0
TABLES = ("newsletter_segment_metrics", "newsletter_metrics", "newsletter_issue_metrics")


@dataclass(frozen=True)
class NewsletterSegmentEngagementDrift:
    segment: str
    recent_metrics: dict[str, float]
    baseline_metrics: dict[str, float]
    drift_type: str
    severity: str
    sample_issue_ids: tuple[str, ...]
    recommended_action: str

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["sample_issue_ids"] = list(self.sample_issue_ids)
        return data


@dataclass(frozen=True)
class NewsletterSegmentEngagementDriftReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    drifts: tuple[NewsletterSegmentEngagementDrift, ...]
    empty_state: dict[str, Any]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_segment_engagement_drift",
            "drifts": [drift.to_dict() for drift in self.drifts],
            "empty_state": dict(self.empty_state),
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def build_newsletter_segment_engagement_drift_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    segment: str | None = None,
    min_delta_pct: float = DEFAULT_MIN_DELTA_PCT,
    now: datetime | None = None,
) -> NewsletterSegmentEngagementDriftReport:
    if days <= 0 or baseline_days <= 0:
        raise ValueError("days and baseline_days must be positive")
    if min_delta_pct < 0:
        raise ValueError("min_delta_pct must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    recent_start = generated_at - timedelta(days=days)
    baseline_start = recent_start - timedelta(days=baseline_days)
    filters = {"days": days, "baseline_days": baseline_days, "segment": segment, "min_delta_pct": min_delta_pct}
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    table = next((name for name in TABLES if name in schema), None)
    if table is None:
        return _report(generated_at, filters, (), 0, TABLES)
    rows = _load_rows(conn, table, schema[table], baseline_start.isoformat(), generated_at.isoformat(), segment)
    by_segment: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for row in rows:
        ts = _parse_ts(row.get("sent_at"))
        if ts is None:
            continue
        window = "recent" if ts >= recent_start else "baseline"
        by_segment.setdefault(row["segment"], {"recent": [], "baseline": []})[window].append(row)
    drifts: list[NewsletterSegmentEngagementDrift] = []
    for name, bucket in by_segment.items():
        if not bucket["recent"] or not bucket["baseline"]:
            continue
        recent = _metrics(bucket["recent"])
        baseline = _metrics(bucket["baseline"])
        candidates = [
            ("click_decline", _decline_pct(recent["click_rate"], baseline["click_rate"])),
            ("open_decline", _decline_pct(recent["open_rate"], baseline["open_rate"])),
            ("unsubscribe_increase", _increase_pct(recent["unsubscribe_rate"], baseline["unsubscribe_rate"])),
        ]
        for drift_type, delta in candidates:
            if delta >= min_delta_pct:
                drifts.append(
                    NewsletterSegmentEngagementDrift(
                        segment=name,
                        recent_metrics=recent,
                        baseline_metrics=baseline,
                        drift_type=drift_type,
                        severity="high" if delta >= min_delta_pct * 2 else "medium",
                        sample_issue_ids=tuple(str(row["issue_id"]) for row in bucket["recent"][:5] if row.get("issue_id") is not None),
                        recommended_action="Review segment fit, subject line promise, links, and suppression rules before the next issue.",
                    )
                )
                break
    drifts.sort(key=lambda item: (item.segment, item.drift_type))
    return _report(generated_at, filters, tuple(drifts), len(rows), ())


def format_newsletter_segment_engagement_drift_json(report: NewsletterSegmentEngagementDriftReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_segment_engagement_drift_text(report: NewsletterSegmentEngagementDriftReport) -> str:
    lines = [
        "Newsletter Segment Engagement Drift",
        f"Recent={report.filters['days']} days; baseline={report.filters['baseline_days']} days; segment={report.filters.get('segment') or 'all'}",
        f"Rows scanned={report.totals['rows_scanned']}; drifts={report.totals['drift_count']}",
        "",
    ]
    if not report.drifts:
        lines.append(report.empty_state["message"])
        return "\n".join(lines)
    for drift in report.drifts:
        lines.append(f"- segment={drift.segment} type={drift.drift_type} severity={drift.severity} issues={','.join(drift.sample_issue_ids) or '-'}")
        lines.append(f"  recent={drift.recent_metrics} baseline={drift.baseline_metrics} action={drift.recommended_action}")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, table: str, columns: set[str], start: str, end: str, segment: str | None) -> list[dict[str, Any]]:
    segment_col = _first(columns, ("segment", "subscriber_segment", "tag"))
    issue_col = _first(columns, ("issue_id", "newsletter_issue_id", "id"))
    ts_col = _first(columns, ("sent_at", "published_at", "created_at"))
    open_col = _first(columns, ("open_rate", "opens_rate"))
    click_col = _first(columns, ("click_rate", "clicks_rate"))
    unsub_col = _first(columns, ("unsubscribe_rate", "unsub_rate"))
    if not segment_col or not ts_col:
        return []
    where = [f"{ts_col} >= ?", f"{ts_col} < ?"]
    params: list[Any] = [start, end]
    if segment:
        where.append(f"{segment_col} = ?")
        params.append(segment)
    sql = f"""SELECT {segment_col} AS segment,
                     {issue_col if issue_col else 'NULL'} AS issue_id,
                     {ts_col} AS sent_at,
                     {open_col if open_col else '0'} AS open_rate,
                     {click_col if click_col else '0'} AS click_rate,
                     {unsub_col if unsub_col else '0'} AS unsubscribe_rate
              FROM {table}
              WHERE {' AND '.join(where)}"""
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _report(generated_at: datetime, filters: dict[str, Any], drifts: tuple[NewsletterSegmentEngagementDrift, ...], scanned: int, missing: tuple[str, ...]) -> NewsletterSegmentEngagementDriftReport:
    return NewsletterSegmentEngagementDriftReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        totals={"rows_scanned": scanned, "drift_count": len(drifts)},
        drifts=drifts,
        empty_state={"is_empty": not drifts, "message": "No newsletter segment engagement drift found." if not missing else "Newsletter segment metric schema is unavailable."},
        missing_tables=missing,
    )


def _metrics(rows: list[dict[str, Any]]) -> dict[str, float]:
    return {
        "open_rate": round(sum(float(row.get("open_rate") or 0) for row in rows) / len(rows), 4),
        "click_rate": round(sum(float(row.get("click_rate") or 0) for row in rows) / len(rows), 4),
        "unsubscribe_rate": round(sum(float(row.get("unsubscribe_rate") or 0) for row in rows) / len(rows), 4),
        "issue_count": float(len(rows)),
    }


def _decline_pct(recent: float, baseline: float) -> float:
    return 0.0 if baseline <= 0 else max(0.0, ((baseline - recent) / baseline) * 100)


def _increase_pct(recent: float, baseline: float) -> float:
    return 100.0 if baseline <= 0 and recent > 0 else (0.0 if baseline <= 0 else max(0.0, ((recent - baseline) / baseline) * 100))


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _first(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
