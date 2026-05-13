"""Detect subscriber growth and churn inflection points from local metrics."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_GROWTH_SPIKE_DELTA = 50
DEFAULT_GROWTH_DROP_DELTA = -25
DEFAULT_CHURN_SPIKE_DELTA = 0.02
DEFAULT_LIMIT = 100


def build_newsletter_subscriber_growth_inflection_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    growth_spike_delta: int = DEFAULT_GROWTH_SPIKE_DELTA,
    growth_drop_delta: int = DEFAULT_GROWTH_DROP_DELTA,
    churn_spike_delta: float = DEFAULT_CHURN_SPIKE_DELTA,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only period-over-period subscriber growth report."""
    if days <= 0 or limit <= 0:
        raise ValueError("days and limit must be positive")
    generated_at = _as_utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    filters = {
        "days": days,
        "growth_spike_delta": growth_spike_delta,
        "growth_drop_delta": growth_drop_delta,
        "churn_spike_delta": churn_spike_delta,
        "limit": limit,
    }
    missing_tables = [] if "newsletter_subscriber_metrics" in schema else ["newsletter_subscriber_metrics"]
    required = {"id", "subscriber_count", "active_subscriber_count", "churn_rate", "fetched_at"}
    missing_columns = {
        "newsletter_subscriber_metrics": sorted(required - schema.get("newsletter_subscriber_metrics", set()))
    } if "newsletter_subscriber_metrics" in schema and required - schema.get("newsletter_subscriber_metrics", set()) else {}
    if missing_tables or missing_columns:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    raw_rows = _load_rows(conn, cutoff, generated_at)
    metric_points: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    previous: dict[str, Any] | None = None
    for raw in raw_rows:
        point = _metric_point(dict(raw), previous)
        metric_points.append(point)
        findings.extend(_findings(point, growth_spike_delta, growth_drop_delta, churn_spike_delta))
        previous = point

    findings.sort(key=lambda item: (item["fetched_at"], item["finding_type"], item["metric_id"]))
    counts = dict(sorted(Counter(item["finding_type"] for item in findings).items()))
    return {
        "artifact_type": "newsletter_subscriber_growth_inflection",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "metric_point_count": len(metric_points),
            "finding_count": len(findings),
            "finding_counts": counts,
        },
        "metric_points": metric_points[:limit],
        "findings": findings[:limit],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_newsletter_subscriber_growth_inflection_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subscriber_growth_inflection_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Newsletter Subscriber Growth Inflection",
        f"Generated: {report['generated_at']}",
        f"Totals: points={totals['metric_point_count']} findings={totals['finding_count']}",
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, cutoff: datetime, now: datetime) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT id, subscriber_count, active_subscriber_count, churn_rate, fetched_at
           FROM newsletter_subscriber_metrics
           WHERE datetime(fetched_at) >= datetime(?) AND datetime(fetched_at) <= datetime(?)
           ORDER BY datetime(fetched_at) ASC, id ASC""",
        (cutoff.isoformat(), now.isoformat()),
    ).fetchall()


def _metric_point(row: dict[str, Any], previous: dict[str, Any] | None) -> dict[str, Any]:
    subscriber_count = _int_or_none(row.get("subscriber_count"))
    active_count = _int_or_none(row.get("active_subscriber_count"))
    churn_rate = _float_or_none(row.get("churn_rate"))
    return {
        "metric_id": int(row["id"]),
        "fetched_at": row.get("fetched_at"),
        "subscriber_count": subscriber_count,
        "active_subscriber_count": active_count,
        "churn_rate": churn_rate,
        "subscriber_delta": _delta(subscriber_count, previous.get("subscriber_count") if previous else None),
        "active_delta": _delta(active_count, previous.get("active_subscriber_count") if previous else None),
        "churn_rate_delta": _delta(churn_rate, previous.get("churn_rate") if previous else None),
    }


def _findings(point: dict[str, Any], spike: int, drop: int, churn_spike: float) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    delta = point["subscriber_delta"]
    if delta is not None and delta >= spike:
        findings.append(_finding("growth_spike", point))
    if delta is not None and delta <= drop:
        findings.append(_finding("growth_drop", point))
    churn_delta = point["churn_rate_delta"]
    if churn_delta is not None and churn_delta >= churn_spike:
        findings.append(_finding("churn_spike", point))
    if point["active_subscriber_count"] is None:
        findings.append(_finding("missing_active_count", point))
    return findings


def _finding(kind: str, point: dict[str, Any]) -> dict[str, Any]:
    return {
        "finding_type": kind,
        "metric_id": point["metric_id"],
        "fetched_at": point["fetched_at"],
        "subscriber_count": point["subscriber_count"],
        "active_subscriber_count": point["active_subscriber_count"],
        "churn_rate": point["churn_rate"],
        "subscriber_delta": point["subscriber_delta"],
        "active_delta": point["active_delta"],
        "churn_rate_delta": point["churn_rate_delta"],
    }


def _empty_report(generated_at: datetime, filters: dict[str, Any], missing_tables: list[str], missing_columns: dict[str, list[str]]) -> dict[str, Any]:
    return {
        "artifact_type": "newsletter_subscriber_growth_inflection",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {"metric_point_count": 0, "finding_count": 0, "finding_counts": {}},
        "metric_points": [],
        "findings": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _delta(value: Any, previous: Any) -> Any:
    if value is None or previous is None:
        return None
    return round(value - previous, 6) if isinstance(value, float) or isinstance(previous, float) else value - previous


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {
        str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    }


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
