"""Alert on newsletter subscriber churn and unsubscribe spikes."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_CHURN_THRESHOLD = 0.05
DEFAULT_UNSUBSCRIBE_THRESHOLD = 10
DEFAULT_NET_LOSS_THRESHOLD = -25
DEFAULT_LIMIT = 25


def build_newsletter_subscriber_churn_alerts_report(
    rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    churn_threshold: float = DEFAULT_CHURN_THRESHOLD,
    unsubscribe_threshold: int = DEFAULT_UNSUBSCRIBE_THRESHOLD,
    net_loss_threshold: int = DEFAULT_NET_LOSS_THRESHOLD,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if churn_threshold < 0:
        raise ValueError("churn_threshold must be non-negative")
    if unsubscribe_threshold < 0:
        raise ValueError("unsubscribe_threshold must be non-negative")
    if net_loss_threshold > 0:
        raise ValueError("net_loss_threshold must be zero or negative")
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    lookback_start = generated_at - timedelta(days=lookback_days)
    normalized = [_normalize_row(row) for row in rows]
    alerts = [
        _alert(row, churn_threshold, unsubscribe_threshold, net_loss_threshold)
        for row in normalized
        if _breaches(row, churn_threshold, unsubscribe_threshold, net_loss_threshold)
    ]
    alerts.sort(key=lambda item: (_severity_rank(item["severity"]), item["fetched_at"] or "", item["metric_id"]), reverse=True)
    return {
        "artifact_type": "newsletter_subscriber_churn_alerts",
        "generated_at": generated_at.isoformat(),
        "thresholds": {
            "lookback_days": lookback_days,
            "lookback_start": lookback_start.isoformat(),
            "lookback_end": generated_at.isoformat(),
            "churn_threshold": churn_threshold,
            "unsubscribe_threshold": unsubscribe_threshold,
            "net_loss_threshold": net_loss_threshold,
            "limit": limit,
        },
        "missing_tables": list(missing_tables),
        "summary": {
            "rows_scanned": len(rows),
            "alert_count": len(alerts),
            "shown_alert_count": min(len(alerts), limit),
        },
        "alert_items": alerts[:limit],
        "severity_summary": dict(sorted(Counter(alert["severity"] for alert in alerts).items())),
    }


def build_newsletter_subscriber_churn_alerts_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = tuple(table for table in ("newsletter_subscriber_metrics",) if table not in schema)
    if missing_tables:
        return build_newsletter_subscriber_churn_alerts_report([], missing_tables=missing_tables, **kwargs)
    now = _utc(kwargs.get("now") or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=kwargs.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
    rows = _load_rows(conn, schema["newsletter_subscriber_metrics"], cutoff=cutoff, window_end=now)
    return build_newsletter_subscriber_churn_alerts_report(
        rows,
        missing_tables=missing_tables,
        **{**kwargs, "now": now},
    )


def format_newsletter_subscriber_churn_alerts_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subscriber_churn_alerts_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Newsletter Subscriber Churn Alerts",
        f"Generated: {report['generated_at']}",
        f"Window: days={thresholds['lookback_days']} start={thresholds['lookback_start']} end={thresholds['lookback_end']}",
        (
            f"Thresholds: churn={thresholds['churn_threshold']:.3f} "
            f"unsubscribes={thresholds['unsubscribe_threshold']} net_loss={thresholds['net_loss_threshold']} "
            f"limit={thresholds['limit']}"
        ),
        f"Totals: rows={summary['rows_scanned']} alerts={summary['alert_count']} shown={summary['shown_alert_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["alert_items"]:
        lines.extend(["", "Alert items:"])
        for item in report["alert_items"]:
            lines.append(
                f"- metric_id={item['metric_id']} severity={item['severity']} churn={item['churn_rate']:.3f} "
                f"unsubscribes={item['unsubscribes']} net_change={item['net_subscriber_change']} breaches={','.join(item['breaches'])}"
            )
    elif not report["missing_tables"]:
        lines.append("No newsletter subscriber churn alerts found.")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str], *, cutoff: datetime, window_end: datetime) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id", default="rowid"),
        _column_expr(columns, "fetched_at", default="NULL"),
        _column_expr(columns, "churn_rate", default="0"),
        _column_expr(columns, "unsubscribes", default="0"),
        _column_expr(columns, "net_subscriber_change", default="0"),
        _column_expr(columns, "subscriber_count", default="NULL"),
    ]
    filters = []
    params: list[Any] = []
    if "fetched_at" in columns:
        filters.append("datetime(fetched_at) >= datetime(?) AND datetime(fetched_at) <= datetime(?)")
        params.extend([_sqlite_ts(cutoff), _sqlite_ts(window_end)])
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_subscriber_metrics {where} ORDER BY datetime(fetched_at) DESC, id DESC", params)]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "metric_id": _int(row.get("id")),
        "fetched_at": row.get("fetched_at"),
        "churn_rate": _float(row.get("churn_rate")),
        "unsubscribes": _int(row.get("unsubscribes")),
        "net_subscriber_change": _int(row.get("net_subscriber_change")),
        "subscriber_count": row.get("subscriber_count"),
    }


def _breaches(row: dict[str, Any], churn_threshold: float, unsubscribe_threshold: int, net_loss_threshold: int) -> list[str]:
    breaches = []
    if row["churn_rate"] >= churn_threshold:
        breaches.append("churn_rate")
    if row["unsubscribes"] >= unsubscribe_threshold:
        breaches.append("unsubscribes")
    if row["net_subscriber_change"] <= net_loss_threshold:
        breaches.append("net_subscriber_change")
    return breaches


def _alert(row: dict[str, Any], churn_threshold: float, unsubscribe_threshold: int, net_loss_threshold: int) -> dict[str, Any]:
    breaches = _breaches(row, churn_threshold, unsubscribe_threshold, net_loss_threshold)
    severity = "critical" if len(breaches) >= 2 or row["net_subscriber_change"] <= net_loss_threshold * 2 else "warning"
    return {**row, "breaches": breaches, "severity": severity}


def _severity_rank(value: str) -> int:
    return {"critical": 2, "warning": 1}.get(value, 0)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], name: str, *, default: str) -> str:
    return name if name in columns else f"{default} AS {name}"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _sqlite_ts(value: datetime) -> str:
    return _utc(value).strftime("%Y-%m-%d %H:%M:%S")


def _int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
