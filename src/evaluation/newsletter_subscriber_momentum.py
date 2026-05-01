"""Newsletter subscriber growth and churn momentum reporting."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
DEFAULT_CHURN_WARNING_RATE = 0.05


def build_newsletter_subscriber_momentum_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    churn_warning_rate: float = DEFAULT_CHURN_WARNING_RATE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only newsletter subscriber momentum report."""
    if days <= 0:
        raise ValueError("days must be positive")
    if churn_warning_rate < 0:
        raise ValueError("churn_warning_rate must be non-negative")

    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    schema = _schema(conn)
    rows = _snapshot_rows(conn, schema, cutoff, now)
    summary = _summary(rows)
    warnings = _warnings(summary, churn_warning_rate)

    return {
        "generated_at": now.isoformat(),
        "lookback_days": days,
        "window": {
            "start": cutoff.isoformat(),
            "end": now.isoformat(),
        },
        "thresholds": {
            "churn_warning_rate": churn_warning_rate,
        },
        "totals": {
            "snapshots": len(rows),
            "warnings": len(warnings),
        },
        "summary": summary,
        "warnings": warnings,
        "empty_state": {
            "is_empty": not rows,
            "schema_present": "newsletter_subscriber_metrics" in schema,
            "message": (
                "No newsletter subscriber metrics found for the selected window."
                if not rows
                else None
            ),
        },
    }


def format_newsletter_subscriber_momentum_json(report: dict[str, Any]) -> str:
    """Render newsletter subscriber momentum as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_subscriber_momentum_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable subscriber momentum report."""
    lines = [
        "Newsletter subscriber momentum",
        f"Generated: {report['generated_at']}",
        f"Lookback: {report['lookback_days']} days",
        (
            "Churn warning threshold: "
            f"{report['thresholds']['churn_warning_rate'] * 100:.2f}%"
        ),
        (
            "Totals: "
            f"snapshots={report['totals']['snapshots']} "
            f"warnings={report['totals']['warnings']}"
        ),
        "",
    ]

    if report["empty_state"]["is_empty"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    summary = report["summary"]
    lines.extend(
        [
            f"First snapshot: {summary['first_snapshot_at']}",
            f"Latest snapshot: {summary['latest_snapshot_at']}",
            f"Subscribers: {_format_int(summary['first_subscriber_count'])} -> "
            f"{_format_int(summary['latest_subscriber_count'])} "
            f"(delta {_format_signed(summary['subscriber_delta'])})",
            f"Active subscribers: {_format_int(summary['first_active_subscriber_count'])} -> "
            f"{_format_int(summary['latest_active_subscriber_count'])} "
            f"(delta {_format_signed(summary['active_subscriber_delta'])})",
            f"Net subscriber change: {_format_signed(summary['net_subscriber_change'])}",
            f"Unsubscribe total: {_format_int(summary['unsubscribe_total'])}",
            f"Average churn: {_format_rate(summary['average_churn_rate'])}",
            "",
            "Warnings:",
        ]
    )
    if not report["warnings"]:
        lines.append("No newsletter subscriber momentum warnings.")
    else:
        for warning in report["warnings"]:
            lines.append(
                "- "
                f"{warning['label']} "
                f"net_change={_format_signed(warning['net_subscriber_change'])} "
                f"active_delta={_format_signed(warning['active_subscriber_delta'])} "
                f"average_churn={_format_rate(warning['average_churn_rate'])}"
            )
    return "\n".join(lines)


def _snapshot_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    required = {
        "id",
        "subscriber_count",
        "active_subscriber_count",
        "unsubscribes",
        "churn_rate",
        "net_subscriber_change",
        "fetched_at",
    }
    if not required.issubset(schema.get("newsletter_subscriber_metrics", set())):
        return []

    raw_rows = conn.execute(
        """SELECT id, subscriber_count, active_subscriber_count, unsubscribes,
                  churn_rate, net_subscriber_change, fetched_at
           FROM newsletter_subscriber_metrics
           ORDER BY fetched_at ASC, id ASC""",
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        fetched_at = _parse_timestamp(row["fetched_at"])
        if fetched_at is None or fetched_at < cutoff or fetched_at > now:
            continue
        rows.append(
            {
                "id": int(row["id"]),
                "subscriber_count": _optional_int(row["subscriber_count"]),
                "active_subscriber_count": _optional_int(
                    row["active_subscriber_count"]
                ),
                "unsubscribes": _optional_int(row["unsubscribes"]),
                "churn_rate": _optional_float(row["churn_rate"]),
                "net_subscriber_change": _optional_int(
                    row["net_subscriber_change"]
                ),
                "fetched_at": fetched_at,
            }
        )
    return rows


def _summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not rows:
        return {
            "first_snapshot_at": None,
            "latest_snapshot_at": None,
            "snapshots_used": 0,
            "first_subscriber_count": None,
            "latest_subscriber_count": None,
            "subscriber_delta": None,
            "first_active_subscriber_count": None,
            "latest_active_subscriber_count": None,
            "active_subscriber_delta": None,
            "net_subscriber_change": None,
            "unsubscribe_total": None,
            "average_churn_rate": None,
        }

    first = rows[0]
    latest = rows[-1]
    net_change = _delta(
        first["net_subscriber_change"],
        latest["net_subscriber_change"],
    )
    if net_change is None:
        net_change = _delta(first["subscriber_count"], latest["subscriber_count"])

    unsubscribe_total = _delta(first["unsubscribes"], latest["unsubscribes"])
    if unsubscribe_total is not None:
        unsubscribe_total = max(unsubscribe_total, 0)
    elif latest["unsubscribes"] is not None:
        unsubscribe_total = max(latest["unsubscribes"], 0)

    churn_values = [
        row["churn_rate"] for row in rows if row["churn_rate"] is not None
    ]
    average_churn_rate = (
        round(sum(churn_values) / len(churn_values), 6)
        if churn_values
        else None
    )

    return {
        "first_snapshot_at": first["fetched_at"].isoformat(),
        "latest_snapshot_at": latest["fetched_at"].isoformat(),
        "snapshots_used": len(rows),
        "first_subscriber_count": first["subscriber_count"],
        "latest_subscriber_count": latest["subscriber_count"],
        "subscriber_delta": _delta(
            first["subscriber_count"],
            latest["subscriber_count"],
        ),
        "first_active_subscriber_count": first["active_subscriber_count"],
        "latest_active_subscriber_count": latest["active_subscriber_count"],
        "active_subscriber_delta": _delta(
            first["active_subscriber_count"],
            latest["active_subscriber_count"],
        ),
        "net_subscriber_change": net_change,
        "unsubscribe_total": unsubscribe_total,
        "average_churn_rate": average_churn_rate,
    }


def _warnings(
    summary: dict[str, Any],
    churn_warning_rate: float,
) -> list[dict[str, Any]]:
    warnings = []
    subscriber_delta = summary["subscriber_delta"]
    active_delta = summary["active_subscriber_delta"]
    net_change = summary["net_subscriber_change"]
    growth_values = (subscriber_delta, active_delta, net_change)
    if any(value is not None and value < 0 for value in growth_values):
        warnings.append(_warning("negative_growth", summary))

    average_churn_rate = summary["average_churn_rate"]
    if average_churn_rate is not None and average_churn_rate > churn_warning_rate:
        warnings.append(_warning("high_churn", summary))
    return warnings


def _warning(label: str, summary: dict[str, Any]) -> dict[str, Any]:
    return {
        "label": label,
        "first_snapshot_at": summary["first_snapshot_at"],
        "latest_snapshot_at": summary["latest_snapshot_at"],
        "subscriber_delta": summary["subscriber_delta"],
        "active_subscriber_delta": summary["active_subscriber_delta"],
        "net_subscriber_change": summary["net_subscriber_change"],
        "unsubscribe_total": summary["unsubscribe_total"],
        "average_churn_rate": summary["average_churn_rate"],
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    return {
        table: {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
        for table in tables
        if table
    }


def _parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _delta(first: int | None, latest: int | None) -> int | None:
    if first is None or latest is None:
        return None
    return latest - first


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _format_int(value: int | None) -> str:
    if value is None:
        return "-"
    return str(value)


def _format_signed(value: int | None) -> str:
    if value is None:
        return "-"
    return f"{value:+d}"


def _format_rate(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value * 100:.2f}%"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)
