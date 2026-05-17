"""Report newsletter send cadence anomalies."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_TARGET_DAYS = 7.0
DEFAULT_TOLERANCE_HOURS = 12.0
REQUIRED_COLUMNS = {"id", "sent_at"}


def build_newsletter_send_cadence_anomalies_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    target_days: float = DEFAULT_TARGET_DAYS,
    tolerance_hours: float = DEFAULT_TOLERANCE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if target_days <= 0:
        raise ValueError("target_days must be positive")
    if tolerance_hours <= 0:
        raise ValueError("tolerance_hours must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    filters = {
        "days": days,
        "target_days": target_days,
        "target_hours": round(target_days * 24, 2),
        "tolerance_hours": tolerance_hours,
        "window_start": cutoff.isoformat(),
        "window_end": generated_at.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    if missing_tables or missing_columns:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    sends = _load_sends(conn, cutoff, generated_at)
    anomalies: list[dict[str, Any]] = []
    gaps: list[float] = []
    target_hours = target_days * 24
    for prev, current in zip(sends, sends[1:]):
        gap_hours = (current["sent_at_dt"] - prev["sent_at_dt"]).total_seconds() / 3600
        gaps.append(gap_hours)
        deviation = gap_hours - target_hours
        if abs(deviation) <= tolerance_hours:
            continue
        anomalies.append(
            {
                "previous_send_id": prev["id"],
                "newsletter_send_id": current["id"],
                "previous_issue_id": prev.get("issue_id"),
                "issue_id": current.get("issue_id"),
                "previous_sent_at": prev["sent_at_dt"].isoformat(),
                "sent_at": current["sent_at_dt"].isoformat(),
                "gap_hours": round(gap_hours, 2),
                "target_hours": round(target_hours, 2),
                "deviation_hours": round(deviation, 2),
                "anomaly_type": "long_gap" if deviation > 0 else "short_gap",
            }
        )

    anomalies.sort(key=lambda row: (-abs(row["deviation_hours"]), row["sent_at"], row["newsletter_send_id"]))
    return {
        "artifact_type": "newsletter_send_cadence_anomalies",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "send_count": len(sends),
            "gap_count": len(gaps),
            "anomaly_count": len(anomalies),
            "long_gap_count": sum(1 for row in anomalies if row["anomaly_type"] == "long_gap"),
            "short_gap_count": sum(1 for row in anomalies if row["anomaly_type"] == "short_gap"),
            "average_gap_hours": round(sum(gaps) / len(gaps), 2) if gaps else None,
        },
        "weekday_hour_drift": _weekday_hour_drift(sends),
        "anomalies": anomalies,
        "missing_tables": [],
        "missing_columns": {},
    }


def format_newsletter_send_cadence_anomalies_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_newsletter_send_cadence_anomalies_text(report: dict[str, Any]) -> str:
    filters = report["filters"]
    totals = report["totals"]
    drift = report["weekday_hour_drift"]
    lines = [
        "Newsletter Send Cadence Anomalies",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} target_days={filters['target_days']} "
            f"tolerance_hours={filters['tolerance_hours']}"
        ),
        (
            f"Totals: sends={totals['send_count']} gaps={totals['gap_count']} "
            f"anomalies={totals['anomaly_count']} long={totals['long_gap_count']} short={totals['short_gap_count']}"
        ),
        (
            f"Drift: primary_weekday={drift['primary_weekday'] or '-'} "
            f"primary_hour={drift['primary_hour']} weekday_count={drift['weekday_count']} "
            f"hour_count={drift['hour_count']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["anomalies"]:
        lines.append("No cadence anomalies found.")
        return "\n".join(lines)
    lines.extend(["", "Anomalies:"])
    for row in report["anomalies"]:
        lines.append(
            f"- {row['anomaly_type']} send={row['newsletter_send_id']} issue={row['issue_id'] or '-'} "
            f"sent={row['sent_at']} gap_h={row['gap_hours']} deviation_h={row['deviation_hours']}"
        )
    return "\n".join(lines)


format_newsletter_send_cadence_anomalies_table = format_newsletter_send_cadence_anomalies_text


def _load_sends(conn: sqlite3.Connection, cutoff: datetime, generated_at: datetime) -> list[dict[str, Any]]:
    columns = _schema(conn)["newsletter_sends"]
    issue_expr = "issue_id" if "issue_id" in columns else "NULL AS issue_id"
    subject_expr = "subject" if "subject" in columns else "NULL AS subject"
    status_filter = "LOWER(COALESCE(status, 'sent')) = 'sent'" if "status" in columns else "1 = 1"
    rows = conn.execute(
        f"""SELECT id, {issue_expr}, {subject_expr}, sent_at
            FROM newsletter_sends
            WHERE sent_at IS NOT NULL
              AND datetime(sent_at) >= datetime(?)
              AND datetime(sent_at) <= datetime(?)
              AND {status_filter}
            ORDER BY datetime(sent_at) ASC, id ASC""",
        (cutoff.isoformat(), generated_at.isoformat()),
    ).fetchall()
    sends = []
    for row in rows:
        sent_at = _parse_dt(row["sent_at"])
        if sent_at is None:
            continue
        item = dict(row)
        item["sent_at_dt"] = sent_at
        sends.append(item)
    return sends


def _weekday_hour_drift(sends: list[dict[str, Any]]) -> dict[str, Any]:
    weekdays = Counter(item["sent_at_dt"].strftime("%A") for item in sends)
    hours = Counter(item["sent_at_dt"].hour for item in sends)
    primary_weekday, weekday_count = _top_counter(weekdays)
    primary_hour, hour_count = _top_counter(hours)
    examples = [
        {
            "newsletter_send_id": item["id"],
            "issue_id": item.get("issue_id"),
            "sent_at": item["sent_at_dt"].isoformat(),
            "weekday": item["sent_at_dt"].strftime("%A"),
            "hour": item["sent_at_dt"].hour,
        }
        for item in sends
        if primary_weekday is not None
        and primary_hour is not None
        and (item["sent_at_dt"].strftime("%A") != primary_weekday or item["sent_at_dt"].hour != primary_hour)
    ][:5]
    return {
        "primary_weekday": primary_weekday,
        "primary_hour": primary_hour,
        "weekday_count": weekday_count,
        "hour_count": hour_count,
        "weekday_distribution": dict(sorted(weekdays.items())),
        "hour_distribution": {str(k): v for k, v in sorted(hours.items())},
        "examples": examples,
    }


def _top_counter(counter: Counter[Any]) -> tuple[Any | None, int]:
    if not counter:
        return None, 0
    value, count = sorted(counter.items(), key=lambda item: (-item[1], item[0]))[0]
    return value, count


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    if "newsletter_sends" not in schema:
        return ["newsletter_sends"], {}
    missing = sorted(REQUIRED_COLUMNS - schema["newsletter_sends"])
    return [], {"newsletter_sends": missing} if missing else {}


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "newsletter_send_cadence_anomalies",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "send_count": 0,
            "gap_count": 0,
            "anomaly_count": 0,
            "long_gap_count": 0,
            "short_gap_count": 0,
            "average_gap_hours": None,
        },
        "weekday_hour_drift": {
            "primary_weekday": None,
            "primary_hour": None,
            "weekday_count": 0,
            "hour_count": 0,
            "weekday_distribution": {},
            "hour_distribution": {},
            "examples": [],
        },
        "anomalies": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))
