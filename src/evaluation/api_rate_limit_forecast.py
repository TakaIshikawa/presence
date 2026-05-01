"""API rate limit exhaustion forecasting."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_HOURS = 24
DEFAULT_REMAINING_WARNING_PERCENT = 20.0


def build_api_rate_limit_forecast_report(
    db_or_conn: Any,
    *,
    hours: int = DEFAULT_HOURS,
    remaining_warning_percent: float = DEFAULT_REMAINING_WARNING_PERCENT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only forecast report from API rate-limit snapshots."""
    if hours <= 0:
        raise ValueError("hours must be positive")
    if not 0 <= remaining_warning_percent <= 100:
        raise ValueError("remaining_warning_percent must be between 0 and 100")

    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(hours=hours)
    schema = _schema(conn)
    rows = _snapshot_rows(conn, schema, cutoff, now)
    resources = _resource_forecasts(rows, now, remaining_warning_percent)
    warnings = [
        warning
        for resource in resources
        for warning in resource["warnings"]
    ]

    return {
        "generated_at": now.isoformat(),
        "lookback_hours": hours,
        "window": {
            "start": cutoff.isoformat(),
            "end": now.isoformat(),
        },
        "thresholds": {
            "remaining_warning_percent": remaining_warning_percent,
        },
        "totals": {
            "resources": len(resources),
            "warnings": len(warnings),
        },
        "resources": resources,
        "warnings": warnings,
        "empty_state": {
            "is_empty": not rows,
            "schema_present": "api_rate_limit_snapshots" in schema,
            "message": (
                "No API rate limit snapshots found for the selected window."
                if not rows
                else None
            ),
        },
    }


def format_api_rate_limit_forecast_json(report: dict[str, Any]) -> str:
    """Render an API rate limit forecast as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_api_rate_limit_forecast_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable API rate limit forecast."""
    lines = [
        "API rate limit forecast",
        f"Generated: {report['generated_at']}",
        f"Lookback: {report['lookback_hours']} hours",
        (
            "Remaining warning threshold: "
            f"{report['thresholds']['remaining_warning_percent']:.1f}%"
        ),
        (
            "Totals: "
            f"resources={report['totals']['resources']} "
            f"warnings={report['totals']['warnings']}"
        ),
        "",
    ]

    if report["empty_state"]["is_empty"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    columns = [
        ("provider", "PROVIDER", 10),
        ("resource", "RESOURCE", 24),
        ("limit", "LIMIT", 8),
        ("remaining", "REMAIN", 8),
        ("remaining_percent", "REMAIN%", 8),
        ("reset_at", "RESET_AT", 25),
        ("projected_exhaustion_at", "EXHAUST_AT", 25),
        ("warning_labels", "WARNINGS", 36),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for resource in report["resources"]:
        rendered = {
            **resource,
            "limit": _format_cell(resource["limit"], 8),
            "remaining": _format_cell(resource["remaining"], 8),
            "remaining_percent": _format_percent(resource["remaining_percent"]),
            "reset_at": resource["reset_at"] or "-",
            "projected_exhaustion_at": resource["projected_exhaustion_at"] or "-",
            "warning_labels": ",".join(
                warning["label"] for warning in resource["warnings"]
            ),
        }
        lines.append(
            "  ".join(
                _format_cell(rendered.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )

    lines.append("")
    lines.append("Warnings:")
    if not report["warnings"]:
        lines.append("No API rate limit forecast warnings.")
    else:
        for warning in report["warnings"]:
            lines.append(
                "- "
                f"{warning['label']} "
                f"{warning['provider']}:{warning['resource']} "
                f"remaining={warning['remaining']} "
                f"remaining_percent={_format_percent(warning['remaining_percent'])} "
                f"projected_exhaustion_at={warning['projected_exhaustion_at'] or '-'}"
            )
    return "\n".join(lines)


def _snapshot_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    required = {
        "provider",
        "endpoint",
        "remaining",
        "limit_value",
        "reset_at",
        "fetched_at",
    }
    if not required.issubset(schema.get("api_rate_limit_snapshots", set())):
        return []

    raw_rows = conn.execute(
        """SELECT id, provider, endpoint, remaining, limit_value, reset_at, fetched_at
           FROM api_rate_limit_snapshots
           ORDER BY provider ASC, endpoint ASC, fetched_at ASC, id ASC""",
    ).fetchall()

    rows: list[dict[str, Any]] = []
    for row in raw_rows:
        fetched_at = _parse_timestamp(row["fetched_at"])
        if fetched_at is None or fetched_at < cutoff or fetched_at > now:
            continue
        rows.append(
            {
                "id": int(row["id"]),
                "provider": str(row["provider"]),
                "resource": str(row["endpoint"] or "default"),
                "remaining": int(row["remaining"]),
                "limit": row["limit_value"],
                "reset_at": row["reset_at"],
                "reset_time": _parse_timestamp(row["reset_at"]),
                "fetched_at": fetched_at,
            }
        )
    return rows


def _resource_forecasts(
    rows: list[dict[str, Any]],
    now: datetime,
    remaining_warning_percent: float,
) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault((row["provider"], row["resource"]), []).append(row)

    resources = []
    for (provider, resource), group in groups.items():
        group.sort(key=lambda row: (row["fetched_at"], row["id"]))
        latest = group[-1]
        comparable = [
            row for row in group if row["reset_at"] == latest["reset_at"]
        ]
        consumption_rate = _consumption_rate(comparable)
        remaining = latest["remaining"]
        reset_time = latest["reset_time"]
        seconds_until_reset = (
            round((reset_time - now).total_seconds(), 2)
            if reset_time is not None
            else None
        )
        projected_exhaustion_at = _projected_exhaustion_at(
            latest["fetched_at"],
            remaining,
            consumption_rate,
        )
        limit = latest["limit"]
        remaining_percent = _remaining_percent(remaining, limit)

        entry = {
            "provider": provider,
            "resource": resource,
            "limit": int(limit) if limit is not None else None,
            "remaining": remaining,
            "remaining_percent": remaining_percent,
            "reset_at": reset_time.isoformat() if reset_time is not None else None,
            "seconds_until_reset": seconds_until_reset,
            "consumption_rate_per_second": consumption_rate,
            "projected_exhaustion_at": (
                projected_exhaustion_at.isoformat()
                if projected_exhaustion_at is not None
                else None
            ),
            "latest_snapshot_at": latest["fetched_at"].isoformat(),
            "snapshots_used": len(comparable),
            "warnings": [],
        }
        entry["warnings"] = _warnings_for_entry(
            entry,
            remaining_warning_percent,
            projected_exhaustion_at,
            reset_time,
        )
        resources.append(entry)

    resources.sort(
        key=lambda entry: (
            not entry["warnings"],
            entry["projected_exhaustion_at"] or "9999",
            entry["provider"],
            entry["resource"],
        )
    )
    return resources


def _consumption_rate(rows: list[dict[str, Any]]) -> float | None:
    if len(rows) < 2:
        return None
    consumed = 0
    first_at = rows[0]["fetched_at"]
    last_at = rows[-1]["fetched_at"]
    for previous, current in zip(rows, rows[1:]):
        consumed += max(previous["remaining"] - current["remaining"], 0)
    elapsed = (last_at - first_at).total_seconds()
    if elapsed <= 0 or consumed <= 0:
        return None
    return round(consumed / elapsed, 8)


def _projected_exhaustion_at(
    latest_at: datetime,
    remaining: int,
    consumption_rate: float | None,
) -> datetime | None:
    if consumption_rate is None or consumption_rate <= 0:
        return None
    if remaining <= 0:
        return latest_at
    return latest_at + timedelta(seconds=remaining / consumption_rate)


def _warnings_for_entry(
    entry: dict[str, Any],
    remaining_warning_percent: float,
    projected_exhaustion_at: datetime | None,
    reset_time: datetime | None,
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    remaining_percent = entry["remaining_percent"]
    if (
        remaining_percent is not None
        and remaining_percent <= remaining_warning_percent
    ):
        warnings.append(_warning("low_remaining", entry))
    if (
        projected_exhaustion_at is not None
        and reset_time is not None
        and projected_exhaustion_at < reset_time
    ):
        warnings.append(_warning("exhaustion_before_reset", entry))
    return warnings


def _warning(label: str, entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "label": label,
        "provider": entry["provider"],
        "resource": entry["resource"],
        "limit": entry["limit"],
        "remaining": entry["remaining"],
        "remaining_percent": entry["remaining_percent"],
        "reset_at": entry["reset_at"],
        "projected_exhaustion_at": entry["projected_exhaustion_at"],
    }


def _remaining_percent(remaining: int, limit: Any) -> float | None:
    if limit is None:
        return None
    try:
        limit_value = int(limit)
    except (TypeError, ValueError):
        return None
    if limit_value <= 0:
        return None
    return round((remaining / limit_value) * 100, 2)


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


def _format_percent(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}%"


def _format_cell(value: Any, width: int) -> str:
    text = "-" if value is None or value == "" else str(value)
    if len(text) <= width:
        return text
    return text[: max(width - 3, 0)] + "..."


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)
