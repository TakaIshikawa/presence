"""Audit API rate-limit snapshot payload integrity."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


ARTIFACT_TYPE = "api_rate_limit_snapshot_payload_integrity"
DEFAULT_DAYS = 7
DEFAULT_LIMIT = 100
DEFAULT_LOW_REMAINING = 5
REASON_ORDER = (
    "malformed_raw_metadata_json",
    "negative_remaining",
    "limit_below_remaining",
    "missing_reset_at_low_remaining",
    "default_endpoint_overuse",
)
REQUIRED_COLUMNS = {"provider", "endpoint", "remaining", "limit_value", "reset_at", "raw_metadata", "fetched_at"}
DEFAULT_ENDPOINTS = {"", "default", "*", "all"}


def build_api_rate_limit_snapshot_payload_integrity_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    low_remaining: int = DEFAULT_LOW_REMAINING,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    """Build a deterministic integrity report from rate-limit snapshot rows."""
    if days <= 0:
        raise ValueError("days must be positive")
    if low_remaining < 0:
        raise ValueError("low_remaining must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    scoped_rows = [_normalize_row(row) for row in rows if _in_window(row, cutoff)]
    providers_with_specific = {
        row["provider"]
        for row in scoped_rows
        if not _is_default_endpoint(row["endpoint"])
    }

    findings: list[dict[str, Any]] = []
    for row in scoped_rows:
        findings.extend(_row_findings(row, low_remaining=low_remaining, providers_with_specific=providers_with_specific))

    findings.sort(key=_sort_key)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "low_remaining": low_remaining, "limit": limit},
        "summary": {
            "snapshot_count": len(scoped_rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
            "by_reason": _counts_by_reason(findings),
            "provider_endpoint_count": len({(row["provider"], row["endpoint"]) for row in scoped_rows}),
        },
        "groups": _groups(findings),
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {table: sorted(columns) for table, columns in sorted((missing_columns or {}).items()) if columns},
        "empty_state": {
            "is_empty": not findings,
            "message": "No API rate-limit snapshot payload integrity gaps found." if not findings else None,
        },
    }


def build_api_rate_limit_snapshot_payload_integrity_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load rate-limit snapshots from SQLite and build the integrity report."""
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "api_rate_limit_snapshots" not in schema:
        return build_api_rate_limit_snapshot_payload_integrity_report(
            [],
            missing_tables=["api_rate_limit_snapshots"],
            **kwargs,
        )
    columns = schema["api_rate_limit_snapshots"]
    missing = sorted(REQUIRED_COLUMNS - columns)
    if missing:
        return build_api_rate_limit_snapshot_payload_integrity_report(
            [],
            missing_columns={"api_rate_limit_snapshots": missing},
            **kwargs,
        )
    return build_api_rate_limit_snapshot_payload_integrity_report(_load_rows(conn), **kwargs)


def format_api_rate_limit_snapshot_payload_integrity_json(report: dict[str, Any]) -> str:
    """Render the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_api_rate_limit_snapshot_payload_integrity_text(report: dict[str, Any]) -> str:
    """Render the report as readable terminal text."""
    summary = report["summary"]
    filters = report["filters"]
    lines = [
        "API Rate-limit Snapshot Payload Integrity",
        f"Generated: {report['generated_at']}",
        f"Filters: days={filters['days']} low_remaining={filters['low_remaining']} limit={filters['limit']}",
        (
            "Totals: "
            f"snapshots={summary['snapshot_count']} "
            f"provider_endpoints={summary['provider_endpoint_count']} "
            f"findings={summary['finding_count']} "
            f"shown={summary['shown_count']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + _format_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines.extend(["", "provider | endpoint | reason | count"])
    for group in report["groups"]:
        lines.append(f"{group['provider']} | {group['endpoint']} | {group['reason']} | {group['count']}")
    lines.extend(["", "snapshot_id | provider | endpoint | reason | remaining | limit_value | reset_at | fetched_at"])
    for item in report["findings"]:
        lines.append(
            f"{_display(item['snapshot_id'])} | {item['provider']} | {item['endpoint']} | {item['reason']} | "
            f"{_display(item['remaining'])} | {_display(item['limit_value'])} | {_display(item['reset_at'])} | "
            f"{_display(item['fetched_at'])}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT id AS snapshot_id,
                  provider,
                  endpoint,
                  remaining,
                  limit_value,
                  reset_at,
                  raw_metadata,
                  fetched_at
           FROM api_rate_limit_snapshots
           ORDER BY provider ASC, endpoint ASC, datetime(fetched_at) ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "snapshot_id": _int_or_none(row.get("snapshot_id") or row.get("id")),
        "provider": _text(row.get("provider"), "unknown"),
        "endpoint": _text(row.get("endpoint"), "default"),
        "remaining": _int_or_none(row.get("remaining")),
        "limit_value": _int_or_none(row.get("limit_value")),
        "reset_at": _clean(row.get("reset_at")) or None,
        "raw_metadata": row.get("raw_metadata"),
        "fetched_at": _parse_timestamp(row.get("fetched_at")),
        "fetched_at_raw": _clean(row.get("fetched_at")) or None,
    }


def _row_findings(row: dict[str, Any], *, low_remaining: int, providers_with_specific: set[str]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    metadata_error = _metadata_error(row["raw_metadata"])
    if metadata_error:
        findings.append(_finding(row, "malformed_raw_metadata_json", detail=metadata_error))
    if row["remaining"] is not None and row["remaining"] < 0:
        findings.append(_finding(row, "negative_remaining"))
    if row["limit_value"] is not None and row["remaining"] is not None and row["limit_value"] < row["remaining"]:
        findings.append(_finding(row, "limit_below_remaining"))
    if row["remaining"] is not None and row["remaining"] <= low_remaining and not row["reset_at"]:
        findings.append(_finding(row, "missing_reset_at_low_remaining"))
    if _is_default_endpoint(row["endpoint"]) and row["provider"] in providers_with_specific:
        findings.append(_finding(row, "default_endpoint_overuse"))
    return findings


def _finding(row: dict[str, Any], reason: str, **extra: Any) -> dict[str, Any]:
    return {
        "snapshot_id": row["snapshot_id"],
        "provider": row["provider"],
        "endpoint": row["endpoint"],
        "reason": reason,
        "remaining": row["remaining"],
        "limit_value": row["limit_value"],
        "reset_at": row["reset_at"],
        "fetched_at": row["fetched_at"].isoformat() if row["fetched_at"] else row["fetched_at_raw"],
        **extra,
    }


def _metadata_error(raw: Any) -> str | None:
    if raw is None or _clean(raw) == "":
        return None
    if isinstance(raw, dict):
        return None
    try:
        parsed = json.loads(str(raw))
    except (TypeError, json.JSONDecodeError) as exc:
        return f"raw_metadata is not valid JSON: {exc}"
    return None if isinstance(parsed, dict) else "raw_metadata must be a JSON object"


def _counts_by_reason(findings: list[dict[str, Any]]) -> dict[str, int]:
    counts = Counter(item["reason"] for item in findings)
    return {reason: counts[reason] for reason in REASON_ORDER}


def _groups(findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counts = Counter((item["provider"], item["endpoint"], item["reason"]) for item in findings)
    return [
        {"provider": provider, "endpoint": endpoint, "reason": reason, "count": count}
        for (provider, endpoint, reason), count in sorted(
            counts.items(),
            key=lambda item: (item[0][0], item[0][1], _reason_rank(item[0][2])),
        )
    ]


def _sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (_reason_rank(item["reason"]), item["provider"], item["endpoint"], item["snapshot_id"] or 0)


def _reason_rank(reason: str) -> int:
    return REASON_ORDER.index(reason) if reason in REASON_ORDER else len(REASON_ORDER)


def _in_window(row: dict[str, Any], cutoff: datetime) -> bool:
    fetched_at = _parse_timestamp(row.get("fetched_at"))
    return fetched_at is None or fetched_at >= cutoff


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _utc(parsed)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({_quote_identifier(str(row[0]))})")} for row in rows}


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _format_missing(missing: dict[str, list[str]]) -> str:
    return "; ".join(f"{table}({', '.join(columns)})" for table, columns in sorted(missing.items()))


def _is_default_endpoint(value: Any) -> bool:
    return _text(value, "default").casefold() in DEFAULT_ENDPOINTS


def _text(value: Any, default: str) -> str:
    text = _clean(value)
    return text or default


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _display(value: Any) -> str:
    return "-" if value is None or value == "" else str(value)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None and str(value).strip() else None
    except (TypeError, ValueError):
        return None


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
