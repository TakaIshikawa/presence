"""Detect suspicious publication attempt payload and error metadata."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any

from .publish_errors import classify_publish_error, normalize_error_category


DEFAULT_DAYS = 7
DEFAULT_MAX_METADATA_BYTES = 8192
SEVERITIES = ("low", "medium", "high")
SEVERITY_FILTERS = ("all", *SEVERITIES)
ANOMALY_TYPES = (
    "success_without_post_id",
    "success_with_error",
    "malformed_response_metadata",
    "missing_url_for_success",
    "oversized_response_metadata",
    "category_mismatch",
)

_SEVERITY_RANK = {"high": 0, "medium": 1, "low": 2}
_ANOMALY_SEVERITY = {
    "success_without_post_id": "high",
    "success_with_error": "high",
    "malformed_response_metadata": "high",
    "missing_url_for_success": "medium",
    "category_mismatch": "medium",
    "oversized_response_metadata": "low",
}
_FIX_HINTS = {
    "success_without_post_id": "Persist the platform post identifier from the successful publisher response.",
    "success_with_error": "Clear error fields on successful attempts or mark the attempt as failed.",
    "malformed_response_metadata": "Store response_metadata as valid JSON before recording the attempt.",
    "missing_url_for_success": "Persist the canonical platform URL for successful attempts.",
    "oversized_response_metadata": "Trim response_metadata to stable diagnostics before storing it.",
    "category_mismatch": "Reclassify the stored error_category from the error text or fix the publisher category mapping.",
}


def build_publication_attempt_payload_anomalies_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    platform: str = "all",
    severity: str = "all",
    max_metadata_bytes: int = DEFAULT_MAX_METADATA_BYTES,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return anomalies for recent publication attempt payload metadata."""
    if days <= 0:
        raise ValueError("days must be positive")
    if severity not in SEVERITY_FILTERS:
        raise ValueError(f"invalid severity: {severity}")
    if max_metadata_bytes <= 0:
        raise ValueError("max_metadata_bytes must be positive")

    conn = _connection(db_or_conn)
    now = _as_utc(now or datetime.now(timezone.utc))
    cutoff = (now - timedelta(days=days)).isoformat()
    schema = _schema(conn)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    anomalies: list[dict[str, Any]] = []

    columns = schema.get("publication_attempts")
    required = {
        "id",
        "content_id",
        "platform",
        "attempted_at",
        "success",
        "platform_post_id",
        "platform_url",
        "error",
        "error_category",
        "response_metadata",
    }
    if not columns:
        missing_tables.append("publication_attempts")
    elif not required.issubset(columns):
        missing_columns["publication_attempts"] = sorted(required - columns)
    else:
        for row in _attempt_rows(conn, cutoff, platform):
            anomalies.extend(
                _anomalies_for_attempt(dict(row), max_metadata_bytes=max_metadata_bytes)
            )

    if severity != "all":
        anomalies = [item for item in anomalies if item["severity"] == severity]
    anomalies.sort(key=_anomaly_sort_key)

    totals_by_type = Counter(item["type"] for item in anomalies)
    totals_by_platform = Counter(item["platform"] for item in anomalies)
    totals_by_severity = Counter(item["severity"] for item in anomalies)

    return {
        "artifact_type": "publication_attempt_payload_anomalies",
        "generated_at": now.isoformat(),
        "window_days": days,
        "platform": platform,
        "severity": severity,
        "max_metadata_bytes": max_metadata_bytes,
        "totals": {
            "anomaly_count": len(anomalies),
            "by_type": {name: totals_by_type.get(name, 0) for name in ANOMALY_TYPES},
            "by_platform": dict(sorted(totals_by_platform.items())),
            "by_severity": {name: totals_by_severity.get(name, 0) for name in SEVERITIES},
        },
        "items": anomalies,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def format_publication_attempt_payload_anomalies_json(report: dict[str, Any]) -> str:
    """Render a publication attempt payload anomaly report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_payload_anomalies_text(report: dict[str, Any]) -> str:
    """Render a concise terminal report for publication attempt payload anomalies."""
    lines = [
        "Publication Attempt Payload Anomaly Report",
        f"Generated: {report['generated_at']}",
        f"Window: {report['window_days']} days",
        f"Platform: {report['platform']}",
        f"Severity: {report['severity']}",
        f"Anomalies: {report['totals']['anomaly_count']}",
        "",
    ]
    if report["missing_tables"] or report["missing_columns"]:
        lines.append(f"Missing tables: {', '.join(report['missing_tables']) or '-'}")
        if report["missing_columns"]:
            rendered = ", ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in report["missing_columns"].items()
            )
            lines.append(f"Missing columns: {rendered}")
        return "\n".join(lines)

    if not report["items"]:
        lines.append("No publication attempt payload anomalies found.")
        return "\n".join(lines)

    lines.append("By type:")
    for anomaly_type, count in report["totals"]["by_type"].items():
        if count:
            lines.append(f"- {anomaly_type}: {count}")

    lines.extend(["", "Items:"])
    for item in report["items"]:
        lines.append(
            "- "
            f"{item['severity']} {item['type']} "
            f"attempt={item['attempt_id']} content={item['content_id']} "
            f"platform={item['platform']} at={item['attempted_at']}"
        )
        lines.append(f"  fix_hint: {item['fix_hint']}")
    return "\n".join(lines)


def _attempt_rows(
    conn: sqlite3.Connection,
    cutoff: str,
    platform: str,
) -> list[sqlite3.Row]:
    where = "WHERE attempted_at >= ?"
    params: list[Any] = [cutoff]
    if platform != "all":
        where += " AND platform = ?"
        params.append(platform)
    return conn.execute(
        f"""SELECT id, content_id, platform, attempted_at, success,
                  platform_post_id, platform_url, error, error_category,
                  response_metadata
           FROM publication_attempts
           {where}
           ORDER BY attempted_at DESC, id DESC""",
        params,
    ).fetchall()


def _anomalies_for_attempt(
    row: dict[str, Any],
    *,
    max_metadata_bytes: int,
) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    success = bool(row.get("success"))
    error = _clean(row.get("error"))
    error_category = _clean(row.get("error_category"))
    post_id = _clean(row.get("platform_post_id"))
    url = _clean(row.get("platform_url"))
    metadata = row.get("response_metadata")

    if success and not post_id:
        anomalies.append(_anomaly(row, "success_without_post_id"))
    if success and (error or error_category):
        anomalies.append(_anomaly(row, "success_with_error"))
    if success and not url:
        anomalies.append(_anomaly(row, "missing_url_for_success"))
    if _metadata_is_malformed(metadata):
        anomalies.append(_anomaly(row, "malformed_response_metadata"))
    if _metadata_size(metadata) > max_metadata_bytes:
        anomalies.append(
            _anomaly(
                row,
                "oversized_response_metadata",
                details={"metadata_bytes": _metadata_size(metadata)},
            )
        )
    if not success and error:
        stored_category = normalize_error_category(error_category)
        classified_category = classify_publish_error(error, platform=_clean(row.get("platform")))
        if classified_category != "unknown" and stored_category != classified_category:
            anomalies.append(
                _anomaly(
                    row,
                    "category_mismatch",
                    details={
                        "stored_error_category": stored_category,
                        "classified_error_category": classified_category,
                    },
                )
            )
    return anomalies


def _anomaly(
    row: dict[str, Any],
    anomaly_type: str,
    *,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    item = {
        "attempt_id": int(row["id"]),
        "content_id": int(row["content_id"]),
        "platform": str(row["platform"]),
        "attempted_at": str(row["attempted_at"]),
        "type": anomaly_type,
        "severity": _ANOMALY_SEVERITY[anomaly_type],
        "fix_hint": _FIX_HINTS[anomaly_type],
    }
    if details:
        item["details"] = details
    return item


def _metadata_is_malformed(value: Any) -> bool:
    if value is None or value == "":
        return False
    if isinstance(value, (dict, list)):
        return False
    try:
        json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return True
    return False


def _metadata_size(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, str):
        return len(value.encode("utf-8"))
    return len(json.dumps(value, sort_keys=True, default=str).encode("utf-8"))


def _anomaly_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    return (
        _SEVERITY_RANK[item["severity"]],
        item["platform"],
        item["type"],
        item["attempted_at"],
        item["attempt_id"],
    )


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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _clean(value: Any) -> str:
    return str(value).strip() if value is not None else ""


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
