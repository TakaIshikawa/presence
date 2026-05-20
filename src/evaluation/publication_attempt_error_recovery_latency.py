"""Measure latency from failed publication attempts to later successful attempts."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from math import ceil
import json
import sqlite3
from typing import Any

from output.publish_errors import normalize_error_category


DEFAULT_LOOKBACK_DAYS = 30
DEFAULT_MIN_COUNT = 1
DEFAULT_LIMIT = 25
PLATFORMS = ("all", "x", "bluesky")
SUCCESS_STATUSES = {"success", "succeeded", "published", "ok", "complete", "completed"}
FAILED_STATUSES = {"failed", "failure", "error", "errored", "rejected", "timeout"}


def build_publication_attempt_error_recovery_latency_report(
    rows: list[dict[str, Any]],
    *,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    platform: str = "all",
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Build a deterministic recovery-latency report from publication attempt rows."""
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if platform not in PLATFORMS:
        raise ValueError(f"invalid platform: {platform}")

    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=lookback_days)
    attempts = [_normalize_row(row) for row in rows]
    attempts = [
        attempt
        for attempt in attempts
        if attempt["attempted_at_dt"] is not None
        and attempt["attempted_at_dt"] <= generated_at
        and (platform == "all" or attempt["platform"] == platform)
    ]
    attempts.sort(key=lambda item: (item["content_id"], item["platform"], item["attempted_at_dt"], item["attempt_id"]))

    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for attempt in attempts:
        grouped[(attempt["content_id"], attempt["platform"])].append(attempt)

    recovered: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    unresolved: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for group_attempts in grouped.values():
        for index, attempt in enumerate(group_attempts):
            if not attempt["failed"] or attempt["attempted_at_dt"] < cutoff:
                continue
            success = next(
                (
                    later
                    for later in group_attempts[index + 1 :]
                    if later["success"] and later["attempted_at_dt"] > attempt["attempted_at_dt"]
                ),
                None,
            )
            bucket_key = (attempt["platform"], attempt["error_category"])
            if success is None:
                unresolved[bucket_key].append(attempt)
                continue
            recovery_hours = round(
                (success["attempted_at_dt"] - attempt["attempted_at_dt"]).total_seconds() / 3600,
                4,
            )
            recovered[bucket_key].append(
                {
                    "failure": attempt,
                    "success": success,
                    "recovery_hours": recovery_hours,
                }
            )

    latency_buckets = [
        _latency_bucket(platform_name, category, items)
        for (platform_name, category), items in recovered.items()
        if len(items) >= min_count
    ]
    latency_buckets.sort(key=lambda item: (-item["p95_recovery_hours"], -item["failure_count"], item["platform"], item["error_category"]))
    unresolved_buckets = [
        _unresolved_bucket(platform_name, category, items)
        for (platform_name, category), items in unresolved.items()
        if len(items) >= min_count
    ]
    unresolved_buckets.sort(key=lambda item: (-item["unresolved_failure_count"], item["platform"], item["error_category"]))

    representative_examples = _representative_examples(recovered, unresolved, limit)
    recovered_count = sum(len(items) for items in recovered.values())
    unresolved_count = sum(len(items) for items in unresolved.values())
    failure_count = recovered_count + unresolved_count

    return {
        "artifact_type": "publication_attempt_error_recovery_latency",
        "generated_at": generated_at.isoformat(),
        "missing_tables": list(missing_tables),
        "thresholds": {
            "lookback_days": lookback_days,
            "lookback_start": cutoff.isoformat(),
            "min_count": min_count,
            "platform": platform,
            "limit": limit,
        },
        "summary": {
            "attempt_rows_scanned": len(rows),
            "eligible_attempt_rows": len(attempts),
            "failure_count": failure_count,
            "recovered_failure_count": recovered_count,
            "unresolved_failure_count": unresolved_count,
            "recovery_rate": _rate(recovered_count, failure_count),
            "latency_bucket_count": len(latency_buckets),
            "unresolved_bucket_count": len(unresolved_buckets),
        },
        "latency_buckets": latency_buckets[:limit],
        "unresolved_buckets": unresolved_buckets[:limit],
        "representative_examples": representative_examples,
    }


def build_publication_attempt_error_recovery_latency_report_from_db(
    db_or_conn: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables = () if "publication_attempts" in schema else ("publication_attempts",)
    rows = _load_rows(conn, schema["publication_attempts"]) if not missing_tables else []
    return build_publication_attempt_error_recovery_latency_report(
        rows,
        missing_tables=missing_tables,
        **kwargs,
    )


def format_publication_attempt_error_recovery_latency_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_error_recovery_latency_text(report: dict[str, Any]) -> str:
    thresholds = report["thresholds"]
    summary = report["summary"]
    lines = [
        "Publication Attempt Error Recovery Latency",
        f"Generated: {report['generated_at']}",
        (
            f"Window: days={thresholds['lookback_days']} start={thresholds['lookback_start']} "
            f"platform={thresholds['platform']}"
        ),
        (
            f"Thresholds: min_count={thresholds['min_count']} limit={thresholds['limit']}"
        ),
        (
            f"Totals: failures={summary['failure_count']} recovered={summary['recovered_failure_count']} "
            f"unresolved={summary['unresolved_failure_count']} recovery_rate={summary['recovery_rate']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["latency_buckets"]:
        lines.extend(["", "Highest latency buckets:"])
        for bucket in report["latency_buckets"]:
            lines.append(
                f"- {bucket['platform']} / {bucket['error_category']}: "
                f"count={bucket['failure_count']} median_h={bucket['median_recovery_hours']} "
                f"p95_h={bucket['p95_recovery_hours']} examples={_join_ids(bucket['representative_attempt_ids'])}"
            )
    if report["unresolved_buckets"]:
        lines.extend(["", "Unresolved failure buckets:"])
        for bucket in report["unresolved_buckets"]:
            lines.append(
                f"- {bucket['platform']} / {bucket['error_category']}: "
                f"unresolved={bucket['unresolved_failure_count']} examples={_join_ids(bucket['representative_attempt_ids'])}"
            )
    if not report["latency_buckets"] and not report["unresolved_buckets"] and not report["missing_tables"]:
        lines.append("No publication attempt recovery latency issues found.")
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, columns: set[str]) -> list[dict[str, Any]]:
    selected = [
        _column_expr(columns, "id", "attempt_id", default="rowid"),
        _column_expr(columns, "content_id", "content_id", default="NULL"),
        _column_expr(columns, "platform", "platform", default="'unknown'"),
        _column_expr(columns, "attempted_at", "attempted_at", default=_column_expr(columns, "created_at", "created_at", default="NULL").split(" AS ")[0]),
        _column_expr(columns, "success", "success", default="NULL"),
        _column_expr(columns, "status", "status", default="NULL"),
        _column_expr(columns, "error_category", "error_category", default="NULL"),
        _column_expr(columns, "error", "error", default=_column_expr(columns, "error_message", "error_message", default="NULL").split(" AS ")[0]),
    ]
    order = "attempted_at" if "attempted_at" in columns else "created_at" if "created_at" in columns else "rowid"
    return [
        dict(row)
        for row in conn.execute(
            f"SELECT {', '.join(selected)} FROM publication_attempts ORDER BY {order} ASC, rowid ASC"
        ).fetchall()
    ]


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    status = _text(row.get("status")).lower()
    success = _is_success(row, status)
    failed = _is_failed(row, status, success)
    return {
        "attempt_id": _text(_first(row, "attempt_id", "id")) or "unknown",
        "content_id": _text(_first(row, "content_id", "generated_content_id")) or "unknown",
        "platform": _text(_first(row, "platform", "channel")).lower() or "unknown",
        "attempted_at": _first(row, "attempted_at", "created_at"),
        "attempted_at_dt": _parse_datetime(_first(row, "attempted_at", "created_at")),
        "success": success,
        "failed": failed,
        "error_category": normalize_error_category(_first(row, "error_category", "category", "error")),
        "error": _text(_first(row, "error", "error_message", "message")),
    }


def _latency_bucket(platform: str, category: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    values = sorted(item["recovery_hours"] for item in items)
    examples = sorted(items, key=lambda item: (-item["recovery_hours"], item["failure"]["attempt_id"]))
    return {
        "platform": platform,
        "error_category": category,
        "failure_count": len(items),
        "median_recovery_hours": _quantile(values, 0.5),
        "p95_recovery_hours": _quantile(values, 0.95),
        "representative_attempt_ids": [item["failure"]["attempt_id"] for item in examples[:3]],
    }


def _unresolved_bucket(platform: str, category: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    examples = sorted(items, key=lambda item: (item["attempted_at_dt"], item["attempt_id"]))
    return {
        "platform": platform,
        "error_category": category,
        "unresolved_failure_count": len(items),
        "oldest_failed_at": examples[0]["attempted_at_dt"].isoformat(),
        "representative_attempt_ids": [item["attempt_id"] for item in examples[:3]],
    }


def _representative_examples(
    recovered: dict[tuple[str, str], list[dict[str, Any]]],
    unresolved: dict[tuple[str, str], list[dict[str, Any]]],
    limit: int,
) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for (platform, category), items in recovered.items():
        for item in items:
            failure = item["failure"]
            success = item["success"]
            examples.append(
                {
                    "status": "recovered",
                    "platform": platform,
                    "error_category": category,
                    "content_id": failure["content_id"],
                    "failed_attempt_id": failure["attempt_id"],
                    "successful_attempt_id": success["attempt_id"],
                    "failed_at": failure["attempted_at_dt"].isoformat(),
                    "recovered_at": success["attempted_at_dt"].isoformat(),
                    "recovery_hours": item["recovery_hours"],
                }
            )
    for (platform, category), items in unresolved.items():
        for failure in items:
            examples.append(
                {
                    "status": "unresolved",
                    "platform": platform,
                    "error_category": category,
                    "content_id": failure["content_id"],
                    "failed_attempt_id": failure["attempt_id"],
                    "successful_attempt_id": None,
                    "failed_at": failure["attempted_at_dt"].isoformat(),
                    "recovered_at": None,
                    "recovery_hours": None,
                }
            )
    examples.sort(
        key=lambda item: (
            item["status"] != "unresolved",
            -(item["recovery_hours"] or 0),
            item["platform"],
            item["error_category"],
            item["failed_attempt_id"],
        )
    )
    return examples[:limit]


def _is_success(row: dict[str, Any], status: str) -> bool:
    if row.get("success") is not None:
        return _truthy(row.get("success"))
    return status in SUCCESS_STATUSES


def _is_failed(row: dict[str, Any], status: str, success: bool) -> bool:
    if success:
        return False
    if row.get("success") is not None:
        return not _truthy(row.get("success"))
    return status in FAILED_STATUSES or bool(_first(row, "error", "error_message", "message"))


def _quantile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    if quantile == 0.5:
        midpoint = len(values) // 2
        if len(values) % 2:
            return _clean_float(values[midpoint])
        return _clean_float((values[midpoint - 1] + values[midpoint]) / 2)
    index = max(0, ceil(quantile * len(values)) - 1)
    return _clean_float(values[index])


def _clean_float(value: float) -> float | int:
    rounded = round(value, 4)
    return int(rounded) if float(rounded).is_integer() else rounded


def _rate(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator else 0.0


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {str(row[0]): {str(column[1]) for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _column_expr(columns: set[str], column: str, output: str, *, default: str) -> str:
    return f"{column} AS {output}" if column in columns else f"{default} AS {output}"


def _parse_datetime(value: Any) -> datetime | None:
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
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "success", "succeeded"}
    return bool(value)


def _join_ids(values: list[str]) -> str:
    return ", ".join(str(value) for value in values) if values else "-"
