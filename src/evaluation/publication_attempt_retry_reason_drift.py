"""Detect retry reason drift across publication attempt time windows."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta
from typing import Any

from ._batch_report_common import (
    bounded_share,
    clean,
    connection,
    dt,
    empty_state,
    flatten_missing,
    json_dumps,
    now_value,
    positive,
    schema,
)


ARTIFACT_TYPE = "publication_attempt_retry_reason_drift"
DEFAULT_BASELINE_DAYS = 14
DEFAULT_CURRENT_DAYS = 7
DEFAULT_LIMIT = 50
DEFAULT_MIN_DELTA = 0.2
DEFAULT_MIN_SAMPLE = 2
SOURCE_TABLES = ("publication_retries", "publication_attempts")


def build_publication_attempt_retry_reason_drift_report(
    rows: list[dict[str, Any]],
    *,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    current_days: int = DEFAULT_CURRENT_DAYS,
    min_delta: float = DEFAULT_MIN_DELTA,
    limit: int = DEFAULT_LIMIT,
    min_sample: int = DEFAULT_MIN_SAMPLE,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now: Any = None,
) -> dict[str, Any]:
    """Build a read-only drift report for retry reasons by platform."""

    positive("baseline_days", baseline_days)
    positive("current_days", current_days)
    positive("limit", limit)
    positive("min_sample", min_sample)
    bounded_share("min_delta", min_delta)

    generated_at = now_value(now)
    current_start = generated_at - timedelta(days=current_days)
    baseline_start = current_start - timedelta(days=baseline_days)
    buckets: dict[str, dict[str, Counter[str]]] = defaultdict(
        lambda: {"baseline": Counter(), "current": Counter()}
    )
    rows_scanned = 0
    rows_in_window = 0

    for row in rows:
        rows_scanned += 1
        attempted_at = dt(row.get("attempted_at") or row.get("created_at"))
        if attempted_at is None or attempted_at < baseline_start or attempted_at > generated_at:
            continue
        platform = _platform(row)
        reason = _reason(row)
        rows_in_window += 1
        if attempted_at < current_start:
            buckets[platform]["baseline"][reason] += 1
        else:
            buckets[platform]["current"][reason] += 1

    drift_rows: list[dict[str, Any]] = []
    findings: list[dict[str, Any]] = []
    for platform in sorted(buckets):
        baseline_counts = buckets[platform]["baseline"]
        current_counts = buckets[platform]["current"]
        baseline_total = sum(baseline_counts.values())
        current_total = sum(current_counts.values())
        for reason in sorted(set(baseline_counts) | set(current_counts)):
            baseline_count = baseline_counts[reason]
            current_count = current_counts[reason]
            baseline_share = _share(baseline_count, baseline_total)
            current_share = _share(current_count, current_total)
            delta_share = round(current_share - baseline_share, 4)
            row = {
                "platform": platform,
                "reason": reason,
                "baseline_count": baseline_count,
                "current_count": current_count,
                "delta_count": current_count - baseline_count,
                "baseline_share": baseline_share,
                "current_share": current_share,
                "delta_share": delta_share,
                "baseline_total": baseline_total,
                "current_total": current_total,
            }
            drift_rows.append(row)
            if (
                baseline_total >= min_sample
                and current_total >= min_sample
                and abs(delta_share) >= min_delta
            ):
                findings.append(
                    {
                        **row,
                        "direction": "increased" if delta_share > 0 else "decreased",
                        "severity": _severity(delta_share, current_count),
                    }
                )

    findings.sort(
        key=lambda item: (
            -abs(item["delta_share"]),
            -item["current_count"],
            item["platform"],
            item["reason"],
        )
    )

    schema_gap = bool(missing_tables or missing_columns)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": generated_at.isoformat(),
        "filters": {
            "baseline_days": baseline_days,
            "current_days": current_days,
            "min_delta": min_delta,
            "min_sample": min_sample,
            "limit": limit,
            "baseline_start": baseline_start.isoformat(),
            "current_start": current_start.isoformat(),
        },
        "totals": {
            "rows_scanned": rows_scanned,
            "rows_in_window": rows_in_window,
            "drift_rows": len(drift_rows),
            "findings": len(findings),
        },
        "drift_rows": drift_rows,
        "findings": findings[:limit],
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            key: sorted(value)
            for key, value in sorted((missing_columns or {}).items())
        },
        "empty_state": empty_state(
            findings,
            "No publication attempt retry reason drift found.",
            schema_gap=schema_gap,
        ),
    }


def build_publication_attempt_retry_reason_drift_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    """Load retry attempt rows from SQLite and build the drift report."""

    conn = connection(db_or_conn)
    db_schema = schema(conn)
    table, missing_columns = _select_source_table(db_schema)
    if table is None:
        missing_tables = (
            []
            if any(name in db_schema for name in SOURCE_TABLES)
            else ["publication_attempts|publication_retries"]
        )
        return build_publication_attempt_retry_reason_drift_report(
            [],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )

    return build_publication_attempt_retry_reason_drift_report(
        _load_rows(conn, table, db_schema[table]),
        missing_tables=[],
        missing_columns={},
        **kwargs,
    )


def format_publication_attempt_retry_reason_drift_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_publication_attempt_retry_reason_drift_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Attempt Retry Reason Drift",
        f"Generated: {report['generated_at']}",
        (
            "Window: "
            f"baseline={report['filters']['baseline_days']}d "
            f"current={report['filters']['current_days']}d "
            f"min_delta={report['filters']['min_delta']}"
        ),
        (
            "Totals: "
            f"rows={report['totals']['rows_scanned']} "
            f"in_window={report['totals']['rows_in_window']} "
            f"drift_rows={report['totals']['drift_rows']} "
            f"findings={report['totals']['findings']}"
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.extend(["", "platform | reason | baseline | current | delta"])
    for finding in report["findings"]:
        lines.append(
            f"{finding['platform']} | {finding['reason']} | "
            f"{finding['baseline_share']:.4f} ({finding['baseline_count']}) | "
            f"{finding['current_share']:.4f} ({finding['current_count']}) | "
            f"{finding['delta_share']:.4f}"
        )
    return "\n".join(lines)


def _select_source_table(db_schema: dict[str, set[str]]) -> tuple[str | None, dict[str, list[str]]]:
    missing_columns: dict[str, list[str]] = {}
    for table in SOURCE_TABLES:
        cols = db_schema.get(table)
        if not cols:
            continue
        missing = []
        if not ({"attempted_at", "created_at"} & cols):
            missing.append("attempted_at|created_at")
        if "platform" not in cols:
            missing.append("platform")
        if missing:
            missing_columns[table] = missing
            continue
        return table, {}
    return None, missing_columns


def _load_rows(conn: Any, table: str, cols: set[str]) -> list[dict[str, Any]]:
    selected = [
        _pick(cols, "attempted_at", "created_at", out="attempted_at"),
        _pick(cols, "platform", out="platform"),
        _pick(cols, "retry_reason", "reason", out="retry_reason"),
        _pick(cols, "error_category", out="error_category"),
        _pick(cols, "status", out="status"),
    ]
    order = "attempted_at" if "attempted_at" in cols else "created_at"
    sql = f"SELECT {', '.join(selected)} FROM {table} ORDER BY {order}, rowid"
    return [dict(row) for row in conn.execute(sql)]


def _pick(cols: set[str], *names: str, out: str) -> str:
    for name in names:
        if name in cols:
            return f"{name} AS {out}"
    return f"NULL AS {out}"


def _platform(row: dict[str, Any]) -> str:
    return clean(row.get("platform"), "unknown").lower()


def _reason(row: dict[str, Any]) -> str:
    return clean(row.get("retry_reason") or row.get("error_category") or row.get("status"), "unknown").lower()


def _share(count: int, total: int) -> float:
    return round(count / total, 4) if total else 0.0


def _severity(delta_share: float, current_count: int) -> float:
    return round(abs(delta_share) * 100 + current_count, 2)
