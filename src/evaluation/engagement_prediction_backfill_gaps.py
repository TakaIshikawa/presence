"""Find engagement predictions that missed post-engagement backfill."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 14
DEFAULT_MAX_ERROR_DELTA = 0.01
ISSUE_TYPES = (
    "missing_content",
    "metrics_available_not_backfilled",
    "stale_without_metrics",
    "prediction_error_mismatch",
)


def build_engagement_prediction_backfill_gaps_report(
    rows: list[dict[str, Any]],
    *,
    days: int = DEFAULT_DAYS,
    max_error_delta: float = DEFAULT_MAX_ERROR_DELTA,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    if days <= 0:
        raise ValueError("days must be positive")
    if max_error_delta < 0:
        raise ValueError("max_error_delta must be non-negative")
    generated_at = _utc(now or datetime.now(timezone.utc))
    cutoff = generated_at - timedelta(days=days)
    findings: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    affected: set[int] = set()

    for row in rows:
        base = {
            "prediction_id": row.get("prediction_id"),
            "content_id": row.get("content_id"),
            "predicted_engagement_score": _float(row.get("predicted_engagement_score")),
            "actual_engagement_score": _float(row.get("actual_engagement_score")),
            "prediction_error": _float(row.get("prediction_error")),
            "content_status": row.get("content_status"),
            "content_type": row.get("content_type"),
            "platform": row.get("platform"),
            "metrics_score": _float(row.get("metrics_score")),
            "prediction_created_at": row.get("prediction_created_at"),
            "metrics_recorded_at": row.get("metrics_recorded_at"),
        }
        content_id = _to_int(row.get("content_id"))
        if row.get("resolved_content_id") is None:
            _record(findings, counts, affected, {**base, "issue_type": "missing_content", "detail": "prediction content_id has no generated_content row"})
            continue

        has_metrics = base["metrics_score"] is not None
        published_x = _is_published_x(row)
        if has_metrics and published_x and (base["actual_engagement_score"] is None or base["prediction_error"] is None):
            _record(findings, counts, affected, {**base, "issue_type": "metrics_available_not_backfilled", "detail": "latest post_engagement metrics exist but prediction backfill fields are missing"})
        if has_metrics and base["prediction_error"] is not None and base["predicted_engagement_score"] is not None:
            expected = abs(base["metrics_score"] - base["predicted_engagement_score"])
            delta = abs(base["prediction_error"] - expected)
            if delta > max_error_delta:
                _record(findings, counts, affected, {**base, "issue_type": "prediction_error_mismatch", "expected_prediction_error": round(expected, 6), "error_delta": round(delta, 6), "detail": "prediction_error does not match latest actual minus predicted score"})
        created_at = _parse_time(row.get("prediction_created_at"))
        if not has_metrics and created_at is not None and created_at < cutoff:
            _record(findings, counts, affected, {**base, "issue_type": "stale_without_metrics", "age_days": round((generated_at - created_at).total_seconds() / 86400, 2), "detail": "prediction is older than the stale threshold and has no post_engagement metrics"})
        if content_id is not None and any(f.get("content_id") == content_id for f in findings):
            affected.add(content_id)

    findings.sort(key=_finding_sort_key)
    return {
        "artifact_type": "engagement_prediction_backfill_gaps",
        "generated_at": generated_at.isoformat(),
        "filters": {"days": days, "max_error_delta": max_error_delta},
        "totals": {
            "prediction_count": len(rows),
            "finding_count": len(findings),
            "issue_counts": {issue: counts[issue] for issue in ISSUE_TYPES},
            "affected_content_ids": sorted(affected),
        },
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {
            table: sorted(columns)
            for table, columns in sorted((missing_columns or {}).items())
            if columns
        },
        "findings": findings,
    }


def build_engagement_prediction_backfill_gaps_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {
        "engagement_predictions": {"id", "content_id"},
        "generated_content": {"id"},
        "post_engagement": {"content_id"},
    }
    missing_tables = sorted(table for table in required if table not in schema)
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    if missing_tables or missing_columns:
        return build_engagement_prediction_backfill_gaps_report(
            [],
            missing_tables=missing_tables,
            missing_columns=missing_columns,
            **kwargs,
        )
    return build_engagement_prediction_backfill_gaps_report(
        _load_rows(conn, schema),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
        **kwargs,
    )


def format_engagement_prediction_backfill_gaps_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_engagement_prediction_backfill_gaps_text(report: dict[str, Any]) -> str:
    totals = report["totals"]
    lines = [
        "Engagement Prediction Backfill Gaps",
        f"Generated: {report['generated_at']}",
        f"Stale after: {report['filters']['days']} days",
        f"Max error delta: {report['filters']['max_error_delta']:g}",
        f"Totals: predictions={totals['prediction_count']} findings={totals['finding_count']} affected_content={len(totals['affected_content_ids'])}",
        "Issue counts: " + ", ".join(f"{issue}={totals['issue_counts'][issue]}" for issue in ISSUE_TYPES),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + ", ".join(f"{table}.{column}" for table, columns in report["missing_columns"].items() for column in columns)
        )
    if not report["findings"]:
        lines.append("No engagement prediction backfill gaps found.")
        return "\n".join(lines)
    lines.extend(["", "Findings:"])
    for finding in report["findings"]:
        lines.append(
            f"- {finding['issue_type']} prediction_id={finding.get('prediction_id')} "
            f"content_id={finding.get('content_id')} metrics_score={_fmt(finding.get('metrics_score'))} "
            f"detail={finding['detail']}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    ep = schema["engagement_predictions"]
    gc = schema["generated_content"]
    pe = schema["post_engagement"]
    predicted = _col(ep, "predicted_engagement_score", "predicted_score", "score", fallback="NULL", alias="ep")
    actual = _col(ep, "actual_engagement_score", "actual_score", fallback="NULL", alias="ep")
    error = _col(ep, "prediction_error", "error", fallback="NULL", alias="ep")
    created = _col(ep, "created_at", "predicted_at", fallback="NULL", alias="ep")
    status = _col(gc, "status", "publication_status", fallback="NULL", alias="gc")
    content_type = _col(gc, "content_type", "type", fallback="NULL", alias="gc")
    platform = _col(gc, "platform", "channel", fallback="'x'", alias="gc")
    metrics_score = _col(pe, "engagement_score", "actual_engagement_score", "score", fallback="NULL", alias="pe")
    recorded_at = _col(pe, "recorded_at", "fetched_at", "created_at", fallback="NULL", alias="pe")
    latest_join = "pe.content_id = ep.content_id"
    if recorded_at != "NULL":
        latest_join += f" AND NOT EXISTS (SELECT 1 FROM post_engagement newer WHERE newer.content_id = pe.content_id AND datetime({_col(pe, 'recorded_at', 'fetched_at', 'created_at', fallback='NULL', alias='newer')}) > datetime({recorded_at}))"
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT ep.id AS prediction_id, ep.content_id, gc.id AS resolved_content_id,
                       {predicted} AS predicted_engagement_score,
                       {actual} AS actual_engagement_score,
                       {error} AS prediction_error,
                       {created} AS prediction_created_at,
                       {status} AS content_status,
                       {content_type} AS content_type,
                       {platform} AS platform,
                       {metrics_score} AS metrics_score,
                       {recorded_at} AS metrics_recorded_at
                FROM engagement_predictions ep
                LEFT JOIN generated_content gc ON gc.id = ep.content_id
                LEFT JOIN post_engagement pe ON {latest_join}
                ORDER BY ep.id ASC"""
        ).fetchall()
    ]


def _record(findings: list[dict[str, Any]], counts: Counter[str], affected: set[int], finding: dict[str, Any]) -> None:
    findings.append(finding)
    counts[finding["issue_type"]] += 1
    content_id = _to_int(finding.get("content_id"))
    if content_id is not None:
        affected.add(content_id)


def _is_published_x(row: dict[str, Any]) -> bool:
    status = _clean(row.get("content_status")).lower()
    platform = _clean(row.get("platform")).lower()
    content_type = _clean(row.get("content_type")).lower()
    return status in {"published", "posted", "sent"} and (platform in {"", "x", "twitter"} or content_type in {"x", "tweet", "thread"})


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")} for row in rows}


def _col(columns: set[str], *names: str, fallback: str, alias: str) -> str:
    for name in names:
        if name in columns:
            return f"{alias}.{name}"
    return fallback


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _utc(parsed)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _float(value: Any) -> float | None:
    try:
        return None if value in (None, "") else float(value)
    except (TypeError, ValueError):
        return None


def _to_int(value: Any) -> int | None:
    try:
        return None if value in (None, "") else int(value)
    except (TypeError, ValueError):
        return None


def _clean(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _fmt(value: Any) -> str:
    return "-" if value is None else f"{value:g}"


def _finding_sort_key(finding: dict[str, Any]) -> tuple[Any, ...]:
    return (ISSUE_TYPES.index(finding["issue_type"]), _to_int(finding.get("content_id")) or 0, _to_int(finding.get("prediction_id")) or 0)
