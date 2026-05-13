"""Compare pipeline run scores against the preceding equal window."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_WINDOW_DAYS = 7
DEFAULT_MIN_RUNS = 3
DEFAULT_LIMIT = 20
REQUIRED_COLUMNS = {
    "batch_id",
    "content_type",
    "final_score",
    "rejection_reason",
    "created_at",
}


def build_pipeline_score_regressions_report(
    db_or_conn: Any,
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_runs: int = DEFAULT_MIN_RUNS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return content types whose score or rejection rate regressed."""

    if window_days <= 0:
        raise ValueError("window_days must be positive")
    if min_runs <= 0:
        raise ValueError("min_runs must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    current_start = generated_at - timedelta(days=window_days)
    previous_start = current_start - timedelta(days=window_days)
    filters = {
        "window_days": window_days,
        "min_runs": min_runs,
        "limit": limit,
        "previous_window_start": previous_start.isoformat(),
        "current_window_start": current_start.isoformat(),
        "window_end": generated_at.isoformat(),
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "pipeline_runs" not in schema:
        return _empty_report(generated_at, filters, ["pipeline_runs"], {})

    missing_required = sorted(REQUIRED_COLUMNS - schema["pipeline_runs"])
    if missing_required:
        return _empty_report(
            generated_at,
            filters,
            [],
            {"pipeline_runs": missing_required},
        )

    rows = _pipeline_rows(
        conn,
        previous_start=previous_start,
        current_start=current_start,
        window_end=generated_at,
    )
    groups = _groups(rows, current_start=current_start)
    items = [
        group
        for group in groups
        if group["current_run_count"] >= min_runs
        and group["previous_run_count"] >= min_runs
        and (
            (group["score_delta"] is not None and group["score_delta"] < 0)
            or group["rejection_rate_delta"] > 0
        )
    ]
    items.sort(
        key=lambda item: (
            item["score_delta"] if item["score_delta"] is not None else 0,
            -item["rejection_rate_delta"],
            str(item["content_type"]),
        )
    )

    return {
        "artifact_type": "pipeline_score_regressions",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": _totals(groups, items),
        "groups": groups,
        "items": items[:limit],
        "missing_tables": [],
        "missing_columns": {},
    }


def format_pipeline_score_regressions_json(report: dict[str, Any]) -> str:
    """Render deterministic JSON for automation."""

    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_score_regressions_text(report: dict[str, Any]) -> str:
    """Render a compact operational summary."""

    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Pipeline Score Regressions",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: window_days={filters['window_days']} "
            f"min_runs={filters['min_runs']} limit={filters['limit']}"
        ),
        (
            f"Totals: current_runs={totals['current_runs']} "
            f"previous_runs={totals['previous_runs']} groups={totals['groups']} "
            f"regressions={totals['regressions']}"
        ),
    ]
    if report.get("missing_tables"):
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report.get("missing_columns"):
        formatted = [
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report["missing_columns"].items())
        ]
        lines.append("Missing columns: " + "; ".join(formatted))
    if not report["groups"]:
        lines.append("No pipeline run groups matched the comparison windows.")
        return "\n".join(lines)
    if not report["items"]:
        lines.append("No score regressions met the thresholds.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Regressions:")
    for item in report["items"]:
        lines.append(
            f"- {item['content_type']} runs={item['previous_run_count']}->{item['current_run_count']} "
            f"score={_fmt(item['previous_average_final_score'])}->{_fmt(item['current_average_final_score'])} "
            f"delta={_fmt(item['score_delta'])} rejection={_fmt_pct(item['previous_rejection_rate'])}->"
            f"{_fmt_pct(item['current_rejection_rate'])} delta={_fmt_pct(item['rejection_rate_delta'])} "
            f"latest={item['latest_created_at'] or '-'} batches={','.join(item['latest_batch_ids']) or '-'}"
        )
    return "\n".join(lines)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    try:
        tables = conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        ).fetchall()
    except sqlite3.Error:
        return {}
    return {str(row[0]): _table_columns(conn, str(row[0])) for row in tables}


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _empty_report(
    generated_at: datetime,
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "pipeline_score_regressions",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "current_runs": 0,
            "previous_runs": 0,
            "groups": 0,
            "regressions": 0,
        },
        "groups": [],
        "items": [],
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def _pipeline_rows(
    conn: sqlite3.Connection,
    *,
    previous_start: datetime,
    current_start: datetime,
    window_end: datetime,
) -> list[dict[str, Any]]:
    query = (
        "SELECT batch_id, content_type, final_score, rejection_reason, created_at "
        "FROM pipeline_runs "
        "WHERE datetime(created_at) >= datetime(?) "
        "AND datetime(created_at) <= datetime(?) "
        "ORDER BY datetime(created_at) DESC, batch_id ASC"
    )
    cursor = conn.execute(query, (previous_start.isoformat(), window_end.isoformat()))
    names = [description[0] for description in cursor.description]
    rows = [_row_to_dict(row, names) for row in cursor.fetchall()]
    for row in rows:
        created = _parse_ts(row.get("created_at"))
        row["window"] = "current" if created and created >= current_start else "previous"
    return rows


def _groups(rows: list[dict[str, Any]], *, current_start: datetime) -> list[dict[str, Any]]:
    del current_start
    buckets: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for row in rows:
        content_type = str(row.get("content_type") or "unknown")
        window = str(row.get("window") or "previous")
        buckets.setdefault(content_type, {"current": [], "previous": []})[window].append(row)

    groups = []
    for content_type, bucket in buckets.items():
        current = bucket["current"]
        previous = bucket["previous"]
        current_avg = _average(row.get("final_score") for row in current)
        previous_avg = _average(row.get("final_score") for row in previous)
        current_rejection = _rejection_rate(current)
        previous_rejection = _rejection_rate(previous)
        groups.append(
            {
                "content_type": content_type,
                "current_run_count": len(current),
                "previous_run_count": len(previous),
                "current_average_final_score": current_avg,
                "previous_average_final_score": previous_avg,
                "score_delta": _delta(current_avg, previous_avg),
                "current_rejection_rate": current_rejection,
                "previous_rejection_rate": previous_rejection,
                "rejection_rate_delta": round(current_rejection - previous_rejection, 4),
                "latest_batch_ids": [
                    str(row["batch_id"])
                    for row in current[:5]
                    if row.get("batch_id") is not None
                ],
                "latest_created_at": current[0].get("created_at") if current else None,
            }
        )
    groups.sort(key=lambda group: str(group["content_type"]))
    return groups


def _totals(groups: list[dict[str, Any]], items: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "current_runs": sum(int(group["current_run_count"]) for group in groups),
        "previous_runs": sum(int(group["previous_run_count"]) for group in groups),
        "groups": len(groups),
        "regressions": len(items),
    }


def _average(values: Any) -> float | None:
    numbers = [_float_or_none(value) for value in values]
    numbers = [value for value in numbers if value is not None]
    if not numbers:
        return None
    return round(sum(numbers) / len(numbers), 4)


def _delta(current: float | None, previous: float | None) -> float | None:
    if current is None or previous is None:
        return None
    return round(current - previous, 4)


def _rejection_rate(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    rejected = sum(1 for row in rows if str(row.get("rejection_reason") or "").strip())
    return round(rejected / len(rows), 4)


def _row_to_dict(row: Any, names: list[str]) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    return dict(zip(names, row))


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
    except ValueError:
        return None


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _fmt(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, float):
        return f"{value:.4g}"
    return str(value)


def _fmt_pct(value: Any) -> str:
    if value is None:
        return "-"
    return f"{float(value) * 100:.1f}%"
