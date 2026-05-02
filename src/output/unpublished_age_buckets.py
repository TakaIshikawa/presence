"""Bucket stale unpublished generated content by age."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any, Iterable


DEFAULT_THRESHOLDS_HOURS = (24.0, 72.0, 168.0, 336.0)
DEFAULT_MIN_AGE_HOURS = 24.0
UNPUBLISHED_STATUSES = {"unpublished", "queued", "held", "review", "pending_review"}
PUBLISHED_STATUSES = {"published", "posted"}
IGNORED_STATUSES = {"abandoned", "cancelled", "dismissed"}


def build_unpublished_age_bucket_report(
    db_or_rows: Any,
    *,
    thresholds_hours: Iterable[float] = DEFAULT_THRESHOLDS_HOURS,
    min_age_hours: float = DEFAULT_MIN_AGE_HOURS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return unpublished/review content grouped into deterministic age buckets."""
    thresholds = _validate_thresholds(tuple(thresholds_hours))
    if min_age_hours < 0:
        raise ValueError("min_age_hours must be non-negative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _maybe_connection(db_or_rows)
    missing_tables: list[str] = []
    missing_columns: dict[str, list[str]] = {}
    if conn is not None:
        schema = _schema(conn)
        missing_tables, missing_columns = _schema_gaps(schema)
        raw_rows = [] if missing_tables or missing_columns else _load_rows(conn, schema)
    else:
        raw_rows = [dict(row) for row in db_or_rows]

    classified = [
        row
        for row in (
            _classify_row(row, now=generated_at, min_age_hours=min_age_hours, thresholds=thresholds)
            for row in raw_rows
        )
        if row is not None
    ]
    classified.sort(key=lambda row: (-row["bucket_threshold_hours"], row["created_at"] or "", row["content_id"]))

    buckets_by_label: dict[str, dict[str, Any]] = {}
    for row in classified:
        label = row["bucket"]
        bucket = buckets_by_label.setdefault(
            label,
            {
                "label": label,
                "threshold_hours": row["bucket_threshold_hours"],
                "count": 0,
                "newest_created_at": None,
                "oldest_created_at": None,
                "records": [],
            },
        )
        bucket["count"] += 1
        created_at = row["created_at"]
        if created_at is not None:
            if bucket["newest_created_at"] is None or created_at > bucket["newest_created_at"]:
                bucket["newest_created_at"] = created_at
            if bucket["oldest_created_at"] is None or created_at < bucket["oldest_created_at"]:
                bucket["oldest_created_at"] = created_at
        bucket["records"].append(row)

    buckets = sorted(
        buckets_by_label.values(),
        key=lambda bucket: (-bucket["threshold_hours"], bucket["label"]),
    )
    by_status = Counter(row["status"] for row in classified)
    by_bucket = {bucket["label"]: bucket["count"] for bucket in buckets}

    return {
        "artifact_type": "unpublished_age_buckets",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "min_age_hours": min_age_hours,
            "thresholds_hours": list(thresholds),
        },
        "counts": {
            "rows_scanned": len(raw_rows),
            "records": len(classified),
            "by_bucket": by_bucket,
            "by_status": dict(sorted(by_status.items())),
        },
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "buckets": buckets,
    }


def format_unpublished_age_bucket_json(report: dict[str, Any]) -> str:
    """Render an unpublished age bucket report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_unpublished_age_bucket_markdown(report: dict[str, Any]) -> str:
    """Render an unpublished age bucket report as a markdown table."""
    lines = [
        "# Unpublished Content Age Buckets",
        "",
        f"Generated: `{report['generated_at']}`",
        "",
    ]
    if report["missing_tables"]:
        lines.extend([f"Missing tables: {', '.join(report['missing_tables'])}", ""])
    if report["missing_columns"]:
        missing = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in report["missing_columns"].items()
        )
        lines.extend([f"Missing columns: {missing}", ""])

    lines.extend(
        [
            "| Bucket | Count | Newest | Oldest | Statuses | Reasons |",
            "| --- | ---: | --- | --- | --- | --- |",
        ]
    )
    if not report["buckets"]:
        lines.append("| none | 0 |  |  |  |  |")
        return "\n".join(lines)

    for bucket in report["buckets"]:
        statuses = Counter(record["status"] for record in bucket["records"])
        reasons = Counter(_markdown_reason(record) for record in bucket["records"])
        lines.append(
            "| "
            + " | ".join(
                [
                    _escape_md(bucket["label"]),
                    str(bucket["count"]),
                    _escape_md(bucket["newest_created_at"] or ""),
                    _escape_md(bucket["oldest_created_at"] or ""),
                    _escape_md(_counter_summary(statuses)),
                    _escape_md(_counter_summary(reasons)),
                ]
            )
            + " |"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    generated_columns = schema["generated_content"]
    queue_states = _state_rows(conn, schema, "publish_queue")
    publication_states = _state_rows(conn, schema, "content_publications")
    attempt_content_ids = _attempt_content_ids(conn, schema)

    select_columns = [
        "id",
        _column_expr(generated_columns, "content_type"),
        _column_expr(generated_columns, "created_at"),
        _column_expr(generated_columns, "published", "0"),
        _column_expr(generated_columns, "published_at"),
        _column_expr(generated_columns, "published_url"),
        _column_expr(generated_columns, "eval_feedback"),
        _column_expr(generated_columns, "curation_quality"),
        _column_expr(generated_columns, "auto_quality"),
    ]
    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT {", ".join(select_columns)}
                FROM generated_content
                ORDER BY created_at ASC, id ASC"""
        ).fetchall()
    ]
    loaded: list[dict[str, Any]] = []
    for row in rows:
        content_id = _int_value(row.get("id"))
        if content_id is None or content_id in attempt_content_ids:
            continue
        row["queue_status"] = _combined_status(queue_states.get(content_id, []))
        row["publication_status"] = _combined_status(publication_states.get(content_id, []))
        row["queue_reasons"] = _combined_reasons(queue_states.get(content_id, []))
        row["publication_reasons"] = _combined_reasons(publication_states.get(content_id, []))
        loaded.append(row)
    return loaded


def _state_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    table: str,
) -> dict[int, list[dict[str, Any]]]:
    columns = schema.get(table)
    if not columns or not {"content_id", "status"}.issubset(columns):
        return {}
    reason_columns = [column for column in ("hold_reason", "error", "error_category") if column in columns]
    select_columns = ["content_id", "status", *reason_columns]
    states: dict[int, list[dict[str, Any]]] = {}
    for row in conn.execute(
        f"""SELECT {", ".join(select_columns)}
            FROM {table}
            WHERE content_id IS NOT NULL
            ORDER BY content_id ASC, id ASC"""
    ).fetchall():
        content_id = _int_value(row["content_id"])
        if content_id is not None:
            states.setdefault(content_id, []).append(dict(row))
    return states


def _attempt_content_ids(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> set[int]:
    columns = schema.get("publication_attempts")
    if not columns or "content_id" not in columns:
        return set()
    return {
        content_id
        for content_id in (
            _int_value(row["content_id"])
            for row in conn.execute(
                """SELECT DISTINCT content_id
                   FROM publication_attempts
                   WHERE content_id IS NOT NULL"""
            ).fetchall()
        )
        if content_id is not None
    }


def _classify_row(
    row: dict[str, Any],
    *,
    now: datetime,
    min_age_hours: float,
    thresholds: tuple[float, ...],
) -> dict[str, Any] | None:
    created = _parse_datetime(row.get("created_at") or row.get("generated_at"))
    if created is None:
        return None
    age_hours = max(0.0, (now - created).total_seconds() / 3600)
    if age_hours < min_age_hours:
        return None

    status = _row_status(row)
    if _status_contains(status, PUBLISHED_STATUSES) or _status_contains(status, IGNORED_STATUSES):
        return None
    if not _status_contains(status, UNPUBLISHED_STATUSES):
        return None

    threshold = _bucket_threshold(age_hours, thresholds)
    if threshold is None:
        return None

    content_id = _int_value(row.get("content_id") or row.get("id"))
    return {
        "content_id": content_id,
        "content_type": _clean(row.get("content_type")),
        "created_at": created.isoformat(),
        "age_hours": round(age_hours, 2),
        "bucket": _bucket_label(threshold),
        "bucket_threshold_hours": threshold,
        "status": status,
        "reason": _row_reason(row),
        "publication_status": _clean(row.get("publication_status")),
        "queue_status": _clean(row.get("queue_status")),
        "curation_quality": _clean(row.get("curation_quality")),
    }


def _row_status(row: dict[str, Any]) -> str:
    explicit = _status(row.get("status"))
    if explicit:
        return explicit
    publication_status = _status(row.get("publication_status"))
    queue_status = _status(row.get("queue_status"))
    if publication_status in PUBLISHED_STATUSES or _legacy_published(row):
        return "published"
    active_statuses = sorted(
        {
            status
            for status in (queue_status, publication_status)
            if status in {"queued", "held", "review", "pending_review"}
        }
    )
    if len(active_statuses) > 1:
        return "mixed:" + ",".join(active_statuses)
    if active_statuses:
        return active_statuses[0]
    if _status(row.get("curation_quality")) in {"review", "pending_review"}:
        return "review"
    published = row.get("published")
    if isinstance(published, str):
        lowered = published.strip().lower()
        if lowered in {"1", "true", "yes", "published"}:
            return "published"
        if lowered in {"-1", "abandoned", "cancelled", "dismissed"}:
            return "abandoned"
    elif published:
        return "published"
    return "unpublished"


def _row_reason(row: dict[str, Any]) -> str | None:
    for key in (
        "reason",
        "hold_reason",
        "queue_reasons",
        "publication_reasons",
        "error",
        "error_category",
        "eval_feedback",
    ):
        value = _clean(row.get(key))
        if value:
            return value
    return None


def _bucket_threshold(age_hours: float, thresholds: tuple[float, ...]) -> float | None:
    matched = [threshold for threshold in thresholds if age_hours >= threshold]
    return matched[-1] if matched else None


def _bucket_label(threshold_hours: float) -> str:
    if threshold_hours % 24 == 0:
        days = threshold_hours / 24
        if days >= 1:
            return f"{days:g}d"
    return f"{threshold_hours:g}h"


def _validate_thresholds(thresholds: tuple[float, ...]) -> tuple[float, ...]:
    if not thresholds:
        raise ValueError("thresholds_hours must not be empty")
    parsed = tuple(float(threshold) for threshold in thresholds)
    if any(threshold <= 0 for threshold in parsed):
        raise ValueError("thresholds_hours values must be positive")
    return tuple(sorted(dict.fromkeys(parsed)))


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {"generated_content": {"id", "created_at", "published"}}
    missing_tables = [table for table in sorted(required) if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = str(row["name"] if isinstance(row, sqlite3.Row) else row[0])
        schema[table] = {
            str(info["name"] if isinstance(info, sqlite3.Row) else info[1])
            for info in conn.execute(f"PRAGMA table_info({_quote_identifier(table)})")
        }
    return schema


def _maybe_connection(db_or_rows: Any) -> sqlite3.Connection | None:
    conn = getattr(db_or_rows, "conn", db_or_rows)
    if isinstance(conn, sqlite3.Connection):
        conn.row_factory = sqlite3.Row
        return conn
    return None


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    return column if column in columns else f"{fallback} AS {column}"


def _parse_datetime(value: Any) -> datetime | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        return _ensure_utc(datetime.fromisoformat(cleaned))
    except ValueError:
        return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _legacy_published(row: dict[str, Any]) -> bool:
    return bool(row.get("published_url") or row.get("published_at"))


def _combined_status(states: list[dict[str, Any]]) -> str:
    statuses = sorted(dict.fromkeys(_status(state.get("status")) for state in states if _status(state.get("status"))))
    if not statuses:
        return "none"
    if len(statuses) == 1:
        return statuses[0]
    if any(status in PUBLISHED_STATUSES for status in statuses):
        return "published"
    return "mixed:" + ",".join(statuses)


def _combined_reasons(states: list[dict[str, Any]]) -> str | None:
    reasons: list[str] = []
    for state in states:
        for key in ("hold_reason", "error", "error_category"):
            reason = _clean(state.get(key))
            if reason:
                reasons.append(reason)
                break
    return "; ".join(dict.fromkeys(reasons)) or None


def _status(value: Any) -> str | None:
    cleaned = _clean(value)
    if not cleaned:
        return None
    return cleaned.lower().replace("-", "_").replace(" ", "_")


def _status_contains(status: str, values: set[str]) -> bool:
    parts = status.removeprefix("mixed:").split(",")
    return any(part in values for part in parts)


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _int_value(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _counter_summary(counter: Counter[str]) -> str:
    return ", ".join(f"{key}={counter[key]}" for key in sorted(counter))


def _markdown_reason(record: dict[str, Any]) -> str:
    return record.get("reason") or "none"


def _escape_md(value: str) -> str:
    return value.replace("|", "\\|")


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'
