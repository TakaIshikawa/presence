"""Find upcoming publish windows that need unpublished content backfill."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS_AHEAD = 7
DEFAULT_MIN_SCORE = 7.0
DEFAULT_LIMIT = 10
HISTORY_DAYS = 90
DAY_NAMES = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
PLATFORM_CONTENT_TYPES = {
    "x": {"x_post", "x_thread", "x_visual"},
    "bluesky": {"x_post", "x_thread", "x_visual", "bluesky_post"},
}


def build_publish_window_backfill_opportunity_report(
    db_or_conn: Any,
    *,
    days_ahead: int = DEFAULT_DAYS_AHEAD,
    min_score: float = DEFAULT_MIN_SCORE,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only report of upcoming windows without suitable queued content."""
    if days_ahead <= 0:
        raise ValueError("days_ahead must be positive")
    if min_score < 0:
        raise ValueError("min_score must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    horizon_end = generated_at + timedelta(days=days_ahead)
    filters = {
        "days_ahead": days_ahead,
        "history_days": HISTORY_DAYS,
        "horizon_end": horizon_end.isoformat(),
        "horizon_start": generated_at.isoformat(),
        "limit": limit,
        "min_score": min_score,
    }
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    availability = {
        "candidate_content_available": "generated_content" in schema,
        "publish_queue_available": "publish_queue" in schema,
        "publication_history_available": "content_publications" in schema,
    }
    if "publish_queue" in missing_tables:
        return _empty_report(
            generated_at=generated_at,
            filters=filters,
            availability=availability,
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    windows = _upcoming_windows(
        conn,
        schema,
        now=generated_at,
        horizon_end=horizon_end,
    )
    queued = _queued_rows(conn, schema, now=generated_at, horizon_end=horizon_end)
    candidates = _candidate_rows(conn, schema, min_score=min_score, now=generated_at)
    opportunities = []
    for window in windows:
        queued_for_window = _queued_for_window(window, queued)
        suitable = [
            row
            for row in queued_for_window
            if _is_suitable_queued(row, window=window, min_score=min_score)
        ]
        if suitable:
            continue
        recommended = _rank_candidates(
            candidates,
            window=window,
            limit=limit,
            now=generated_at,
        )
        reason = "empty_window" if not queued_for_window else "underfilled_window"
        opportunities.append(
            {
                "window": window,
                "reason": reason,
                "queued_count": len(queued_for_window),
                "suitable_queued_count": len(suitable),
                "recommended_content": recommended,
            }
        )

    opportunities.sort(
        key=lambda item: (
            item["window"]["start_time"],
            item["window"]["platform"],
            item["window"].get("content_type") or "",
        )
    )
    opportunities = opportunities[:limit]
    reason_counts = Counter(item["reason"] for item in opportunities)
    return {
        "artifact_type": "publish_window_backfill_opportunities",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "availability": availability,
        "totals": {
            "candidate_count": len(candidates),
            "opportunity_count": len(opportunities),
            "queued_count": len(queued),
            "window_count": len(windows),
            "by_reason": {
                "empty_window": reason_counts.get("empty_window", 0),
                "underfilled_window": reason_counts.get("underfilled_window", 0),
            },
        },
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "opportunities": opportunities,
    }


def format_publish_window_backfill_opportunities_json(report: dict[str, Any]) -> str:
    """Serialize the backfill report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_window_backfill_opportunities_text(report: dict[str, Any]) -> str:
    """Render the backfill report for operator review."""
    filters = report["filters"]
    totals = report["totals"]
    lines = [
        "Publish Window Backfill Opportunities",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days_ahead={filters['days_ahead']} "
            f"min_score={filters['min_score']} limit={filters['limit']}"
        ),
        (
            f"Totals: windows={totals['window_count']} queued={totals['queued_count']} "
            f"candidates={totals['candidate_count']} opportunities={totals['opportunity_count']} "
            f"empty={totals['by_reason']['empty_window']} "
            f"underfilled={totals['by_reason']['underfilled_window']}"
        ),
        (
            "Availability: "
            + " ".join(
                f"{key}={int(value)}" for key, value in sorted(report["availability"].items())
            )
        ),
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report["missing_columns"].items())
                if columns
            )
        )
    if not report["opportunities"]:
        lines.extend(["", "No publish window backfill opportunities found."])
        return "\n".join(lines)

    lines.extend(["", "Opportunities:"])
    for item in report["opportunities"]:
        window = item["window"]
        ids = [row["content_id"] for row in item["recommended_content"]]
        lines.append(
            f"- start={window['start_time']} platform={window['platform']} "
            f"content_type={window['content_type'] or '-'} reason={item['reason']} "
            f"queued={item['queued_count']} recommendations={_format_ids(ids)}"
        )
    return "\n".join(lines)


def _upcoming_windows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    now: datetime,
    horizon_end: datetime,
) -> list[dict[str, Any]]:
    if "content_publications" not in schema or "generated_content" not in schema:
        return []
    cp_columns = schema["content_publications"]
    gc_columns = schema["generated_content"]
    required = {"content_id", "platform", "status", "published_at"}
    if not required.issubset(cp_columns) or "id" not in gc_columns:
        return []
    select_columns = [
        "cp.platform AS platform",
        "cp.published_at AS published_at",
        _column_expr(gc_columns, "content_type", "gc", "content_type"),
    ]
    cutoff = now - timedelta(days=HISTORY_DAYS)
    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT {", ".join(select_columns)}
                FROM content_publications cp
                INNER JOIN generated_content gc ON gc.id = cp.content_id
                WHERE cp.status = 'published'
                  AND cp.published_at IS NOT NULL
                  AND cp.published_at >= ?
                ORDER BY cp.published_at ASC, cp.id ASC""",
            (cutoff.isoformat(),),
        ).fetchall()
    ]
    counts: Counter[tuple[str, str | None, int, int]] = Counter()
    for row in rows:
        published_at = _parse_datetime(row.get("published_at"))
        platform = _text(row.get("platform")).lower()
        if published_at is None or platform not in PLATFORM_CONTENT_TYPES:
            continue
        key = (
            platform,
            _optional_text(row.get("content_type")),
            published_at.weekday(),
            published_at.hour,
        )
        counts[key] += 1

    windows: list[dict[str, Any]] = []
    for (platform, content_type, weekday, hour), sample_size in sorted(
        counts.items(),
        key=lambda item: (-item[1], item[0][0], item[0][1] or "", item[0][2], item[0][3]),
    ):
        for start in _next_occurrences(now, horizon_end, weekday, hour):
            windows.append(
                {
                    "platform": platform,
                    "content_type": content_type,
                    "day_name": DAY_NAMES[weekday],
                    "day_of_week": weekday,
                    "hour_utc": hour,
                    "start_time": start.isoformat(),
                    "historical_sample_size": sample_size,
                }
            )
    windows.sort(
        key=lambda row: (
            row["start_time"],
            row["platform"],
            row.get("content_type") or "",
            -row["historical_sample_size"],
        )
    )
    return windows


def _queued_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    now: datetime,
    horizon_end: datetime,
) -> list[dict[str, Any]]:
    if "publish_queue" not in schema:
        return []
    pq_columns = schema["publish_queue"]
    if not {"id", "scheduled_at"}.issubset(pq_columns):
        return []
    gc_columns = schema.get("generated_content", set())
    joins = ""
    select_columns = [
        "pq.id AS queue_id",
        _column_expr(pq_columns, "content_id", "pq", "content_id"),
        "pq.scheduled_at AS scheduled_at",
        _column_expr(pq_columns, "platform", "pq", "platform"),
        _column_expr(pq_columns, "status", "pq", "status"),
        "NULL AS content_type",
        "NULL AS eval_score",
    ]
    if "generated_content" in schema and "content_id" in pq_columns and "id" in gc_columns:
        joins = "LEFT JOIN generated_content gc ON gc.id = pq.content_id"
        select_columns[-2] = _column_expr(gc_columns, "content_type", "gc", "content_type")
        select_columns[-1] = _column_expr(gc_columns, "eval_score", "gc", "eval_score")
    rows = conn.execute(
        f"""SELECT {", ".join(select_columns)}
            FROM publish_queue pq
            {joins}
            WHERE pq.scheduled_at >= ?
              AND pq.scheduled_at < ?
            ORDER BY pq.scheduled_at ASC, pq.id ASC""",
        (now.isoformat(), horizon_end.isoformat()),
    ).fetchall()
    return [dict(row) for row in rows]


def _candidate_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    min_score: float,
    now: datetime,
) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    columns = schema["generated_content"]
    if "id" not in columns:
        return []
    select_columns = [
        "gc.id AS content_id",
        _column_expr(columns, "content_type", "gc", "content_type"),
        _column_expr(columns, "eval_score", "gc", "eval_score"),
        _column_expr(columns, "created_at", "gc", "created_at"),
        _column_expr(columns, "source_commits", "gc", "source_commits"),
        _column_expr(columns, "source_messages", "gc", "source_messages"),
        _column_expr(columns, "published", "gc", "published"),
        _column_expr(columns, "published_url", "gc", "published_url"),
        _column_expr(columns, "published_at", "gc", "published_at"),
    ]
    rows = conn.execute(
        f"""SELECT {", ".join(select_columns)}
            FROM generated_content gc
            WHERE {_unpublished_filter(columns)}
            ORDER BY gc.id ASC"""
    ).fetchall()
    candidates = []
    for row in rows:
        item = dict(row)
        score = _number(item.get("eval_score"))
        if score is None or score < min_score:
            continue
        created_at = _parse_datetime(item.get("created_at"))
        source_count = len(_json_list(item.get("source_commits"))) + len(
            _json_list(item.get("source_messages"))
        )
        item["eval_score"] = score
        item["age_days"] = None if created_at is None else max(0, (now - created_at).days)
        item["source_count"] = source_count
        candidates.append(item)
    return candidates


def _rank_candidates(
    candidates: list[dict[str, Any]],
    *,
    window: dict[str, Any],
    limit: int,
    now: datetime,
) -> list[dict[str, Any]]:
    ranked = []
    for row in candidates:
        platform_fit = _platform_fit(row.get("content_type"), window["platform"])
        content_type_fit = 1 if row.get("content_type") == window.get("content_type") else 0
        if not platform_fit:
            continue
        age_days = row.get("age_days")
        freshness_score = 0.0 if age_days is None else max(0.0, 30.0 - float(age_days))
        rank_score = (
            float(row["eval_score"]) * 10
            + content_type_fit * 5
            + platform_fit * 3
            + freshness_score
            + min(3, int(row.get("source_count") or 0))
        )
        ranked.append(
            {
                "content_id": int(row["content_id"]),
                "content_type": row.get("content_type"),
                "eval_score": row["eval_score"],
                "age_days": age_days,
                "platform_fit": bool(platform_fit),
                "content_type_fit": bool(content_type_fit),
                "source_count": int(row.get("source_count") or 0),
                "rank_score": round(rank_score, 2),
            }
        )
    ranked.sort(
        key=lambda row: (
            -row["rank_score"],
            -row["eval_score"],
            row["age_days"] if row["age_days"] is not None else 999999,
            row["content_id"],
        )
    )
    return ranked[:limit]


def _queued_for_window(
    window: dict[str, Any],
    queued_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    start = _parse_datetime(window["start_time"])
    if start is None:
        return []
    end = start + timedelta(hours=1)
    matches = []
    for row in queued_rows:
        scheduled = _parse_datetime(row.get("scheduled_at"))
        if scheduled is None or scheduled < start or scheduled >= end:
            continue
        platform = _text(row.get("platform")).lower() or "all"
        if platform not in {window["platform"], "all"}:
            continue
        matches.append(row)
    return matches


def _is_suitable_queued(
    row: dict[str, Any],
    *,
    window: dict[str, Any],
    min_score: float,
) -> bool:
    if _text(row.get("status")).lower() not in {"queued", "held"}:
        return False
    score = _number(row.get("eval_score"))
    if score is None or score < min_score:
        return False
    content_type = row.get("content_type")
    return _platform_fit(content_type, window["platform"]) and (
        window.get("content_type") is None or content_type == window.get("content_type")
    )


def _platform_fit(content_type: Any, platform: str) -> int:
    normalized = _optional_text(content_type)
    if normalized is None:
        return 1
    return 1 if normalized in PLATFORM_CONTENT_TYPES.get(platform, set()) else 0


def _next_occurrences(
    now: datetime,
    horizon_end: datetime,
    weekday: int,
    hour: int,
) -> list[datetime]:
    days_ahead = (weekday - now.weekday()) % 7
    candidate = now.replace(hour=hour, minute=0, second=0, microsecond=0) + timedelta(
        days=days_ahead
    )
    if candidate < now:
        candidate += timedelta(days=7)
    starts = []
    while candidate < horizon_end:
        starts.append(candidate)
        candidate += timedelta(days=7)
    return starts


def _empty_report(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    availability: dict[str, bool],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]],
) -> dict[str, Any]:
    return {
        "artifact_type": "publish_window_backfill_opportunities",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "availability": availability,
        "totals": {
            "candidate_count": 0,
            "opportunity_count": 0,
            "queued_count": 0,
            "window_count": 0,
            "by_reason": {"empty_window": 0, "underfilled_window": 0},
        },
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "opportunities": [],
    }


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[list[str], dict[str, list[str]]]:
    required = {
        "publish_queue": {"id", "scheduled_at"},
        "generated_content": {"id", "content_type", "eval_score"},
        "content_publications": {"content_id", "platform", "status", "published_at"},
    }
    missing_tables = [table for table in required if table not in schema]
    missing_columns = {
        table: sorted(columns - schema.get(table, set()))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _unpublished_filter(columns: set[str]) -> str:
    filters = []
    if "published" in columns:
        filters.append("(gc.published IS NULL OR gc.published = 0)")
    if "published_url" in columns:
        filters.append("(gc.published_url IS NULL OR TRIM(gc.published_url) = '')")
    if "published_at" in columns:
        filters.append("(gc.published_at IS NULL OR TRIM(gc.published_at) = '')")
    return " AND ".join(filters) if filters else "1 = 1"


def _column_expr(
    columns: set[str],
    column: str,
    alias: str,
    output: str | None = None,
) -> str:
    output_name = output or column
    if column in columns:
        return f"{alias}.{column} AS {output_name}"
    return f"NULL AS {output_name}"


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
    ).fetchall()
    return {
        row["name"]: {info["name"] for info in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(str(value))
    except (TypeError, json.JSONDecodeError):
        return []
    return parsed if isinstance(parsed, list) else []


def _number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _format_ids(values: list[int]) -> str:
    return ",".join(str(value) for value in values) if values else "-"
