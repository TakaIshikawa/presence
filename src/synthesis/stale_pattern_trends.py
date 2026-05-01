"""Read-only trend report for stale rhetorical patterns in generated content."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import re
import sqlite3
from typing import Any

from .stale_patterns import STALE_PATTERNS


DEFAULT_DAYS = 30
DEFAULT_LIMIT_EXAMPLES = 3


def build_stale_pattern_trends(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    content_type: str = "all",
    limit_examples: int = DEFAULT_LIMIT_EXAMPLES,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Scan recent generated content and summarize stale-pattern recurrence."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit_examples < 0:
        raise ValueError("limit_examples must be non-negative")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    schema = _schema(conn)
    now = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    if "generated_content" not in schema:
        return _empty_report(now, days, content_type, limit_examples, cutoff, schema)

    rows = _content_rows(
        conn,
        schema,
        cutoff=cutoff,
        content_type=content_type,
    )

    pattern_summaries = _new_pattern_summaries()
    dimension_counts: dict[str, dict[str, dict[str, int]]] = {
        "content_type": {},
        "content_format": {},
        "prompt_version": {},
        "publication_status": {},
    }
    hit_content_ids: set[int] = set()

    for row in rows:
        content_id = int(row["content_id"])
        values = {
            "content_type": _value(row.get("content_type")),
            "content_format": _value(row.get("content_format")),
            "prompt_version": _value(row.get("prompt_version")),
            "publication_status": _publication_status(row),
        }
        for dimension, value in values.items():
            _increment_dimension(dimension_counts[dimension], value, "scanned_count")

        matched = _matched_patterns(str(row.get("content") or ""))
        if not matched:
            continue

        hit_content_ids.add(content_id)
        for dimension, value in values.items():
            _increment_dimension(dimension_counts[dimension], value, "hit_count")

        for pattern in matched:
            summary = pattern_summaries[pattern["pattern_id"]]
            summary["hit_count"] += 1
            for dimension, value in values.items():
                counts = summary["breakdowns"][dimension].setdefault(value, 0)
                summary["breakdowns"][dimension][value] = counts + 1
            if len(summary["examples"]) < limit_examples:
                summary["examples"].append(
                    {
                        "content_id": content_id,
                        "content_type": row.get("content_type"),
                        "content_format": row.get("content_format"),
                        "prompt_version": row.get("prompt_version"),
                        "publication_status": values["publication_status"],
                        "excerpt": _excerpt(str(row.get("content") or "")),
                    }
                )

    scanned_count = len(rows)
    patterns = []
    for summary in pattern_summaries.values():
        item = {
            **summary,
            "hit_rate": _rate(summary["hit_count"], scanned_count),
            "breakdowns": {
                key: dict(sorted(values.items()))
                for key, values in summary["breakdowns"].items()
            },
        }
        patterns.append(item)
    patterns.sort(key=lambda item: (-item["hit_count"], item["pattern_id"]))

    return {
        "generated_at": now.isoformat(),
        "filters": {
            "days": days,
            "content_type": content_type,
            "limit_examples": limit_examples,
            "cutoff": cutoff.isoformat(),
        },
        "summary": {
            "scanned_count": scanned_count,
            "hit_content_count": len(hit_content_ids),
            "pattern_hit_count": sum(item["hit_count"] for item in patterns),
            "hit_content_rate": _rate(len(hit_content_ids), scanned_count),
        },
        "dimensions": {
            dimension: _dimension_rates(counts)
            for dimension, counts in dimension_counts.items()
        },
        "patterns": patterns,
    }


def format_stale_pattern_trends_json(report: dict[str, Any]) -> str:
    """Render the stale-pattern report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_stale_pattern_trends_text(report: dict[str, Any]) -> str:
    """Render a compact operator-facing stale-pattern report."""
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Stale pattern trend report",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} content_type={filters['content_type']} "
            f"limit_examples={filters['limit_examples']}"
        ),
        (
            "Totals: "
            f"scanned={summary['scanned_count']} "
            f"hit_content={summary['hit_content_count']} "
            f"pattern_hits={summary['pattern_hit_count']} "
            f"hit_rate={summary['hit_content_rate']:.3f}"
        ),
        "",
    ]
    hit_patterns = [pattern for pattern in report["patterns"] if pattern["hit_count"]]
    if not hit_patterns:
        lines.append("No stale patterns found.")
        return "\n".join(lines)

    lines.append("Patterns")
    columns = [
        ("pattern_id", "PATTERN", 16),
        ("hit_count", "HITS", 5),
        ("hit_rate", "RATE", 7),
        ("regex", "REGEX", 56),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for pattern in hit_patterns:
        rendered = dict(pattern)
        rendered["hit_rate"] = f"{pattern['hit_rate']:.3f}"
        lines.append(
            "  ".join(
                _clip(rendered.get(key), width).ljust(width)
                for key, _, width in columns
            )
        )
        for example in pattern["examples"]:
            lines.append(
                "  "
                f"- content_id={example['content_id']} "
                f"status={example['publication_status']} "
                f"{_clip(example['excerpt'], 84)}"
            )
    return "\n".join(lines)


def _content_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
    content_type: str,
) -> list[dict[str, Any]]:
    gc = schema["generated_content"]
    if not {"id", "content"}.issubset(gc):
        return []

    select = {
        "content_id": "gc.id",
        "content": "gc.content",
        "content_type": _column_expr(gc, "content_type", alias="gc"),
        "content_format": _column_expr(gc, "content_format", alias="gc"),
        "created_at": _column_expr(gc, "created_at", alias="gc"),
        "published": _column_expr(gc, "published", "0", alias="gc"),
        "published_at": _column_expr(gc, "published_at", alias="gc"),
        "published_url": _column_expr(gc, "published_url", alias="gc"),
        "tweet_id": _column_expr(gc, "tweet_id", alias="gc"),
    }
    prompt_join = ""
    prompt_select = "NULL"
    if "engagement_predictions" in schema:
        ep = schema["engagement_predictions"]
        if {"content_id", "prompt_version"}.issubset(ep):
            prompt_join = """
           LEFT JOIN (
               SELECT content_id, MAX(prompt_version) AS prompt_version
               FROM engagement_predictions
               WHERE prompt_version IS NOT NULL
               GROUP BY content_id
           ) ep ON ep.content_id = gc.id"""
            prompt_select = "ep.prompt_version"

    filters = []
    params: list[Any] = []
    if "created_at" in gc:
        filters.append("gc.created_at >= ?")
        params.append(cutoff.isoformat())
    if content_type != "all" and "content_type" in gc:
        filters.append("gc.content_type = ?")
        params.append(content_type)
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""

    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   {select['content_id']} AS content_id,
                   {select['content']} AS content,
                   {select['content_type']} AS content_type,
                   {select['content_format']} AS content_format,
                   {select['created_at']} AS created_at,
                   {select['published']} AS published,
                   {select['published_at']} AS generated_published_at,
                   {select['published_url']} AS published_url,
                   {select['tweet_id']} AS tweet_id,
                   {prompt_select} AS prompt_version
               FROM generated_content gc
               {prompt_join}
               {where_clause}
               ORDER BY {select['created_at']} DESC, gc.id DESC""",
            params,
        ).fetchall()
    ]
    if not rows:
        return rows

    cp_statuses = _statuses_by_content(conn, schema, "content_publications")
    pq_statuses = _statuses_by_content(conn, schema, "publish_queue")
    for row in rows:
        content_id = int(row["content_id"])
        row["content_publication_statuses"] = cp_statuses.get(content_id, [])
        row["publish_queue_statuses"] = pq_statuses.get(content_id, [])
    return rows


def _statuses_by_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    table: str,
) -> dict[int, list[str]]:
    columns = schema.get(table)
    if not columns or not {"content_id", "status"}.issubset(columns):
        return {}
    rows = conn.execute(
        f"""SELECT content_id, status
            FROM {table}
            WHERE status IS NOT NULL
            ORDER BY content_id ASC, status ASC"""
    ).fetchall()
    statuses: dict[int, list[str]] = {}
    for row in rows:
        statuses.setdefault(int(row["content_id"]), []).append(str(row["status"]))
    return statuses


def _publication_status(row: dict[str, Any]) -> str:
    statuses = {
        str(status)
        for status in [
            *row.get("content_publication_statuses", []),
            *row.get("publish_queue_statuses", []),
        ]
        if status
    }
    if (
        _truthy(row.get("published"))
        or row.get("generated_published_at")
        or row.get("published_url")
        or row.get("tweet_id")
        or "published" in statuses
    ):
        return "published"
    for status in ("held", "queued", "failed", "cancelled"):
        if status in statuses:
            return status
    return "unpublished"


def _matched_patterns(text: str) -> list[dict[str, str]]:
    matches = []
    for index, pattern in enumerate(STALE_PATTERNS, start=1):
        if pattern.search(text):
            matches.append(_pattern_metadata(index, pattern))
    return matches


def _new_pattern_summaries() -> dict[str, dict[str, Any]]:
    summaries = {}
    for index, pattern in enumerate(STALE_PATTERNS, start=1):
        metadata = _pattern_metadata(index, pattern)
        summaries[metadata["pattern_id"]] = {
            **metadata,
            "hit_count": 0,
            "breakdowns": {
                "content_type": {},
                "content_format": {},
                "prompt_version": {},
                "publication_status": {},
            },
            "examples": [],
        }
    return summaries


def _pattern_metadata(index: int, pattern: re.Pattern[str]) -> dict[str, str]:
    return {
        "pattern_id": f"stale_pattern_{index:02d}",
        "regex": pattern.pattern,
    }


def _increment_dimension(counts: dict[str, dict[str, int]], value: str, key: str) -> None:
    bucket = counts.setdefault(value, {"scanned_count": 0, "hit_count": 0})
    bucket[key] += 1


def _dimension_rates(counts: dict[str, dict[str, int]]) -> list[dict[str, Any]]:
    rows = []
    for value, item in counts.items():
        scanned_count = item["scanned_count"]
        hit_count = item["hit_count"]
        rows.append(
            {
                "value": value,
                "scanned_count": scanned_count,
                "hit_count": hit_count,
                "hit_rate": _rate(hit_count, scanned_count),
            }
        )
    return sorted(rows, key=lambda item: (-item["hit_count"], item["value"]))


def _empty_report(
    now: datetime,
    days: int,
    content_type: str,
    limit_examples: int,
    cutoff: datetime,
    schema: dict[str, set[str]],
) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "filters": {
            "days": days,
            "content_type": content_type,
            "limit_examples": limit_examples,
            "cutoff": cutoff.isoformat(),
        },
        "summary": {
            "scanned_count": 0,
            "hit_content_count": 0,
            "pattern_hit_count": 0,
            "hit_content_rate": 0.0,
        },
        "dimensions": {
            "content_type": [],
            "content_format": [],
            "prompt_version": [],
            "publication_status": [],
        },
        "patterns": [
            {
                **summary,
                "hit_rate": 0.0,
                "breakdowns": {
                    key: dict(sorted(values.items()))
                    for key, values in summary["breakdowns"].items()
                },
            }
            for summary in _new_pattern_summaries().values()
        ],
        "missing_required_tables": [
            table for table in ("generated_content",) if table not in schema
        ],
    }


def _rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return round(numerator / denominator, 4)


def _excerpt(text: str, width: int = 160) -> str:
    compact = " ".join(text.split())
    if len(compact) <= width:
        return compact
    return compact[: max(0, width - 3)] + "..."


def _value(value: Any) -> str:
    if value is None or value == "":
        return "unknown"
    return str(value)


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


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


def _column_expr(
    columns: set[str],
    column: str,
    fallback: str = "NULL",
    *,
    alias: str = "gc",
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _clip(value: Any, width: int) -> str:
    text = "-" if value is None else str(value).replace("\n", " ")
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."
