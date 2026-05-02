"""Read-only failover recommendations for stuck publication targets."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any

from .publish_errors import classify_publish_error, normalize_error_category


DEFAULT_DAYS = 7
DEFAULT_MIN_CONFIDENCE = 0.7
SUPPORTED_PLATFORMS = ("all", "x", "bluesky")
STUCK_STATUSES = ("failed", "held")
HIGH_FAILURE_RATE = 0.5


@dataclass(frozen=True)
class PublishFailoverRecommendation:
    content_id: int
    source_platform: str
    recommended_platform: str
    variant_id: int
    variant_type: str
    source_status: str
    failure_context: str
    error_category: str
    confidence_score: float
    reason_codes: tuple[str, ...]
    publication_id: int | None = None
    queue_id: int | None = None
    attempt_count: int = 0
    last_error_at: str | None = None
    hold_reason: str | None = None
    source_failure_rate: float | None = None
    source_failure_count: int = 0
    source_total_count: int = 0


def build_publish_failover_report(
    db_or_conn: Any,
    *,
    platform: str = "all",
    days: int = DEFAULT_DAYS,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Recommend alternate platform variants for failed or held source publications."""
    if platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"platform must be one of: {', '.join(SUPPORTED_PLATFORMS)}")
    if days <= 0:
        raise ValueError("days must be positive")
    if min_confidence < 0 or min_confidence > 1:
        raise ValueError("min_confidence must be between 0 and 1")

    conn = _connection(db_or_conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    since = generated_at - timedelta(days=days)
    filters = {
        "platform": platform,
        "days": days,
        "min_confidence": float(min_confidence),
        "since": since.isoformat(),
    }
    schema = _schema(conn)
    missing = _missing_required(schema)
    if missing:
        return _report(generated_at, filters, [], missing)

    failure_rates = _recent_failure_rates(conn, schema, since=since)
    published_targets = _published_targets(conn, schema)
    stuck_rows = _stuck_source_rows(conn, schema, platform=platform, since=since)
    variant_rows = _alternate_variants(conn, schema, content_ids=_content_ids(stuck_rows))

    recommendations: list[PublishFailoverRecommendation] = []
    for source in stuck_rows:
        source_platform = str(source["source_platform"])
        rate = failure_rates.get(source_platform, {"failures": 0, "total": 0, "rate": 0.0})
        if rate["rate"] < HIGH_FAILURE_RATE:
            continue
        for variant in variant_rows.get(int(source["content_id"]), []):
            target_platform = str(variant["platform"])
            if target_platform == source_platform:
                continue
            if (int(source["content_id"]), target_platform) in published_targets:
                continue
            item = _recommendation(source, variant, rate)
            if item.confidence_score >= min_confidence:
                recommendations.append(item)

    recommendations.sort(
        key=lambda item: (
            -item.confidence_score,
            item.source_platform,
            item.recommended_platform,
            item.content_id,
            item.variant_id,
        )
    )
    return _report(generated_at, filters, recommendations, missing)


def format_publish_failover_json(report: dict[str, Any]) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publish_failover_text(report: dict[str, Any]) -> str:
    """Render failover recommendations as a concise operator report."""
    filters = report["filters"]
    lines = [
        "Publish Failover Planner",
        f"Generated: {report['generated_at']}",
        (
            "Filters: "
            f"platform={filters['platform']} "
            f"days={filters['days']} "
            f"min_confidence={filters['min_confidence']:g}"
        ),
        f"Total: {report['totals']['items']}",
    ]
    if report.get("missing_required"):
        lines.append("Missing required schema: " + ", ".join(report["missing_required"]))
    if not report["items"]:
        lines.extend(["", "No publish failover recommendations found."])
        return "\n".join(lines)

    lines.extend(
        [
            "",
            "Items:",
            "  Content  From      To        Variant  Confidence  Context",
            "  -------  --------  --------  -------  ----------  ------------------------------",
        ]
    )
    for item in report["items"]:
        lines.append(
            f"  {item['content_id']:<7}  "
            f"{item['source_platform']:<8}  "
            f"{item['recommended_platform']:<8}  "
            f"{item['variant_id']:<7}  "
            f"{item['confidence_score']:<10.2f}  "
            f"{item['failure_context']}"
        )
    return "\n".join(lines)


def _stuck_source_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    platform: str,
    since: datetime,
) -> list[dict[str, Any]]:
    rows: dict[tuple[int, str], dict[str, Any]] = {}
    for row in _publication_sources(conn, schema, platform=platform, since=since):
        rows[(int(row["content_id"]), str(row["source_platform"]))] = row
    for row in _queue_sources(conn, schema, platform=platform, since=since):
        key = (int(row["content_id"]), str(row["source_platform"]))
        if key not in rows:
            rows[key] = row
        else:
            merged = dict(rows[key])
            merged["queue_id"] = row.get("queue_id")
            merged["hold_reason"] = row.get("hold_reason") or merged.get("hold_reason")
            merged["queue_error"] = row.get("queue_error") or merged.get("queue_error")
            rows[key] = merged
    return sorted(rows.values(), key=lambda row: (row["source_platform"], row["content_id"]))


def _publication_sources(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    platform: str,
    since: datetime,
) -> list[dict[str, Any]]:
    columns = schema["content_publications"]
    select = {
        "content_id": "content_id",
        "source_platform": "platform",
        "source_status": "LOWER(status)",
        "publication_id": _column_expr(columns, "id"),
        "queue_id": "NULL",
        "error": _column_expr(columns, "error"),
        "error_category": _column_expr(columns, "error_category"),
        "attempt_count": _column_expr(columns, "attempt_count", "0"),
        "last_error_at": _column_expr(columns, "last_error_at"),
        "updated_at": _column_expr(columns, "updated_at"),
        "hold_reason": "NULL",
        "queue_error": "NULL",
    }
    time_expr = _time_expr(columns)
    filters = ["LOWER(status) IN ('failed', 'held')", f"datetime({time_expr}) >= datetime(?)"]
    params: list[Any] = [since.isoformat()]
    if platform != "all":
        filters.append("platform = ?")
        params.append(platform)
    rows = conn.execute(
        f"""SELECT {', '.join(f'{expr} AS {alias}' for alias, expr in select.items())}
            FROM content_publications
            WHERE {' AND '.join(filters)}
            ORDER BY platform ASC, content_id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _queue_sources(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    platform: str,
    since: datetime,
) -> list[dict[str, Any]]:
    if "publish_queue" not in schema:
        return []
    columns = schema["publish_queue"]
    if not {"content_id", "platform", "status"}.issubset(columns):
        return []
    select = {
        "content_id": "content_id",
        "queue_platform": _column_expr(columns, "platform", "'x'"),
        "source_status": "LOWER(status)",
        "publication_id": "NULL",
        "queue_id": _column_expr(columns, "id"),
        "error": _column_expr(columns, "error"),
        "error_category": _column_expr(columns, "error_category"),
        "attempt_count": "0",
        "last_error_at": "NULL",
        "updated_at": _column_expr(columns, "updated_at", _column_expr(columns, "created_at")),
        "hold_reason": _column_expr(columns, "hold_reason"),
        "queue_error": _column_expr(columns, "error"),
    }
    time_expr = _queue_time_expr(columns)
    filters = ["LOWER(status) IN ('failed', 'held')", f"datetime({time_expr}) >= datetime(?)"]
    params: list[Any] = [since.isoformat()]
    if platform != "all":
        filters.append("(platform = ? OR platform = 'all')")
        params.append(platform)
    query = f"""SELECT {', '.join(f'{expr} AS {alias}' for alias, expr in select.items())}
                FROM publish_queue
                WHERE {' AND '.join(filters)}
                ORDER BY platform ASC, content_id ASC"""
    expanded: list[dict[str, Any]] = []
    for row in conn.execute(query, params).fetchall():
        item = dict(row)
        for source_platform in _expand_queue_platform(item.pop("queue_platform"), platform):
            expanded.append(item | {"source_platform": source_platform})
    return expanded


def _alternate_variants(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    content_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    if not content_ids:
        return {}
    columns = schema["content_variants"]
    select = {
        "variant_id": _column_expr(columns, "id"),
        "content_id": "content_id",
        "platform": "platform",
        "variant_type": _column_expr(columns, "variant_type", "'post'"),
        "selected": _column_expr(columns, "selected", "0"),
        "created_at": _column_expr(columns, "created_at"),
    }
    rows = conn.execute(
        f"""SELECT {', '.join(f'{expr} AS {alias}' for alias, expr in select.items())}
            FROM content_variants
            WHERE content_id IN ({', '.join('?' for _ in content_ids)})
              AND platform IN ('x', 'bluesky')
            ORDER BY content_id ASC, platform ASC, selected DESC, created_at DESC, id DESC""",
        content_ids,
    ).fetchall()
    grouped: dict[int, list[dict[str, Any]]] = {}
    seen: set[tuple[int, str]] = set()
    for row in rows:
        item = dict(row)
        key = (int(item["content_id"]), str(item["platform"]))
        if key in seen:
            continue
        seen.add(key)
        grouped.setdefault(int(item["content_id"]), []).append(item)
    return grouped


def _recent_failure_rates(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    since: datetime,
) -> dict[str, dict[str, float | int]]:
    counts = {name: {"failures": 0, "total": 0, "rate": 0.0} for name in SUPPORTED_PLATFORMS if name != "all"}
    if "content_publications" in schema and {"platform", "status"}.issubset(schema["content_publications"]):
        columns = schema["content_publications"]
        time_expr = _time_expr(columns)
        rows = conn.execute(
            f"""SELECT platform, LOWER(status) AS status, COUNT(*) AS count
                FROM content_publications
                WHERE platform IN ('x', 'bluesky')
                  AND LOWER(status) IN ('published', 'failed', 'held')
                  AND datetime({time_expr}) >= datetime(?)
                GROUP BY platform, LOWER(status)""",
            (since.isoformat(),),
        ).fetchall()
        _add_rate_counts(counts, rows)
    if "publication_attempts" in schema and {"platform", "success", "attempted_at"}.issubset(schema["publication_attempts"]):
        rows = conn.execute(
            """SELECT platform,
                      CASE WHEN success = 1 THEN 'published' ELSE 'failed' END AS status,
                      COUNT(*) AS count
               FROM publication_attempts
               WHERE platform IN ('x', 'bluesky')
                 AND datetime(attempted_at) >= datetime(?)
               GROUP BY platform, CASE WHEN success = 1 THEN 'published' ELSE 'failed' END""",
            (since.isoformat(),),
        ).fetchall()
        _add_rate_counts(counts, rows)
    if "publish_queue" in schema and {"platform", "status"}.issubset(schema["publish_queue"]):
        columns = schema["publish_queue"]
        time_expr = _queue_time_expr(columns)
        rows = conn.execute(
            f"""SELECT platform, LOWER(status) AS status, COUNT(*) AS count
                FROM publish_queue
                WHERE platform IN ('x', 'bluesky')
                  AND LOWER(status) IN ('published', 'failed', 'held')
                  AND datetime({time_expr}) >= datetime(?)
                GROUP BY platform, LOWER(status)""",
            (since.isoformat(),),
        ).fetchall()
        _add_rate_counts(counts, rows)
        all_rows = conn.execute(
            f"""SELECT LOWER(status) AS status, COUNT(*) AS count
                FROM publish_queue
                WHERE platform = 'all'
                  AND LOWER(status) IN ('published', 'failed', 'held')
                  AND datetime({time_expr}) >= datetime(?)
                GROUP BY LOWER(status)""",
            (since.isoformat(),),
        ).fetchall()
        for row in all_rows:
            for target in ("x", "bluesky"):
                count = int(row["count"] or 0)
                counts[target]["total"] = int(counts[target]["total"]) + count
                if str(row["status"]) in STUCK_STATUSES:
                    counts[target]["failures"] = int(counts[target]["failures"]) + count
    for data in counts.values():
        total = int(data["total"])
        data["rate"] = (int(data["failures"]) / total) if total else 0.0
    return counts


def _published_targets(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> set[tuple[int, str]]:
    published: set[tuple[int, str]] = set()
    if "content_publications" in schema and {"content_id", "platform", "status"}.issubset(schema["content_publications"]):
        rows = conn.execute(
            """SELECT content_id, platform
               FROM content_publications
               WHERE platform IN ('x', 'bluesky')
                 AND LOWER(status) = 'published'""",
        ).fetchall()
        published.update((int(row["content_id"]), str(row["platform"])) for row in rows)
    if "generated_content" in schema:
        columns = schema["generated_content"]
        selects = ["id AS content_id"]
        if "published" in columns:
            selects.append("published")
        if "tweet_id" in columns:
            selects.append("tweet_id")
        if "bluesky_uri" in columns:
            selects.append("bluesky_uri")
        rows = conn.execute(f"SELECT {', '.join(selects)} FROM generated_content").fetchall()
        for row in rows:
            item = dict(row)
            content_id = int(item["content_id"])
            if item.get("published") == 1 or item.get("tweet_id"):
                published.add((content_id, "x"))
            if item.get("bluesky_uri"):
                published.add((content_id, "bluesky"))
    return published


def _recommendation(
    source: dict[str, Any],
    variant: dict[str, Any],
    rate: dict[str, float | int],
) -> PublishFailoverRecommendation:
    error = source.get("error") or source.get("queue_error") or source.get("hold_reason")
    category = (
        normalize_error_category(source.get("error_category"))
        if source.get("error_category") is not None
        else classify_publish_error(error, platform=source.get("source_platform"))
    )
    reason_codes = [
        "source_status_stuck",
        "alternate_variant_available",
        "source_failure_rate_high",
        "target_not_published",
    ]
    if int(variant.get("selected") or 0) == 1:
        reason_codes.append("selected_variant")
    confidence = min(
        0.99,
        0.55 + (0.35 * float(rate["rate"])) + (0.10 if int(variant.get("selected") or 0) == 1 else 0.0),
    )
    return PublishFailoverRecommendation(
        content_id=int(source["content_id"]),
        source_platform=str(source["source_platform"]),
        recommended_platform=str(variant["platform"]),
        variant_id=int(variant["variant_id"]),
        variant_type=str(variant["variant_type"]),
        source_status=str(source["source_status"]),
        failure_context=_failure_context(source, category),
        error_category=category,
        confidence_score=round(confidence, 2),
        reason_codes=tuple(reason_codes),
        publication_id=_optional_int(source.get("publication_id")),
        queue_id=_optional_int(source.get("queue_id")),
        attempt_count=_int(source.get("attempt_count")),
        last_error_at=_optional_str(source.get("last_error_at") or source.get("updated_at")),
        hold_reason=_optional_str(source.get("hold_reason")),
        source_failure_rate=round(float(rate["rate"]), 3),
        source_failure_count=int(rate["failures"]),
        source_total_count=int(rate["total"]),
    )


def _report(
    generated_at: datetime,
    filters: dict[str, Any],
    items: list[PublishFailoverRecommendation],
    missing_required: list[str],
) -> dict[str, Any]:
    rows = [asdict(item) for item in items]
    for row in rows:
        row["reason_codes"] = list(row["reason_codes"])
    by_source = {name: 0 for name in SUPPORTED_PLATFORMS if name != "all"}
    by_target = {name: 0 for name in SUPPORTED_PLATFORMS if name != "all"}
    for row in rows:
        by_source[row["source_platform"]] = by_source.get(row["source_platform"], 0) + 1
        by_target[row["recommended_platform"]] = by_target.get(row["recommended_platform"], 0) + 1
    return {
        "artifact_type": "publish_failover_plan",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "totals": {
            "items": len(rows),
            "by_source_platform": dict(sorted(by_source.items())),
            "by_recommended_platform": dict(sorted(by_target.items())),
        },
        "missing_required": missing_required,
        "items": rows,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view')").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row[0]
        schema[table] = {column[1] for column in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    return schema


def _missing_required(schema: dict[str, set[str]]) -> list[str]:
    missing: list[str] = []
    if "content_publications" not in schema and "publish_queue" not in schema:
        missing.append("content_publications_or_publish_queue")
    if "content_variants" not in schema:
        missing.append("content_variants")
    elif not {"content_id", "platform", "id"}.issubset(schema["content_variants"]):
        for column in sorted({"content_id", "platform", "id"} - schema["content_variants"]):
            missing.append(f"content_variants.{column}")
    if "content_publications" in schema:
        for column in sorted({"content_id", "platform", "status"} - schema["content_publications"]):
            missing.append(f"content_publications.{column}")
    return missing


def _column_expr(columns: set[str], column: str, fallback: str = "NULL") -> str:
    return column if column in columns else fallback


def _time_expr(columns: set[str]) -> str:
    candidates = [
        column
        for column in ("last_error_at", "updated_at", "published_at", "created_at")
        if column in columns
    ]
    if not candidates:
        return "'1970-01-01T00:00:00+00:00'"
    return f"COALESCE({', '.join(candidates)})"


def _queue_time_expr(columns: set[str]) -> str:
    candidates = [
        column for column in ("updated_at", "scheduled_at", "created_at") if column in columns
    ]
    if not candidates:
        return "'1970-01-01T00:00:00+00:00'"
    return f"COALESCE({', '.join(candidates)})"


def _expand_queue_platform(queue_platform: Any, platform_filter: str) -> list[str]:
    value = str(queue_platform or "x")
    if value == "all":
        platforms = ["x", "bluesky"]
    elif value in {"x", "bluesky"}:
        platforms = [value]
    else:
        platforms = []
    if platform_filter != "all":
        return [platform for platform in platforms if platform == platform_filter]
    return platforms


def _add_rate_counts(
    counts: dict[str, dict[str, float | int]],
    rows: list[sqlite3.Row],
) -> None:
    for row in rows:
        platform = str(row["platform"])
        if platform not in counts:
            continue
        count = int(row["count"] or 0)
        counts[platform]["total"] = int(counts[platform]["total"]) + count
        if str(row["status"]) in STUCK_STATUSES:
            counts[platform]["failures"] = int(counts[platform]["failures"]) + count


def _content_ids(rows: list[dict[str, Any]]) -> list[int]:
    return sorted({int(row["content_id"]) for row in rows})


def _failure_context(source: dict[str, Any], category: str) -> str:
    status = str(source.get("source_status") or "unknown")
    if status == "held" and source.get("hold_reason"):
        return f"held: {source['hold_reason']}"
    error = source.get("error") or source.get("queue_error")
    if error:
        return f"{category}: {_excerpt(error)}"
    return status


def _excerpt(value: Any, width: int = 80) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None
