"""Select content variants that should be refreshed after newer context changes."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 30
ACTION_REFRESH_SELECTED = "refresh_selected_variant"
ACTION_REFRESH_VARIANT = "refresh_variant"
ACTION_REVIEW_LOW_RESONANCE = "refresh_with_performance_context"

REASON_BASE_CONTENT_NEWER = "base_content_newer"
REASON_SELECTED_COPY_NEWER = "selected_platform_copy_newer"
REASON_FEEDBACK_NEWER = "feedback_newer"
REASON_PUBLICATION_OUTCOME_NEWER = "publication_outcome_newer"
REASON_PERFORMANCE_CONTEXT_NEWER = "performance_context_newer"
REASON_LOW_RESONANCE = "low_resonance_outcome"

_ENGAGEMENT_TABLES = (
    ("post_engagement", "fetched_at", "engagement_score", {"x", "twitter"}),
    ("bluesky_engagement", "fetched_at", "engagement_score", {"bluesky"}),
    ("linkedin_engagement", "fetched_at", "engagement_score", {"linkedin"}),
    ("mastodon_engagement", "fetched_at", "engagement_score", {"mastodon"}),
)


@dataclass(frozen=True)
class ContentVariantRefreshRecommendation:
    """One content variant that should be regenerated or reviewed."""

    variant_id: int
    content_id: int
    platform: str
    variant_type: str
    selected: bool
    priority: int
    action: str
    reasons: list[str]
    variant_created_at: str | None
    newest_context_at: str | None
    content_type: str | None = None
    auto_quality: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def select_stale_content_variants(
    db_or_conn: Any,
    *,
    platform: str | None = None,
    days: int | None = DEFAULT_DAYS,
    now: datetime | None = None,
) -> list[ContentVariantRefreshRecommendation]:
    """Return variants older than newer content, feedback, or performance context."""
    if days is not None and days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "content_variants" not in schema or "generated_content" not in schema:
        return []

    now = _as_utc(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days) if days is not None else None
    rows = _variant_rows(conn, schema, platform=platform)
    latest_by_content = _latest_context_by_content(conn, schema)
    latest_selected_by_key = _latest_selected_by_content_platform(conn, schema)

    recommendations = []
    for row in rows:
        recommendation = _recommendation_for_row(
            row,
            latest_by_content=latest_by_content,
            latest_selected_by_key=latest_selected_by_key,
        )
        if recommendation is None:
            continue
        if cutoff is not None:
            newest_context = _parse_timestamp(recommendation.newest_context_at)
            if newest_context is None or newest_context < cutoff:
                continue
        recommendations.append(recommendation)

    return sorted(
        recommendations,
        key=lambda item: (
            -item.priority,
            item.platform,
            item.content_id,
            item.variant_type,
            item.variant_id,
        ),
    )


def build_content_variant_freshness_report(
    db_or_conn: Any,
    *,
    platform: str | None = None,
    days: int | None = DEFAULT_DAYS,
    mark_stale_dry_run: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a deterministic read-only freshness report."""
    if platform is not None and not platform.strip():
        raise ValueError("platform must not be blank")

    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    recommendations = select_stale_content_variants(
        db_or_conn,
        platform=platform,
        days=days,
        now=now,
    )
    counts = {
        "recommendations": len(recommendations),
        "selected": sum(1 for item in recommendations if item.selected),
        "low_resonance": sum(
            1 for item in recommendations if REASON_LOW_RESONANCE in item.reasons
        ),
        "by_action": _count_by_action(recommendations),
    }
    return {
        "artifact_type": "content_variant_freshness",
        "generated_at": generated_at,
        "filters": {
            "platform": platform,
            "days": days,
            "mark_stale_dry_run": mark_stale_dry_run,
        },
        "counts": counts,
        "recommendations": [item.to_dict() for item in recommendations],
        "dry_run_plan": _dry_run_plan(recommendations) if mark_stale_dry_run else [],
    }


def format_content_variant_freshness_json(report: dict[str, Any]) -> str:
    """Render a freshness report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_content_variant_freshness_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable freshness report."""
    if not report["recommendations"]:
        return "No stale content variants found."

    lines = [
        "Content Variant Freshness",
        (
            f"Counts: recommendations={report['counts']['recommendations']} "
            f"selected={report['counts']['selected']} "
            f"low_resonance={report['counts']['low_resonance']}"
        ),
        "",
        "Recommendations",
    ]
    for item in report["recommendations"]:
        lines.append(
            "  - "
            f"priority={item['priority']} action={item['action']} "
            f"variant={item['variant_id']} content={item['content_id']} "
            f"platform={item['platform']} type={item['variant_type']} "
            f"selected={'yes' if item['selected'] else 'no'}"
        )
        lines.append(f"    reasons: {', '.join(item['reasons'])}")
        lines.append(
            "    "
            f"variant_created_at={item['variant_created_at']} "
            f"newest_context_at={item['newest_context_at']}"
        )

    if report["dry_run_plan"]:
        lines.extend(["", "Dry-run stale-mark plan"])
        for item in report["dry_run_plan"]:
            lines.append(
                "  - "
                f"would_mark_stale variant={item['variant_id']} "
                f"content={item['content_id']} platform={item['platform']} "
                f"type={item['variant_type']} action={item['action']} "
                f"reasons={','.join(item['reasons'])}"
            )
    return "\n".join(lines)


def _recommendation_for_row(
    row: dict[str, Any],
    *,
    latest_by_content: dict[int, dict[str, Any]],
    latest_selected_by_key: dict[tuple[int, str], dict[str, Any]],
) -> ContentVariantRefreshRecommendation | None:
    content_id = int(row["content_id"])
    platform = str(row["platform"])
    variant_created = _parse_timestamp(row.get("variant_created_at"))
    if variant_created is None:
        variant_created = datetime.min.replace(tzinfo=timezone.utc)

    reasons: list[str] = []
    newest_context_at: datetime | None = None
    for context in latest_by_content.get(content_id, {}).values():
        if not _context_applies_to_platform(context, platform):
            continue
        if context["at"] > variant_created:
            reasons.append(context["reason"])
            newest_context_at = _max_datetime(newest_context_at, context["at"])

    selected_context = latest_selected_by_key.get((content_id, platform))
    if (
        selected_context is not None
        and int(selected_context["variant_id"]) != int(row["variant_id"])
        and selected_context["at"] > variant_created
    ):
        reasons.append(REASON_SELECTED_COPY_NEWER)
        newest_context_at = _max_datetime(newest_context_at, selected_context["at"])

    if not reasons:
        return None

    deduped_reasons = _unique(reasons)
    selected = bool(row.get("selected"))
    action = _recommended_action(selected, deduped_reasons)
    return ContentVariantRefreshRecommendation(
        variant_id=int(row["variant_id"]),
        content_id=content_id,
        platform=platform,
        variant_type=str(row["variant_type"]),
        selected=selected,
        priority=_priority(selected, deduped_reasons),
        action=action,
        reasons=deduped_reasons,
        variant_created_at=row.get("variant_created_at"),
        newest_context_at=newest_context_at.isoformat() if newest_context_at else None,
        content_type=row.get("content_type"),
        auto_quality=row.get("auto_quality"),
    )


def _variant_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    platform: str | None,
) -> list[dict[str, Any]]:
    cv = schema["content_variants"]
    gc = schema["generated_content"]
    filters = []
    params: list[Any] = []
    if platform is not None:
        filters.append("cv.platform = ?")
        params.append(platform)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    content_type_expr = "gc.content_type" if "content_type" in gc else "NULL"
    auto_quality_expr = "gc.auto_quality" if "auto_quality" in gc else "NULL"
    selected_expr = "cv.selected" if "selected" in cv else "0"
    created_expr = "cv.created_at" if "created_at" in cv else "NULL"

    rows = conn.execute(
        f"""SELECT cv.id AS variant_id,
                  cv.content_id,
                  cv.platform,
                  cv.variant_type,
                  {selected_expr} AS selected,
                  {created_expr} AS variant_created_at,
                  {content_type_expr} AS content_type,
                  {auto_quality_expr} AS auto_quality
           FROM content_variants cv
           INNER JOIN generated_content gc ON gc.id = cv.content_id
           {where}
           ORDER BY cv.platform ASC, cv.content_id ASC, cv.variant_type ASC, cv.id ASC""",
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def _latest_context_by_content(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    latest: dict[int, dict[str, Any]] = {}
    _merge_context(latest, _base_content_context(conn, schema))
    _merge_context(latest, _feedback_context(conn, schema))
    _merge_context(latest, _publication_context(conn, schema))
    _merge_context(latest, _engagement_context(conn, schema))
    return latest


def _base_content_context(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    columns = schema.get("generated_content", set())
    if "id" not in columns:
        return {}
    timestamp_expr = _coalesced_timestamp_expr(
        "gc",
        columns,
        ("updated_at", "last_retry_at", "published_at", "created_at"),
    )
    auto_quality_expr = "gc.auto_quality" if "auto_quality" in columns else "NULL"
    rows = conn.execute(
        f"""SELECT gc.id AS content_id,
                  {timestamp_expr} AS context_at,
                  {auto_quality_expr} AS auto_quality
           FROM generated_content gc"""
    ).fetchall()
    context: dict[int, dict[str, Any]] = {}
    for row in rows:
        content_id = int(row["content_id"])
        parsed = _parse_timestamp(row["context_at"])
        if parsed is not None:
            context.setdefault(content_id, {})["base_content"] = {
                "reason": REASON_BASE_CONTENT_NEWER,
                "at": parsed,
            }
        if row["auto_quality"] == "low_resonance":
            low_at = parsed or datetime.min.replace(tzinfo=timezone.utc)
            context.setdefault(content_id, {})["low_resonance"] = {
                "reason": REASON_LOW_RESONANCE,
                "at": low_at,
            }
    return context


def _feedback_context(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    columns = schema.get("content_feedback", set())
    if not columns or not {"content_id", "created_at"}.issubset(columns):
        return {}
    rows = conn.execute(
        """SELECT content_id, MAX(datetime(created_at)) AS context_at
           FROM content_feedback
           GROUP BY content_id"""
    ).fetchall()
    return _rows_to_context(rows, "feedback", REASON_FEEDBACK_NEWER)


def _publication_context(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    context: dict[int, dict[str, Any]] = {}
    if "content_publications" in schema:
        columns = schema["content_publications"]
        timestamp_expr = _coalesced_timestamp_expr(
            "cp",
            columns,
            ("updated_at", "published_at", "last_error_at", "next_retry_at"),
        )
        platform_expr = "cp.platform" if "platform" in columns else "NULL"
        rows = conn.execute(
            f"""SELECT cp.content_id,
                      {platform_expr} AS platform,
                      MAX(datetime({timestamp_expr})) AS context_at
               FROM content_publications cp
               GROUP BY cp.content_id, {platform_expr}"""
        ).fetchall()
        _merge_context(
            context,
            _rows_to_context(
                rows,
                "publication",
                REASON_PUBLICATION_OUTCOME_NEWER,
                platform_column="platform",
            ),
        )

    if "publication_attempts" in schema:
        columns = schema["publication_attempts"]
        timestamp_expr = _coalesced_timestamp_expr(
            "pa",
            columns,
            ("attempted_at", "created_at"),
        )
        rows = conn.execute(
            f"""SELECT pa.content_id, MAX(datetime({timestamp_expr})) AS context_at
               FROM publication_attempts pa
               GROUP BY pa.content_id"""
        ).fetchall()
        _merge_context(context, _rows_to_context(rows, "publication_attempt", REASON_PUBLICATION_OUTCOME_NEWER))

    if "publish_queue" in schema:
        columns = schema["publish_queue"]
        timestamp_expr = _coalesced_timestamp_expr(
            "pq",
            columns,
            ("published_at", "scheduled_at", "created_at"),
        )
        platform_expr = "pq.platform" if "platform" in columns else "NULL"
        rows = conn.execute(
            f"""SELECT pq.content_id, MAX(datetime({timestamp_expr})) AS context_at
                      , {platform_expr} AS platform
               FROM publish_queue pq
               GROUP BY pq.content_id, {platform_expr}"""
        ).fetchall()
        _merge_context(
            context,
            _rows_to_context(
                rows,
                "publish_queue",
                REASON_PUBLICATION_OUTCOME_NEWER,
                platform_column="platform",
            ),
        )
    return context


def _engagement_context(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[int, dict[str, Any]]:
    context: dict[int, dict[str, Any]] = {}
    for table, timestamp_column, score_column, platforms in _ENGAGEMENT_TABLES:
        columns = schema.get(table, set())
        if not {"content_id", timestamp_column}.issubset(columns):
            continue
        score_expr = f"MIN(COALESCE({score_column}, 0)) AS min_score" if score_column in columns else "0 AS min_score"
        rows = conn.execute(
            f"""SELECT content_id,
                      MAX(datetime({timestamp_column})) AS context_at,
                      {score_expr}
               FROM {table}
               GROUP BY content_id"""
        ).fetchall()
        for row in rows:
            content_id = int(row["content_id"])
            parsed = _parse_timestamp(row["context_at"])
            if parsed is None:
                continue
            content_context = context.setdefault(content_id, {})
            content_context[f"engagement:{table}"] = {
                "reason": REASON_PERFORMANCE_CONTEXT_NEWER,
                "at": parsed,
                "platforms": platforms,
            }
            if float(row["min_score"] or 0) <= 0:
                content_context[f"low_resonance:{table}"] = {
                    "reason": REASON_LOW_RESONANCE,
                    "at": parsed,
                    "platforms": platforms,
                }
    return context


def _latest_selected_by_content_platform(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
) -> dict[tuple[int, str], dict[str, Any]]:
    columns = schema.get("content_variants", set())
    if not {"content_id", "platform", "id", "selected"}.issubset(columns):
        return {}
    created_expr = "created_at" if "created_at" in columns else "NULL"
    rows = conn.execute(
        f"""SELECT id AS variant_id, content_id, platform, {created_expr} AS selected_at
           FROM content_variants
           WHERE selected = 1
           ORDER BY content_id ASC, platform ASC, datetime({created_expr}) DESC, id DESC"""
    ).fetchall()
    selected: dict[tuple[int, str], dict[str, Any]] = {}
    for row in rows:
        parsed = _parse_timestamp(row["selected_at"])
        if parsed is None:
            continue
        selected.setdefault(
            (int(row["content_id"]), str(row["platform"])),
            {"variant_id": int(row["variant_id"]), "at": parsed},
        )
    return selected


def _rows_to_context(
    rows: list[sqlite3.Row],
    key_prefix: str,
    reason: str,
    *,
    platform_column: str | None = None,
) -> dict[int, dict[str, Any]]:
    context: dict[int, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        parsed = _parse_timestamp(row["context_at"])
        if parsed is None:
            continue
        content_id = int(row["content_id"])
        item = {
            "reason": reason,
            "at": parsed,
        }
        if platform_column is not None:
            item["platform"] = row[platform_column]
        context.setdefault(content_id, {})[f"{key_prefix}:{index}"] = item
    return context


def _dry_run_plan(
    recommendations: list[ContentVariantRefreshRecommendation],
) -> list[dict[str, Any]]:
    return [
        {
            "operation": "mark_stale",
            "variant_id": item.variant_id,
            "content_id": item.content_id,
            "platform": item.platform,
            "variant_type": item.variant_type,
            "action": item.action,
            "reasons": item.reasons,
        }
        for item in recommendations
    ]


def _recommended_action(selected: bool, reasons: list[str]) -> str:
    if REASON_LOW_RESONANCE in reasons:
        return ACTION_REVIEW_LOW_RESONANCE
    if selected:
        return ACTION_REFRESH_SELECTED
    return ACTION_REFRESH_VARIANT


def _priority(selected: bool, reasons: list[str]) -> int:
    priority = len(reasons)
    if selected:
        priority += 10
    if REASON_LOW_RESONANCE in reasons:
        priority += 8
    if REASON_FEEDBACK_NEWER in reasons:
        priority += 4
    if REASON_SELECTED_COPY_NEWER in reasons:
        priority += 2
    return priority


def _count_by_action(
    recommendations: list[ContentVariantRefreshRecommendation],
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in recommendations:
        counts[item.action] = counts.get(item.action, 0) + 1
    return dict(sorted(counts.items()))


def _merge_context(
    target: dict[int, dict[str, Any]],
    source: dict[int, dict[str, Any]],
) -> None:
    for content_id, contexts in source.items():
        target.setdefault(content_id, {}).update(contexts)


def _coalesced_timestamp_expr(
    alias: str,
    columns: set[str],
    candidates: tuple[str, ...],
) -> str:
    available = [f"{alias}.{column}" for column in candidates if column in columns]
    if not available:
        return "NULL"
    if len(available) == 1:
        return available[0]
    return f"COALESCE({', '.join(available)})"


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _max_datetime(left: datetime | None, right: datetime) -> datetime:
    return right if left is None or right > left else left


def _context_applies_to_platform(context: dict[str, Any], platform: str) -> bool:
    context_platform = context.get("platform")
    if context_platform in (None, "", "all"):
        context_platforms = context.get("platforms")
        return not context_platforms or platform in context_platforms
    return str(context_platform) == platform


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        _row_value(row, "name", 0): _table_columns(conn, _row_value(row, "name", 0))
        for row in rows
    }


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    try:
        return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})")}
    except sqlite3.Error:
        return set()


def _row_value(row: Any, key: str, index: int) -> str:
    try:
        return str(row[key])
    except (TypeError, KeyError, IndexError):
        return str(row[index])


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    return _as_utc(parsed)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
