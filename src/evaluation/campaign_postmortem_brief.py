"""Operator-facing campaign postmortem brief generation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
ENGAGEMENT_TABLES = (
    ("post_engagement", "x"),
    ("bluesky_engagement", "bluesky"),
    ("linkedin_engagement", "linkedin"),
    ("mastodon_engagement", "mastodon"),
)


def build_campaign_postmortem_brief(
    db_or_conn: Any,
    *,
    campaign_id: int,
    days: int = DEFAULT_DAYS,
    include_posts: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build a read-only postmortem brief for one campaign."""
    if campaign_id <= 0:
        raise ValueError("campaign_id must be positive")
    if days <= 0:
        raise ValueError("days must be positive")

    conn = _connection(db_or_conn)
    schema = _schema(conn)
    generated_at = _aware(now or datetime.now(timezone.utc))
    filters = {
        "campaign_id": campaign_id,
        "days": days,
        "include_posts": include_posts,
    }
    missing_required = [
        table for table in ("content_campaigns", "planned_topics") if table not in schema
    ]
    if missing_required:
        return _empty_brief(
            generated_at=generated_at,
            filters=filters,
            missing_required_tables=missing_required,
            unknown_optional_signals=_unknown_optional(schema),
        )

    campaign = _load_campaign(conn, schema, campaign_id)
    if campaign is None:
        return _empty_brief(
            generated_at=generated_at,
            filters=filters,
            missing_required_tables=[],
            unknown_optional_signals=_unknown_optional(schema),
            campaign={"id": campaign_id, "name": f"Campaign {campaign_id}", "found": False},
        )

    cutoff = generated_at - timedelta(days=days)
    planned_topics = _load_planned_topics(conn, schema, campaign_id)
    content_ids = sorted(
        {int(row["content_id"]) for row in planned_topics if row.get("content_id") is not None}
    )
    publications = _load_publications(conn, schema, content_ids)
    engagement = _load_engagement(conn, schema, content_ids)
    baselines = _platform_baselines(engagement)
    posts = [
        _post_row(row, publications.get(int(row["content_id"]), []), engagement, baselines)
        for row in planned_topics
        if row.get("content_id") is not None and _row_in_window(row, cutoff, generated_at)
    ]
    missed_topics = [
        _missed_topic(row)
        for row in planned_topics
        if row.get("content_id") is None
        and str(row.get("topic_status") or "planned").lower() != "skipped"
        and _row_in_window(row, cutoff, generated_at)
    ]
    generated_unpublished = [
        _generated_unpublished(post) for post in posts if post["publish_status"] != "published"
    ]

    summary = _summary(planned_topics, posts, missed_topics, generated_unpublished)
    wins = _wins(posts)
    misses = _misses(missed_topics, generated_unpublished, posts)
    follow_ups = _follow_ups(wins, missed_topics, generated_unpublished, posts)

    brief: dict[str, Any] = {
        "artifact_type": "campaign_postmortem_brief",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "campaign": _campaign_payload(campaign),
        "summary": summary,
        "wins": wins,
        "misses": misses,
        "recommended_follow_ups": follow_ups,
        "format_performance": _format_performance(posts),
        "platform_performance": _platform_performance(posts),
        "publication_latency": _publication_latency(posts),
        "missed_planned_topics": missed_topics,
        "generated_unpublished_content": generated_unpublished,
        "missing_required_tables": [],
        "unknown_optional_signals": _unknown_optional(schema),
    }
    if include_posts:
        brief["posts"] = posts
    return brief


def format_json_brief(brief: dict[str, Any]) -> str:
    """Render the brief as deterministic JSON."""
    return json.dumps(brief, indent=2, sort_keys=True)


def format_markdown_brief(brief: dict[str, Any]) -> str:
    """Render the brief as stable Markdown."""
    campaign = brief.get("campaign") or {}
    summary = brief.get("summary") or {}
    title = campaign.get("name") or f"Campaign {brief['filters']['campaign_id']}"
    lines = [
        f"# Campaign Postmortem Brief: {title}",
        "",
        f"- Campaign ID: {campaign.get('id', brief['filters']['campaign_id'])}",
        f"- Status: {campaign.get('status') or 'n/a'}",
        f"- Window: {campaign.get('start_date') or 'n/a'} to {campaign.get('end_date') or 'n/a'}",
        f"- Lookback: {brief['filters']['days']} days",
        f"- Generated: {brief['generated_at']}",
    ]
    if campaign.get("goal"):
        lines.append(f"- Goal: {campaign['goal']}")
    if campaign.get("found") is False:
        lines.append("- Campaign row: not found")
    if brief.get("missing_required_tables"):
        lines.append("- Missing required tables: " + ", ".join(brief["missing_required_tables"]))
    if brief.get("unknown_optional_signals"):
        lines.append("- Unknown optional signals: " + ", ".join(brief["unknown_optional_signals"]))

    lines.extend(
        [
            "",
            "## Summary",
            f"- Planned topics: {summary.get('planned_topics', 0)}",
            f"- Generated topics: {summary.get('generated_topics', 0)}",
            f"- Published posts: {summary.get('published_posts', 0)}",
            f"- Missed planned topics: {summary.get('missed_planned_topics', 0)}",
            f"- Generated but unpublished: {summary.get('generated_unpublished', 0)}",
            f"- Average normalized engagement: {_format_float(summary.get('avg_normalized_engagement'))}",
        ]
    )

    lines.extend(["", "## Wins"])
    if brief.get("wins"):
        for win in brief["wins"]:
            lines.append(
                "- "
                f"{win['label']}: {win['reason']} "
                f"(normalized {_format_float(win.get('normalized_engagement'))})"
            )
    else:
        lines.append("- No clear wins found in the selected window.")

    lines.extend(["", "## Misses"])
    if brief.get("misses"):
        for miss in brief["misses"]:
            lines.append(f"- {miss['label']}: {miss['reason']}")
    else:
        lines.append("- No missed planned topics or unpublished generated content found.")

    lines.extend(["", "## Format Performance"])
    if brief.get("format_performance"):
        for item in brief["format_performance"]:
            lines.append(
                "- "
                f"{item['format']}: posts={item['post_count']} "
                f"published={item['published_count']} "
                f"avg_normalized={_format_float(item['avg_normalized_engagement'])}"
            )
    else:
        lines.append("- No format performance data available.")

    lines.extend(["", "## Recommended Follow-Ups"])
    if brief.get("recommended_follow_ups"):
        for item in brief["recommended_follow_ups"]:
            lines.append(f"- {item['action']}: {item['reason']}")
    else:
        lines.append("- No follow-ups recommended.")

    if brief["filters"].get("include_posts"):
        lines.extend(["", "## Posts"])
        for post in brief.get("posts", []):
            lines.append(
                "- "
                f"#{post['content_id']} {post['topic']} "
                f"[{post['publish_status']}] "
                f"platforms={','.join(post['published_platforms']) or '-'} "
                f"normalized={_format_float(post['normalized_engagement'])}"
            )
    return "\n".join(lines)


def _load_campaign(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign_id: int,
) -> dict[str, Any] | None:
    columns = schema["content_campaigns"]
    if "id" not in columns:
        return None
    selected = {
        "id": "id",
        "name": _column_expr("content_campaigns", columns, "name"),
        "goal": _column_expr("content_campaigns", columns, "goal"),
        "start_date": _column_expr("content_campaigns", columns, "start_date"),
        "end_date": _column_expr("content_campaigns", columns, "end_date"),
        "status": _column_expr("content_campaigns", columns, "status"),
        "created_at": _column_expr("content_campaigns", columns, "created_at"),
    }
    row = conn.execute(
        f"""SELECT {', '.join(f'{expr} AS {alias}' for alias, expr in selected.items())}
            FROM content_campaigns
            WHERE id = ?""",
        (campaign_id,),
    ).fetchone()
    return dict(row) if row else None


def _load_planned_topics(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    campaign_id: int,
) -> list[dict[str, Any]]:
    columns = schema["planned_topics"]
    if "campaign_id" not in columns:
        return []
    gc_columns = schema.get("generated_content", set())
    select = {
        "planned_topic_id": _column_expr("pt", columns, "id"),
        "campaign_id": _column_expr("pt", columns, "campaign_id"),
        "topic": _column_expr("pt", columns, "topic", "''"),
        "angle": _column_expr("pt", columns, "angle"),
        "target_date": _column_expr("pt", columns, "target_date"),
        "topic_status": _column_expr("pt", columns, "status"),
        "content_id": _column_expr("pt", columns, "content_id"),
        "planned_at": _column_expr("pt", columns, "created_at"),
        "content_type": _column_expr("gc", gc_columns, "content_type"),
        "content_format": _column_expr("gc", gc_columns, "content_format"),
        "content": _column_expr("gc", gc_columns, "content"),
        "eval_score": _column_expr("gc", gc_columns, "eval_score"),
        "auto_quality": _column_expr("gc", gc_columns, "auto_quality"),
        "generated_at": _column_expr("gc", gc_columns, "created_at"),
        "legacy_published": _column_expr("gc", gc_columns, "published"),
        "legacy_published_at": _column_expr("gc", gc_columns, "published_at"),
    }
    join = ""
    if "generated_content" in schema and "content_id" in columns and "id" in gc_columns:
        join = "LEFT JOIN generated_content gc ON gc.id = pt.content_id"
    rows = conn.execute(
        f"""SELECT {', '.join(f'{expr} AS {alias}' for alias, expr in select.items())}
            FROM planned_topics pt
            {join}
            WHERE pt.campaign_id = ?
            ORDER BY pt.target_date ASC NULLS LAST,
                     pt.created_at ASC NULLS LAST,
                     pt.id ASC""",
        (campaign_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_publications(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    columns = schema.get("content_publications")
    if not content_ids or not columns or not {"content_id", "platform"}.issubset(columns):
        return {}
    select = {
        "content_id": "cp.content_id",
        "platform": _column_expr("cp", columns, "platform"),
        "status": _column_expr("cp", columns, "status"),
        "published_at": _column_expr("cp", columns, "published_at"),
        "platform_url": _column_expr("cp", columns, "platform_url"),
        "error_category": _column_expr("cp", columns, "error_category"),
        "updated_at": _column_expr("cp", columns, "updated_at"),
    }
    placeholders = ",".join("?" for _ in content_ids)
    rows = conn.execute(
        f"""SELECT {', '.join(f'{expr} AS {alias}' for alias, expr in select.items())}
            FROM content_publications cp
            WHERE cp.content_id IN ({placeholders})
            ORDER BY cp.content_id ASC, cp.platform ASC""",
        tuple(content_ids),
    ).fetchall()
    publications: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        publications.setdefault(int(item["content_id"]), []).append(item)
    return publications


def _load_engagement(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    content_ids: list[int],
) -> dict[int, dict[str, dict[str, Any]]]:
    metrics: dict[int, dict[str, dict[str, Any]]] = {content_id: {} for content_id in content_ids}
    for table, platform in ENGAGEMENT_TABLES:
        columns = schema.get(table)
        if not content_ids or not columns or not {"content_id", "engagement_score"}.issubset(columns):
            continue
        fetched_expr = _column_expr(table, columns, "fetched_at", "''")
        id_expr = _column_expr(table, columns, "id", "0")
        placeholders = ",".join("?" for _ in content_ids)
        rows = conn.execute(
            f"""SELECT content_id, engagement_score
                FROM (
                    SELECT content_id, engagement_score,
                           ROW_NUMBER() OVER (
                               PARTITION BY content_id ORDER BY {fetched_expr} DESC, {id_expr} DESC
                           ) AS rn
                    FROM {table}
                    WHERE engagement_score IS NOT NULL
                      AND content_id IN ({placeholders})
                )
                WHERE rn = 1""",
            tuple(content_ids),
        ).fetchall()
        for row in rows:
            metrics.setdefault(int(row["content_id"]), {})[platform] = {
                "score": round(float(row["engagement_score"] or 0.0), 2),
            }
    return metrics


def _post_row(
    topic: dict[str, Any],
    publications: list[dict[str, Any]],
    engagement: dict[int, dict[str, dict[str, Any]]],
    baselines: dict[str, float],
) -> dict[str, Any]:
    content_id = int(topic["content_id"])
    published = [
        item for item in publications if str(item.get("status") or "").lower() == "published"
    ]
    legacy_published = bool(topic.get("legacy_published") == 1 or topic.get("legacy_published_at"))
    published_platforms = sorted(
        {str(item.get("platform")) for item in published if item.get("platform")}
    )
    if legacy_published and not published_platforms:
        published_platforms = ["legacy"]
    publish_status = _publish_status(legacy_published, publications, published_platforms)
    platform_scores = engagement.get(content_id, {})
    normalized_scores = {
        platform: round(score["score"] / baselines[platform], 2)
        for platform, score in platform_scores.items()
        if baselines.get(platform, 0.0) > 0
    }
    normalized = (
        round(sum(normalized_scores.values()) / len(normalized_scores), 2)
        if normalized_scores
        else None
    )
    first_published_at = _first_timestamp(
        [item.get("published_at") for item in published] + [topic.get("legacy_published_at")]
    )
    generated_at = topic.get("generated_at")
    return {
        "planned_topic_id": int(topic["planned_topic_id"]),
        "content_id": content_id,
        "topic": str(topic.get("topic") or ""),
        "angle": topic.get("angle"),
        "target_date": topic.get("target_date"),
        "content_type": topic.get("content_type"),
        "content_format": topic.get("content_format") or topic.get("content_type") or "unknown",
        "generated_at": generated_at,
        "publish_status": publish_status,
        "published_at": first_published_at,
        "published_platforms": published_platforms,
        "raw_engagement": {
            platform: value["score"] for platform, value in sorted(platform_scores.items())
        },
        "normalized_platform_engagement": dict(sorted(normalized_scores.items())),
        "normalized_engagement": normalized,
        "publication_latency_hours": _latency_hours(generated_at, first_published_at),
        "eval_score": _float_or_none(topic.get("eval_score")),
        "auto_quality": topic.get("auto_quality"),
        "content_preview": _preview(topic.get("content")),
    }


def _summary(
    planned_topics: list[dict[str, Any]],
    posts: list[dict[str, Any]],
    missed_topics: list[dict[str, Any]],
    generated_unpublished: list[dict[str, Any]],
) -> dict[str, Any]:
    unique_planned = {row["planned_topic_id"]: row for row in planned_topics}
    published_posts = [post for post in posts if post["publish_status"] == "published"]
    normalized = [
        post["normalized_engagement"]
        for post in published_posts
        if post.get("normalized_engagement") is not None
    ]
    return {
        "planned_topics": len(unique_planned),
        "generated_topics": sum(1 for row in unique_planned.values() if row.get("content_id") is not None),
        "posts_in_window": len(posts),
        "published_posts": len(published_posts),
        "missed_planned_topics": len(missed_topics),
        "generated_unpublished": len(generated_unpublished),
        "avg_normalized_engagement": _average(normalized),
    }


def _wins(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates = [
        post for post in posts
        if post["publish_status"] == "published" and post.get("normalized_engagement") is not None
    ]
    candidates.sort(
        key=lambda post: (
            -(post["normalized_engagement"] or 0.0),
            post["published_at"] or "",
            post["content_id"],
        )
    )
    wins = []
    for post in candidates[:3]:
        wins.append(
            {
                "type": "top_published_post",
                "label": f"content #{post['content_id']} {post['topic']}",
                "content_id": post["content_id"],
                "topic": post["topic"],
                "format": post["content_format"],
                "normalized_engagement": post["normalized_engagement"],
                "reason": "Published post outperformed its platform baseline.",
            }
        )
    return wins


def _misses(
    missed_topics: list[dict[str, Any]],
    generated_unpublished: list[dict[str, Any]],
    posts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    misses = []
    for topic in missed_topics:
        misses.append(
            {
                "type": "missed_planned_topic",
                "label": f"planned topic #{topic['planned_topic_id']} {topic['topic']}",
                "planned_topic_id": topic["planned_topic_id"],
                "reason": "Planned topic did not produce generated content.",
            }
        )
    for item in generated_unpublished:
        misses.append(
            {
                "type": "generated_unpublished_content",
                "label": f"content #{item['content_id']} {item['topic']}",
                "content_id": item["content_id"],
                "planned_topic_id": item["planned_topic_id"],
                "reason": "Content was generated but has no published outcome.",
            }
        )
    low = [
        post for post in posts
        if post["publish_status"] == "published"
        and post.get("normalized_engagement") is not None
        and post["normalized_engagement"] < 0.75
    ]
    low.sort(key=lambda post: (post["normalized_engagement"] or 0.0, post["content_id"]))
    for post in low[:3]:
        misses.append(
            {
                "type": "low_normalized_engagement",
                "label": f"content #{post['content_id']} {post['topic']}",
                "content_id": post["content_id"],
                "reason": "Published content trailed comparable platform performance.",
            }
        )
    return misses


def _follow_ups(
    wins: list[dict[str, Any]],
    missed_topics: list[dict[str, Any]],
    generated_unpublished: list[dict[str, Any]],
    posts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    follow_ups = []
    for topic in missed_topics[:3]:
        follow_ups.append(
            {
                "type": "cover_missed_topic",
                "action": f"Cover missed topic: {topic['topic']}",
                "reason": "This was planned but never generated; schedule it or mark it skipped before the next campaign.",
                "planned_topic_id": topic["planned_topic_id"],
            }
        )
    for item in generated_unpublished[:3]:
        follow_ups.append(
            {
                "type": "publish_generated_content",
                "action": f"Review unpublished content #{item['content_id']}",
                "reason": "Generated campaign content is available but has not reached an audience.",
                "content_id": item["content_id"],
            }
        )
    for win in wins[:2]:
        follow_ups.append(
            {
                "type": "repurpose_win",
                "action": f"Repurpose content #{win['content_id']}",
                "reason": "This was the strongest normalized performer in the campaign window.",
                "content_id": win["content_id"],
            }
        )
    if not follow_ups:
        action = "Close campaign and carry forward the best format mix"
        if not posts:
            action = "Create a next campaign plan from the missed topic inventory"
        follow_ups.append(
            {
                "type": "campaign_review",
                "action": action,
                "reason": "No missed or unpublished rows were found in the selected window.",
            }
        )
    return follow_ups


def _format_performance(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for post in posts:
        groups.setdefault(str(post.get("content_format") or "unknown"), []).append(post)
    rows = []
    for format_name, items in groups.items():
        scores = [
            item["normalized_engagement"]
            for item in items
            if item.get("normalized_engagement") is not None
        ]
        rows.append(
            {
                "format": format_name,
                "post_count": len(items),
                "published_count": sum(1 for item in items if item["publish_status"] == "published"),
                "avg_normalized_engagement": _average(scores),
            }
        )
    rows.sort(
        key=lambda row: (
            -(row["avg_normalized_engagement"] or 0.0),
            -row["published_count"],
            row["format"],
        )
    )
    return rows


def _platform_performance(posts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    platforms = sorted({platform for post in posts for platform in post["raw_engagement"]})
    rows = []
    for platform in platforms:
        raw_scores = [post["raw_engagement"][platform] for post in posts if platform in post["raw_engagement"]]
        norm_scores = [
            post["normalized_platform_engagement"][platform]
            for post in posts
            if platform in post["normalized_platform_engagement"]
        ]
        rows.append(
            {
                "platform": platform,
                "metric_count": len(raw_scores),
                "avg_raw_engagement": _average(raw_scores),
                "avg_normalized_engagement": _average(norm_scores),
            }
        )
    return rows


def _publication_latency(posts: list[dict[str, Any]]) -> dict[str, Any]:
    latencies = [
        post["publication_latency_hours"]
        for post in posts
        if post.get("publication_latency_hours") is not None
    ]
    if not latencies:
        return {"count": 0, "avg_hours": None, "max_hours": None}
    return {
        "count": len(latencies),
        "avg_hours": _average(latencies),
        "max_hours": round(max(latencies), 2),
    }


def _platform_baselines(
    engagement: dict[int, dict[str, dict[str, Any]]],
) -> dict[str, float]:
    values: dict[str, list[float]] = {}
    for platform_scores in engagement.values():
        for platform, metric in platform_scores.items():
            values.setdefault(platform, []).append(float(metric["score"]))
    return {
        platform: round(sum(scores) / len(scores), 4)
        for platform, scores in values.items()
        if scores
    }


def _empty_brief(
    *,
    generated_at: datetime,
    filters: dict[str, Any],
    missing_required_tables: list[str],
    unknown_optional_signals: list[str],
    campaign: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "artifact_type": "campaign_postmortem_brief",
        "generated_at": generated_at.isoformat(),
        "filters": filters,
        "campaign": campaign or {"id": filters["campaign_id"], "name": f"Campaign {filters['campaign_id']}", "found": False},
        "summary": {
            "planned_topics": 0,
            "generated_topics": 0,
            "posts_in_window": 0,
            "published_posts": 0,
            "missed_planned_topics": 0,
            "generated_unpublished": 0,
            "avg_normalized_engagement": None,
        },
        "wins": [],
        "misses": [],
        "recommended_follow_ups": [
            {
                "type": "campaign_review",
                "action": "Create or select a campaign with planned topic rows",
                "reason": "No matching campaign rows were available for this brief.",
            }
        ],
        "format_performance": [],
        "platform_performance": [],
        "publication_latency": {"count": 0, "avg_hours": None, "max_hours": None},
        "missed_planned_topics": [],
        "generated_unpublished_content": [],
        "missing_required_tables": missing_required_tables,
        "unknown_optional_signals": unknown_optional_signals,
    }


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if conn is None:
        raise ValueError("database connection is not available")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row[0]
        schema[table] = {column[1] for column in conn.execute(f"PRAGMA table_info({table})")}
    return schema


def _unknown_optional(schema: dict[str, set[str]]) -> list[str]:
    optional = []
    for table in ("generated_content", "content_publications", "publish_queue"):
        if table not in schema:
            optional.append(table)
    for table, _platform in ENGAGEMENT_TABLES:
        if table not in schema:
            optional.append(table)
        elif "engagement_score" not in schema[table]:
            optional.append(f"{table}.engagement_score")
    return optional


def _column_expr(
    alias: str,
    columns: set[str],
    column: str,
    fallback: str = "NULL",
) -> str:
    return f"{alias}.{column}" if column in columns else fallback


def _publish_status(
    legacy_published: bool,
    publications: list[dict[str, Any]],
    published_platforms: list[str],
) -> str:
    if legacy_published or published_platforms:
        return "published"
    statuses = {str(item.get("status") or "").lower() for item in publications}
    if "failed" in statuses:
        return "publish_failed"
    if statuses:
        return "queued"
    return "unpublished"


def _missed_topic(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "planned_topic_id": int(row["planned_topic_id"]),
        "topic": str(row.get("topic") or ""),
        "angle": row.get("angle"),
        "target_date": row.get("target_date"),
        "status": row.get("topic_status") or "planned",
    }


def _generated_unpublished(post: dict[str, Any]) -> dict[str, Any]:
    return {
        "planned_topic_id": post["planned_topic_id"],
        "content_id": post["content_id"],
        "topic": post["topic"],
        "angle": post["angle"],
        "publish_status": post["publish_status"],
        "content_format": post["content_format"],
    }


def _campaign_payload(campaign: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": int(campaign["id"]),
        "name": campaign.get("name") or f"Campaign {campaign['id']}",
        "goal": campaign.get("goal"),
        "start_date": campaign.get("start_date"),
        "end_date": campaign.get("end_date"),
        "status": campaign.get("status"),
        "found": True,
    }


def _row_in_window(
    row: dict[str, Any],
    cutoff: datetime,
    now: datetime,
) -> bool:
    for field in ("target_date", "generated_at", "legacy_published_at", "planned_at"):
        parsed = _parse_timestamp(row.get(field))
        if parsed is not None and cutoff <= parsed <= now:
            return True
    return False


def _first_timestamp(values: list[Any]) -> str | None:
    parsed = [
        (_parse_timestamp(value), str(value))
        for value in values
        if value and _parse_timestamp(value) is not None
    ]
    if not parsed:
        return None
    parsed.sort(key=lambda item: item[0])
    return parsed[0][1]


def _latency_hours(start: Any, end: Any) -> float | None:
    start_ts = _parse_timestamp(start)
    end_ts = _parse_timestamp(end)
    if start_ts is None or end_ts is None:
        return None
    return round((end_ts - start_ts).total_seconds() / 3600.0, 2)


def _parse_timestamp(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if len(text) == 10:
            parsed = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)
        else:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _average(values: list[float]) -> float | None:
    if not values:
        return None
    return round(sum(float(value) for value in values) / len(values), 2)


def _preview(content: Any, width: int = 120) -> str:
    text = " ".join(str(content or "").split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _format_float(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}"
