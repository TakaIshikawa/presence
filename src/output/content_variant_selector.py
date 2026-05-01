"""Select the strongest stored content variant for a platform."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


ENGAGEMENT_TABLES = {
    "x": ("post_engagement", "engagement_score"),
    "twitter": ("post_engagement", "engagement_score"),
    "mastodon": ("mastodon_engagement", "engagement_score"),
    "bluesky": ("bluesky_engagement", "engagement_score"),
    "linkedin": ("linkedin_engagement", "engagement_score"),
}

PREFERRED_VARIANT_TYPES = {
    "blog_post": {"blog", "post"},
    "blog_seed": {"seed", "summary"},
    "newsletter": {"subject", "summary"},
    "x_post": {"post"},
    "x_thread": {"thread"},
}


def select_content_variant(
    db: Any,
    *,
    content_id: int,
    platform: str,
    apply: bool = False,
) -> dict[str, Any]:
    """Rank variants and optionally mark the best candidate selected."""
    content = db.get_generated_content(content_id)
    if not content:
        raise ValueError(f"generated_content id {content_id} does not exist")

    candidates = db.list_content_variants(content_id, platform=platform)
    if not candidates:
        raise ValueError(
            "no eligible content variants for "
            f"content_id={content_id}, platform={platform}"
        )

    history = _historical_engagement_by_variant_type(
        db,
        content=content,
        platform=platform,
        variant_types={row["variant_type"] for row in candidates},
    )
    ranked = _rank_candidates(content, platform, candidates, history)
    selected = ranked[0]

    if apply:
        db.select_content_variant(
            content_id,
            platform,
            selected["variant_type"],
        )
        selected["selected_after"] = True

    return {
        "content_id": content_id,
        "platform": platform,
        "apply": apply,
        "selected_variant_id": selected["id"],
        "selected_variant_type": selected["variant_type"],
        "history_source": history["source"],
        "history_fallback": history["fallback"],
        "candidates": ranked,
    }


def _rank_candidates(
    content: dict[str, Any],
    platform: str,
    candidates: list[dict[str, Any]],
    history: dict[str, Any],
) -> list[dict[str, Any]]:
    freshness = _freshness_scores(candidates)
    ranked = []
    for row in candidates:
        variant_type = row["variant_type"]
        historical = history["variant_types"].get(
            variant_type,
            {"count": 0, "average_score": 0.0, "component": 0.0},
        )
        components = {
            "platform_match": 10.0 if row["platform"] == platform else 0.0,
            "variant_type": _variant_type_component(content, variant_type),
            "selected_state": 0.75 if row.get("selected") else 0.0,
            "historical_engagement": historical["component"],
            "freshness": freshness.get(row["id"], 0.0),
        }
        score = round(sum(components.values()), 4)
        ranked.append(
            {
                "id": row["id"],
                "content_id": row["content_id"],
                "platform": row["platform"],
                "variant_type": variant_type,
                "selected_before": bool(row.get("selected")),
                "created_at": row.get("created_at"),
                "score": score,
                "components": components,
                "historical": {
                    "count": historical["count"],
                    "average_score": historical["average_score"],
                },
                "content_preview": _preview(row.get("content") or ""),
            }
        )

    return sorted(
        ranked,
        key=lambda item: (
            -item["score"],
            -item["components"]["historical_engagement"],
            -item["components"]["selected_state"],
            -item["components"]["freshness"],
            item["id"],
        ),
    )


def _historical_engagement_by_variant_type(
    db: Any,
    *,
    content: dict[str, Any],
    platform: str,
    variant_types: set[str],
) -> dict[str, Any]:
    table_info = ENGAGEMENT_TABLES.get(platform)
    if not table_info or not variant_types:
        return _empty_history("no_supported_engagement_table")

    table, score_column = table_info
    if not _table_exists(db, table):
        return _empty_history(f"missing_table:{table}")

    placeholders = ", ".join("?" for _ in variant_types)
    similarity_column = (
        "content_format" if content.get("content_format") else "content_type"
    )
    similarity_value = content.get(similarity_column)
    if similarity_value is None:
        return _empty_history("missing_similarity_key")

    params: list[Any] = [
        platform,
        int(content["id"]),
        similarity_value,
        *sorted(variant_types),
    ]
    cursor = db.conn.execute(
        f"""SELECT cv.variant_type,
                  COUNT(e.id) AS sample_count,
                  AVG(COALESCE(e.{score_column}, 0)) AS average_score
           FROM content_variants cv
           INNER JOIN generated_content gc ON gc.id = cv.content_id
           INNER JOIN {table} e ON e.content_id = cv.content_id
           WHERE cv.platform = ?
             AND cv.selected = 1
             AND cv.content_id != ?
             AND gc.{similarity_column} = ?
             AND cv.variant_type IN ({placeholders})
           GROUP BY cv.variant_type""",
        params,
    )
    stats = {}
    max_average = 0.0
    for row in cursor.fetchall():
        average = float(row["average_score"] or 0.0)
        stats[row["variant_type"]] = {
            "count": int(row["sample_count"] or 0),
            "average_score": round(average, 4),
            "component": 0.0,
        }
        max_average = max(max_average, average)

    if max_average > 0:
        for item in stats.values():
            item["component"] = round((item["average_score"] / max_average) * 8.0, 4)

    for variant_type in variant_types:
        stats.setdefault(
            variant_type,
            {"count": 0, "average_score": 0.0, "component": 0.0},
        )

    return {
        "source": table,
        "fallback": not any(item["count"] for item in stats.values()),
        "variant_types": stats,
    }


def _empty_history(reason: str) -> dict[str, Any]:
    return {"source": reason, "fallback": True, "variant_types": {}}


def _variant_type_component(content: dict[str, Any], variant_type: str) -> float:
    content_format = content.get("content_format")
    if content_format and variant_type == content_format:
        return 3.0
    preferred = PREFERRED_VARIANT_TYPES.get(content.get("content_type"), set())
    if variant_type in preferred:
        return 2.0
    return 1.0


def _freshness_scores(candidates: list[dict[str, Any]]) -> dict[int, float]:
    ordered = sorted(
        candidates,
        key=lambda row: (_parse_timestamp(row.get("created_at")), int(row["id"])),
    )
    if len(ordered) == 1:
        return {int(ordered[0]["id"]): 0.5}
    denominator = len(ordered) - 1
    return {
        int(row["id"]): round(index / denominator, 4)
        for index, row in enumerate(ordered)
    }


def _parse_timestamp(value: str | None) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=timezone.utc)
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _preview(value: str, width: int = 96) -> str:
    text = " ".join(value.split())
    if len(text) <= width:
        return text
    return text[: width - 3] + "..."


def _table_exists(db: Any, table: str) -> bool:
    row = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None
