"""Rank open content ideas using engagement from matching historical topics."""

from __future__ import annotations

import json
import re
import string
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_LIMIT = 10

_PUNCT_TRANSLATION = str.maketrans({char: " " for char in string.punctuation})
_PRIORITY_BONUS = {"high": 2.0, "normal": 0.0, "low": -1.0}


@dataclass(frozen=True)
class ContentIdeaPromotionCandidate:
    """One content idea that matches a historically resonant topic."""

    idea_id: int
    topic: str
    priority: str
    score: float
    matched_content_ids: tuple[int, ...]
    score_reasons: tuple[str, ...] = field(default_factory=tuple)
    note: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["matched_content_ids"] = list(self.matched_content_ids)
        payload["score_reasons"] = list(self.score_reasons)
        return payload


@dataclass(frozen=True)
class ContentIdeaPromotionReport:
    """Promotion candidate report plus applied filters."""

    days: int
    limit: int
    include_snoozed: bool
    engagement_after: str
    candidates: tuple[ContentIdeaPromotionCandidate, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "days": self.days,
            "limit": self.limit,
            "include_snoozed": self.include_snoozed,
            "engagement_after": self.engagement_after,
            "candidate_count": len(self.candidates),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


def normalize_topic_text(value: object | None) -> str:
    """Normalize an idea or content topic for exact topic matching."""
    text = str(value or "").casefold()
    text = text.translate(_PUNCT_TRANSLATION)
    return re.sub(r"\s+", " ", text).strip()


def build_content_idea_promotion_report(
    db,
    *,
    days: int = DEFAULT_DAYS,
    limit: int = DEFAULT_LIMIT,
    include_snoozed: bool = False,
    now: datetime | None = None,
) -> ContentIdeaPromotionReport:
    """Return ranked open content ideas with engagement-backed score reasons."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit < 0:
        raise ValueError("limit must be non-negative")

    now = _ensure_aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    if limit == 0:
        return ContentIdeaPromotionReport(
            days=days,
            limit=limit,
            include_snoozed=include_snoozed,
            engagement_after=cutoff.isoformat(),
            candidates=(),
        )

    ideas = _load_open_content_ideas(db, include_snoozed=include_snoozed, now=now)
    topic_rows = _load_topic_engagement_rows(db, cutoff=cutoff)
    rows_by_topic: dict[str, list[dict[str, Any]]] = {}
    for row in topic_rows:
        normalized = normalize_topic_text(row.get("topic"))
        if not normalized:
            continue
        rows_by_topic.setdefault(normalized, []).append(row)

    candidates: list[ContentIdeaPromotionCandidate] = []
    for idea in ideas:
        normalized_topic = normalize_topic_text(idea.get("topic"))
        if not normalized_topic:
            continue
        matches = rows_by_topic.get(normalized_topic, [])
        if not matches:
            continue
        candidates.append(_build_candidate(idea, matches))

    candidates.sort(
        key=lambda candidate: (
            -candidate.score,
            _priority_rank(candidate.priority),
            candidate.topic,
            candidate.idea_id,
        )
    )
    return ContentIdeaPromotionReport(
        days=days,
        limit=limit,
        include_snoozed=include_snoozed,
        engagement_after=cutoff.isoformat(),
        candidates=tuple(candidates[:limit]),
    )


def format_content_idea_promotion_json(report: ContentIdeaPromotionReport) -> str:
    """Format a promotion report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_content_idea_promotion_text(report: ContentIdeaPromotionReport) -> str:
    """Format a promotion report for terminal review."""
    lines = [
        "",
        "=" * 70,
        "Content Idea Promotion Candidates",
        "=" * 70,
        "",
        f"Lookback: {report.days} days",
        f"Candidates: {len(report.candidates)}",
    ]
    if not report.candidates:
        lines.extend(["", "- none", "", "=" * 70])
        return "\n".join(lines)

    for index, candidate in enumerate(report.candidates, start=1):
        content_ids = ", ".join(f"#{content_id}" for content_id in candidate.matched_content_ids)
        lines.append("")
        lines.append(
            f"{index}. idea #{candidate.idea_id} [{candidate.priority}] "
            f"{candidate.topic} - score {candidate.score:.2f}"
        )
        lines.append(f"   Matched content: {content_ids}")
        for reason in candidate.score_reasons:
            lines.append(f"   - {reason}")
        if candidate.note:
            lines.append(f"   Note: {_shorten(candidate.note)}")

    lines.extend(["", "=" * 70])
    return "\n".join(lines)


def _load_topic_engagement_rows(db, *, cutoff: datetime) -> list[dict[str, Any]]:
    cursor = db.conn.execute(
        """WITH latest_x AS (
               SELECT content_id, engagement_score, fetched_at
               FROM (
                   SELECT content_id, engagement_score, fetched_at,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM post_engagement
               )
               WHERE rn = 1
           ),
           latest_linkedin AS (
               SELECT content_id, engagement_score, fetched_at
               FROM (
                   SELECT content_id, engagement_score, fetched_at,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM linkedin_engagement
               )
               WHERE rn = 1
           ),
           latest_bluesky AS (
               SELECT content_id, engagement_score, fetched_at
               FROM (
                   SELECT content_id, engagement_score, fetched_at,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM bluesky_engagement
               )
               WHERE rn = 1
           )
           SELECT ct.topic,
                  ct.confidence,
                  gc.id AS content_id,
                  lx.engagement_score AS x_score,
                  lx.fetched_at AS x_fetched_at,
                  ll.engagement_score AS linkedin_score,
                  ll.fetched_at AS linkedin_fetched_at,
                  lb.engagement_score AS bluesky_score,
                  lb.fetched_at AS bluesky_fetched_at
           FROM content_topics ct
           INNER JOIN generated_content gc ON gc.id = ct.content_id
           LEFT JOIN latest_x lx ON lx.content_id = gc.id
           LEFT JOIN latest_linkedin ll ON ll.content_id = gc.id
           LEFT JOIN latest_bluesky lb ON lb.content_id = gc.id
           WHERE gc.published != -1
             AND ct.confidence >= 0.4
           ORDER BY ct.topic ASC, gc.id ASC"""
    )
    rows: list[dict[str, Any]] = []
    for row in cursor.fetchall():
        item = dict(row)
        platform_scores = _recent_platform_scores(item, cutoff=cutoff)
        if not platform_scores:
            continue
        item["platform_scores"] = platform_scores
        item["engagement_score"] = sum(platform_scores.values())
        rows.append(item)
    return rows


def _load_open_content_ideas(
    db,
    *,
    include_snoozed: bool,
    now: datetime,
) -> list[dict[str, Any]]:
    filters = ["status = 'open'"]
    params: list[object] = []
    if not include_snoozed:
        filters.append("(snoozed_until IS NULL OR datetime(snoozed_until) <= datetime(?))")
        params.append(now.isoformat())
    cursor = db.conn.execute(
        f"""SELECT *
            FROM content_ideas
            WHERE {' AND '.join(filters)}
            ORDER BY
                CASE priority
                    WHEN 'high' THEN 0
                    WHEN 'normal' THEN 1
                    WHEN 'low' THEN 2
                    ELSE 3
                END,
                created_at ASC,
                id ASC""",
        params,
    )
    return [dict(row) for row in cursor.fetchall()]


def _recent_platform_scores(row: dict[str, Any], *, cutoff: datetime) -> dict[str, float]:
    scores: dict[str, float] = {}
    for platform, score_key, fetched_key in (
        ("x", "x_score", "x_fetched_at"),
        ("linkedin", "linkedin_score", "linkedin_fetched_at"),
        ("bluesky", "bluesky_score", "bluesky_fetched_at"),
    ):
        score = row.get(score_key)
        fetched_at = _parse_datetime(row.get(fetched_key))
        if score is None or fetched_at is None or fetched_at < cutoff:
            continue
        scores[platform] = float(score)
    return scores


def _build_candidate(
    idea: dict[str, Any],
    rows: list[dict[str, Any]],
) -> ContentIdeaPromotionCandidate:
    content_scores: dict[int, float] = {}
    platform_totals: dict[str, float] = {}
    seen_content_ids: set[int] = set()
    for row in rows:
        content_id = int(row["content_id"])
        if content_id in seen_content_ids:
            continue
        seen_content_ids.add(content_id)
        content_scores[content_id] = float(row["engagement_score"] or 0.0)
        for platform, score in row["platform_scores"].items():
            platform_totals[platform] = platform_totals.get(platform, 0.0) + score

    scores = list(content_scores.values())
    avg_score = sum(scores) / len(scores)
    max_score = max(scores)
    platform_count = len(platform_totals)
    sample_count = len(content_scores)
    priority = str(idea.get("priority") or "normal")
    score = avg_score + (max_score * 0.25) + min(sample_count, 5) + _PRIORITY_BONUS.get(priority, 0.0)

    topic = str(idea.get("topic") or "").strip()
    reasons = [
        f"Matched {sample_count} historical content item{'s' if sample_count != 1 else ''} on topic '{topic}'",
        f"Average matched engagement {avg_score:.1f}; best {max_score:.1f}",
        f"Recent snapshots available on {', '.join(sorted(platform_totals))}",
    ]
    if priority != "normal":
        reasons.append(f"{priority} priority adjusted ranking")

    return ContentIdeaPromotionCandidate(
        idea_id=int(idea["id"]),
        topic=topic,
        priority=priority,
        score=round(score, 2),
        matched_content_ids=tuple(
            content_id
            for content_id, _score in sorted(
                content_scores.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ),
        score_reasons=tuple(reasons),
        note=str(idea.get("note") or ""),
    )


def _parse_datetime(value: object | None) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _ensure_aware(datetime.fromisoformat(text))
    except ValueError:
        return None


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _priority_rank(priority: str) -> int:
    return {"high": 0, "normal": 1, "low": 2}.get(priority, 3)


def _shorten(text: str, width: int = 92) -> str:
    value = " ".join(text.split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."
