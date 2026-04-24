"""Seed rewrite ideas from published posts that underperformed."""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


SOURCE_NAME = "low_resonance_rewriter"
DEFAULT_DAYS = 30
DEFAULT_LIMIT = 10
DEFAULT_MIN_SCORE_GAP = 0.0


class LowResonanceRewriterError(ValueError):
    """Raised when rewrite idea selection cannot be performed."""


@dataclass(frozen=True)
class RewriteCandidate:
    """Low-resonance content that can be turned into a rewrite idea."""

    source_content_id: int
    source_content_type: str
    content: str
    topic: str
    note: str
    source: str
    priority: str
    source_metadata: dict[str, Any]
    published_at: str | None
    engagement_score: float
    expected_score: float | None
    score_gap: float | None
    like_count: int
    repost_count: int
    reply_count: int
    quote_count: int
    published_url: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RewriteSeedResult:
    """Outcome from creating or previewing one rewrite idea."""

    status: str
    source_content_id: int
    topic: str
    idea_id: int | None
    reason: str
    note: str
    candidate: RewriteCandidate

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["candidate"] = self.candidate.to_dict()
        return data


class LowResonanceRewriter:
    """Find low-resonance posts and persist reviewable rewrite ideas."""

    def __init__(self, db: Any) -> None:
        self.db = db

    def find_candidates(
        self,
        *,
        days: int = DEFAULT_DAYS,
        limit: int = DEFAULT_LIMIT,
        min_score_gap: float = DEFAULT_MIN_SCORE_GAP,
        priority: str = "normal",
        now: datetime | None = None,
    ) -> list[RewriteCandidate]:
        if days <= 0:
            raise LowResonanceRewriterError("days must be positive")
        if limit <= 0:
            return []
        priority = self.db._normalize_content_idea_priority(priority)
        cutoff = self._cutoff(days, now)

        cursor = self.db.conn.execute(
            """WITH latest_x AS (
                   SELECT content_id, engagement_score, like_count,
                          retweet_count AS repost_count, reply_count, quote_count,
                          fetched_at, 'x' AS platform,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id
                              ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM post_engagement
               ),
               latest_bluesky AS (
                   SELECT content_id, engagement_score, like_count,
                          repost_count, reply_count, quote_count, fetched_at,
                          'bluesky' AS platform,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id
                              ORDER BY fetched_at DESC, id DESC
                          ) AS rn
                   FROM bluesky_engagement
               ),
               latest_engagement AS (
                   SELECT * FROM latest_x WHERE rn = 1
                   UNION ALL
                   SELECT * FROM latest_bluesky WHERE rn = 1
               ),
               selected_engagement AS (
                   SELECT content_id, engagement_score, like_count, repost_count,
                          reply_count, quote_count, fetched_at, platform,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id
                              ORDER BY engagement_score ASC, fetched_at DESC
                          ) AS rn
                   FROM latest_engagement
               ),
               latest_prediction AS (
                   SELECT content_id, predicted_score, hook_strength, specificity,
                          emotional_resonance, novelty, actionability,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id
                              ORDER BY created_at DESC, id DESC
                          ) AS rn
                   FROM engagement_predictions
               ),
               ranked_topics AS (
                   SELECT content_id, topic, subtopic, confidence,
                          ROW_NUMBER() OVER (
                              PARTITION BY content_id
                              ORDER BY confidence DESC, id ASC
                          ) AS rn
                   FROM content_topics
               )
               SELECT gc.id, gc.content_type, gc.content, gc.eval_score,
                      gc.published_url, gc.published_at, gc.auto_quality,
                      le.platform, le.engagement_score, le.like_count,
                      le.repost_count, le.reply_count, le.quote_count,
                      le.fetched_at, lp.predicted_score, lp.hook_strength,
                      lp.specificity, lp.emotional_resonance, lp.novelty,
                      lp.actionability, rt.topic, rt.subtopic
               FROM generated_content gc
               INNER JOIN selected_engagement le
                       ON le.content_id = gc.id AND le.rn = 1
               LEFT JOIN latest_prediction lp ON lp.content_id = gc.id AND lp.rn = 1
               LEFT JOIN ranked_topics rt ON rt.content_id = gc.id AND rt.rn = 1
               WHERE gc.published = 1
                 AND gc.auto_quality = 'low_resonance'
                 AND gc.published_at IS NOT NULL
                 AND gc.published_at >= ?
                 AND le.engagement_score IS NOT NULL
                 AND (COALESCE(lp.predicted_score, gc.eval_score, le.engagement_score)
                      - le.engagement_score) >= ?
               ORDER BY
                   (COALESCE(lp.predicted_score, gc.eval_score, le.engagement_score)
                    - le.engagement_score) DESC,
                   le.engagement_score ASC,
                   gc.published_at DESC,
                   gc.id ASC
               LIMIT ?""",
            (cutoff, float(min_score_gap), int(limit)),
        )

        return [
            self._row_to_candidate(dict(row), priority=priority)
            for row in cursor.fetchall()
        ]

    def seed_ideas(
        self,
        *,
        days: int = DEFAULT_DAYS,
        limit: int = DEFAULT_LIMIT,
        min_score_gap: float = DEFAULT_MIN_SCORE_GAP,
        dry_run: bool = False,
        priority: str = "normal",
        now: datetime | None = None,
    ) -> list[RewriteSeedResult]:
        candidates = self.find_candidates(
            days=days,
            limit=limit,
            min_score_gap=min_score_gap,
            priority=priority,
            now=now,
        )
        results: list[RewriteSeedResult] = []
        for candidate in candidates:
            existing = self.db.find_active_content_idea_for_source_metadata(
                source=SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
            if existing:
                results.append(
                    RewriteSeedResult(
                        status="skipped",
                        source_content_id=candidate.source_content_id,
                        topic=candidate.topic,
                        idea_id=int(existing["id"]),
                        reason=f"{existing['status']} duplicate",
                        note=candidate.note,
                        candidate=candidate,
                    )
                )
                continue

            if dry_run:
                results.append(
                    RewriteSeedResult(
                        status="skipped",
                        source_content_id=candidate.source_content_id,
                        topic=candidate.topic,
                        idea_id=None,
                        reason="dry run",
                        note=candidate.note,
                        candidate=candidate,
                    )
                )
                continue

            idea_id = self.db.add_content_idea(
                note=candidate.note,
                topic=candidate.topic,
                priority=candidate.priority,
                source=SOURCE_NAME,
                source_metadata=candidate.source_metadata,
            )
            results.append(
                RewriteSeedResult(
                    status="created",
                    source_content_id=candidate.source_content_id,
                    topic=candidate.topic,
                    idea_id=int(idea_id),
                    reason="created",
                    note=candidate.note,
                    candidate=candidate,
                )
            )
        return results

    def _row_to_candidate(
        self,
        row: dict[str, Any],
        *,
        priority: str,
    ) -> RewriteCandidate:
        engagement_score = float(row.get("engagement_score") or 0.0)
        expected_score = row.get("predicted_score")
        if expected_score is None:
            expected_score = row.get("eval_score")
        expected = float(expected_score) if expected_score is not None else None
        score_gap = round(expected - engagement_score, 2) if expected is not None else None
        topic = self._topic(row)
        note = self._rewrite_note(row, topic=topic, expected_score=expected, score_gap=score_gap)
        metadata = {
            "source": SOURCE_NAME,
            "source_content_id": int(row["id"]),
            "source_content_type": row.get("content_type"),
            "published_at": row.get("published_at"),
            "published_url": row.get("published_url"),
            "platform": row.get("platform"),
            "engagement_score": engagement_score,
            "expected_score": expected,
            "score_gap": score_gap,
            "auto_quality": row.get("auto_quality"),
            "topic": topic,
        }
        return RewriteCandidate(
            source_content_id=int(row["id"]),
            source_content_type=str(row.get("content_type") or ""),
            content=str(row.get("content") or ""),
            topic=topic,
            note=note,
            source=SOURCE_NAME,
            priority=priority,
            source_metadata=metadata,
            published_at=row.get("published_at"),
            engagement_score=engagement_score,
            expected_score=expected,
            score_gap=score_gap,
            like_count=int(row.get("like_count") or 0),
            repost_count=int(row.get("repost_count") or 0),
            reply_count=int(row.get("reply_count") or 0),
            quote_count=int(row.get("quote_count") or 0),
            published_url=row.get("published_url"),
        )

    @staticmethod
    def _cutoff(days: int, now: datetime | None) -> str:
        if now is None:
            return f"-{days} days"
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        cutoff = now.astimezone(timezone.utc).timestamp() - (days * 86400)
        return datetime.fromtimestamp(cutoff, tz=timezone.utc).isoformat()

    @staticmethod
    def _topic(row: dict[str, Any]) -> str:
        topic = str(row.get("topic") or "").strip()
        if topic:
            return topic
        content_type = str(row.get("content_type") or "content").replace("_", " ")
        words = re.findall(r"[A-Za-z0-9][A-Za-z0-9'-]*", str(row.get("content") or ""))
        fallback = " ".join(words[:5]).strip()
        return fallback or content_type

    @staticmethod
    def _rewrite_note(
        row: dict[str, Any],
        *,
        topic: str,
        expected_score: float | None,
        score_gap: float | None,
    ) -> str:
        engagement = float(row.get("engagement_score") or 0.0)
        metrics = (
            f"{int(row.get('like_count') or 0)} likes, "
            f"{int(row.get('repost_count') or 0)} reposts, "
            f"{int(row.get('reply_count') or 0)} replies"
        )
        gap_phrase = (
            f"expected about {expected_score:.1f} but landed at {engagement:.1f}"
            if expected_score is not None
            else f"landed at {engagement:.1f}"
        )
        weakest = LowResonanceRewriter._weakest_prediction_dimension(row)
        weakness = (
            f" The weakest predicted dimension was {weakest[0].replace('_', ' ')} "
            f"({weakest[1]:.1f}), so rewrite with a sharper reader payoff."
            if weakest is not None
            else " Rewrite with a more concrete hook, clearer stakes, and a specific reader payoff."
        )
        gap_suffix = f", gap {score_gap:.1f}" if score_gap is not None else ""
        return (
            f"Rewrite low-resonance {topic}: the published post {gap_phrase}"
            f"{gap_suffix} with {metrics}.{weakness} Source excerpt: "
            f"{LowResonanceRewriter._shorten(row.get('content'), 140)}"
        )

    @staticmethod
    def _weakest_prediction_dimension(row: dict[str, Any]) -> tuple[str, float] | None:
        scores: list[tuple[str, float]] = []
        for key in (
            "hook_strength",
            "specificity",
            "emotional_resonance",
            "novelty",
            "actionability",
        ):
            value = row.get(key)
            if value is not None:
                scores.append((key, float(value)))
        return min(scores, key=lambda item: item[1]) if scores else None

    @staticmethod
    def _shorten(text: object, width: int) -> str:
        value = " ".join(str(text or "").split())
        if len(value) <= width:
            return value
        return value[: max(0, width - 3)].rstrip() + "..."
