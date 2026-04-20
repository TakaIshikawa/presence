"""Prompt context for presence growth.

This module turns existing feedback loops into concrete generation guidance:
voice memory from prior posts, content mix planning from the calendar/topic
data, and outcome learning from engagement metrics.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class PresenceContext:
    voice_memory: str = ""
    content_mix: str = ""
    outcome_learning: str = ""

    def render(self) -> str:
        sections = [
            section.strip()
            for section in (
                self.voice_memory,
                self.content_mix,
                self.outcome_learning,
            )
            if section and section.strip()
        ]
        if not sections:
            return ""
        return "\n\n".join(sections) + "\n"


class PresenceContextBuilder:
    """Build prompt-ready context for growing account presence."""

    CONTENT_TYPE_ROLES = {
        "x_post": (
            "Role for this post: fast feedback loop. One concrete observation, "
            "sharp enough to invite replies."
        ),
        "x_long_post": (
            "Role for this post: depth and credibility. Develop one real lesson "
            "from the work; do not chase virality at the cost of substance."
        ),
        "x_thread": (
            "Role for this thread: teach a reusable pattern through a specific "
            "builder story."
        ),
        "blog_post": (
            "Role for this post: durable artifact. Preserve the reasoning and "
            "tradeoffs so it can become newsletter/source material later."
        ),
        "x_visual": (
            "Role for this visual: pattern interrupt. Use a relatable tension, "
            "meme, chart, or comparison that is legible in seconds."
        ),
    }

    def __init__(self, db):
        self.db = db

    def build(self, content_type: str) -> PresenceContext:
        return PresenceContext(
            voice_memory=self.build_voice_memory(content_type),
            content_mix=self.build_content_mix(content_type),
            outcome_learning=self.build_outcome_learning(content_type),
        )

    def build_prompt_section(self, content_type: str) -> str:
        return self.build(content_type).render()

    def build_voice_memory(self, content_type: str) -> str:
        top_posts = self._rows(
            "get_top_performing_posts", limit=3, content_type=content_type
        )
        if not top_posts and content_type not in ("x_post", "x_thread"):
            top_posts = self._rows(
                "get_top_performing_posts", limit=3, content_type="x_post"
            )
        good_posts = self._rows(
            "get_curated_posts", quality="good", content_type=content_type, limit=2
        )
        low_posts = self._rows(
            "get_auto_classified_posts",
            quality="low_resonance",
            content_type=content_type,
            limit=2,
        )

        if not top_posts and not good_posts and not low_posts:
            return (
                "VOICE MEMORY:\n"
                "- Default to direct builder notes: concrete moment, practical implication, no polish theater.\n"
                "- Sound like a person reporting what they learned while shipping.\n"
            )

        lines = [
            "VOICE MEMORY (learned from your own posts):",
            "Lean into:",
        ]
        for row in (top_posts + good_posts)[:4]:
            score = row.get("engagement_score")
            label = f"engagement {score:.1f}" if isinstance(score, (int, float)) else "curated good"
            lines.append(f"- ({label}) {self._snippet(row.get('content', ''))}")

        if low_posts:
            lines.append("Avoid repeating:")
            for row in low_posts[:2]:
                lines.append(f"- {self._snippet(row.get('content', ''))}")

        lines.extend(
            [
                "Voice rules:",
                "- Prefer specific builder friction over abstract principles.",
                "- Keep the abundance-through-technology worldview, but prove it through what was built.",
                "- Avoid generic AI thought-leadership phrasing unless the source material forces it.",
            ]
        )
        return "\n".join(lines)

    def build_content_mix(self, content_type: str) -> str:
        role = self.CONTENT_TYPE_ROLES.get(
            content_type,
            "Role for this post: add one useful, specific signal to the broader presence mix.",
        )
        frequencies = self._rows("get_topic_frequency", days=30)
        gaps = self._list("get_topic_gaps", days=30, min_gap_days=7)
        planned = self._rows("get_planned_topics", status="planned")

        lines = [
            "CONTENT MIX PLAN:",
            f"- {role}",
        ]

        if planned:
            plan = planned[0]
            angle = plan.get("angle") or "no explicit angle"
            lines.append(
                f"- Next planned topic: {plan.get('topic')} ({angle}). Use only if today's work genuinely supports it."
            )

        if gaps:
            lines.append(
                "- Under-covered topics to consider when relevant: "
                + ", ".join(str(g) for g in gaps[:5])
            )

        if frequencies:
            top = frequencies[0]
            lines.append(
                f"- Recent saturation watch: {top.get('topic')} has appeared {top.get('count')} times in 30 days."
            )

        lines.append(
            "- Do not optimize every post for the same job; vary between reach, credibility, usefulness, and relationship-building."
        )
        return "\n".join(lines)

    def build_outcome_learning(self, content_type: str) -> str:
        stats = self._dict("get_engagement_calibration_stats", content_type)
        format_stats = self._rows("get_format_engagement_stats", days=90)
        prediction = self._dict("get_prediction_accuracy", days=30)
        profile = self._dict("get_latest_profile_metrics", platform="x")

        lines = ["OUTCOME LEARNING (use real results, not evaluator vibes):"]

        if stats.get("total_classified"):
            lines.append(
                "- Classified history: "
                f"{stats.get('resonated_count', 0)} resonated, "
                f"{stats.get('low_resonance_count', 0)} low-resonance."
            )
            if stats.get("scored_7plus_zero_pct"):
                lines.append(
                    "- Evaluator caution: "
                    f"{stats['scored_7plus_zero_pct']}% of 7+ scored posts later had low resonance."
                )
        else:
            lines.append("- Not enough classified post history yet; favor safe specificity over broad claims.")

        best_formats = [
            row for row in format_stats
            if row.get("format") and (row.get("count") or 0) >= 1
        ][:3]
        if best_formats:
            formatted = ", ".join(
                f"{row['format']} ({float(row.get('avg_engagement') or 0):.1f})"
                for row in best_formats
            )
            lines.append(f"- Formats with signal: {formatted}.")

        if prediction.get("count"):
            mae = prediction.get("mae")
            corr = prediction.get("correlation")
            lines.append(
                f"- Prediction feedback: {prediction['count']} posts with actuals"
                + (f", MAE {mae}" if mae is not None else "")
                + (f", correlation {corr}" if corr is not None else "")
                + "."
            )

        if profile.get("follower_count") is not None:
            lines.append(
                f"- Current X profile snapshot: {profile['follower_count']} followers. Optimize for replies and follows, not just likes."
            )

        lines.append(
            "- If current trends conflict with historical performance, bridge them through concrete work instead of chasing the trend directly."
        )
        return "\n".join(lines)

    def _rows(self, method_name: str, *args, **kwargs) -> list[dict]:
        value = self._safe_call(method_name, *args, **kwargs)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, dict) or hasattr(row, "keys")]
        return []

    def _dict(self, method_name: str, *args, **kwargs) -> dict[str, Any]:
        value = self._safe_call(method_name, *args, **kwargs)
        return dict(value) if isinstance(value, dict) else {}

    def _list(self, method_name: str, *args, **kwargs) -> list:
        value = self._safe_call(method_name, *args, **kwargs)
        return value if isinstance(value, list) else []

    def _safe_call(self, method_name: str, *args, **kwargs):
        method = getattr(self.db, method_name, None)
        if not callable(method):
            return None
        try:
            return method(*args, **kwargs)
        except Exception:
            return None

    @staticmethod
    def _snippet(text: str, max_len: int = 180) -> str:
        cleaned = " ".join(str(text).split())
        if len(cleaned) <= max_len:
            return cleaned
        return cleaned[: max_len - 1].rstrip() + "..."
