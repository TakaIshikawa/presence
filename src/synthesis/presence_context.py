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
    campaign_context: str = ""
    github_activity: str = ""
    idea_inbox: str = ""
    feedback_memory: str = ""
    outcome_learning: str = ""

    def render(self) -> str:
        sections = [
            section.strip()
            for section in (
                self.voice_memory,
                self.content_mix,
                self.campaign_context,
                self.github_activity,
                self.idea_inbox,
                self.feedback_memory,
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
            campaign_context=self.build_campaign_context(),
            github_activity=self.build_github_activity_context(),
            idea_inbox=self.build_idea_inbox(),
            feedback_memory=self.build_feedback_memory(content_type),
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
        campaign, limits_reached, _ = self._active_campaign_limit_status()
        planned = self._planned_topics_respecting_pacing(campaign, limits_reached)

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

    def build_campaign_context(self) -> str:
        campaign, limits_reached, limit_reason = self._active_campaign_limit_status()
        planned = self._planned_topics_respecting_pacing(campaign, limits_reached)
        next_topic = planned[0] if planned else {}

        if not campaign and not next_topic:
            return ""

        lines = [
            "CAMPAIGN CONTEXT (optional planning signal):",
            "- Use this only when the source prompts or commits genuinely support it; never invent evidence to satisfy the campaign.",
        ]

        if campaign:
            name = campaign.get("name")
            goal = campaign.get("goal")
            start = campaign.get("start_date")
            end = campaign.get("end_date")
            if name:
                lines.append(f"- Active campaign: {name}.")
            if goal:
                lines.append(f"- Goal: {goal}.")
            if start or end:
                window = " to ".join(
                    value for value in (start or "unspecified start", end or "unspecified end")
                    if value
                )
                lines.append(f"- Date window: {window}.")
            if limits_reached:
                lines.append(f"- Campaign pacing limit reached: {limit_reason}. Skip planned campaign topics for this run.")

        if next_topic and not limits_reached:
            topic = next_topic.get("topic")
            angle = next_topic.get("angle") or "no explicit angle"
            target = next_topic.get("target_date")
            topic_line = f"- Next planned topic: {topic} ({angle})"
            if target:
                topic_line += f", target {target}"
            topic_line += "."
            lines.append(topic_line)

        return "\n".join(lines)

    def build_idea_inbox(self) -> str:
        ideas = self._rows(
            "get_content_ideas",
            status="open",
            priority="high",
            limit=3,
        )
        if not ideas:
            return ""

        lines = [
            "IDEA INBOX (optional seed notes):",
            "- Use only if the source prompts or current work genuinely support the idea; do not force it.",
        ]
        for idea in ideas[:3]:
            topic = idea.get("topic")
            source = idea.get("source")
            detail = self._snippet(idea.get("note", ""))
            if topic:
                detail = f"{topic}: {detail}"
            if source:
                detail += f" (source: {source})"
            lines.append(f"- {detail}")
        return "\n".join(lines)

    def build_feedback_memory(self, content_type: str) -> str:
        feedback = self._rows(
            "get_recent_content_feedback",
            content_type=content_type,
            feedback_types=["reject", "revise", "prefer"],
            limit=6,
        )
        if not feedback and content_type not in ("x_post", "x_thread"):
            feedback = self._rows(
                "get_recent_content_feedback",
                content_type="x_post",
                feedback_types=["reject", "revise", "prefer"],
                limit=6,
            )
        if not feedback:
            return ""

        lines = [
            "FEEDBACK MEMORY (explicit user edits and rejections):",
            "- Treat rejected and revised examples as durable negative signal; do not reuse their phrasing or topic framing unless new source material clearly changes the point.",
        ]

        negative = [
            row for row in feedback
            if row.get("feedback_type") in {"reject", "revise"}
        ]
        if negative:
            lines.append("Avoid:")
            for row in negative[:4]:
                note = self._snippet(row.get("notes", ""), max_len=100)
                original = self._snippet(row.get("content", ""), max_len=120)
                if note:
                    lines.append(
                        f"- {row.get('feedback_type')}: {note}. Original: {original}"
                    )
                else:
                    lines.append(f"- {row.get('feedback_type')}: {original}")

        preferred = [
            row for row in feedback
            if row.get("feedback_type") == "prefer" or row.get("replacement_text")
        ]
        if preferred:
            lines.append("Prefer:")
            for row in preferred[:3]:
                replacement = self._snippet(row.get("replacement_text", ""), max_len=140)
                note = self._snippet(row.get("notes", ""), max_len=100)
                if replacement and note:
                    lines.append(f"- {note}. Better phrasing: {replacement}")
                elif replacement:
                    lines.append(f"- Better phrasing: {replacement}")
                elif note:
                    lines.append(f"- {note}")

        return "\n".join(lines)

    def build_github_activity_context(self) -> str:
        recent = self._rows("get_recent_github_activity", days=7, limit=5)
        unresolved = self._rows("get_unresolved_github_activity", limit=5)

        by_id = {}
        for row in recent + unresolved:
            activity_id = self._activity_id(row)
            if activity_id and activity_id not in by_id:
                by_id[activity_id] = row
        activity = list(by_id.values())
        if not activity:
            return ""

        lines = [
            "GITHUB ACTIVITY CONTEXT (issues, PRs, releases, and discussions):",
            "- Use this as source context only when it connects to the commits/prompts; do not imply unresolved work is finished.",
        ]

        recent_ids = {self._activity_id(row) for row in recent[:5]}
        unresolved_ids = {self._activity_id(row) for row in unresolved[:5]}
        for row in activity[:6]:
            markers = []
            activity_id = self._activity_id(row)
            if activity_id in recent_ids:
                markers.append("recently updated")
            if activity_id in unresolved_ids:
                markers.append("unresolved")
            marker = f" [{', '.join(markers)}]" if markers else ""
            title = self._snippet(row.get("title", ""), max_len=90)
            repo = row.get("repo_name") or "unknown repo"
            number = row.get("number")
            activity_type = self._activity_label(row.get("activity_type"))
            state = row.get("state") or "unknown"
            labels = row.get("labels") or []
            label_text = f"; labels: {', '.join(str(label) for label in labels[:3])}" if labels else ""
            lines.append(
                f"- {repo} {activity_type} #{number}: {title} ({state}{marker}{label_text})"
            )
        return "\n".join(lines)

    def _active_campaign_limit_status(self) -> tuple[dict[str, Any], bool, str]:
        campaign = self._dict("get_active_campaign")
        if not campaign:
            return {}, False, ""

        campaign_id = campaign.get("id")
        daily_limit = campaign.get("daily_limit")
        weekly_limit = campaign.get("weekly_limit")
        if campaign_id is None or (daily_limit is None and weekly_limit is None):
            return campaign, False, ""

        counts = self._dict("get_campaign_pacing_counts", campaign_id)
        if not counts:
            return campaign, False, ""

        if daily_limit is not None and counts.get("daily_count", 0) >= daily_limit:
            return campaign, True, f"daily limit reached ({counts.get('daily_count', 0)}/{daily_limit})"
        if weekly_limit is not None and counts.get("weekly_count", 0) >= weekly_limit:
            return campaign, True, f"weekly limit reached ({counts.get('weekly_count', 0)}/{weekly_limit})"
        return campaign, False, ""

    def _planned_topics_respecting_pacing(
        self,
        campaign: dict[str, Any],
        limits_reached: bool,
    ) -> list[dict]:
        planned = self._rows("get_planned_topics", status="planned")
        campaign_id = campaign.get("id")
        if not limits_reached or campaign_id is None:
            return planned
        return [
            topic for topic in planned
            if topic.get("campaign_id") != campaign_id
        ]

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

    @staticmethod
    def _activity_id(row: dict) -> str:
        if row.get("activity_id"):
            return str(row["activity_id"])
        repo = row.get("repo_name")
        number = row.get("number")
        activity_type = row.get("activity_type")
        if repo is None or number is None or activity_type is None:
            return ""
        return f"{repo}#{number}:{activity_type}"

    @staticmethod
    def _activity_label(activity_type: str | None) -> str:
        if activity_type == "pull_request":
            return "PR"
        if activity_type == "issue":
            return "issue"
        if activity_type == "discussion":
            return "discussion"
        return str(activity_type or "activity")
