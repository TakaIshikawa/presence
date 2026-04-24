"""Multi-stage synthesis pipeline for content generation."""

import json
import logging
import random
import re
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from typing import Optional

from storage.db import Database
from synthesis.generator import ContentGenerator
from synthesis.evaluator_v2 import CrossModelEvaluator, ComparisonResult
from synthesis.refiner import ContentRefiner, RefinementResult
from synthesis.few_shot import FewShotSelector
from synthesis.presence_context import PresenceContextBuilder
from synthesis.stale_patterns import STALE_PATTERNS
from synthesis.claim_checker import ClaimChecker, ClaimCheckResult
from synthesis.thread_validator import validate_thread
from synthesis.persona_guard import PersonaGuard, PersonaGuardConfig, PersonaGuardResult
from evaluation.topic_performance import TopicPerformanceAnalyzer
from model_usage import evaluate_model_usage_budget

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    batch_id: str
    candidates: list[str]
    comparison: ComparisonResult
    refinement: Optional[RefinementResult]
    final_content: str
    final_score: float
    source_prompts: list[str]
    source_commits: list[str]
    filter_stats: Optional[dict] = None
    predicted_engagement: Optional[float] = None
    engagement_prediction_detail: Optional[dict] = None
    knowledge_ids: list[tuple[int, float]] = None  # (knowledge_id, relevance_score) for lineage tracking
    content_format: Optional[str] = None  # Format used for generation (e.g., 'micro_story', 'bold_claim')
    predictor_override: bool = False  # True when predictor tie-breaker changed best_idx
    predictor_override_detail: Optional[dict] = None  # {evaluator_top, predictor_top, margin, ...}
    planned_topic_id: Optional[int] = None  # planned_topics.id used for campaign guidance
    claim_check_summary: Optional[dict] = None  # Persistable final-content claim-check summary
    persona_guard_summary: Optional[dict] = None  # Persistable persona guard summary
    model_usage_started_at: Optional[str] = None
    estimated_model_cost: float = 0.0
    estimated_daily_model_cost: float = 0.0
    budget_rejection_reason: Optional[str] = None

    def save_claim_check_summary(self, db: Database, content_id: int) -> None:
        """Persist final claim-check summary after generated content is inserted."""
        if self.claim_check_summary:
            db.save_claim_check_summary(content_id, **self.claim_check_summary)

    def save_persona_guard_summary(self, db: Database, content_id: int) -> None:
        """Persist final persona guard summary after generated content is inserted."""
        if self.persona_guard_summary:
            db.save_persona_guard_summary(content_id, self.persona_guard_summary)


def _claim_check_summary(result: ClaimCheckResult) -> dict:
    """Convert a claim-check result into the stored generated-content summary."""
    unsupported_count = len(result.unsupported_claims)
    supported_count = max(0, len(result.claims) - unsupported_count)
    return {
        "supported_count": supported_count,
        "unsupported_count": unsupported_count,
        "annotation_text": "\n".join(result.annotations) if result.annotations else None,
    }


class SynthesisPipeline:
    """Orchestrates the full multi-stage content generation pipeline.

    Stages:
    1. Few-shot retrieval — select high-performing examples
    2. Multi-candidate generation — 3 candidates via temperature variation
    3. Cross-model evaluation — Opus ranks candidates comparatively
    4. Guided refinement — Sonnet refines best candidate using Opus feedback
    5. Final gate — Opus picks original vs refined
    """

    # Skip refinement if score is already very high
    SKIP_REFINE_ABOVE = 9.0
    # Skip refinement if score is too low to be worth refining
    SKIP_REFINE_BELOW = 5.0

    # Post format directives for structural variety
    POST_FORMATS = [
        (
            "micro_story",
            "FORMAT: Micro-story. Start in the middle of the action. "
            "'I was debugging X when I noticed...' or 'Three hours into refactoring, I realized...' "
            "Show what happened — do NOT state a conclusion upfront.",
        ),
        (
            "question",
            "FORMAT: Open with a genuine question from your work today. "
            "'Why does X always lead to Y?' or 'Has anyone else hit this?' "
            "Then briefly share what you found. End with the question lingering.",
        ),
        (
            "contrarian",
            "FORMAT: Challenge a common belief. State what's conventionally believed, "
            "then share what your specific experience showed differently. "
            "'Everyone says X. I just spent 3 hours finding the opposite.'",
        ),
        (
            "tip",
            "FORMAT: One actionable tip someone can use in 5 minutes. "
            "'Next time you hit X, try Y instead.' Ground it in what you just built. "
            "No preamble — lead with the tip.",
        ),
        (
            "observation",
            "FORMAT: A surprising observation with no conclusion. "
            "'Noticed something odd: when I X, Y happens consistently.' "
            "Let the reader draw their own meaning. Resist explaining why.",
        ),
    ]

    # Long-form post format directives — control essay structure
    LONG_POST_FORMATS = [
        (
            "deep_dive",
            "FORMAT: Deep-dive into one technical decision you made today. "
            "What were the options? What did you pick and why? What would you "
            "tell someone facing the same choice? Write it as a short essay.",
        ),
        (
            "retrospective",
            "FORMAT: Before/after comparison. Start with how things were, "
            "describe what you changed, and end with what's different now. "
            "Focus on the 'why' — what made the old way insufficient.",
        ),
        (
            "framework",
            "FORMAT: Present a reusable mental framework you discovered while building. "
            "'When X happens, try Y because Z.' Ground it in today's specific work "
            "but make it applicable beyond your project.",
        ),
        (
            "narrative",
            "FORMAT: Tell the full story of a problem you hit today. "
            "What were you trying to do? What went wrong? How did you figure it out? "
            "Write it like you're recounting the experience to a colleague.",
        ),
        (
            "analysis",
            "FORMAT: Analyze a trade-off you faced in today's work. "
            "What were the competing concerns? What did you sacrifice and why? "
            "What would have changed your decision?",
        ),
    ]

    # Visual post format directives — short text to accompany an image
    VISUAL_POST_FORMATS = [
        (
            "annotated_insight",
            "FORMAT: A punchy observation that the accompanying image will visualize. "
            "Frame the insight — the image shows the evidence. Keep it under 200 chars.",
        ),
        (
            "data_callout",
            "FORMAT: Commentary on a surprising metric or comparison the image highlights. "
            "'Didn't expect this number...' or 'The gap here tells you everything.'",
        ),
        (
            "meme_commentary",
            "FORMAT: Relatable developer moment with a touch of humor. "
            "The image captures the situation — your text adds the punchline or reaction.",
        ),
        (
            "trend_linked",
            "FORMAT: Connect today's work to a theme people are already reacting to right now. "
            "Use one real bridge from current discourse if it genuinely fits. "
            "Your text should name the tension or angle, not summarize the trend.",
        ),
    ]

    # Thread format directives — control how TWEET 1 hooks the reader
    THREAD_FORMATS = [
        (
            "mid_action",
            "THREAD HOOK: Start Tweet 1 mid-action. Drop the reader into a moment. "
            "'I was halfway through a refactor when something unexpected happened.' "
            "No labels, no preamble — open with a scene.",
        ),
        (
            "bold_claim",
            "THREAD HOOK: Open Tweet 1 with a bold, specific claim the thread will prove. "
            "'Most system failures happen before a single line of code runs.' "
            "Make it falsifiable and surprising. The thread is the evidence.",
        ),
        (
            "question_hook",
            "THREAD HOOK: Start Tweet 1 with a genuine question that came up in your work. "
            "'Why do systems silently give up instead of failing loudly?' "
            "The thread walks through what you found. End with the question evolved, not answered.",
        ),
        (
            "surprising_result",
            "THREAD HOOK: Lead Tweet 1 with a concrete, unexpected result. "
            "'Gave two approaches the same problem. One finished in 3 minutes, the other looped for an hour.' "
            "State the outcome first — the thread explains why.",
        ),
        (
            "contrarian_thread",
            "THREAD HOOK: Open Tweet 1 by challenging a common practice. "
            "'Stop writing detailed specs before building. Seriously.' "
            "The thread unpacks what works better and why.",
        ),
    ]


    def __init__(
        self,
        api_key: str,
        generator_model: str,
        evaluator_model: str,
        db: Database,
        num_candidates: int = 3,
        anthropic_timeout: float = 300.0,
        embedder=None,
        semantic_threshold: float = 0.82,
        knowledge_store=None,
        engagement_predictor=None,
        format_weighting_enabled: bool = True,
        format_cooldown_recent_posts: int = 5,
        format_cooldown_penalty: float = 0.5,
        claim_check_enabled: bool = True,
        persona_guard_enabled: bool = True,
        persona_guard_min_score: float = 0.55,
        persona_guard_min_phrase_overlap: float = 0.08,
        persona_guard_max_banned_markers: int = 0,
        persona_guard_max_abstraction_ratio: float = 0.18,
        persona_guard_min_grounding_score: float = 0.5,
        persona_guard_recent_limit: int = 20,
        persona_guard_min_recent_posts: int = 3,
        restricted_prompt_behavior: str = "strict",
        feedback_lookback_days: int = 30,
        feedback_max_items: int = 6,
        max_estimated_cost_per_run: float | None = None,
        max_daily_estimated_cost: float | None = None,
    ):
        self.api_key = api_key
        self.generator = ContentGenerator(
            api_key,
            generator_model,
            timeout=anthropic_timeout,
            db=db,
            feedback_lookback_days=feedback_lookback_days,
            feedback_max_items=feedback_max_items,
        )
        self.evaluator = CrossModelEvaluator(
            api_key, evaluator_model, timeout=anthropic_timeout, db=db
        )
        self.refiner = ContentRefiner(
            refine_api_key=api_key,
            refine_model=generator_model,
            gate_api_key=api_key,
            gate_model=evaluator_model,
            timeout=anthropic_timeout,
            db=db,
        )
        self.db = db
        self.few_shot_selector = FewShotSelector(db)
        self.num_candidates = num_candidates
        self.embedder = embedder
        self.semantic_threshold = semantic_threshold
        self.knowledge_store = knowledge_store
        self.engagement_predictor = engagement_predictor
        self.format_weighting_enabled = format_weighting_enabled
        self.format_cooldown_recent_posts = format_cooldown_recent_posts
        self.format_cooldown_penalty = format_cooldown_penalty
        self.claim_check_enabled = claim_check_enabled
        self.restricted_prompt_behavior = restricted_prompt_behavior
        self.claim_checker = ClaimChecker()
        self.persona_guard_config = PersonaGuardConfig(
            enabled=persona_guard_enabled,
            min_score=persona_guard_min_score,
            min_phrase_overlap=persona_guard_min_phrase_overlap,
            max_banned_markers=persona_guard_max_banned_markers,
            max_abstraction_ratio=persona_guard_max_abstraction_ratio,
            min_grounding_score=persona_guard_min_grounding_score,
            recent_limit=persona_guard_recent_limit,
            min_recent_posts=persona_guard_min_recent_posts,
        )
        self.persona_guard = PersonaGuard(self.persona_guard_config)
        self.presence_context_builder = PresenceContextBuilder(db)
        self.max_estimated_cost_per_run = max_estimated_cost_per_run
        self.max_daily_estimated_cost = max_daily_estimated_cost

    def _build_presence_context(self, content_type: str) -> str:
        """Build the prompt-ready presence context for generation."""
        return self.presence_context_builder.build_prompt_section(content_type)

    def _apply_budget_gate(
        self, result: PipelineResult, run_started_at: datetime
    ) -> PipelineResult:
        """Attach budget status after generation; callers still save content."""
        check = evaluate_model_usage_budget(
            self.db,
            run_started_at=run_started_at,
            max_estimated_cost_per_run=self.max_estimated_cost_per_run,
            max_daily_estimated_cost=self.max_daily_estimated_cost,
        )
        result.model_usage_started_at = run_started_at.isoformat()
        result.estimated_model_cost = check.run_cost
        result.estimated_daily_model_cost = check.daily_cost
        result.budget_rejection_reason = check.reason if check.exceeded else None
        if result.budget_rejection_reason:
            result.comparison = self._append_reject_reason(
                result.comparison, result.budget_rejection_reason
            )
        return result

    # Character limits per content type
    CHAR_LIMITS = {
        "x_post": 280,
        "x_long_post": 2000,
        "x_visual": 200,
    }

    @staticmethod
    def _extract_opening(text: str, max_len: int = 100) -> str:
        """Extract the opening clause of a post for repetition comparison.

        For threads, strips the 'TWEET 1:\n' prefix to compare actual content.
        """
        # Strip thread prefix to get to actual content
        stripped = re.sub(r"^TWEET\s+\d+:\s*\n?", "", text).strip()
        # Split on em-dash, colon, or period — whichever comes first
        match = re.split(r'[—:\.]', stripped, maxsplit=1)
        opening = match[0].strip().lower() if match else stripped[:max_len].lower()
        return opening[:max_len]

    def _filter_repetitive(
        self, candidates: list[str], content_type: str
    ) -> tuple[list[str], int]:
        """Remove candidates whose opening is too similar to recent posts.

        Returns (filtered_candidates, rejection_count).
        """
        recent = self.db.get_recent_published_content(content_type, limit=20)
        if not recent:
            return candidates, 0

        recent_openings = [self._extract_opening(p["content"]) for p in recent]

        filtered = []
        rejected = 0
        for candidate in candidates:
            opening = self._extract_opening(candidate)
            is_repetitive = any(
                SequenceMatcher(None, opening, ro).ratio() > 0.55
                for ro in recent_openings
            )
            if is_repetitive:
                logger.debug(f"  Rejected as repetitive: {opening[:40]}...")
                rejected += 1
            else:
                filtered.append(candidate)

        return filtered, rejected

    def _filter_stale_patterns(
        self, candidates: list[str]
    ) -> tuple[list[str], int, list[str]]:
        """Reject candidates matching overused rhetorical patterns.

        Returns (filtered_candidates, rejection_count, matched_patterns).
        """
        filtered = []
        rejected = 0
        matched_patterns = []
        for candidate in candidates:
            matches = [p.pattern for p in STALE_PATTERNS if p.search(candidate)]
            if matches:
                logger.debug(f"  Rejected stale pattern: {candidate[:50]}...")
                rejected += 1
                matched_patterns.extend(matches)
            else:
                filtered.append(candidate)
        return filtered, rejected, matched_patterns

    def _build_avoidance_context(self) -> str:
        """Build a list of recent topics for the generator to avoid repeating."""
        recent = self.db.get_recent_published_content_all(limit=10)
        if not recent:
            return ""

        topics = []
        for r in recent:
            content = r["content"]
            stripped = re.sub(r"^TWEET\s+\d+:\s*\n?", "", content).strip()
            first_sentence = stripped.split(".")[0].strip()
            if first_sentence and len(first_sentence) > 10:
                topics.append(f"- {first_sentence[:120]}")

        if not topics:
            return ""

        # Detect dominant topic and add explicit warning
        dominant_warning = ""
        all_text = " ".join(t.lower() for t in topics)
        agent_mentions = len(re.findall(r'\bagent', all_text))
        if len(topics) > 0 and agent_mentions / len(topics) > 0.6:
            dominant_warning = (
                f"\nWARNING: {agent_mentions}/{len(topics)} of your recent posts "
                f"are about AI agents. Your audience has seen enough of this topic. "
                f"Write about something DIFFERENT — architecture, testing, "
                f"data modeling, performance, developer tooling, or any other "
                f"aspect of what you built.\n"
            )

        return (
            "TOPICS RECENTLY COVERED (do NOT repeat these themes — "
            "find a DIFFERENT angle from the source material):\n"
            + "\n".join(topics)
            + dominant_warning
            + "\n\n"
        )

    def _build_pattern_context(self) -> str:
        """Build pattern context from latest analysis for injection into generator."""
        raw = self.db.get_meta("pattern_analysis")
        if not raw or not isinstance(raw, str):
            return ""

        try:
            analysis = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return ""

        rules = analysis.get("actionable_rules", [])
        if not rules:
            return ""

        confidence = analysis.get("confidence", "low")
        resonated_count = analysis.get("resonated_count", 0)

        if confidence == "high":
            header = "ENGAGEMENT PATTERNS (learned from audience data — follow these):"
        elif confidence == "medium":
            header = "ENGAGEMENT PATTERNS (learned from audience data — follow these when relevant):"
        else:
            header = (
                f"ENGAGEMENT PATTERNS (based on limited data — {resonated_count} "
                f"resonated posts. Use as suggestions, not hard rules):"
            )

        lines = [header]
        for rule in rules:
            lines.append(f"- {rule}")
        lines.append("")
        return "\n".join(lines)

    def _campaign_limit_status(
        self,
        campaign: dict,
        now: datetime | None = None,
    ) -> tuple[bool, str]:
        """Return whether the active campaign has reached its pacing limits."""
        campaign_id = campaign.get("id")
        if campaign_id is None:
            return False, ""

        daily_limit = campaign.get("daily_limit")
        weekly_limit = campaign.get("weekly_limit")
        if daily_limit is None and weekly_limit is None:
            return False, ""

        try:
            counts = self.db.get_campaign_pacing_counts(campaign_id, now=now)
        except Exception as e:
            logger.warning(f"  Campaign pacing check skipped: {e}")
            return False, ""

        if daily_limit is not None and counts["daily_count"] >= daily_limit:
            return True, (
                f"daily limit reached ({counts['daily_count']}/{daily_limit})"
            )
        if weekly_limit is not None and counts["weekly_count"] >= weekly_limit:
            return True, (
                f"weekly limit reached ({counts['weekly_count']}/{weekly_limit})"
            )
        return False, ""

    def _select_planned_campaign_topic(
        self,
        campaign: dict,
        limits_reached: bool,
    ) -> dict | None:
        """Choose the next planned topic unless campaign pacing is capped."""
        if limits_reached:
            return None

        planned = self.db.get_planned_topics(status="planned")
        campaign_id = campaign.get("id")
        for topic in planned:
            if campaign_id is None or topic.get("campaign_id") == campaign_id:
                return topic
        return None

    def _build_campaign_context(self) -> tuple[str, int | None]:
        """Build campaign guidance and return the planned topic ID, if any."""
        try:
            campaign = self.db.get_active_campaign()
        except Exception as e:
            logger.debug(f"Campaign context unavailable: {e}")
            return "", None

        if not campaign or not isinstance(campaign, dict):
            return "", None

        limits_reached, limit_reason = self._campaign_limit_status(campaign)
        planned_topic = self._select_planned_campaign_topic(campaign, limits_reached)

        lines = ["CAMPAIGN CONTEXT"]
        lines.append(f"Campaign: {campaign['name']}")
        if campaign.get("goal"):
            lines.append(f"Goal: {campaign['goal']}")
        if campaign.get("start_date") or campaign.get("end_date"):
            start = campaign.get("start_date") or "open"
            end = campaign.get("end_date") or "open"
            lines.append(f"Window: {start} to {end}")
        if limits_reached:
            lines.append(
                f"Campaign pacing limit reached: {limit_reason}. "
                "Skip planned campaign topics for this run."
            )
        elif planned_topic:
            topic = planned_topic["topic"]
            angle = planned_topic.get("angle")
            if angle:
                lines.append(f"Next planned topic: {topic} ({angle})")
            else:
                lines.append(f"Next planned topic: {topic}")
            lines.append(
                "Use this only when the source prompts or commits genuinely support it."
            )
        else:
            lines.append("No planned campaign topic is ready for this run.")
        lines.append("")
        return "\n".join(lines), planned_topic.get("id") if planned_topic else None

    def _filter_topic_saturated(
        self, candidates: list[str], threshold: float = 0.65
    ) -> tuple[list[str], int]:
        """Reject candidates whose average similarity to recent posts exceeds threshold.

        Unlike semantic dedup (max similarity to any single post), this catches
        candidates thematically similar to the bulk of recent content.

        Returns (filtered_candidates, rejection_count).
        """
        if not self.embedder:
            return candidates, 0

        recent = self.db.get_recent_published_content_all(limit=10)
        recent_with_embeddings = [
            r for r in recent if r.get("content_embedding")
        ]
        if len(recent_with_embeddings) < 3:
            return candidates, 0

        from knowledge.embeddings import deserialize_embedding, cosine_similarity

        recent_embeddings = [
            deserialize_embedding(r["content_embedding"])
            for r in recent_with_embeddings
        ]

        try:
            candidate_embeddings = self.embedder.embed_batch(candidates)
        except Exception as e:
            logger.warning(
                f"  Topic saturation filter skipped (embedding error): {e}"
            )
            return candidates, 0

        filtered = []
        rejected = 0
        for candidate, c_emb in zip(candidates, candidate_embeddings):
            avg_sim = sum(
                cosine_similarity(c_emb, r_emb)
                for r_emb in recent_embeddings
            ) / len(recent_embeddings)
            if avg_sim > threshold:
                logger.debug(
                    f"  Rejected topic-saturated (avg_sim={avg_sim:.3f}): "
                    f"{candidate[:40]}..."
                )
                rejected += 1
            else:
                filtered.append(candidate)

        return filtered, rejected

    def _filter_semantic_duplicates(self, candidates: list[str]) -> list[str]:
        """Remove candidates semantically similar to recently published content."""
        if not self.embedder:
            return candidates

        recent = self.db.get_recent_published_content_all(limit=30)
        recent_with_embeddings = [
            r for r in recent if r.get("content_embedding")
        ]
        if not recent_with_embeddings:
            return candidates

        from knowledge.embeddings import deserialize_embedding, cosine_similarity

        recent_embeddings = [
            deserialize_embedding(r["content_embedding"])
            for r in recent_with_embeddings
        ]

        try:
            candidate_embeddings = self.embedder.embed_batch(candidates)
        except Exception as e:
            logger.warning(
                f"  Semantic dedup skipped (embedding error): {e}"
            )
            return candidates

        filtered = []
        for candidate, c_emb in zip(candidates, candidate_embeddings):
            max_sim = max(
                cosine_similarity(c_emb, r_emb)
                for r_emb in recent_embeddings
            )
            if max_sim > self.semantic_threshold:
                best_idx = max(
                    range(len(recent_embeddings)),
                    key=lambda j: cosine_similarity(c_emb, recent_embeddings[j]),
                )
                matched = recent_with_embeddings[best_idx]["content"][:60]
                logger.debug(f"  Rejected semantic duplicate (sim={max_sim:.3f}): "
                      f"{candidate[:40]}... ~ {matched}...")
            else:
                filtered.append(candidate)

        return filtered

    def _filter_unsupported_claims(
        self,
        candidates: list[str],
        source_prompts: list[str],
        source_commits: list[str],
        linked_knowledge: list[str] | None = None,
    ) -> tuple[list[str], int, list[str]]:
        """Reject candidates with unsupported risky metrics or factual claims."""
        if not self.claim_check_enabled:
            return candidates, 0, []

        filtered = []
        rejected = 0
        annotations = []
        for candidate in candidates:
            result = self.claim_checker.check(
                candidate,
                source_prompts=source_prompts,
                source_commits=source_commits,
                linked_knowledge=linked_knowledge,
            )
            if result.supported:
                filtered.append(candidate)
            else:
                rejected += 1
                annotations.extend(result.annotations)
                logger.debug(
                    "  Rejected unsupported claim: %s",
                    "; ".join(result.annotations),
                )

        return filtered, rejected, annotations

    def _check_final_claims(
        self,
        content: str,
        source_prompts: list[str],
        source_commits: list[str],
        linked_knowledge: list[str] | None = None,
    ) -> ClaimCheckResult:
        """Annotate final content if refinement introduced unsupported claims."""
        if not self.claim_check_enabled:
            return ClaimCheckResult(claims=[], unsupported_claims=[], annotations=[])

        return self.claim_checker.check(
            content,
            source_prompts=source_prompts,
            source_commits=source_commits,
            linked_knowledge=linked_knowledge,
        )

    def _check_persona_guard(
        self,
        content: str,
        content_type: str,
    ) -> PersonaGuardResult:
        """Evaluate final content against recent published author voice."""
        try:
            recent = self.db.get_recent_published_content_all(
                limit=self.persona_guard_config.recent_limit
            )
        except Exception as e:
            logger.warning(f"  Persona guard skipped (recent content unavailable): {e}")
            return PersonaGuardResult(
                passed=True,
                score=1.0,
                reasons=["recent published content unavailable"],
                metrics={"content_type": content_type},
                checked=False,
                status="skipped",
            )
        return self.persona_guard.check(content, recent)

    @staticmethod
    def _append_reject_reason(
        comparison: ComparisonResult, reason: str
    ) -> ComparisonResult:
        """Return a copy of comparison with an additional rejection reason."""
        reject_reason = (
            f"{comparison.reject_reason}; {reason}"
            if comparison.reject_reason
            else reason
        )
        return ComparisonResult(
            ranking=comparison.ranking,
            best_score=comparison.best_score,
            groundedness=comparison.groundedness,
            rawness=comparison.rawness,
            narrative_specificity=comparison.narrative_specificity,
            voice=comparison.voice,
            engagement_potential=comparison.engagement_potential,
            best_feedback=comparison.best_feedback,
            improvement=comparison.improvement,
            reject_reason=reject_reason,
            raw_response=comparison.raw_response,
        )

    def _select_format_directives(
        self,
        num: int,
        content_type: str = "x_post",
        weights: Optional[dict[str, float]] = None,
        recommended_formats: Optional[list[str]] = None,
        reorder_recommended_by_weights: bool = False,
    ) -> tuple[list[str], list[str]]:
        """Select format directives for candidate generation.

        Uses weighted random selection if weights are provided, otherwise uniform sampling.

        Args:
            num: Number of formats to select
            content_type: Type of content ('x_post' or 'x_thread')
            weights: Optional dict mapping format name to selection weight
            recommended_formats: Optional prioritized list of format names
            reorder_recommended_by_weights: Re-rank recommendations by weights

        Returns:
            (directives, format_names): Lists of format directives and their names
        """
        if content_type == "x_thread":
            formats = self.THREAD_FORMATS
        elif content_type == "x_long_post":
            formats = self.LONG_POST_FORMATS
        elif content_type == "x_visual":
            formats = self.VISUAL_POST_FORMATS
        else:
            formats = self.POST_FORMATS

        if recommended_formats:
            formats_by_name = dict(formats)
            recommended = [
                name for name in recommended_formats if name in formats_by_name
            ]
            if reorder_recommended_by_weights and weights:
                recommended = sorted(
                    recommended,
                    key=lambda name: weights.get(name, 1.0),
                    reverse=True,
                )
            selected = [(name, formats_by_name[name]) for name in recommended[:num]]
            if len(selected) < num:
                selected_names = {name for name, _ in selected}
                remaining = [
                    format_pair for format_pair in formats
                    if format_pair[0] not in selected_names
                ]
                selected.extend(
                    random.sample(remaining, min(num - len(selected), len(remaining)))
                )
        elif weights:
            # Weighted selection with replacement (allows duplicates if num > len(formats))
            format_list = list(formats)
            format_weights = [
                weights.get(name, 1.0) for name, _ in format_list
            ]
            selected = random.choices(format_list, weights=format_weights, k=num)
        else:
            # Uniform sampling without replacement
            selected = random.sample(formats, min(num, len(formats)))

        directives = [directive for _, directive in selected]
        format_names = [name for name, _ in selected]
        return directives, format_names

    def _recent_content_format_counts(
        self, content_type: str, limit: int
    ) -> dict[str, int]:
        """Count recently generated formats for the same content type."""
        if limit <= 0:
            return {}

        conn = getattr(self.db, "conn", None)
        if not isinstance(conn, sqlite3.Connection):
            return {}

        try:
            cursor = conn.execute(
                """SELECT content_format
                   FROM generated_content
                   WHERE content_type = ? AND content_format IS NOT NULL
                   ORDER BY datetime(created_at) DESC, id DESC
                   LIMIT ?""",
                (content_type, limit),
            )
        except sqlite3.Error as e:
            logger.debug(f"  Format cooldown lookup failed (non-fatal): {e}")
            return {}

        counts: dict[str, int] = {}
        for row in cursor.fetchall():
            name = row["content_format"] if hasattr(row, "keys") else row[0]
            if name:
                counts[name] = counts.get(name, 0) + 1
        return counts

    def _apply_format_cooldown(
        self,
        content_type: str,
        weights: Optional[dict[str, float]],
    ) -> tuple[dict[str, float] | None, dict]:
        """Reduce selection weights for formats used in recent generations."""
        recent_limit = max(0, int(self.format_cooldown_recent_posts or 0))
        penalty = max(0.0, float(self.format_cooldown_penalty or 0.0))
        metadata = {
            "recent_posts": recent_limit,
            "penalty": penalty,
            "recent_counts": {},
            "format_penalties": {},
            "adjusted_weights": {},
            "selected_formats": [],
            "selected_format": None,
            "selected_format_cooldown_penalty": 0.0,
        }

        if recent_limit <= 0 or penalty <= 0:
            return weights, metadata

        counts = self._recent_content_format_counts(content_type, recent_limit)
        metadata["recent_counts"] = counts
        if not counts:
            return weights, metadata

        base_weights = dict(weights or {})
        adjusted = dict(base_weights)
        for format_name, count in counts.items():
            base = max(0.0, float(base_weights.get(format_name, 1.0)))
            multiplier = max(0.01, (1.0 - min(penalty, 1.0)) ** count)
            adjusted_weight = base * multiplier
            adjusted[format_name] = adjusted_weight
            metadata["format_penalties"][format_name] = round(1.0 - multiplier, 6)
            metadata["adjusted_weights"][format_name] = round(adjusted_weight, 6)

        return adjusted, metadata

    def _enforce_char_limit(
        self, candidates: list[str], max_chars: int
    ) -> tuple[list[str], int]:
        """Validate and condense candidates that exceed character limit.

        Returns (valid_candidates, rejection_count).
        """
        valid = []
        rejected = 0
        for i, text in enumerate(candidates):
            if len(text) <= max_chars:
                valid.append(text)
                continue

            # Try condensing up to 2 times
            condensed = text
            for attempt in range(2):
                logger.debug(f"  Candidate {i} is {len(condensed)} chars (limit {max_chars}), condensing (attempt {attempt + 1})...")
                condensed = self.generator.condense(condensed, max_chars)
                if len(condensed) <= max_chars:
                    logger.debug(f"  Condensed to {len(condensed)} chars")
                    valid.append(condensed)
                    break
            else:
                logger.warning(f"  Still {len(condensed)} chars after 2 condense attempts, discarding")
                rejected += 1

        if not valid:
            # Fallback: take shortest original, truncate at sentence boundary
            shortest = min(candidates, key=len)
            sentences = shortest.split(". ")
            truncated = ""
            for s in sentences:
                candidate = (truncated + ". " + s).strip(". ") + "." if truncated else s
                if len(candidate) <= max_chars:
                    truncated = candidate
                else:
                    break
            valid.append(truncated or shortest[:max_chars])
            logger.warning(f"  All candidates over limit, truncated shortest to {len(valid[0])} chars")

        return valid, rejected

    def _build_calibration_context(self) -> str:
        """Build calibration context for the engagement predictor.

        Wraps PredictionCalibrator to tolerate missing/limited history.
        Returns an empty string when calibration is unavailable.
        """
        try:
            from evaluation.prediction_calibrator import PredictionCalibrator
            calibrator = PredictionCalibrator(self.db)
            report = calibrator.compute_calibration_report(days=30)
            return calibrator.generate_calibration_context(report)
        except Exception as e:
            logger.debug(f"Calibration context generation failed (non-fatal): {e}")
            return ""

    def _build_topic_history_context(
        self,
        source_prompts: list[str],
        source_commits: list[str],
        candidates: list[str],
        content_type: str,
        platform: str,
    ) -> str:
        """Build optional topic engagement history for comparative evaluation."""
        try:
            analyzer = TopicPerformanceAnalyzer(self.db)
            return analyzer.build_evaluation_context(
                source_texts=source_prompts + source_commits,
                candidate_texts=candidates,
                content_type=content_type,
                platform=platform,
            )
        except Exception as e:
            logger.debug(f"Topic history context unavailable (non-fatal): {e}")
            return ""

    def run(
        self,
        prompts: list[str],
        commits: list[dict],
        content_type: str = "x_post",
        threshold: float = 0.7,
        platform: str = "x",
    ) -> PipelineResult:
        """Execute the full multi-stage pipeline."""
        run_started_at = datetime.now(timezone.utc)
        batch_id = str(uuid.uuid4())[:8]
        source_commit_messages = [c["message"] for c in commits]

        # Stage 0: Load curation signals and engagement calibration
        too_specific_posts = self.db.get_curated_posts(
            quality="too_specific", content_type=content_type, limit=5
        )
        low_resonance_posts = self.db.get_auto_classified_posts(
            quality="low_resonance", content_type=content_type, limit=3
        )
        resonated_posts = self.db.get_auto_classified_posts(
            quality="resonated", content_type=content_type, limit=3
        )

        # Stage 0.5: Engagement calibration stats
        engagement_stats = self.db.get_engagement_calibration_stats(content_type)

        # Build negative examples with source annotations for the evaluator
        negative_examples = []
        for p in too_specific_posts:
            negative_examples.append((p["content"], "too_specific"))
        for p in low_resonance_posts:
            negative_examples.append((p["content"], "low_resonance"))

        exclude_ids = (
            {p["id"] for p in too_specific_posts}
            | {p["id"] for p in low_resonance_posts}
        )

        # Stage 1: Few-shot retrieval
        examples = self.few_shot_selector.get_examples(
            content_type=content_type, limit=3, exclude_ids=exclude_ids
        )
        few_shot_text = self.few_shot_selector.format_examples(examples)
        reference_examples = [ex.content for ex in examples] if examples else None

        # Stage 2: Multi-candidate generation with format variation
        avoidance_context = self._build_avoidance_context()
        pattern_context = self._build_pattern_context()
        campaign_context, planned_topic_id = self._build_campaign_context()
        if campaign_context:
            pattern_context = (pattern_context + "\n" + campaign_context).strip() + "\n"
        presence_context = self._build_presence_context(content_type)
        if presence_context:
            pattern_context = (pattern_context + "\n" + presence_context).strip() + "\n"

        # Stage 1.5: Trend context from curated sources
        trend_context = ""
        trend_knowledge_ids = []
        if self.knowledge_store:
            from synthesis.trend_context import TrendContextBuilder
            trend_builder = TrendContextBuilder(
                knowledge_store=self.knowledge_store,
                api_key=self.api_key,
                model=self.generator.model,
                db=self.db,
                restricted_prompt_behavior=self.restricted_prompt_behavior,
            )
            trend_context, trend_knowledge_ids = trend_builder.build_context_with_ids()
            trend_hooks = trend_builder.build_hook_context(prompts=prompts, commits=commits)
            if trend_hooks:
                trend_context = (trend_context + "\n" + trend_hooks).strip() + "\n"
        linked_knowledge = [trend_context] if trend_context else []

        # Stage 1.6: Load format recommendations/weights if enabled
        recommended_formats = None
        format_weights = None
        if self.format_weighting_enabled:
            try:
                from evaluation.format_performance import FormatPerformanceAnalyzer
                analyzer = FormatPerformanceAnalyzer(self.db)
                recommended_formats = analyzer.get_recommended_formats(
                    content_type=content_type,
                    platform=platform,
                    limit=self.num_candidates,
                    days=90,
                )
                format_weights = analyzer.compute_selection_weights(days=90)
                if recommended_formats:
                    logger.debug(f"  Recommended formats: {recommended_formats}")
                if format_weights:
                    logger.debug(f"  Format weights: {format_weights}")
            except Exception as e:
                logger.debug(f"  Format weighting failed (non-fatal): {e}")

        format_selection_weights, format_cooldown_stats = self._apply_format_cooldown(
            content_type, format_weights
        )
        if format_cooldown_stats["format_penalties"]:
            logger.debug(
                "  Format cooldown penalties: "
                f"{format_cooldown_stats['format_penalties']}"
            )

        format_directives, format_names = self._select_format_directives(
            self.num_candidates,
            content_type,
            weights=format_selection_weights,
            recommended_formats=recommended_formats,
            reorder_recommended_by_weights=bool(
                format_cooldown_stats["format_penalties"]
            ),
        )
        if content_type == "x_visual" and trend_context:
            trend_directive = next(
                directive
                for name, directive in self.VISUAL_POST_FORMATS
                if name == "trend_linked"
            )
            if "trend_linked" not in format_names:
                if format_directives:
                    format_directives[-1] = trend_directive
                    format_names[-1] = "trend_linked"
                else:
                    format_directives = [trend_directive]
                    format_names = ["trend_linked"]
        candidates = self.generator.generate_candidates(
            prompts=prompts,
            commits=commits,
            content_type=content_type,
            few_shot_examples=few_shot_text,
            num_candidates=self.num_candidates,
            format_directives=format_directives,
            avoidance_context=avoidance_context,
            pattern_context=pattern_context,
            trend_context=trend_context,
        )
        candidate_texts = [c.content for c in candidates]

        # Stage 2.5: Character limit enforcement
        char_limit_rejected = 0
        char_limit = self.CHAR_LIMITS.get(content_type)
        if char_limit:
            candidate_texts, char_limit_rejected = self._enforce_char_limit(candidate_texts, char_limit)

        # Stage 2.6: Repetition filter
        candidate_texts, repetition_rejected = self._filter_repetitive(candidate_texts, content_type)

        # Stage 2.7: Stale pattern filter
        candidate_texts, stale_pattern_rejected, stale_patterns_matched = self._filter_stale_patterns(candidate_texts)

        # Stage 2.75: Topic saturation filter
        topic_saturated_rejected = 0
        if candidate_texts:
            candidate_texts, topic_saturated_rejected = self._filter_topic_saturated(candidate_texts)

        filter_stats = {
            "char_limit_rejected": char_limit_rejected,
            "repetition_rejected": repetition_rejected,
            "stale_pattern_rejected": stale_pattern_rejected,
            "stale_patterns_matched": stale_patterns_matched,
            "topic_saturated_rejected": topic_saturated_rejected,
            "format_selection": {
                **format_cooldown_stats,
                "selected_formats": format_names,
            },
        }

        # Stage 2.8: Semantic dedup filter
        if candidate_texts:
            candidate_texts = self._filter_semantic_duplicates(candidate_texts)

        # Stage 2.9: Unsupported claim filter
        claim_check_rejected = 0
        claim_check_annotations = []
        if candidate_texts:
            candidate_texts, claim_check_rejected, claim_check_annotations = (
                self._filter_unsupported_claims(
                    candidate_texts,
                    source_prompts=prompts,
                    source_commits=source_commit_messages,
                    linked_knowledge=linked_knowledge,
                )
            )
        filter_stats["claim_check_rejected"] = claim_check_rejected
        filter_stats["claim_check_annotations"] = claim_check_annotations

        # All candidates filtered — reject rather than publish stale/repetitive content
        if not candidate_texts:
            logger.warning("  All candidates filtered")
            return self._apply_budget_gate(PipelineResult(
                batch_id=batch_id,
                candidates=[],
                comparison=ComparisonResult(
                    ranking=[],
                    best_score=0,
                    groundedness=0,
                    rawness=0,
                    narrative_specificity=0,
                    voice=0,
                    engagement_potential=0,
                    best_feedback="",
                    improvement="",
                    reject_reason="All candidates filtered (repetitive, stale, duplicate, or unsupported claims)",
                    raw_response="",
                ),
                refinement=None,
                final_content="",
                final_score=0,
                source_prompts=prompts,
                source_commits=source_commit_messages,
                filter_stats=filter_stats,
                planned_topic_id=planned_topic_id,
            ), run_started_at)

        # Stage 3: Cross-model evaluation with engagement and topic calibration
        topic_history_context = self._build_topic_history_context(
            source_prompts=prompts,
            source_commits=source_commit_messages,
            candidates=candidate_texts,
            content_type=content_type,
            platform=platform,
        )
        comparison = self.evaluator.evaluate(
            candidates=candidate_texts,
            source_prompts=prompts,
            source_commits=source_commit_messages,
            reference_examples=reference_examples,
            negative_examples=negative_examples or None,
            calibration_resonated=resonated_posts or None,
            calibration_low_resonance=low_resonance_posts or None,
            engagement_stats=engagement_stats,
            topic_history_context=topic_history_context or None,
        )

        best_idx = comparison.ranking[0] if comparison.ranking else 0

        # Stage 3.5: Engagement prediction as conservative tie-breaker.
        # Run the predictor on ALL surviving candidates before refinement. If the
        # predictor's top differs from the evaluator's top by a clear margin AND
        # the alternative is a decent post on its own, override best_idx.
        all_predictions: list = []
        predictor_override = False
        predictor_override_detail: Optional[dict] = None
        if self.engagement_predictor and len(candidate_texts) >= 2:
            try:
                calibration_context = self._build_calibration_context()
                all_predictions = self.engagement_predictor.predict_batch(
                    tweets=[
                        {"id": str(i), "text": t}
                        for i, t in enumerate(candidate_texts)
                    ],
                    prompt_version="v1",
                    calibration_context=calibration_context,
                )
                if len(all_predictions) == len(candidate_texts):
                    evaluator_top_idx = best_idx
                    predictor_top_idx = max(
                        range(len(all_predictions)),
                        key=lambda i: all_predictions[i].predicted_score,
                    )
                    if predictor_top_idx != evaluator_top_idx:
                        eval_pred = all_predictions[evaluator_top_idx].predicted_score
                        pred_pred = all_predictions[predictor_top_idx].predicted_score
                        margin = pred_pred - eval_pred
                        if pred_pred >= 6.0 and margin >= 1.5:
                            logger.info(
                                f"  Predictor override: candidate {predictor_top_idx} "
                                f"(pred={pred_pred:.1f}) over candidate {evaluator_top_idx} "
                                f"(pred={eval_pred:.1f}, margin={margin:.1f})"
                            )
                            best_idx = predictor_top_idx
                            predictor_override = True
                            predictor_override_detail = {
                                "evaluator_top": evaluator_top_idx,
                                "predictor_top": predictor_top_idx,
                                "evaluator_pred_score": eval_pred,
                                "predictor_pred_score": pred_pred,
                                "margin": margin,
                            }
            except Exception as e:
                logger.warning(f"  Engagement prediction (tie-breaker) failed: {e}")
                all_predictions = []

        best_content = candidate_texts[best_idx]
        final_content = best_content
        final_score = comparison.best_score
        refinement = None

        # Capture the format used for the best candidate
        best_format = format_names[best_idx] if best_idx < len(format_names) else None
        filter_stats["format_selection"]["selected_format"] = best_format
        filter_stats["format_selection"]["selected_format_cooldown_penalty"] = (
            filter_stats["format_selection"]["format_penalties"].get(best_format, 0.0)
            if best_format
            else 0.0
        )

        # Stage 4 & 5: Refinement + final gate
        should_refine = (
            self.SKIP_REFINE_BELOW <= comparison.best_score < self.SKIP_REFINE_ABOVE
            and comparison.improvement
            and comparison.reject_reason is None
        )

        if should_refine:
            refinement = self.refiner.refine_and_gate(
                content=best_content,
                best_feedback=comparison.best_feedback,
                improvement=comparison.improvement,
                content_type=content_type,
            )
            final_content = refinement.final_content
            # Use the higher of gate score and evaluator score — the evaluator
            # is more granular and the gate tends to cluster at round numbers
            final_score = max(refinement.final_score, comparison.best_score)

        # Final character limit check (refinement may have expanded)
        if char_limit and len(final_content) > char_limit:
            logger.info(f"  Final content is {len(final_content)} chars, condensing...")
            condensed = self.generator.condense(final_content, char_limit)
            if len(condensed) <= char_limit:
                final_content = condensed
                logger.info(f"  Condensed to {len(final_content)} chars")
            else:
                # Hard truncate at sentence boundary
                sentences = final_content.split(". ")
                truncated = ""
                for s in sentences:
                    candidate_text = (truncated + ". " + s).strip(". ") + "." if truncated else s
                    if len(candidate_text) <= char_limit:
                        truncated = candidate_text
                    else:
                        break
                final_content = truncated or final_content[:char_limit]
                logger.warning(f"  Hard truncated to {len(final_content)} chars")

        persona_guard_result = self._check_persona_guard(final_content, content_type)
        persona_guard_summary = persona_guard_result.to_summary()
        filter_stats["persona_guard"] = persona_guard_summary
        if not persona_guard_result.passed:
            guard_reason = (
                "Persona guard failed: " + "; ".join(persona_guard_result.reasons)
            )
            logger.warning("  %s", guard_reason)
            comparison = self._append_reject_reason(comparison, guard_reason)
            knowledge_ids = [(kid, 0.3) for kid in trend_knowledge_ids]
            return self._apply_budget_gate(PipelineResult(
                batch_id=batch_id,
                candidates=candidate_texts,
                comparison=comparison,
                refinement=refinement,
                final_content=final_content,
                final_score=0,
                source_prompts=prompts,
                source_commits=source_commit_messages,
                filter_stats=filter_stats,
                predicted_engagement=None,
                engagement_prediction_detail=None,
                knowledge_ids=knowledge_ids,
                content_format=best_format,
                predictor_override=predictor_override,
                predictor_override_detail=predictor_override_detail,
                planned_topic_id=planned_topic_id,
                claim_check_summary=None,
                persona_guard_summary=persona_guard_summary,
            ), run_started_at)

        final_claim_check = self._check_final_claims(
            final_content,
            source_prompts=prompts,
            source_commits=source_commit_messages,
            linked_knowledge=linked_knowledge,
        )
        filter_stats["claim_check_final_unsupported"] = final_claim_check.annotations
        claim_check_summary = (
            _claim_check_summary(final_claim_check)
            if self.claim_check_enabled
            else None
        )

        if content_type == "x_thread":
            thread_validation = validate_thread(final_content)
            filter_stats["thread_validation_valid"] = thread_validation.valid
            filter_stats["thread_validation_failures"] = (
                thread_validation.failure_reasons
            )
            if not thread_validation.valid:
                validation_reason = (
                    "Thread validation failed: "
                    + "; ".join(thread_validation.failure_reasons)
                )
                logger.warning("  %s", validation_reason)
                comparison = self._append_reject_reason(
                    comparison, validation_reason
                )
                knowledge_ids = [(kid, 0.3) for kid in trend_knowledge_ids]
                return self._apply_budget_gate(PipelineResult(
                    batch_id=batch_id,
                    candidates=candidate_texts,
                    comparison=comparison,
                    refinement=refinement,
                    final_content=final_content,
                    final_score=0,
                    source_prompts=prompts,
                    source_commits=source_commit_messages,
                    filter_stats=filter_stats,
                    predicted_engagement=None,
                    engagement_prediction_detail=None,
                    knowledge_ids=knowledge_ids,
                    content_format=best_format,
                    predictor_override=predictor_override,
                    predictor_override_detail=predictor_override_detail,
                    planned_topic_id=planned_topic_id,
                    claim_check_summary=claim_check_summary,
                    persona_guard_summary=persona_guard_summary,
                ), run_started_at)

        # Stage 6: Engagement prediction logging.
        # Reuse the prediction computed in Stage 3.5 for the chosen candidate
        # when available. Fall back to a single predict call for the lone-
        # candidate path where Stage 3.5 is skipped.
        predicted_engagement = None
        engagement_prediction_detail = None
        pred = None
        if all_predictions and best_idx < len(all_predictions):
            pred = all_predictions[best_idx]
        elif self.engagement_predictor:
            try:
                calibration_context = self._build_calibration_context()
                predictions = self.engagement_predictor.predict_batch(
                    tweets=[{"id": "draft", "text": final_content}],
                    prompt_version="v1",
                    calibration_context=calibration_context,
                )
                if predictions:
                    pred = predictions[0]
            except Exception as e:
                logger.warning(f"  Engagement prediction failed: {e}")

        if pred is not None:
            predicted_engagement = pred.predicted_score
            engagement_prediction_detail = {
                "predicted_score": pred.predicted_score,
                "hook_strength": pred.hook_strength,
                "specificity": pred.specificity,
                "emotional_resonance": pred.emotional_resonance,
                "novelty": pred.novelty,
                "actionability": pred.actionability,
                "prompt_type": pred.prompt_type,
                "prompt_version": pred.prompt_version_label or "v1",
                "prompt_hash": pred.prompt_hash,
            }
            logger.info(
                f"  Predicted engagement: {predicted_engagement:.1f} "
                f"(hook={pred.hook_strength:.1f}, spec={pred.specificity:.1f}, "
                f"emotion={pred.emotional_resonance:.1f}, "
                f"novelty={pred.novelty:.1f}, action={pred.actionability:.1f})"
            )

        # Compile knowledge IDs: trend items get default relevance of 0.3
        knowledge_ids = [(kid, 0.3) for kid in trend_knowledge_ids]

        return self._apply_budget_gate(PipelineResult(
            batch_id=batch_id,
            candidates=candidate_texts,
            comparison=comparison,
            refinement=refinement,
            final_content=final_content,
            final_score=final_score,
            source_prompts=prompts,
            source_commits=source_commit_messages,
            filter_stats=filter_stats,
            predicted_engagement=predicted_engagement,
            engagement_prediction_detail=engagement_prediction_detail,
            knowledge_ids=knowledge_ids,
            content_format=best_format,
            predictor_override=predictor_override,
            predictor_override_detail=predictor_override_detail,
            planned_topic_id=planned_topic_id,
            claim_check_summary=claim_check_summary,
            persona_guard_summary=persona_guard_summary,
        ), run_started_at)
