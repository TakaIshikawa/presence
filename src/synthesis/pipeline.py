"""Multi-stage synthesis pipeline for content generation."""

import json
import logging
import random
import re
import uuid
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Optional

from storage.db import Database
from synthesis.generator import ContentGenerator
from synthesis.evaluator_v2 import CrossModelEvaluator, ComparisonResult
from synthesis.refiner import ContentRefiner, RefinementResult
from synthesis.few_shot import FewShotSelector
from synthesis.stale_patterns import STALE_PATTERNS

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
    ):
        self.api_key = api_key
        self.generator = ContentGenerator(api_key, generator_model, timeout=anthropic_timeout)
        self.evaluator = CrossModelEvaluator(api_key, evaluator_model, timeout=anthropic_timeout)
        self.refiner = ContentRefiner(
            refine_api_key=api_key,
            refine_model=generator_model,
            gate_api_key=api_key,
            gate_model=evaluator_model,
            timeout=anthropic_timeout,
        )
        self.db = db
        self.few_shot_selector = FewShotSelector(db)
        self.num_candidates = num_candidates
        self.embedder = embedder
        self.semantic_threshold = semantic_threshold
        self.knowledge_store = knowledge_store

    # Character limits per content type
    CHAR_LIMITS = {
        "x_post": 280,
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

    def _select_format_directives(self, num: int, content_type: str = "x_post") -> list[str]:
        """Select format directives for candidate generation, favoring variety."""
        formats = self.THREAD_FORMATS if content_type == "x_thread" else self.POST_FORMATS
        selected = random.sample(formats, min(num, len(formats)))
        return [directive for _, directive in selected]

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

    def run(
        self,
        prompts: list[str],
        commits: list[dict],
        content_type: str = "x_post",
        threshold: float = 0.7,
    ) -> PipelineResult:
        """Execute the full multi-stage pipeline."""
        batch_id = str(uuid.uuid4())[:8]

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

        # Stage 1.5: Trend context from curated sources
        trend_context = ""
        if self.knowledge_store:
            from synthesis.trend_context import TrendContextBuilder
            trend_builder = TrendContextBuilder(
                knowledge_store=self.knowledge_store,
                api_key=self.api_key,
                model=self.generator.model,
                db=self.db,
            )
            trend_context = trend_builder.build_context()

        format_directives = self._select_format_directives(self.num_candidates, content_type)
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
        }

        # Stage 2.8: Semantic dedup filter
        if candidate_texts:
            candidate_texts = self._filter_semantic_duplicates(candidate_texts)

        # All candidates filtered — reject rather than publish stale/repetitive content
        if not candidate_texts:
            logger.warning("  All candidates filtered (repetitive, stale, or semantic duplicate)")
            return PipelineResult(
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
                    reject_reason="All candidates filtered (repetitive or stale patterns)",
                    raw_response="",
                ),
                refinement=None,
                final_content="",
                final_score=0,
                source_prompts=prompts,
                source_commits=[c["message"] for c in commits],
                filter_stats=filter_stats,
            )

        # Stage 3: Cross-model evaluation with engagement calibration
        comparison = self.evaluator.evaluate(
            candidates=candidate_texts,
            source_prompts=prompts,
            source_commits=[c["message"] for c in commits],
            reference_examples=reference_examples,
            negative_examples=negative_examples or None,
            calibration_resonated=resonated_posts or None,
            calibration_low_resonance=low_resonance_posts or None,
            engagement_stats=engagement_stats,
        )

        best_idx = comparison.ranking[0] if comparison.ranking else 0
        best_content = candidate_texts[best_idx]
        final_content = best_content
        final_score = comparison.best_score
        refinement = None

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

        return PipelineResult(
            batch_id=batch_id,
            candidates=candidate_texts,
            comparison=comparison,
            refinement=refinement,
            final_content=final_content,
            final_score=final_score,
            source_prompts=prompts,
            source_commits=[c["message"] for c in commits],
            filter_stats=filter_stats,
        )
