"""Multi-stage synthesis pipeline for content generation."""

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

    # Overused rhetorical patterns to reject
    STALE_PATTERNS = [
        re.compile(r"(?i)^AI\s"),
        re.compile(r"(?i)isn.t about .{5,40}[—\-].{0,5}it.s about"),
        re.compile(r"(?i)\bbreakthrough\b"),
        re.compile(r"(?i)perfect (prompts?|memory|agents?|handoffs?|context)"),
        re.compile(r"\d+ commits? across \d+"),
        re.compile(r"(?i)^(TWEET 1:\s*\n)?Today.s (insight|breakthrough|lesson)"),
    ]

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
            "'I was halfway through a refactor when the agent did something unexpected.' "
            "No labels, no preamble — open with a scene.",
        ),
        (
            "bold_claim",
            "THREAD HOOK: Open Tweet 1 with a bold, specific claim the thread will prove. "
            "'Most AI agent failures happen before a single line of code runs.' "
            "Make it falsifiable and surprising. The thread is the evidence.",
        ),
        (
            "question_hook",
            "THREAD HOOK: Start Tweet 1 with a genuine question that came up in your work. "
            "'Why do agents silently give up instead of asking for help?' "
            "The thread walks through what you found. End with the question evolved, not answered.",
        ),
        (
            "surprising_result",
            "THREAD HOOK: Lead Tweet 1 with a concrete, unexpected result. "
            "'Gave two agents the same task. One finished in 3 minutes, the other looped for an hour.' "
            "State the outcome first — the thread explains why.",
        ),
        (
            "contrarian_thread",
            "THREAD HOOK: Open Tweet 1 by challenging a common practice. "
            "'Stop giving your AI agent detailed instructions. Seriously.' "
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
    ):
        self.generator = ContentGenerator(api_key, generator_model)
        self.evaluator = CrossModelEvaluator(api_key, evaluator_model)
        self.refiner = ContentRefiner(
            refine_api_key=api_key,
            refine_model=generator_model,
            gate_api_key=api_key,
            gate_model=evaluator_model,
        )
        self.db = db
        self.few_shot_selector = FewShotSelector(db)
        self.num_candidates = num_candidates

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

    def _filter_repetitive(self, candidates: list[str], content_type: str) -> list[str]:
        """Remove candidates whose opening is too similar to recent posts."""
        recent = self.db.get_recent_published_content(content_type, limit=20)
        if not recent:
            return candidates

        recent_openings = [self._extract_opening(p["content"]) for p in recent]

        filtered = []
        for candidate in candidates:
            opening = self._extract_opening(candidate)
            is_repetitive = any(
                SequenceMatcher(None, opening, ro).ratio() > 0.55
                for ro in recent_openings
            )
            if is_repetitive:
                print(f"  Rejected as repetitive: {opening[:40]}...")
            else:
                filtered.append(candidate)

        return filtered

    def _filter_stale_patterns(self, candidates: list[str]) -> list[str]:
        """Reject candidates matching overused rhetorical patterns."""
        filtered = []
        for candidate in candidates:
            matches = [p.pattern for p in self.STALE_PATTERNS if p.search(candidate)]
            if matches:
                print(f"  Rejected stale pattern: {candidate[:50]}...")
            else:
                filtered.append(candidate)
        return filtered

    def _select_format_directives(self, num: int, content_type: str = "x_post") -> list[str]:
        """Select format directives for candidate generation, favoring variety."""
        formats = self.THREAD_FORMATS if content_type == "x_thread" else self.POST_FORMATS
        selected = random.sample(formats, min(num, len(formats)))
        return [directive for _, directive in selected]

    def _enforce_char_limit(self, candidates: list[str], max_chars: int) -> list[str]:
        """Validate and condense candidates that exceed character limit."""
        valid = []
        for i, text in enumerate(candidates):
            if len(text) <= max_chars:
                valid.append(text)
                continue

            # Try condensing up to 2 times
            condensed = text
            for attempt in range(2):
                print(f"  Candidate {i} is {len(condensed)} chars (limit {max_chars}), condensing (attempt {attempt + 1})...")
                condensed = self.generator.condense(condensed, max_chars)
                if len(condensed) <= max_chars:
                    print(f"  Condensed to {len(condensed)} chars")
                    valid.append(condensed)
                    break
            else:
                print(f"  Still {len(condensed)} chars after 2 condense attempts, discarding")

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
            print(f"  All candidates over limit, truncated shortest to {len(valid[0])} chars")

        return valid

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
        format_directives = self._select_format_directives(self.num_candidates, content_type)
        candidates = self.generator.generate_candidates(
            prompts=prompts,
            commits=commits,
            content_type=content_type,
            few_shot_examples=few_shot_text,
            num_candidates=self.num_candidates,
            format_directives=format_directives,
        )
        candidate_texts = [c.content for c in candidates]

        # Stage 2.5: Character limit enforcement
        char_limit = self.CHAR_LIMITS.get(content_type)
        if char_limit:
            candidate_texts = self._enforce_char_limit(candidate_texts, char_limit)

        # Stage 2.6: Repetition filter
        candidate_texts = self._filter_repetitive(candidate_texts, content_type)

        # Stage 2.7: Stale pattern filter
        candidate_texts = self._filter_stale_patterns(candidate_texts)

        # All candidates filtered — reject rather than publish stale/repetitive content
        if not candidate_texts:
            print("  All candidates filtered (repetitive or stale patterns)")
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
            print(f"  Final content is {len(final_content)} chars, condensing...")
            condensed = self.generator.condense(final_content, char_limit)
            if len(condensed) <= char_limit:
                final_content = condensed
                print(f"  Condensed to {len(final_content)} chars")
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
                print(f"  Hard truncated to {len(final_content)} chars")

        return PipelineResult(
            batch_id=batch_id,
            candidates=candidate_texts,
            comparison=comparison,
            refinement=refinement,
            final_content=final_content,
            final_score=final_score,
            source_prompts=prompts,
            source_commits=[c["message"] for c in commits],
        )
