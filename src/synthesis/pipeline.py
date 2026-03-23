"""Multi-stage synthesis pipeline for content generation."""

import uuid
from dataclasses import dataclass, field
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
        self.few_shot_selector = FewShotSelector(db)
        self.num_candidates = num_candidates

    # Character limits per content type
    CHAR_LIMITS = {
        "x_post": 280,
    }

    def _enforce_char_limit(self, candidates: list[str], max_chars: int) -> list[str]:
        """Validate and condense candidates that exceed character limit."""
        valid = []
        for i, text in enumerate(candidates):
            if len(text) <= max_chars:
                valid.append(text)
                continue

            # Try condensing once
            print(f"  Candidate {i} is {len(text)} chars (limit {max_chars}), condensing...")
            condensed = self.generator.condense(text, max_chars)
            if len(condensed) <= max_chars:
                print(f"  Condensed to {len(condensed)} chars")
                valid.append(condensed)
            else:
                print(f"  Still {len(condensed)} chars after condensing, discarding")

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

        # Stage 1: Few-shot retrieval
        examples = self.few_shot_selector.get_examples(
            content_type=content_type, limit=3
        )
        few_shot_text = self.few_shot_selector.format_examples(examples)
        reference_examples = [ex.content for ex in examples] if examples else None

        # Stage 2: Multi-candidate generation
        candidates = self.generator.generate_candidates(
            prompts=prompts,
            commits=commits,
            content_type=content_type,
            few_shot_examples=few_shot_text,
            num_candidates=self.num_candidates,
        )
        candidate_texts = [c.content for c in candidates]

        # Stage 2.5: Character limit enforcement
        char_limit = self.CHAR_LIMITS.get(content_type)
        if char_limit:
            candidate_texts = self._enforce_char_limit(candidate_texts, char_limit)

        # Stage 3: Cross-model evaluation
        comparison = self.evaluator.evaluate(
            candidates=candidate_texts,
            source_prompts=prompts,
            source_commits=[c["message"] for c in commits],
            reference_examples=reference_examples,
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
            final_score = refinement.final_score

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
