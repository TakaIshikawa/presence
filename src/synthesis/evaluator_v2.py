"""Cross-model comparative evaluator for generated content."""

import re
import anthropic
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class ComparisonResult:
    ranking: list[int]
    best_score: float
    groundedness: float
    authenticity: float
    narrative_specificity: float
    voice: float
    engagement_potential: float
    best_feedback: str
    improvement: str
    reject_reason: Optional[str]
    raw_response: str

    def passes_threshold(self, threshold: float = 0.7) -> bool:
        return self.best_score >= threshold * 10


class CrossModelEvaluator:
    """Evaluates multiple candidates comparatively using a different model than the generator."""

    PROMPTS_DIR = Path(__file__).parent / "prompts"
    CANDIDATE_LABELS = "ABCDEFGHIJ"

    def __init__(self, api_key: str, model: str = "claude-opus-4-20250514"):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=300.0)
        self.model = model

    def _load_prompt(self) -> str:
        prompt_file = self.PROMPTS_DIR / "evaluator_comparative.txt"
        return prompt_file.read_text()

    STATIC_NEGATIVE_EXAMPLES = [
        (
            "Built termination detection with JSON fallback parsing when structured responses fail.",
            "Uses implementation-specific jargon (\"JSON fallback parsing\", \"termination detection\") "
            "that only makes sense to someone reading this codebase."
        ),
        (
            "Added consolidation tracking, auto-restart hooks, and error handling across 4 commits.",
            "Lists internal features (\"consolidation tracking\", \"auto-restart hooks\") as if they're insights. "
            "This is a changelog, not a transferable observation."
        ),
        (
            "Assignment preservation, session boundaries, and state transitions matter more than prompt engineering.",
            "\"Assignment preservation\" and \"session boundaries\" are project-internal concepts. "
            "The reader has no idea what assignments are being preserved or why."
        ),
    ]

    ANNOTATION_BY_SOURCE = {
        "too_specific": "Scored well in evaluation but contains project-specific jargon meaningless to outside readers.",
        "low_resonance": "Scored well in evaluation but got zero audience engagement — likely too generic or abstract to provoke a reaction.",
    }

    def _build_negative_section(self, negative_examples: list = None) -> str:
        """Build the negative examples section from curated/auto-classified posts and static seeds.

        negative_examples can be:
        - list of (content, source) tuples where source is 'too_specific' or 'low_resonance'
        - list of plain strings (legacy, treated as 'too_specific')
        """
        items = []

        if negative_examples:
            for ex in negative_examples[:5]:
                if isinstance(ex, tuple):
                    content, source = ex
                    annotation = self.ANNOTATION_BY_SOURCE.get(source, self.ANNOTATION_BY_SOURCE["too_specific"])
                else:
                    content, annotation = ex, self.ANNOTATION_BY_SOURCE["too_specific"]
                items.append(f"- \"{content}\"\n  Problem: {annotation}")

        # Fill remaining slots with static examples
        remaining = 3 - len(items)
        if remaining > 0:
            for content, annotation in self.STATIC_NEGATIVE_EXAMPLES[:remaining]:
                items.append(f"- \"{content}\"\n  Problem: {annotation}")

        if not items:
            return ""

        return (
            "NEGATIVE EXAMPLES — posts that scored well but failed with real audiences. "
            "Penalize candidates that follow these patterns:\n\n"
            + "\n\n".join(items)
        )

    def evaluate(
        self,
        candidates: list[str],
        source_prompts: list[str],
        source_commits: list[str],
        reference_examples: list[str] = None,
        negative_examples: list[str] = None,
    ) -> ComparisonResult:
        """Evaluate multiple candidates comparatively and return ranking with feedback."""
        template = self._load_prompt()

        # Format candidates
        candidates_text = "\n\n".join(
            f"CANDIDATE {self.CANDIDATE_LABELS[i]}:\n{c}"
            for i, c in enumerate(candidates)
        )

        # Format reference examples
        if reference_examples:
            reference_section = (
                "REFERENCE EXAMPLES (posts that performed well — use as quality calibration):\n\n"
                + "\n\n".join(f"- {ex}" for ex in reference_examples)
            )
        else:
            reference_section = ""

        negative_examples_section = self._build_negative_section(negative_examples)

        filled = template.format(
            candidates=candidates_text,
            source_prompts="\n".join(f"- {p[:500]}" for p in source_prompts[:5]),
            source_commits="\n".join(f"- {c}" for c in source_commits[:10]),
            reference_section=reference_section,
            negative_examples_section=negative_examples_section,
        )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=800,
            messages=[{"role": "user", "content": filled}],
        )

        return self._parse_response(response.content[0].text, len(candidates))

    def _parse_criterion_score(self, response: str, name: str) -> float:
        """Extract a per-criterion score from the response."""
        pattern = rf"{name}:\s*(\d+(?:\.\d+)?)"
        match = re.search(pattern, response)
        return float(match.group(1)) if match else 5.0

    def _parse_response(self, response: str, num_candidates: int) -> ComparisonResult:
        """Parse the structured comparative evaluation response."""
        # Parse ranking
        ranking_match = re.search(r"RANKING:\s*(.+)", response)
        ranking = []
        if ranking_match:
            letters = re.findall(r"[A-Z]", ranking_match.group(1))
            ranking = [
                self.CANDIDATE_LABELS.index(l)
                for l in letters
                if l in self.CANDIDATE_LABELS[:num_candidates]
            ]
        # Fallback: if parsing fails, assume first candidate is best
        if not ranking:
            ranking = list(range(num_candidates))

        # Parse per-criterion scores
        groundedness = self._parse_criterion_score(response, "GROUNDEDNESS")
        authenticity = self._parse_criterion_score(response, "AUTHENTICITY")
        narrative_specificity = self._parse_criterion_score(response, "NARRATIVE_SPECIFICITY")
        voice = self._parse_criterion_score(response, "VOICE")
        engagement_potential = self._parse_criterion_score(response, "ENGAGEMENT_POTENTIAL")

        # Compute weighted average — GROUNDEDNESS counts double
        best_score = (
            groundedness * 2 + authenticity + narrative_specificity + voice + engagement_potential
        ) / 6

        # Parse feedback
        feedback_match = re.search(
            r"BEST_FEEDBACK:\s*(.+?)(?=\n(?:IMPROVEMENT|REJECT)|\Z)",
            response, re.DOTALL
        )
        best_feedback = feedback_match.group(1).strip() if feedback_match else ""

        # Parse improvement suggestion
        improvement_match = re.search(
            r"IMPROVEMENT:\s*(.+?)(?=\n(?:REJECT)|\Z)",
            response, re.DOTALL
        )
        improvement = improvement_match.group(1).strip() if improvement_match else ""

        # Parse reject reason
        reject_match = re.search(r"REJECT_REASON:\s*(.+?)$", response, re.DOTALL)
        reject_text = reject_match.group(1).strip() if reject_match else "none"
        reject_reason = None if reject_text.lower() == "none" else reject_text

        # Auto-reject if groundedness is critically low
        if groundedness <= 3.0 and reject_reason is None:
            reject_reason = f"Groundedness score too low ({groundedness}/10) — likely contains fabricated claims"

        return ComparisonResult(
            ranking=ranking,
            best_score=best_score,
            groundedness=groundedness,
            authenticity=authenticity,
            narrative_specificity=narrative_specificity,
            voice=voice,
            engagement_potential=engagement_potential,
            best_feedback=best_feedback,
            improvement=improvement,
            reject_reason=reject_reason,
            raw_response=response,
        )
