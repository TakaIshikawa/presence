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

    def evaluate(
        self,
        candidates: list[str],
        source_prompts: list[str],
        source_commits: list[str],
        reference_examples: list[str] = None,
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

        filled = template.format(
            candidates=candidates_text,
            source_prompts="\n".join(f"- {p[:500]}" for p in source_prompts[:5]),
            source_commits="\n".join(f"- {c}" for c in source_commits[:10]),
            reference_section=reference_section,
        )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=800,
            messages=[{"role": "user", "content": filled}],
        )

        return self._parse_response(response.content[0].text, len(candidates))

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

        # Parse score
        score_match = re.search(r"BEST_SCORE:\s*(\d+(?:\.\d+)?)", response)
        best_score = float(score_match.group(1)) if score_match else 5.0

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

        return ComparisonResult(
            ranking=ranking,
            best_score=best_score,
            best_feedback=best_feedback,
            improvement=improvement,
            reject_reason=reject_reason,
            raw_response=response,
        )
