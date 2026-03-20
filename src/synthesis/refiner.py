"""Content refinement based on evaluation feedback."""

import re
import anthropic
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class RefinementResult:
    original: str
    refined: str
    picked: str  # 'REFINED' or 'ORIGINAL'
    final_score: float
    final_content: str


class ContentRefiner:
    """Refines content using evaluation feedback, then gates with a final comparison."""

    PROMPTS_DIR = Path(__file__).parent / "prompts"

    def __init__(
        self,
        refine_api_key: str,
        refine_model: str,
        gate_api_key: str,
        gate_model: str,
    ):
        self.refine_client = anthropic.Anthropic(api_key=refine_api_key, timeout=300.0)
        self.refine_model = refine_model
        self.gate_client = anthropic.Anthropic(api_key=gate_api_key, timeout=300.0)
        self.gate_model = gate_model

    def refine_and_gate(
        self,
        content: str,
        best_feedback: str,
        improvement: str,
    ) -> RefinementResult:
        """Refine content based on feedback, then pick the better version."""
        # Stage 1: Refine
        refined = self._refine(content, best_feedback, improvement)

        # Stage 2: Final gate — pick original vs refined
        return self._final_gate(content, refined)

    def _refine(self, content: str, best_feedback: str, improvement: str) -> str:
        template = (self.PROMPTS_DIR / "refiner.txt").read_text()
        filled = template.format(
            content=content,
            best_feedback=best_feedback,
            improvement=improvement,
        )

        response = self.refine_client.messages.create(
            model=self.refine_model,
            max_tokens=500,
            messages=[{"role": "user", "content": filled}],
        )
        return response.content[0].text.strip()

    def _final_gate(self, original: str, refined: str) -> RefinementResult:
        template = (self.PROMPTS_DIR / "final_gate.txt").read_text()
        filled = template.format(original=original, refined=refined)

        response = self.gate_client.messages.create(
            model=self.gate_model,
            max_tokens=200,
            messages=[{"role": "user", "content": filled}],
        )

        text = response.content[0].text
        pick_match = re.search(r"PICK:\s*(REFINED|ORIGINAL)", text, re.IGNORECASE)
        score_match = re.search(r"SCORE:\s*(\d+(?:\.\d+)?)", text)

        picked = pick_match.group(1).upper() if pick_match else "REFINED"
        score = float(score_match.group(1)) if score_match else 5.0
        final_content = refined if picked == "REFINED" else original

        return RefinementResult(
            original=original,
            refined=refined,
            picked=picked,
            final_score=score,
            final_content=final_content,
        )
