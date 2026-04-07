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

    FORMAT_CONSTRAINTS = {
        "x_post": ("- Max 280 characters\n- Single tweet format", 500),
        "x_thread": ("- Each tweet max 280 characters\n- Keep TWEET N: format\n- 3-5 tweets", 2000),
        "blog_post": ("- 800-1200 words\n- Keep ## section headers\n- Keep TITLE: format", 4000),
    }

    def __init__(
        self,
        refine_api_key: str,
        refine_model: str,
        gate_api_key: str,
        gate_model: str,
        timeout: float = 300.0,
    ):
        self.refine_client = anthropic.Anthropic(api_key=refine_api_key, timeout=timeout)
        self.refine_model = refine_model
        self.gate_client = anthropic.Anthropic(api_key=gate_api_key, timeout=timeout)
        self.gate_model = gate_model

    def refine_and_gate(
        self,
        content: str,
        best_feedback: str,
        improvement: str,
        content_type: str = "x_post",
    ) -> RefinementResult:
        """Refine content based on feedback, then pick the better version."""
        refined = self._refine(content, best_feedback, improvement, content_type)
        return self._final_gate(content, refined, content_type)

    def _refine(
        self, content: str, best_feedback: str, improvement: str, content_type: str
    ) -> str:
        template = (self.PROMPTS_DIR / "refiner.txt").read_text()
        constraints, max_tokens = self.FORMAT_CONSTRAINTS.get(
            content_type, self.FORMAT_CONSTRAINTS["x_post"]
        )
        filled = template.format(
            content=content,
            best_feedback=best_feedback,
            improvement=improvement,
            format_constraints=constraints,
        )

        response = self.refine_client.messages.create(
            model=self.refine_model,
            max_tokens=max_tokens,
            messages=[{"role": "user", "content": filled}],
        )
        return response.content[0].text.strip()

    def _final_gate(self, original: str, refined: str, content_type: str = "x_post") -> RefinementResult:
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
