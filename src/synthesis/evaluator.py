"""LLM-as-judge evaluator for generated content."""

import re
import anthropic
from pathlib import Path
from dataclasses import dataclass
from typing import Optional


@dataclass
class EvalResult:
    authenticity: float
    insight_depth: float
    clarity: float
    voice_match: float
    accessibility: float
    overall: float
    feedback: str

    def passes_threshold(self, threshold: float = 0.7) -> bool:
        return self.overall >= threshold * 10


class ContentEvaluator:
    PROMPTS_DIR = Path(__file__).parent / "prompts"

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        self.client = anthropic.Anthropic(api_key=api_key)
        self.model = model

    def _load_prompt(self) -> str:
        prompt_file = self.PROMPTS_DIR / "evaluator.txt"
        return prompt_file.read_text()

    def evaluate(
        self,
        content_type: str,
        content: str,
        source_prompts: list[str],
        source_commits: list[str]
    ) -> EvalResult:
        """Evaluate generated content quality."""
        template = self._load_prompt()

        filled = template.format(
            content_type=content_type,
            content=content,
            source_prompts="\n".join(f"- {p}" for p in source_prompts),
            source_commits="\n".join(f"- {c}" for c in source_commits)
        )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=500,
            messages=[{"role": "user", "content": filled}]
        )

        return self._parse_eval_response(response.content[0].text)

    def _parse_eval_response(self, response: str) -> EvalResult:
        """Parse the structured evaluation response."""
        def extract_score(pattern: str) -> float:
            match = re.search(pattern, response)
            if match:
                return float(match.group(1))
            return 5.0  # default middle score

        authenticity = extract_score(r"AUTHENTICITY:\s*(\d+(?:\.\d+)?)/10")
        insight_depth = extract_score(r"INSIGHT_DEPTH:\s*(\d+(?:\.\d+)?)/10")
        clarity = extract_score(r"CLARITY:\s*(\d+(?:\.\d+)?)/10")
        voice_match = extract_score(r"VOICE_MATCH:\s*(\d+(?:\.\d+)?)/10")
        accessibility = extract_score(r"ACCESSIBILITY:\s*(\d+(?:\.\d+)?)/10")
        overall = extract_score(r"OVERALL:\s*(\d+(?:\.\d+)?)/10")

        feedback_match = re.search(r"FEEDBACK:\s*(.+?)(?:\n|$)", response, re.DOTALL)
        feedback = feedback_match.group(1).strip() if feedback_match else ""

        return EvalResult(
            authenticity=authenticity,
            insight_depth=insight_depth,
            clarity=clarity,
            voice_match=voice_match,
            accessibility=accessibility,
            overall=overall,
            feedback=feedback
        )
