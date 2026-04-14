"""Analyze content patterns that drive engagement vs. zero engagement."""

import json
import logging
import re
import anthropic
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class PatternAnalysis:
    """Structured output of pattern analysis."""
    positive_patterns: list[str]
    negative_patterns: list[str]
    key_differences: list[str]
    actionable_rules: list[str]
    analyzed_at: str
    raw_response: str
    confidence: str = "low"  # "low" (<10 resonated), "medium" (10-25), "high" (>25)


class PatternAnalyzer:
    """Analyzes resonated vs low_resonance posts to extract engagement patterns."""

    MIN_RESONATED = 3

    def __init__(
        self,
        api_key: str,
        model: str = "claude-opus-4-20250514",
        timeout: float = 300.0,
    ):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model

    def analyze(
        self,
        resonated: list[dict],
        low_resonance: list[dict],
    ) -> Optional[PatternAnalysis]:
        """Analyze patterns across classified posts.

        Returns None if insufficient data (< MIN_RESONATED resonated posts).
        """
        if len(resonated) < self.MIN_RESONATED:
            logger.info(
                f"Not enough resonated posts for analysis "
                f"(have {len(resonated)}, need {self.MIN_RESONATED})"
            )
            return None

        prompt = self._build_prompt(resonated, low_resonance)

        response = self.client.messages.create(
            model=self.model,
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}],
        )

        analysis = self._parse_response(response.content[0].text)

        # Set confidence based on sample size
        n = len(resonated)
        if n >= 25:
            analysis.confidence = "high"
        elif n >= 10:
            analysis.confidence = "medium"
        else:
            analysis.confidence = "low"

        return analysis

    def _build_prompt(
        self, resonated: list[dict], low_resonance: list[dict]
    ) -> str:
        """Build analysis prompt with all classified posts."""
        res_text = "\n\n".join(
            f"[Score: {p.get('engagement_score', '?')}] {p['content'][:300]}"
            for p in resonated
        )
        low_text = "\n\n".join(
            f"{p['content'][:300]}"
            for p in low_resonance[:20]  # Cap to avoid prompt bloat
        )

        if len(resonated) < 10:
            rule_phrasing = (
                'Use "Consider..." or "Try..." phrasing since the sample size '
                f'is small ({len(resonated)} posts). Avoid "Always..." or "Never...".'
            )
        else:
            rule_phrasing = (
                'Use imperative phrasing ("Always open with...", "Never state..."). '
                'E.g., "Always open with a specific moment of surprise or confusion, '
                'not a general statement"'
            )

        return f"""You are analyzing X (Twitter) posts from a tech founder's account to understand what drives engagement.

POSTS THAT GOT ENGAGEMENT (likes, replies, retweets):
{res_text}

POSTS THAT GOT ZERO ENGAGEMENT (audience scrolled past):
{low_text}

Analyze what structurally and linguistically differentiates the engaging posts from the zero-engagement posts.

Return your analysis as JSON wrapped in <json></json> tags with exactly these fields:

- "positive_patterns": 3-5 patterns seen in engaging posts (be specific about structure, not just "good writing")
- "negative_patterns": 3-5 patterns seen in zero-engagement posts
- "key_differences": 3-5 direct contrasts between the two groups
- "actionable_rules": 3-5 concrete rules a content generator should follow to maximize engagement. {rule_phrasing}

Focus on structural patterns (opening style, sentence structure, use of specifics, narrative arc) rather than topic choice. Be concrete and actionable.

<json>
{{your JSON here}}
</json>"""

    def _parse_response(self, response: str) -> PatternAnalysis:
        """Parse structured JSON from Claude's response."""
        # Extract JSON from <json> tags
        match = re.search(r"<json>\s*(.*?)\s*</json>", response, re.DOTALL)
        if match:
            json_text = match.group(1)
        else:
            # Fallback: try to find JSON object directly
            json_match = re.search(r"\{.*\}", response, re.DOTALL)
            json_text = json_match.group(0) if json_match else "{}"

        try:
            data = json.loads(json_text)
        except json.JSONDecodeError:
            logger.warning("Failed to parse pattern analysis JSON, using defaults")
            data = {}

        return PatternAnalysis(
            positive_patterns=data.get("positive_patterns", []),
            negative_patterns=data.get("negative_patterns", []),
            key_differences=data.get("key_differences", []),
            actionable_rules=data.get("actionable_rules", []),
            analyzed_at=datetime.now(timezone.utc).isoformat(),
            raw_response=response,
        )
