"""Standalone engagement predictor for backtesting."""

import re
import anthropic
from pathlib import Path
from dataclasses import dataclass


@dataclass
class EngagementPrediction:
    tweet_id: str
    tweet_text: str
    predicted_score: float
    hook_strength: float
    specificity: float
    emotional_resonance: float
    novelty: float
    actionability: float
    raw_response: str


class EngagementPredictor:
    """Predicts engagement potential for arbitrary tweets without source context."""

    PROMPTS_DIR = Path(__file__).parent / "prompts"

    CRITERIA = [
        "HOOK_STRENGTH",
        "SPECIFICITY",
        "EMOTIONAL_RESONANCE",
        "NOVELTY",
        "ACTIONABILITY",
        "PREDICTED_ENGAGEMENT",
    ]

    def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250514"):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=300.0)
        self.model = model

    def _load_prompt(self, version: str = "v1") -> str:
        prompt_file = self.PROMPTS_DIR / f"predict_engagement_{version}.txt"
        return prompt_file.read_text()

    def predict_batch(
        self,
        tweets: list[dict],
        account_context: str = "",
        prompt_version: str = "v1",
    ) -> list[EngagementPrediction]:
        """Score a batch of tweets for predicted engagement.

        Args:
            tweets: List of {"id": str, "text": str} dicts.
            account_context: e.g. "Account: @user, 5K followers. Bio: ..."
            prompt_version: Which prompt template to use (v1, v2, etc.)

        Returns:
            List of EngagementPrediction, one per input tweet.
        """
        template = self._load_prompt(prompt_version)

        tweets_text = "\n\n".join(
            f"TWEET_{i + 1} (id={t['id']}):\n{t['text']}"
            for i, t in enumerate(tweets)
        )

        filled = template.format(
            tweets=tweets_text,
            account_context=account_context,
            num_tweets=len(tweets),
            first_tweet_id=tweets[0]["id"] if tweets else "...",
        )

        response = self.client.messages.create(
            model=self.model,
            max_tokens=200 * len(tweets),
            messages=[{"role": "user", "content": filled}],
        )

        return self._parse_batch_response(response.content[0].text, tweets)

    def _parse_batch_response(
        self, response: str, tweets: list[dict]
    ) -> list[EngagementPrediction]:
        """Parse multi-tweet scoring response into individual predictions."""
        predictions = []

        # Split response into per-tweet blocks
        blocks = self._split_into_blocks(response, len(tweets))

        for i, tweet in enumerate(tweets):
            block = blocks[i] if i < len(blocks) else response
            predictions.append(
                EngagementPrediction(
                    tweet_id=tweet["id"],
                    tweet_text=tweet["text"],
                    predicted_score=self._extract_score(block, "PREDICTED_ENGAGEMENT"),
                    hook_strength=self._extract_score(block, "HOOK_STRENGTH"),
                    specificity=self._extract_score(block, "SPECIFICITY"),
                    emotional_resonance=self._extract_score(block, "EMOTIONAL_RESONANCE"),
                    novelty=self._extract_score(block, "NOVELTY"),
                    actionability=self._extract_score(block, "ACTIONABILITY"),
                    raw_response=block,
                )
            )
        return predictions

    @staticmethod
    def _split_into_blocks(response: str, num_tweets: int) -> list[str]:
        """Split response text into per-tweet blocks using TWEET_N markers."""
        pattern = r"TWEET_\d+\s*\(id=[^)]*\)\s*:"
        splits = list(re.finditer(pattern, response))

        if not splits:
            return [response]

        blocks = []
        for i, match in enumerate(splits):
            start = match.start()
            end = splits[i + 1].start() if i + 1 < len(splits) else len(response)
            blocks.append(response[start:end].strip())

        return blocks

    @staticmethod
    def _extract_score(text: str, criterion: str) -> float:
        """Extract a numeric score for a criterion from response text."""
        pattern = rf"{criterion}:\s*(\d+(?:\.\d+)?)"
        match = re.search(pattern, text)
        return float(match.group(1)) if match else 5.0
