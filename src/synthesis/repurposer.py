"""Content repurposer for transforming high-performing posts into different formats."""

import logging

import anthropic
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

from storage.db import Database

logger = logging.getLogger(__name__)


@dataclass
class RepurposeCandidate:
    content_id: int
    original_content: str
    original_type: str  # 'x_post' or 'x_thread'
    engagement_score: float
    target_type: str  # what to repurpose into


@dataclass
class RepurposeResult:
    source_id: int
    target_type: str
    content: str
    generation_prompt: str  # for observability


class ContentRepurposer:
    """Transforms high-performing content into different formats."""

    PROMPTS_DIR = Path(__file__).parent / "prompts"

    def __init__(self, api_key: str, model: str, db: Database, timeout: float = 300.0):
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model
        self.db = db

    def find_candidates(
        self,
        min_engagement: float = 10.0,
        max_age_days: int = 14,
        already_repurposed: Optional[set[int]] = None,
    ) -> list[RepurposeCandidate]:
        """Find high-performing posts eligible for repurposing.

        Args:
            min_engagement: Minimum engagement score threshold
            max_age_days: Maximum age of posts to consider
            already_repurposed: Set of content IDs to exclude (optional)

        Returns:
            List of RepurposeCandidate objects ordered by engagement score descending
        """
        rows = self.db.get_repurpose_candidates(min_engagement, max_age_days)

        candidates = []
        for row in rows:
            content_id = row["id"]
            if already_repurposed and content_id in already_repurposed:
                continue

            # Determine target type based on source type
            source_type = row["content_type"]
            if source_type == "x_post":
                target_type = "x_thread"
            elif source_type == "x_thread":
                target_type = "blog_seed"
            else:
                continue  # Skip unknown types

            candidates.append(
                RepurposeCandidate(
                    content_id=content_id,
                    original_content=row["content"],
                    original_type=source_type,
                    engagement_score=row.get("engagement_score", 0.0),
                    target_type=target_type,
                )
            )

        return candidates

    def expand_post_to_thread(self, candidate: RepurposeCandidate) -> RepurposeResult:
        """Expand a single high-performing post into a detailed thread.

        Args:
            candidate: The post to expand

        Returns:
            RepurposeResult with generated thread content
        """
        prompt_path = self.PROMPTS_DIR / "repurpose_post_to_thread.txt"
        prompt_template = prompt_path.read_text()

        filled_prompt = prompt_template.format(original_content=candidate.original_content)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2000,
                messages=[{"role": "user", "content": filled_prompt}],
            )
        except anthropic.APIConnectionError as e:
            error_name = type(e).__name__
            logger.error(f"Failed to connect to Anthropic API: {error_name}: {e}")
            raise
        except anthropic.APIStatusError as e:
            error_name = type(e).__name__
            logger.error(f"Anthropic API status error: {error_name}: {e}")
            raise

        generated_content = response.content[0].text.strip()

        return RepurposeResult(
            source_id=candidate.content_id,
            target_type="x_thread",
            content=generated_content,
            generation_prompt=filled_prompt,
        )

    def expand_to_blog_seed(self, candidate: RepurposeCandidate) -> RepurposeResult:
        """Generate a blog post outline/draft from a resonant post or thread.

        Args:
            candidate: The post or thread to expand

        Returns:
            RepurposeResult with blog seed content
        """
        prompt_path = self.PROMPTS_DIR / "repurpose_to_blog_seed.txt"
        prompt_template = prompt_path.read_text()

        filled_prompt = prompt_template.format(original_content=candidate.original_content)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=2500,
                messages=[{"role": "user", "content": filled_prompt}],
            )
        except anthropic.APIConnectionError as e:
            error_name = type(e).__name__
            logger.error(f"Failed to connect to Anthropic API: {error_name}: {e}")
            raise
        except anthropic.APIStatusError as e:
            error_name = type(e).__name__
            logger.error(f"Anthropic API status error: {error_name}: {e}")
            raise

        generated_content = response.content[0].text.strip()

        return RepurposeResult(
            source_id=candidate.content_id,
            target_type="blog_seed",
            content=generated_content,
            generation_prompt=filled_prompt,
        )
