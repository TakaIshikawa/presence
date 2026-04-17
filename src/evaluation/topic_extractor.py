"""Topic extraction for content calendar and gap analysis.

Classifies content into predefined topics using lightweight LLM calls,
with batch processing support for efficiency.
"""

import json
import logging
from typing import Optional

import anthropic
from anthropic import Anthropic

logger = logging.getLogger(__name__)


class TopicExtractionError(Exception):
    """Base exception for topic extraction errors."""
    pass


class TopicExtractionAPIError(TopicExtractionError):
    """Raised when Anthropic API calls fail."""
    pass


class TopicExtractionParseError(TopicExtractionError):
    """Raised when response parsing fails."""
    pass

# Predefined topic taxonomy
TOPIC_TAXONOMY = [
    "architecture",
    "testing",
    "debugging",
    "ai-agents",
    "developer-tools",
    "performance",
    "data-modeling",
    "devops",
    "open-source",
    "product-thinking",
    "workflow",
    "other",
]


class TopicExtractor:
    """Extracts topics from content using lightweight LLM classification."""

    def __init__(self, api_key: str, model: str = "claude-haiku-4-5-20251001"):
        self.client = Anthropic(api_key=api_key)
        self.model = model

    def extract_topics(self, content: str) -> list[tuple[str, str, float]]:
        """Extract topics from a single piece of content.

        Args:
            content: The content text to classify

        Returns:
            List of (topic, subtopic, confidence) tuples. May return multiple
            topics if the content covers several areas.
        """
        prompt = self._build_extraction_prompt(content)

        try:
            response = self.client.messages.create(
                model=self.model,
                max_tokens=500,
                messages=[{"role": "user", "content": prompt}],
            )

            text = response.content[0].text.strip()
            return self._parse_extraction_response(text)

        except TopicExtractionError:
            # Re-raise our own exceptions
            raise
        except (anthropic.APIConnectionError, anthropic.APIStatusError) as e:
            # Anthropic API errors - connection failures, auth errors, rate limits, etc.
            error_name = type(e).__name__
            logger.error(f"Topic extraction API error: {error_name}: {e}")
            raise TopicExtractionAPIError(
                f"Anthropic API call failed: {error_name}: {e}"
            ) from e
        except (IndexError, AttributeError, TypeError, ValueError) as e:
            # Handle response structure/parsing errors - empty content, missing attributes, type issues, value errors
            error_name = type(e).__name__
            logger.error(f"Topic extraction failed: {error_name}: {e}")
            raise TopicExtractionAPIError(
                f"Topic extraction failed: {error_name}: {e}"
            ) from e

    def batch_extract(self, contents: list[str]) -> list[list[tuple[str, str, float]]]:
        """Extract topics for multiple contents efficiently.

        Args:
            contents: List of content texts to classify

        Returns:
            List of topic lists, one per input content
        """
        # For now, process serially. Could be optimized with batching API.
        results = []
        for i, content in enumerate(contents):
            logger.debug(f"Extracting topics for content {i+1}/{len(contents)}")
            results.append(self.extract_topics(content))
        return results

    def _build_extraction_prompt(self, content: str) -> str:
        """Build the topic extraction prompt."""
        taxonomy_list = ", ".join(TOPIC_TAXONOMY)

        return f"""Classify this content into one or more topics from this taxonomy:
{taxonomy_list}

Content:
{content}

Instructions:
- Choose 1-2 primary topics that best describe the content
- For each topic, provide a specific subtopic (free-form, 2-5 words)
- Assign a confidence score (0.0-1.0) for each topic
- Use "other" only if none of the predefined topics fit

Respond in this JSON format:
[
  {{"topic": "testing", "subtopic": "integration testing patterns", "confidence": 0.9}},
  {{"topic": "architecture", "subtopic": "state management", "confidence": 0.7}}
]

JSON response:"""

    def _parse_extraction_response(self, response_text: str) -> list[tuple[str, str, float]]:
        """Parse the JSON response from the LLM."""
        try:
            # Extract JSON from response (might have markdown code blocks)
            json_text = response_text
            if "```json" in response_text:
                json_text = response_text.split("```json")[1].split("```")[0].strip()
            elif "```" in response_text:
                json_text = response_text.split("```")[1].split("```")[0].strip()

            data = json.loads(json_text)

            if not isinstance(data, list):
                logger.warning(f"Expected list, got {type(data)}: {data}")
                return [("other", "", 0.5)]

            results = []
            for item in data:
                topic = item.get("topic", "other")
                subtopic = item.get("subtopic", "")
                confidence = float(item.get("confidence", 0.5))

                # Validate topic is in taxonomy
                if topic not in TOPIC_TAXONOMY:
                    logger.warning(f"Invalid topic '{topic}', using 'other'")
                    topic = "other"

                # Clamp confidence
                confidence = max(0.0, min(1.0, confidence))

                results.append((topic, subtopic, confidence))

            return results if results else [("other", "", 0.5)]

        except (json.JSONDecodeError, ValueError, KeyError) as e:
            logger.error(f"Failed to parse topic extraction response: {e}")
            logger.debug(f"Response text: {response_text}")
            return [("other", "", 0.5)]
