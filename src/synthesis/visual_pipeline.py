"""Visual post pipeline — generates text + image for X posts with media."""

import logging
import re
from dataclasses import dataclass
from typing import Optional

import anthropic

from synthesis.pipeline import SynthesisPipeline, PipelineResult
from synthesis.image_generator import ImageGenerator, GeneratedImage

logger = logging.getLogger(__name__)

# Prompt for Claude to decide image type and content
IMAGE_PROMPT_TEMPLATE = """\
Given this tweet text and source material, describe a visual that makes the post feel immediate, concrete, and a little sharper than the text alone.

Tweet text: "{tweet_text}"

Source commits:
{commits}

Choose ONE image type and provide the content:

1. ANNOTATED — A key phrase or insight displayed as styled text on a card background.
   Respond: ANNOTATED | title | body

2. COMPARISON — A before/after or contrast showing transformation.
   Respond: COMPARISON | title | before_text | after_text

3. METRIC — A single key number or statistic highlighted prominently.
   Respond: METRIC | label | value | context

4. MEME — Top/bottom caption format for a relatable developer moment.
   Respond: MEME | top_text | bottom_text

Rules:
- Choose the type that best visualizes the tweet's core insight
- Keep text concise — this will be rendered as an image
- Prefer contrast, receipts, or a concrete artifact over generic inspiration
- If the tweet has tension between human effort and system output, prefer COMPARISON
- If the tweet has a deadpan or relatable failure mode, make the image carry that joke visually
- If the tweet reads like a punchline or complaint, prefer MEME
- Title/label: 3-6 words max
- Body/before/after/top/bottom: short fragments, not full prose
- Value: a single number, percentage, or short measurement
- Avoid bland labels like "Key Insight" or "Improvement"

Respond with ONLY one line in the format above.
"""

LEADING_FILLER_RE = re.compile(
    r'^(developer|engineer|me|manual testing|ai agent|assistant|bot|system|before|after)\s*:\s*',
    re.IGNORECASE,
)


@dataclass
class VisualPipelineResult:
    """Combined text + image pipeline result."""
    pipeline_result: PipelineResult
    image: GeneratedImage
    image_prompt: str  # the prompt used to decide image content
    image_alt_text: str = ""


class VisualPipeline:
    """Orchestrates text generation + image generation for visual posts."""

    def __init__(
        self,
        synthesis_pipeline: SynthesisPipeline,
        image_generator: ImageGenerator,
        api_key: str,
        model: str,
        timeout: float = 300.0,
    ):
        self.synthesis_pipeline = synthesis_pipeline
        self.image_generator = image_generator
        self.client = anthropic.Anthropic(api_key=api_key, timeout=timeout)
        self.model = model

    @staticmethod
    def _tighten_text(text: str, max_words: int = 8, max_chars: int = 48) -> str:
        """Compress verbose model output into short renderable fragments."""
        text = LEADING_FILLER_RE.sub("", text.strip().strip('"'))
        text = re.sub(r"\s+", " ", text)
        words = text.split()
        if len(words) > max_words:
            text = " ".join(words[:max_words])
        if len(text) > max_chars:
            text = text[: max_chars - 1].rstrip(" ,;:.") + "…"
        return text

    def _build_forced_meme_spec(self, tweet_text: str) -> str:
        """Force meme rendering when the selected text format is meme commentary."""
        top = self._tighten_text(tweet_text, max_words=7, max_chars=42)
        bottom = "the agents already shipped it"
        return f"MEME | {top} | {bottom}"

    def _normalize_meme_caption(self, text: str) -> str:
        """Collapse meme captions into a single short line."""
        parts = [p.strip() for p in re.split(r"\n+", text) if p.strip()]
        if not parts:
            return ""

        first = parts[0]
        if len(parts) == 1:
            return self._tighten_text(first, max_words=16, max_chars=110)

        second = parts[1].strip().strip('"')
        if second:
            combined = f"{first} {second}"
        else:
            combined = first
        return self._tighten_text(combined, max_words=18, max_chars=110)

    def run(
        self,
        prompts: list[str],
        commits: list[dict],
        threshold: float = 0.7,
    ) -> Optional[VisualPipelineResult]:
        """Run the visual post pipeline.

        1. Generate tweet text via standard pipeline (content_type="x_visual")
        2. Use Claude to decide image type and content
        3. Generate image via ImageGenerator
        """
        # Stage 1: Generate text
        pipeline_result = self.synthesis_pipeline.run(
            prompts=prompts,
            commits=commits,
            content_type="x_visual",
            threshold=threshold,
        )

        if not pipeline_result.candidates:
            logger.warning("No candidates generated for visual post")
            return VisualPipelineResult(
                pipeline_result=pipeline_result,
                image=GeneratedImage(path="", prompt_used="", provider="none", style="none", alt_text=""),
                image_prompt="",
                image_alt_text="",
            )

        if pipeline_result.content_format == "meme_commentary":
            pipeline_result.final_content = self._normalize_meme_caption(
                pipeline_result.final_content
            )

        # Stage 2: Decide image content
        if pipeline_result.content_format == "meme_commentary":
            image_spec = self._build_forced_meme_spec(pipeline_result.final_content)
        else:
            commits_text = "\n".join(
                f"- [{c.get('repo_name', '')}] {c.get('message', '')}"
                for c in commits[:5]
            )
            image_prompt = IMAGE_PROMPT_TEMPLATE.format(
                tweet_text=pipeline_result.final_content,
                commits=commits_text,
            )

            try:
                response = self.client.messages.create(
                    model=self.model,
                    max_tokens=200,
                    messages=[{"role": "user", "content": image_prompt}],
                )
                image_spec = response.content[0].text.strip()
            except Exception as e:
                logger.warning(f"Image prompt generation failed: {e}")
                # Fallback: use tweet text as annotated image
                image_spec = f"ANNOTATED | Insight | {pipeline_result.final_content[:100]}"

        # Stage 3: Parse image spec and generate
        image = self._generate_from_spec(image_spec)

        return VisualPipelineResult(
            pipeline_result=pipeline_result,
            image=image,
            image_prompt=image_spec,
            image_alt_text=image.alt_text,
        )

    def _generate_from_spec(self, spec: str) -> GeneratedImage:
        """Parse Claude's image spec and generate the image."""
        parts = [p.strip() for p in spec.split("|")]
        image_type = parts[0].upper() if parts else "ANNOTATED"

        try:
            if image_type == "COMPARISON" and len(parts) >= 4:
                return self.image_generator.generate(
                    style="comparison",
                    title=self._tighten_text(parts[1], max_words=4, max_chars=28),
                    before=self._tighten_text(parts[2], max_words=7, max_chars=42),
                    after=self._tighten_text(parts[3], max_words=9, max_chars=54),
                )
            elif image_type == "METRIC" and len(parts) >= 3:
                return self.image_generator.generate(
                    style="metric",
                    metric=self._tighten_text(parts[1], max_words=4, max_chars=24),
                    value=self._tighten_text(parts[2], max_words=4, max_chars=18),
                    context=self._tighten_text(parts[3], max_words=8, max_chars=48) if len(parts) > 3 else "",
                )
            elif image_type == "MEME" and len(parts) >= 3:
                return self.image_generator.generate(
                    style="meme",
                    top_text=self._tighten_text(parts[1], max_words=6, max_chars=36),
                    bottom_text=self._tighten_text(parts[2], max_words=8, max_chars=44),
                )
            else:
                # Default to annotated
                title = self._tighten_text(parts[1], max_words=5, max_chars=32) if len(parts) > 1 else "Insight"
                body = self._tighten_text(parts[2], max_words=12, max_chars=96) if len(parts) > 2 else ""
                return self.image_generator.generate(
                    style="annotated",
                    title=title,
                    body=body,
                )
        except Exception as e:
            logger.warning(f"Image generation failed, using fallback: {e}")
            return self.image_generator.generate(
                style="annotated",
                title="Insight",
                body=spec[:100],
            )
