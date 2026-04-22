"""Image generation for visual posts.

Uses a strategy pattern: PillowImageProvider is the default (zero cost, instant).
Future providers (GPT Image, Ideogram) can be added by implementing the same interface.
"""

import logging
import re
from dataclasses import dataclass
from typing import Optional, Protocol

logger = logging.getLogger(__name__)


@dataclass
class GeneratedImage:
    """Result of image generation."""
    path: str           # local file path to generated image
    prompt_used: str    # the prompt/description that generated it
    provider: str       # 'pillow', 'gpt_image', etc.
    style: str          # 'annotated', 'comparison', 'metric'
    alt_text: str = ""  # human-readable image description for publishing


class ImageProvider(Protocol):
    """Protocol for image generation providers."""
    def generate(
        self,
        style: str,
        title: str,
        body: str,
        before: str,
        after: str,
        metric: str,
        value: str,
        context: str,
        top_text: str,
        bottom_text: str,
        palette: str,
        output_dir: Optional[str],
    ) -> str:
        """Generate an image and return the file path."""
        ...


def _clean_alt_fragment(text: str) -> str:
    """Normalize rendered text fragments for alt text."""
    text = re.sub(r"\s+", " ", (text or "").strip().strip('"'))
    text = re.sub(r"^(ANNOTATED|COMPARISON|METRIC|MEME)\s*\|\s*", "", text, flags=re.IGNORECASE)
    return text.strip(" |")


def _limit_alt_text(text: str, max_chars: int = 300) -> str:
    """Keep alt text concise and below common platform limits."""
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip(" ,;:.") + "..."


def build_alt_text(
    style: str,
    title: str = "",
    body: str = "",
    before: str = "",
    after: str = "",
    metric: str = "",
    value: str = "",
    context: str = "",
    top_text: str = "",
    bottom_text: str = "",
) -> str:
    """Build human-readable alt text from rendered template content."""
    style = (style or "annotated").lower()
    title = _clean_alt_fragment(title)
    body = _clean_alt_fragment(body)
    before = _clean_alt_fragment(before)
    after = _clean_alt_fragment(after)
    metric = _clean_alt_fragment(metric)
    value = _clean_alt_fragment(value)
    context = _clean_alt_fragment(context)
    top_text = _clean_alt_fragment(top_text)
    bottom_text = _clean_alt_fragment(bottom_text)

    if style == "comparison":
        parts = [f'Comparison graphic titled "{title}".' if title else "Comparison graphic."]
        if before:
            parts.append(f"Before: {before}.")
        if after:
            parts.append(f"After: {after}.")
        return _limit_alt_text(" ".join(parts))
    if style == "metric":
        label = metric or "Metric"
        text = f"Metric graphic showing {label}: {value}." if value else f"Metric graphic showing {label}."
        if context:
            text = f"{text} {context}."
        return _limit_alt_text(text)
    if style == "meme":
        parts = ["Meme-style graphic."]
        if top_text:
            parts.append(f'Top text: "{top_text}".')
        if bottom_text:
            parts.append(f'Bottom text: "{bottom_text}".')
        return _limit_alt_text(" ".join(parts))

    if title and body:
        return _limit_alt_text(f'Text graphic headed "{title}" with body text: {body}.')
    if title:
        return _limit_alt_text(f'Text graphic headed "{title}".')
    if body:
        return _limit_alt_text(f"Text graphic: {body}.")
    return "Generated text graphic for the post."


class PillowImageProvider:
    """Template-based image generation using Pillow."""

    def __init__(self, output_dir: Optional[str] = None):
        self.output_dir = output_dir

    def generate(
        self,
        style: str = "annotated",
        title: str = "",
        body: str = "",
        before: str = "",
        after: str = "",
        metric: str = "",
        value: str = "",
        context: str = "",
        top_text: str = "",
        bottom_text: str = "",
        palette: str = "dark",
        output_dir: Optional[str] = None,
    ) -> str:
        """Generate an image using Pillow templates.

        Args:
            style: Image style — 'annotated', 'comparison', 'metric', or 'meme'
            title: Title text (for annotated/comparison)
            body: Body text (for annotated)
            before/after: Text for comparison panels
            metric/value/context: For metric highlight
            palette: Color palette — 'dark' or 'light'

        Returns:
            Path to the generated PNG file.
        """
        from synthesis.image_templates import (
            render_annotated_text,
            render_comparison,
            render_metric_highlight,
            render_meme_text,
        )

        if style == "comparison":
            return render_comparison(
                before=before,
                after=after,
                title=title,
                style=palette,
                output_dir=output_dir or self.output_dir,
            )
        elif style == "metric":
            return render_metric_highlight(
                metric=metric,
                value=value,
                context=context,
                style=palette,
                output_dir=output_dir or self.output_dir,
            )
        elif style == "meme":
            return render_meme_text(
                top_text=top_text,
                bottom_text=bottom_text,
                style=palette,
                output_dir=output_dir or self.output_dir,
            )
        else:
            return render_annotated_text(
                title=title,
                body=body,
                style=palette,
                output_dir=output_dir or self.output_dir,
            )


class ImageGenerator:
    """High-level image generator with provider abstraction."""

    def __init__(self, provider: str = "pillow", output_dir: Optional[str] = None):
        if provider == "pillow":
            self._provider = PillowImageProvider(output_dir=output_dir)
        else:
            raise ValueError(f"Unknown image provider: {provider}")
        self._provider_name = provider
        self._output_dir = output_dir

    def generate(
        self,
        style: str = "annotated",
        title: str = "",
        body: str = "",
        before: str = "",
        after: str = "",
        metric: str = "",
        value: str = "",
        context: str = "",
        top_text: str = "",
        bottom_text: str = "",
        palette: str = "dark",
        output_dir: Optional[str] = None,
    ) -> GeneratedImage:
        """Generate an image.

        Args:
            style: 'annotated', 'comparison', 'metric', or 'meme'
            title, body: For annotated style
            before, after: For comparison style
            metric, value, context: For metric style
            top_text, bottom_text: For meme style
            palette: 'dark' or 'light'

        Returns:
            GeneratedImage with path and metadata.
        """
        prompt = f"{style}: {title or metric or 'untitled'}"
        path = self._provider.generate(
            style=style,
            title=title,
            body=body,
            before=before,
            after=after,
            metric=metric,
            value=value,
            context=context,
            top_text=top_text,
            bottom_text=bottom_text,
            palette=palette,
            output_dir=output_dir or self._output_dir,
        )
        alt_text = build_alt_text(
            style=style,
            title=title,
            body=body,
            before=before,
            after=after,
            metric=metric,
            value=value,
            context=context,
            top_text=top_text,
            bottom_text=bottom_text,
        )
        return GeneratedImage(
            path=path,
            prompt_used=prompt,
            provider=self._provider_name,
            style=style,
            alt_text=alt_text,
        )
