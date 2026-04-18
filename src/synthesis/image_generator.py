"""Image generation for visual posts.

Uses a strategy pattern: PillowImageProvider is the default (zero cost, instant).
Future providers (GPT Image, Ideogram) can be added by implementing the same interface.
"""

import logging
from dataclasses import dataclass
from typing import Optional, Protocol

from synthesis.image_templates import (
    render_annotated_text,
    render_comparison,
    render_metric_highlight,
    render_meme_text,
)

logger = logging.getLogger(__name__)


@dataclass
class GeneratedImage:
    """Result of image generation."""
    path: str           # local file path to generated image
    prompt_used: str    # the prompt/description that generated it
    provider: str       # 'pillow', 'gpt_image', etc.
    style: str          # 'annotated', 'comparison', 'metric'


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
        return GeneratedImage(
            path=path,
            prompt_used=prompt,
            provider=self._provider_name,
            style=style,
        )
