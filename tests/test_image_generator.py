"""Tests for image generation module."""

import os
import tempfile

import pytest
from PIL import Image

from synthesis.image_templates import (
    render_annotated_text,
    render_comparison,
    render_metric_highlight,
    render_meme_text,
    WIDTH,
    HEIGHT,
)
from synthesis.image_generator import ImageGenerator, GeneratedImage, PillowImageProvider


class TestRenderAnnotatedText:
    def test_generates_png_file(self, tmp_path):
        path = render_annotated_text(
            title="Testing insight",
            body="When you write tests first, you discover design problems earlier.",
            output_path=str(tmp_path / "test.png"),
        )
        assert os.path.exists(path)
        assert path.endswith(".png")

    def test_correct_dimensions(self, tmp_path):
        path = render_annotated_text(
            title="Title",
            body="Body text here.",
            output_path=str(tmp_path / "test.png"),
        )
        img = Image.open(path)
        assert img.size == (WIDTH, HEIGHT)

    def test_light_palette(self, tmp_path):
        path = render_annotated_text(
            title="Light mode",
            body="Testing light palette.",
            style="light",
            output_path=str(tmp_path / "light.png"),
        )
        assert os.path.exists(path)

    def test_long_text_wraps(self, tmp_path):
        path = render_annotated_text(
            title="A very long title that should wrap across multiple lines in the image",
            body="A very long body that goes on and on. " * 10,
            output_path=str(tmp_path / "long.png"),
        )
        assert os.path.exists(path)
        img = Image.open(path)
        assert img.size == (WIDTH, HEIGHT)


class TestRenderComparison:
    def test_generates_png_file(self, tmp_path):
        path = render_comparison(
            before="Manual testing after every change",
            after="Automated test suite runs in 3 seconds",
            output_path=str(tmp_path / "comp.png"),
        )
        assert os.path.exists(path)

    def test_correct_dimensions(self, tmp_path):
        path = render_comparison(
            before="Before",
            after="After",
            output_path=str(tmp_path / "comp.png"),
        )
        img = Image.open(path)
        assert img.size == (WIDTH, HEIGHT)

    def test_with_title(self, tmp_path):
        path = render_comparison(
            before="Old way",
            after="New way",
            title="Authentication refactor",
            output_path=str(tmp_path / "comp_title.png"),
        )
        assert os.path.exists(path)


class TestRenderMetricHighlight:
    def test_generates_png_file(self, tmp_path):
        path = render_metric_highlight(
            metric="Test coverage",
            value="94%",
            context="Up from 62% after adding integration tests",
            output_path=str(tmp_path / "metric.png"),
        )
        assert os.path.exists(path)


class TestRenderMemeText:
    def test_generates_png_file(self, tmp_path):
        path = render_meme_text(
            top_text="me trusting the migration",
            bottom_text="prod proving otherwise",
            output_path=str(tmp_path / "meme.png"),
        )
        assert os.path.exists(path)

    def test_correct_dimensions(self, tmp_path):
        path = render_meme_text(
            top_text="ME",
            bottom_text="THE BUG",
            output_path=str(tmp_path / "meme.png"),
        )
        img = Image.open(path)
        assert img.size == (WIDTH, HEIGHT)

    def test_correct_dimensions(self, tmp_path):
        path = render_metric_highlight(
            metric="Latency",
            value="3ms",
            output_path=str(tmp_path / "metric.png"),
        )
        img = Image.open(path)
        assert img.size == (WIDTH, HEIGHT)

    def test_without_context(self, tmp_path):
        path = render_metric_highlight(
            metric="Build time",
            value="12s",
            output_path=str(tmp_path / "metric_no_ctx.png"),
        )
        assert os.path.exists(path)


class TestPillowImageProvider:
    def test_annotated_style(self, tmp_path):
        provider = PillowImageProvider(output_dir=str(tmp_path))
        path = provider.generate(
            style="annotated",
            title="Test",
            body="Body",
        )
        assert os.path.exists(path)

    def test_comparison_style(self, tmp_path):
        provider = PillowImageProvider(output_dir=str(tmp_path))
        path = provider.generate(
            style="comparison",
            before="Before",
            after="After",
        )
        assert os.path.exists(path)

    def test_metric_style(self, tmp_path):
        provider = PillowImageProvider(output_dir=str(tmp_path))
        path = provider.generate(
            style="metric",
            metric="Speed",
            value="10x",
        )
        assert os.path.exists(path)

    def test_meme_style(self, tmp_path):
        provider = PillowImageProvider(output_dir=str(tmp_path))
        path = provider.generate(
            style="meme",
            top_text="me trusting the hotfix",
            bottom_text="prod choosing violence",
        )
        assert os.path.exists(path)


class TestImageGenerator:
    def test_generate_returns_generated_image(self, tmp_path):
        gen = ImageGenerator(provider="pillow", output_dir=str(tmp_path))
        result = gen.generate(style="annotated", title="Test", body="Body")
        assert isinstance(result, GeneratedImage)
        assert os.path.exists(result.path)
        assert result.provider == "pillow"
        assert result.style == "annotated"

    def test_generate_comparison(self, tmp_path):
        gen = ImageGenerator(provider="pillow", output_dir=str(tmp_path))
        result = gen.generate(
            style="comparison",
            before="Old code",
            after="New code",
        )
        assert result.style == "comparison"
        assert os.path.exists(result.path)

    def test_generate_metric(self, tmp_path):
        gen = ImageGenerator(provider="pillow", output_dir=str(tmp_path))
        result = gen.generate(
            style="metric",
            metric="Requests/sec",
            value="1.2K",
            context="After async refactor",
        )
        assert result.style == "metric"
        assert os.path.exists(result.path)

    def test_generate_meme(self, tmp_path):
        gen = ImageGenerator(provider="pillow", output_dir=str(tmp_path))
        result = gen.generate(
            style="meme",
            top_text="me trusting default configs",
            bottom_text="the logs two minutes later",
        )
        assert result.style == "meme"
        assert os.path.exists(result.path)

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown image provider"):
            ImageGenerator(provider="nonexistent")

    def test_prompt_used_captured(self, tmp_path):
        gen = ImageGenerator(provider="pillow", output_dir=str(tmp_path))
        result = gen.generate(style="annotated", title="My Insight")
        assert "My Insight" in result.prompt_used

    def test_generate_uses_configured_output_dir(self, tmp_path):
        gen = ImageGenerator(provider="pillow", output_dir=str(tmp_path))
        result = gen.generate(style="annotated", title="Stored Here")
        assert os.path.commonpath([result.path, str(tmp_path)]) == str(tmp_path)
