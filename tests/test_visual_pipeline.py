"""Tests for the visual post pipeline."""

import os
from unittest.mock import MagicMock, patch

import pytest

from synthesis.visual_pipeline import VisualPipeline, VisualPipelineResult
from synthesis.image_generator import ImageGenerator, GeneratedImage
from synthesis.pipeline import SynthesisPipeline, PipelineResult
from synthesis.evaluator_v2 import ComparisonResult
from synthesis.generator import ContentGenerator


# --- Helpers ---


def _make_pipeline_result(content="Check out this insight", score=8.0):
    return PipelineResult(
        batch_id="test123",
        candidates=[content],
        comparison=ComparisonResult(
            ranking=[0],
            best_score=score,
            groundedness=8.0,
            rawness=7.0,
            narrative_specificity=7.0,
            voice=7.0,
            engagement_potential=7.0,
            best_feedback="Good visual post",
            improvement="",
            reject_reason=None,
            raw_response="",
        ),
        refinement=None,
        final_content=content,
        final_score=score,
        source_prompts=["prompt"],
        source_commits=["commit"],
        content_format="annotated_insight",
    )


def _make_empty_pipeline_result():
    return PipelineResult(
        batch_id="test123",
        candidates=[],
        comparison=ComparisonResult(
            ranking=[],
            best_score=0,
            groundedness=0,
            rawness=0,
            narrative_specificity=0,
            voice=0,
            engagement_potential=0,
            best_feedback="",
            improvement="",
            reject_reason="All filtered",
            raw_response="",
        ),
        refinement=None,
        final_content="",
        final_score=0,
        source_prompts=["prompt"],
        source_commits=["commit"],
    )


SAMPLE_PROMPTS = ["Worked on caching layer"]
SAMPLE_COMMITS = [{"sha": "abc", "repo_name": "proj", "message": "add cache"}]


class TestVisualPipelineContentType:
    def test_x_visual_in_content_type_config(self):
        assert "x_visual" in ContentGenerator.CONTENT_TYPE_CONFIG
        cfg = ContentGenerator.CONTENT_TYPE_CONFIG["x_visual"]
        assert cfg["template"] == "x_visual_v2"
        assert cfg["max_tokens"] == 150

    def test_char_limit_200(self):
        assert SynthesisPipeline.CHAR_LIMITS.get("x_visual") == 200

    def test_visual_post_formats_exist(self):
        assert len(SynthesisPipeline.VISUAL_POST_FORMATS) == 4
        names = [n for n, _ in SynthesisPipeline.VISUAL_POST_FORMATS]
        assert "annotated_insight" in names
        assert "data_callout" in names
        assert "meme_commentary" in names
        assert "trend_linked" in names


class TestVisualPipelineRun:
    def test_generates_text_and_image(self):
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = _make_pipeline_result()

        image_gen = ImageGenerator(provider="pillow")

        # Mock Claude response for image spec
        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(text="ANNOTATED | Key Insight | Testing is faster than debugging")]
        mock_client.messages.create.return_value = mock_response

        vp = VisualPipeline(
            synthesis_pipeline=mock_pipeline,
            image_generator=image_gen,
            api_key="test",
            model="test-model",
        )
        vp.client = mock_client

        result = vp.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        assert isinstance(result, VisualPipelineResult)
        assert result.pipeline_result.final_content == "Check out this insight"
        assert os.path.exists(result.image.path)
        assert result.image.style == "annotated"

    def test_comparison_image_type(self):
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = _make_pipeline_result()

        image_gen = ImageGenerator(provider="pillow")

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(
            text="COMPARISON | Auth refactor | 5 functions, 200 lines | 1 function, 40 lines"
        )]
        mock_client.messages.create.return_value = mock_response

        vp = VisualPipeline(
            synthesis_pipeline=mock_pipeline,
            image_generator=image_gen,
            api_key="test",
            model="test-model",
        )
        vp.client = mock_client

        result = vp.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)
        assert result.image.style == "comparison"
        assert os.path.exists(result.image.path)

    def test_metric_image_type(self):
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = _make_pipeline_result()

        image_gen = ImageGenerator(provider="pillow")

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(
            text="METRIC | Build time | 3.2s | Down from 45s after caching"
        )]
        mock_client.messages.create.return_value = mock_response

        vp = VisualPipeline(
            synthesis_pipeline=mock_pipeline,
            image_generator=image_gen,
            api_key="test",
            model="test-model",
        )
        vp.client = mock_client

        result = vp.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)
        assert result.image.style == "metric"
        assert os.path.exists(result.image.path)

    def test_meme_image_type(self):
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = _make_pipeline_result(content="This build is checking my attitude")

        image_gen = ImageGenerator(provider="pillow")

        mock_client = MagicMock()
        mock_response = MagicMock()
        mock_response.content = [MagicMock(
            text="MEME | me trusting default configs | prod teaching me character development"
        )]
        mock_client.messages.create.return_value = mock_response

        vp = VisualPipeline(
            synthesis_pipeline=mock_pipeline,
            image_generator=image_gen,
            api_key="test",
            model="test-model",
        )
        vp.client = mock_client

        result = vp.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)
        assert result.image.style == "meme"
        assert os.path.exists(result.image.path)

    def test_meme_commentary_forces_meme_without_classifier_call(self):
        mock_pipeline = MagicMock()
        pr = _make_pipeline_result(content="I keep typing slash commands like they help", score=8.0)
        pr.content_format = "meme_commentary"
        mock_pipeline.run.return_value = pr

        image_gen = ImageGenerator(provider="pillow")
        mock_client = MagicMock()

        vp = VisualPipeline(
            synthesis_pipeline=mock_pipeline,
            image_generator=image_gen,
            api_key="test",
            model="test-model",
        )
        vp.client = mock_client

        result = vp.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)
        assert result.image.style == "meme"
        assert result.image_prompt.startswith("MEME |")
        mock_client.messages.create.assert_not_called()
        assert result.pipeline_result.final_content == "I keep typing slash commands like they help"

    def test_meme_commentary_collapses_multiline_caption(self):
        mock_pipeline = MagicMock()
        pr = _make_pipeline_result(
            content="I keep typing `/usage` like the API is going to tell me something I want to hear.\n\nSpoiler: it never does.",
            score=8.0,
        )
        pr.content_format = "meme_commentary"
        mock_pipeline.run.return_value = pr

        image_gen = ImageGenerator(provider="pillow")
        vp = VisualPipeline(
            synthesis_pipeline=mock_pipeline,
            image_generator=image_gen,
            api_key="test",
            model="test-model",
        )
        vp.client = MagicMock()

        result = vp.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)
        assert "\n" not in result.pipeline_result.final_content
        assert result.pipeline_result.final_content == (
            "I keep typing `/usage` like the API is going to tell me something I want to hear. Spoiler:"
        )

    def test_no_candidates_returns_result_with_empty_image(self):
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = _make_empty_pipeline_result()

        image_gen = ImageGenerator(provider="pillow")

        vp = VisualPipeline(
            synthesis_pipeline=mock_pipeline,
            image_generator=image_gen,
            api_key="test",
            model="test-model",
        )

        result = vp.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)
        assert result.pipeline_result.final_score == 0
        assert result.image.path == ""

    def test_image_prompt_failure_falls_back_to_annotated(self):
        mock_pipeline = MagicMock()
        mock_pipeline.run.return_value = _make_pipeline_result()

        image_gen = ImageGenerator(provider="pillow")

        mock_client = MagicMock()
        mock_client.messages.create.side_effect = Exception("API error")

        vp = VisualPipeline(
            synthesis_pipeline=mock_pipeline,
            image_generator=image_gen,
            api_key="test",
            model="test-model",
        )
        vp.client = mock_client

        result = vp.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)
        # Should fallback to annotated with tweet text
        assert result.image.style == "annotated"
        assert os.path.exists(result.image.path)


class TestGenerateFromSpec:
    def _make_vp(self):
        mock_pipeline = MagicMock()
        image_gen = ImageGenerator(provider="pillow")
        return VisualPipeline(
            synthesis_pipeline=mock_pipeline,
            image_generator=image_gen,
            api_key="test",
            model="test-model",
        )

    def test_annotated_spec(self):
        vp = self._make_vp()
        result = vp._generate_from_spec("ANNOTATED | Title Here | Body text goes here")
        assert result.style == "annotated"
        assert os.path.exists(result.path)

    def test_comparison_spec(self):
        vp = self._make_vp()
        result = vp._generate_from_spec("COMPARISON | Refactor | Old way | New way")
        assert result.style == "comparison"

    def test_comparison_spec_tightens_verbose_labels(self):
        vp = self._make_vp()
        with patch.object(vp.image_generator, "generate", wraps=vp.image_generator.generate) as mock_generate:
            result = vp._generate_from_spec(
                'COMPARISON | Validation Plans | Developer: manually testing same happy path for 12th time | '
                'AI Agent: wrote 53 comprehensive test cases that all pass with detailed coverage'
            )
        assert result.style == "comparison"
        kwargs = mock_generate.call_args.kwargs
        assert kwargs["before"] == "manually testing same happy path for 12th"
        assert kwargs["after"] == "wrote 53 comprehensive test cases that all pass with"

    def test_metric_spec(self):
        vp = self._make_vp()
        result = vp._generate_from_spec("METRIC | Latency | 3ms | After optimization")
        assert result.style == "metric"

    def test_meme_spec(self):
        vp = self._make_vp()
        result = vp._generate_from_spec("MEME | me trusting default configs | prod teaching me character development")
        assert result.style == "meme"

    def test_malformed_spec_defaults_to_annotated(self):
        vp = self._make_vp()
        result = vp._generate_from_spec("some random text")
        assert result.style == "annotated"

    def test_unknown_type_defaults_to_annotated(self):
        vp = self._make_vp()
        result = vp._generate_from_spec("CHART | data | values")
        assert result.style == "annotated"


class TestPromptTemplateExists:
    def test_visual_template_exists(self):
        from pathlib import Path
        template = Path(__file__).parent.parent / "src" / "synthesis" / "prompts" / "x_visual_v2.txt"
        assert template.exists()

    def test_visual_template_has_placeholders(self):
        from pathlib import Path
        template_path = Path(__file__).parent.parent / "src" / "synthesis" / "prompts" / "x_visual_v2.txt"
        content = template_path.read_text()
        assert "{few_shot_section}" in content
        assert "{format_directive}" in content
        assert "{commits}" in content
        assert "{prompts}" in content
