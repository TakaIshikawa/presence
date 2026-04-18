"""Tests for long-form post pipeline extension."""

from unittest.mock import MagicMock, patch

import pytest

from synthesis.pipeline import SynthesisPipeline
from synthesis.evaluator_v2 import ComparisonResult
from synthesis.generator import ContentGenerator, GeneratedContent
from synthesis.refiner import RefinementResult


# --- Helpers ---


def _make_comparison(best_score=8.0, ranking=None, improvement="Add depth"):
    return ComparisonResult(
        ranking=ranking or [0, 1, 2],
        best_score=best_score,
        groundedness=8.0,
        rawness=7.0,
        narrative_specificity=7.0,
        voice=7.0,
        engagement_potential=7.0,
        best_feedback="Strong essay",
        improvement=improvement,
        reject_reason=None,
        raw_response="",
    )


def _make_candidates(texts=None, content_type="x_long_post"):
    texts = texts or [
        "A deep dive into a decision. " * 10,  # ~300 chars
        "Retrospective on today's refactor. " * 10,
        "Framework for evaluating trade-offs. " * 10,
    ]
    return [
        GeneratedContent(
            content_type=content_type,
            content=t,
            source_prompts=["prompt"],
            source_commits=["commit"],
        )
        for t in texts
    ]


SAMPLE_PROMPTS = ["Refactored the authentication module"]
SAMPLE_COMMITS = [{"sha": "abc123", "repo_name": "my-project", "message": "refactor: simplify auth flow"}]


def _build_pipeline():
    """Build a pipeline with fully mocked internals."""
    db = MagicMock()
    db.get_recent_published_content.return_value = []
    db.get_recent_published_content_all.return_value = []
    db.get_curated_posts.return_value = []
    db.get_auto_classified_posts.return_value = []
    db.get_engagement_calibration_stats.return_value = None
    db.get_meta.return_value = None

    with patch("synthesis.pipeline.ContentRefiner") as MockRefiner, \
         patch("synthesis.pipeline.CrossModelEvaluator") as MockEval, \
         patch("synthesis.pipeline.ContentGenerator") as MockGen:

        pipeline = SynthesisPipeline(
            api_key="test-key",
            generator_model="claude-sonnet-4-20250514",
            evaluator_model="claude-opus-4-20250514",
            db=db,
            num_candidates=3,
        )

        return pipeline, MockGen, MockEval, MockRefiner


class TestLongPostContentType:
    def test_content_type_config_exists(self):
        assert "x_long_post" in ContentGenerator.CONTENT_TYPE_CONFIG
        cfg = ContentGenerator.CONTENT_TYPE_CONFIG["x_long_post"]
        assert cfg["template"] == "x_long_post_v2"
        assert cfg["max_tokens"] == 1000

    def test_char_limit_is_2000(self):
        assert SynthesisPipeline.CHAR_LIMITS.get("x_long_post") == 2000


class TestLongPostFormats:
    def test_long_post_formats_exist(self):
        assert len(SynthesisPipeline.LONG_POST_FORMATS) == 5

    def test_format_names(self):
        names = [name for name, _ in SynthesisPipeline.LONG_POST_FORMATS]
        assert "deep_dive" in names
        assert "retrospective" in names
        assert "framework" in names
        assert "narrative" in names
        assert "analysis" in names

    def test_select_format_directives_uses_long_post_formats(self):
        pipeline, _, _, _ = _build_pipeline()
        directives, names = pipeline._select_format_directives(3, "x_long_post")
        assert len(directives) == 3
        assert len(names) == 3
        # All names should be from LONG_POST_FORMATS
        valid_names = {n for n, _ in SynthesisPipeline.LONG_POST_FORMATS}
        for name in names:
            assert name in valid_names

    def test_select_format_directives_x_post_uses_post_formats(self):
        pipeline, _, _, _ = _build_pipeline()
        _, names = pipeline._select_format_directives(3, "x_post")
        valid_names = {n for n, _ in SynthesisPipeline.POST_FORMATS}
        for name in names:
            assert name in valid_names

    def test_select_format_directives_x_thread_uses_thread_formats(self):
        pipeline, _, _, _ = _build_pipeline()
        _, names = pipeline._select_format_directives(3, "x_thread")
        valid_names = {n for n, _ in SynthesisPipeline.THREAD_FORMATS}
        for name in names:
            assert name in valid_names


class TestLongPostCharLimit:
    def test_enforce_char_limit_passes_under_2000(self):
        pipeline, _, _, _ = _build_pipeline()
        text = "A" * 1500
        result, rejected = pipeline._enforce_char_limit([text], 2000)
        assert len(result) == 1
        assert rejected == 0

    def test_enforce_char_limit_condenses_over_2000(self):
        pipeline, _, _, _ = _build_pipeline()
        # Mock the condense method to return shorter text
        pipeline.generator.condense = MagicMock(return_value="A" * 1800)
        text = "A" * 2500
        result, rejected = pipeline._enforce_char_limit([text], 2000)
        assert len(result) == 1
        assert rejected == 0
        pipeline.generator.condense.assert_called()


class TestLongPostPipelineRun:
    def test_pipeline_run_with_long_post(self):
        pipeline, _, _, _ = _build_pipeline()

        candidates = _make_candidates()
        pipeline.generator.generate_candidates = MagicMock(return_value=candidates)
        pipeline.evaluator.evaluate = MagicMock(return_value=_make_comparison())
        pipeline.refiner.refine_and_gate = MagicMock(
            return_value=RefinementResult(
                original=candidates[0].content,
                refined="Refined essay content " * 15,
                picked="REFINED",
                final_score=8.5,
                final_content="Refined essay content " * 15,
            )
        )
        pipeline.few_shot_selector.get_examples = MagicMock(return_value=[])
        pipeline.few_shot_selector.format_examples = MagicMock(return_value="")

        result = pipeline.run(
            prompts=SAMPLE_PROMPTS,
            commits=SAMPLE_COMMITS,
            content_type="x_long_post",
            threshold=0.7,
        )

        assert result.final_score >= 8.0
        assert result.batch_id
        assert result.candidates
        # Generator should have been called with x_long_post type
        call_kwargs = pipeline.generator.generate_candidates.call_args
        assert call_kwargs.kwargs.get("content_type") == "x_long_post"


class TestPromptTemplateExists:
    def test_template_file_exists(self):
        from pathlib import Path
        template = Path(__file__).parent.parent / "src" / "synthesis" / "prompts" / "x_long_post_v2.txt"
        assert template.exists()

    def test_template_has_required_placeholders(self):
        from pathlib import Path
        template_path = Path(__file__).parent.parent / "src" / "synthesis" / "prompts" / "x_long_post_v2.txt"
        content = template_path.read_text()
        assert "{few_shot_section}" in content
        assert "{format_directive}" in content
        assert "{commits}" in content
        assert "{prompts}" in content
        assert "{commit_count}" in content
        assert "{historical_section}" in content
        assert "{avoidance_context}" in content
        assert "{pattern_context}" in content
        assert "{trend_context}" in content
