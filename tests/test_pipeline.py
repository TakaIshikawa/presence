"""Tests for the multi-stage synthesis pipeline."""

from unittest.mock import MagicMock, patch

import pytest

from synthesis.pipeline import SynthesisPipeline, PipelineResult
from synthesis.evaluator_v2 import ComparisonResult
from synthesis.generator import GeneratedContent
from synthesis.refiner import RefinementResult


# --- Helpers ---


def _make_comparison(
    best_score=8.0,
    groundedness=8.0,
    ranking=None,
    improvement="Add more detail",
    reject_reason=None,
):
    return ComparisonResult(
        ranking=ranking or [0, 1, 2],
        best_score=best_score,
        groundedness=groundedness,
        rawness=7.0,
        narrative_specificity=7.0,
        voice=7.0,
        engagement_potential=7.0,
        best_feedback="Strong candidate",
        improvement=improvement,
        reject_reason=reject_reason,
        raw_response="",
    )


def _make_refinement(picked="REFINED", final_score=8.5):
    return RefinementResult(
        original="Original post",
        refined="Refined post",
        picked=picked,
        final_score=final_score,
        final_content="Refined post" if picked == "REFINED" else "Original post",
    )


def _make_candidates(texts=None):
    texts = texts or ["Candidate A text", "Candidate B text", "Candidate C text"]
    return [
        GeneratedContent(
            content_type="x_post",
            content=t,
            source_prompts=["prompt"],
            source_commits=["commit"],
        )
        for t in texts
    ]


SAMPLE_PROMPTS = ["Worked on error handling in the CLI"]
SAMPLE_COMMITS = [{"sha": "abc123", "repo_name": "my-project", "message": "fix: handle timeout errors"}]


# --- Pipeline construction ---


class TestPipelineConstruction:
    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    def test_init_creates_components(self, MockGen, MockEval, MockRefiner):
        db = MagicMock()
        pipeline = SynthesisPipeline(
            api_key="test-key",
            generator_model="claude-sonnet-4-20250514",
            evaluator_model="claude-opus-4-20250514",
            db=db,
            num_candidates=3,
        )
        MockGen.assert_called_once_with("test-key", "claude-sonnet-4-20250514")
        MockEval.assert_called_once_with("test-key", "claude-opus-4-20250514")
        assert pipeline.num_candidates == 3


# --- Content type routing ---


class TestContentTypeRouting:
    """Verify the pipeline handles different content types correctly."""

    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_x_post_enforces_char_limit(self, MockFS, MockGen, MockEval, MockRefiner):
        db = MagicMock()
        db.get_curated_posts.return_value = []
        db.get_auto_classified_posts.return_value = []
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)

        # Candidates: 2 within limit, 1 over
        short_texts = ["Short post A", "Short post B"]
        long_text = "x" * 300  # Over 280 char limit
        pipeline.generator.generate_candidates.return_value = _make_candidates(
            short_texts + [long_text]
        )
        pipeline.generator.condense.return_value = "x" * 270  # Condensed within limit

        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=9.5)
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="x_post")

        # condense was called for the over-limit candidate
        pipeline.generator.condense.assert_called()
        assert result.final_content is not None

    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_x_thread_no_char_limit(self, MockFS, MockGen, MockEval, MockRefiner):
        db = MagicMock()
        db.get_curated_posts.return_value = []
        db.get_auto_classified_posts.return_value = []
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)

        long_thread = "TWEET 1: " + "a" * 200 + "\nTWEET 2: " + "b" * 200
        pipeline.generator.generate_candidates.return_value = _make_candidates(
            [long_thread, long_thread, long_thread]
        )
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=9.5)
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="x_thread")

        # No char limit enforcement for threads
        pipeline.generator.condense.assert_not_called()

    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_blog_post_no_char_limit(self, MockFS, MockGen, MockEval, MockRefiner):
        db = MagicMock()
        db.get_curated_posts.return_value = []
        db.get_auto_classified_posts.return_value = []
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)

        blog = "TITLE: My Blog Post\n\n## Section 1\n" + "content " * 200
        pipeline.generator.generate_candidates.return_value = _make_candidates(
            [blog, blog, blog]
        )
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=9.5)
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="blog_post")

        pipeline.generator.condense.assert_not_called()
        assert result.final_content == blog


# --- Refinement gating ---


class TestRefinementGating:
    """Test the skip/refine logic based on score thresholds."""

    def _build_pipeline(self):
        db = MagicMock()
        db.get_curated_posts.return_value = []
        db.get_auto_classified_posts.return_value = []
        db.get_recent_published_content.return_value = []

        with patch("synthesis.pipeline.ContentRefiner") as MockRefiner, \
             patch("synthesis.pipeline.CrossModelEvaluator") as MockEval, \
             patch("synthesis.pipeline.ContentGenerator") as MockGen, \
             patch("synthesis.pipeline.FewShotSelector") as MockFS:
            pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)

        candidates = _make_candidates(["Short A", "Short B", "Short C"])
        pipeline.generator.generate_candidates.return_value = candidates
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        return pipeline

    def test_score_above_9_skips_refinement(self):
        pipeline = self._build_pipeline()
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=9.5)

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        pipeline.refiner.refine_and_gate.assert_not_called()
        assert result.refinement is None
        assert result.final_score == 9.5

    def test_score_below_5_skips_refinement(self):
        pipeline = self._build_pipeline()
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=4.0)

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        pipeline.refiner.refine_and_gate.assert_not_called()
        assert result.refinement is None

    def test_score_in_range_triggers_refinement(self):
        pipeline = self._build_pipeline()
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=7.0)
        pipeline.refiner.refine_and_gate.return_value = _make_refinement(
            picked="REFINED", final_score=8.0
        )

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        pipeline.refiner.refine_and_gate.assert_called_once()
        assert result.refinement is not None
        assert result.final_content == "Refined post"

    def test_refinement_uses_max_of_gate_and_eval_score(self):
        pipeline = self._build_pipeline()
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=7.5)
        # Gate score is lower than eval score
        pipeline.refiner.refine_and_gate.return_value = _make_refinement(
            picked="REFINED", final_score=6.0
        )

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        # Should use the higher score (eval: 7.5 > gate: 6.0)
        assert result.final_score == 7.5

    def test_no_improvement_skips_refinement(self):
        pipeline = self._build_pipeline()
        pipeline.evaluator.evaluate.return_value = _make_comparison(
            best_score=7.0, improvement=""
        )

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        pipeline.refiner.refine_and_gate.assert_not_called()

    def test_reject_reason_skips_refinement(self):
        pipeline = self._build_pipeline()
        pipeline.evaluator.evaluate.return_value = _make_comparison(
            best_score=7.0, reject_reason="All candidates too generic"
        )

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        pipeline.refiner.refine_and_gate.assert_not_called()


# --- Repetition filtering ---


class TestRepetitionFilter:
    def test_extract_opening_splits_on_em_dash(self):
        text = "Debugging is about context—not just reading error messages."
        opening = SynthesisPipeline._extract_opening(text)
        assert opening == "debugging is about context"

    def test_extract_opening_splits_on_colon(self):
        text = "The lesson: always validate inputs before processing."
        opening = SynthesisPipeline._extract_opening(text)
        assert opening == "the lesson"

    def test_extract_opening_splits_on_period(self):
        text = "Error handling matters. Most devs ignore it."
        opening = SynthesisPipeline._extract_opening(text)
        assert opening == "error handling matters"

    def test_extract_opening_max_len(self):
        text = "a" * 100
        opening = SynthesisPipeline._extract_opening(text, max_len=50)
        assert len(opening) <= 50

    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_repetitive_candidates_filtered(self, MockFS, MockGen, MockEval, MockRefiner):
        db = MagicMock()
        db.get_curated_posts.return_value = []
        db.get_auto_classified_posts.return_value = []
        # Recent published post with similar opening
        db.get_recent_published_content.return_value = [
            {"content": "Debugging is about context—the error message is just the starting point."}
        ]

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)

        # All three candidates have similar openings to the published post
        similar_candidates = [
            "Debugging is about context—you need to understand the system.",
            "Debugging is about context: read the logs carefully.",
            "A different approach—test-driven development changes everything.",
        ]
        pipeline.generator.generate_candidates.return_value = _make_candidates(similar_candidates)
        pipeline.evaluator.evaluate.return_value = _make_comparison(
            best_score=9.5, ranking=[0]
        )
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        # The evaluator should receive filtered candidates
        eval_call_args = pipeline.evaluator.evaluate.call_args
        evaluated_candidates = eval_call_args[1]["candidates"]
        # At least the dissimilar one should survive
        assert any("test-driven" in c for c in evaluated_candidates)

    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_keeps_at_least_one_if_all_repetitive(self, MockFS, MockGen, MockEval, MockRefiner):
        db = MagicMock()
        db.get_curated_posts.return_value = []
        db.get_auto_classified_posts.return_value = []
        db.get_recent_published_content.return_value = [
            {"content": "Same opening everywhere—this is the pattern."}
        ]

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)

        all_similar = [
            "Same opening everywhere—version A.",
            "Same opening everywhere—version B.",
            "Same opening everywhere—version C.",
        ]
        pipeline.generator.generate_candidates.return_value = _make_candidates(all_similar)
        pipeline.evaluator.evaluate.return_value = _make_comparison(
            best_score=9.5, ranking=[0]
        )
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        # Should keep at least one candidate (first one)
        eval_call_args = pipeline.evaluator.evaluate.call_args
        evaluated_candidates = eval_call_args[1]["candidates"]
        assert len(evaluated_candidates) >= 1


# --- Curation signals ---


class TestCurationSignals:
    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_negative_examples_passed_to_evaluator(self, MockFS, MockGen, MockEval, MockRefiner):
        db = MagicMock()
        db.get_curated_posts.return_value = [
            {"id": 10, "content": "Too specific jargon post"}
        ]
        db.get_auto_classified_posts.return_value = [
            {"id": 20, "content": "Low resonance generic post"}
        ]
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)
        pipeline.generator.generate_candidates.return_value = _make_candidates(
            ["Short A", "Short B", "Short C"]
        )
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=9.5)
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        eval_call_args = pipeline.evaluator.evaluate.call_args
        neg_examples = eval_call_args[1]["negative_examples"]
        assert len(neg_examples) == 2
        assert ("Too specific jargon post", "too_specific") in neg_examples
        assert ("Low resonance generic post", "low_resonance") in neg_examples

    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_negative_ids_excluded_from_few_shot(self, MockFS, MockGen, MockEval, MockRefiner):
        db = MagicMock()
        db.get_curated_posts.return_value = [{"id": 10, "content": "bad"}]
        db.get_auto_classified_posts.return_value = [{"id": 20, "content": "bland"}]
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)
        pipeline.generator.generate_candidates.return_value = _make_candidates(
            ["Short A", "Short B", "Short C"]
        )
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=9.5)
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        fs_call_args = pipeline.few_shot_selector.get_examples.call_args
        exclude_ids = fs_call_args[1]["exclude_ids"]
        assert 10 in exclude_ids
        assert 20 in exclude_ids


# --- Character limit enforcement edge cases ---


class TestCharLimitEnforcement:
    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_all_candidates_over_limit_truncates_shortest(
        self, MockFS, MockGen, MockEval, MockRefiner
    ):
        db = MagicMock()
        db.get_curated_posts.return_value = []
        db.get_auto_classified_posts.return_value = []
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)

        over_limit = [
            "First sentence here. Second sentence follows. Third one is the last." + "x" * 250,
            "x" * 400,
            "x" * 500,
        ]
        pipeline.generator.generate_candidates.return_value = _make_candidates(over_limit)
        # condense also returns over-limit text
        pipeline.generator.condense.return_value = "x" * 290
        pipeline.evaluator.evaluate.return_value = _make_comparison(
            best_score=9.5, ranking=[0]
        )
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        # Final content should be within 280 chars (fallback truncation)
        assert len(result.final_content) <= 280

    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_post_refinement_char_limit_check(
        self, MockFS, MockGen, MockEval, MockRefiner
    ):
        """Refinement may expand content past char limit — verify re-enforcement."""
        db = MagicMock()
        db.get_curated_posts.return_value = []
        db.get_auto_classified_posts.return_value = []
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)

        pipeline.generator.generate_candidates.return_value = _make_candidates(
            ["Within limit text", "Another within limit", "Third within"]
        )
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=7.0)
        # Refinement produces over-limit content
        pipeline.refiner.refine_and_gate.return_value = RefinementResult(
            original="Within limit text",
            refined="x" * 300,  # over 280
            picked="REFINED",
            final_score=8.0,
            final_content="x" * 300,
        )
        pipeline.generator.condense.return_value = "x" * 270  # condensed within limit
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS, content_type="x_post")

        # Post-refinement condense should have been called
        pipeline.generator.condense.assert_called()
        assert len(result.final_content) <= 280


# --- Pipeline result structure ---


class TestPipelineResult:
    @patch("synthesis.pipeline.ContentRefiner")
    @patch("synthesis.pipeline.CrossModelEvaluator")
    @patch("synthesis.pipeline.ContentGenerator")
    @patch("synthesis.pipeline.FewShotSelector")
    def test_result_carries_source_metadata(self, MockFS, MockGen, MockEval, MockRefiner):
        db = MagicMock()
        db.get_curated_posts.return_value = []
        db.get_auto_classified_posts.return_value = []
        db.get_recent_published_content.return_value = []

        pipeline = SynthesisPipeline("key", "gen-model", "eval-model", db)
        pipeline.generator.generate_candidates.return_value = _make_candidates(
            ["Post A", "Post B", "Post C"]
        )
        pipeline.evaluator.evaluate.return_value = _make_comparison(best_score=9.5)
        pipeline.few_shot_selector.get_examples.return_value = []
        pipeline.few_shot_selector.format_examples.return_value = ""

        result = pipeline.run(SAMPLE_PROMPTS, SAMPLE_COMMITS)

        assert result.source_prompts == SAMPLE_PROMPTS
        assert result.source_commits == [c["message"] for c in SAMPLE_COMMITS]
        assert len(result.batch_id) == 8  # UUID[:8]
        assert isinstance(result.comparison, ComparisonResult)
