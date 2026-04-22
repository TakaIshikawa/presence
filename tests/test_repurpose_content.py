"""Tests for scripts/repurpose_content.py script-level logic."""

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from synthesis.evaluator_v2 import ComparisonResult
from synthesis.repurposer import RepurposeCandidate, RepurposeResult


# --- Helpers ---


def _make_config(daily_post_cap=3, static_site="/tmp/fake-site"):
    config = MagicMock()
    config.paths.database = ":memory:"
    config.paths.static_site = static_site
    config.blog.manifest_path = None
    config.anthropic.api_key = "test-key"
    config.synthesis.model = "gen-model"
    config.synthesis.eval_model = "eval-model"
    config.synthesis.eval_threshold = 0.7
    config.synthesis.daily_post_cap = daily_post_cap
    config.timeouts.anthropic_seconds = 300
    config.x.api_key = "xk"
    config.x.api_secret = "xs"
    config.x.access_token = "xt"
    config.x.access_token_secret = "xts"
    return config


def _mock_script_context(config, db):
    @contextmanager
    def _ctx():
        yield (config, db)
    return _ctx


def _make_comparison(best_score=8.0, reject_reason=None):
    return ComparisonResult(
        ranking=[0],
        best_score=best_score,
        groundedness=8.0,
        rawness=7.0,
        narrative_specificity=7.0,
        voice=7.0,
        engagement_potential=7.0,
        best_feedback="Strong candidate",
        improvement="Add more detail",
        reject_reason=reject_reason,
        raw_response="",
    )


def _make_candidate(content_id=123, target_type="x_thread", engagement_score=15.0):
    return RepurposeCandidate(
        content_id=content_id,
        original_content="Original post content",
        original_type="x_post",
        engagement_score=engagement_score,
        target_type=target_type,
    )


def _make_repurpose_result(source_id=123, target_type="x_thread"):
    return RepurposeResult(
        source_id=source_id,
        target_type=target_type,
        content="TWEET 1: Expanded thread content\nTWEET 2: More insights",
        generation_prompt="Test prompt",
    )


# Shared decorator stack for patching repurpose_content dependencies.
def _repurpose_patches(func):
    @patch("repurpose_content.update_monitoring")
    @patch("repurpose_content.parse_thread_content")
    @patch("repurpose_content.XClient")
    @patch("repurpose_content.CrossModelEvaluator")
    @patch("repurpose_content.ContentRepurposer")
    @patch("repurpose_content.script_context")
    def wrapper(self, mock_ctx, MockRepurposer, MockEvaluator,
                MockXClient, mock_parse_thread, mock_monitoring,
                *args, **kwargs):
        return func(
            self,
            mock_ctx=mock_ctx,
            MockRepurposer=MockRepurposer,
            MockEvaluator=MockEvaluator,
            MockXClient=MockXClient,
            mock_parse_thread=mock_parse_thread,
            mock_monitoring=mock_monitoring,
        )
    return wrapper


# --- Tests ---


class TestMainExitsEarlyDailyCap:
    @_repurpose_patches
    def test_exits_early_when_daily_cap_reached(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config(daily_post_cap=3)
        db = MagicMock()
        db.count_posts_today.return_value = 3  # Already at cap
        mock_ctx.return_value = _mock_script_context(config, db)()

        import repurpose_content
        repurpose_content.main()

        db.count_posts_today.assert_called_once_with("x_thread")
        MockRepurposer.assert_not_called()
        mock_monitoring.assert_not_called()


class TestMainExitsEarlyNoCandidates:
    @_repurpose_patches
    def test_exits_early_when_no_candidates_found(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config()
        db = MagicMock()
        db.count_posts_today.return_value = 0
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockRepurposer.return_value.find_candidates.return_value = []

        import repurpose_content
        repurpose_content.main()

        MockRepurposer.return_value.find_candidates.assert_called_once_with(
            min_engagement=10.0,
            max_age_days=14,
        )
        MockEvaluator.assert_not_called()
        mock_monitoring.assert_called_once_with("repurpose")


class TestLinkedInVariantRefresh:
    def test_cli_refreshes_linkedin_variants_without_publishing(self):
        config = _make_config()
        db = MagicMock()
        db.list_generated_content_for_variant_refresh.return_value = [
            {
                "id": 42,
                "content_type": "x_post",
                "content": "Tweeting this on X. #python #ai #launch #ops #build #extra",
            }
        ]

        import repurpose_content

        with patch("repurpose_content.script_context") as mock_ctx, \
             patch("repurpose_content.ContentRepurposer") as MockRepurposer, \
             patch("repurpose_content.CrossModelEvaluator") as MockEvaluator, \
             patch("repurpose_content.XClient") as MockXClient, \
             patch("repurpose_content.update_monitoring") as mock_monitoring:
            mock_ctx.return_value = _mock_script_context(config, db)()

            repurpose_content.main(["--linkedin-variants", "--limit", "25"])

        db.list_generated_content_for_variant_refresh.assert_called_once_with(limit=25)
        db.upsert_content_variant.assert_called_once()
        _, kwargs = db.upsert_content_variant.call_args
        assert kwargs["content_id"] == 42
        assert kwargs["platform"] == "linkedin"
        assert kwargs["variant_type"] == "post"
        assert "Tweeting" not in kwargs["content"]
        assert " X" not in kwargs["content"]
        assert "#extra" not in kwargs["content"]
        MockRepurposer.assert_not_called()
        MockEvaluator.assert_not_called()
        MockXClient.assert_not_called()
        mock_monitoring.assert_called_once_with("repurpose")

    def test_cli_refreshes_single_linkedin_variant_by_content_id(self):
        config = _make_config()
        db = MagicMock()
        db.get_generated_content.return_value = {
            "id": 99,
            "content_type": "x_thread",
            "content": "TWEET 1: First point\nTWEET 2: Second point",
        }

        import repurpose_content

        with patch("repurpose_content.script_context") as mock_ctx, \
             patch("repurpose_content.parse_thread_content") as mock_parse_thread, \
             patch("repurpose_content.update_monitoring"):
            mock_ctx.return_value = _mock_script_context(config, db)()
            mock_parse_thread.return_value = ["First point", "Second point"]

            repurpose_content.main(["--linkedin-variants", "--content-id", "99"])

        db.get_generated_content.assert_called_once_with(99)
        db.list_generated_content_for_variant_refresh.assert_not_called()
        db.upsert_content_variant.assert_called_once()
        mock_parse_thread.assert_called_once_with("TWEET 1: First point\nTWEET 2: Second point")


class TestMainRoutesToExpandPostToThread:
    @_repurpose_patches
    def test_routes_to_expand_post_to_thread_for_x_thread_target(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config()
        db = MagicMock()
        db.count_posts_today.return_value = 0
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        candidate = _make_candidate(target_type="x_thread")
        MockRepurposer.return_value.find_candidates.return_value = [candidate]

        result = _make_repurpose_result(target_type="x_thread")
        MockRepurposer.return_value.expand_post_to_thread.return_value = result

        comparison = _make_comparison(best_score=8.0)
        MockEvaluator.return_value.evaluate.return_value = comparison

        mock_parse_thread.return_value = ["tweet1", "tweet2"]
        post_result = MagicMock(success=True, url="https://x.com/thread/456", tweet_id="tw456")
        MockXClient.return_value.post_thread.return_value = post_result

        import repurpose_content
        repurpose_content.main()

        MockRepurposer.return_value.expand_post_to_thread.assert_called_once_with(candidate)
        MockRepurposer.return_value.expand_to_blog_seed.assert_not_called()


class TestMainRoutesToExpandToBlogSeed:
    @_repurpose_patches
    def test_routes_to_expand_to_blog_seed_for_blog_seed_target(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config()
        db = MagicMock()
        db.count_posts_today.return_value = 0
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        candidate = _make_candidate(target_type="blog_seed")
        MockRepurposer.return_value.find_candidates.return_value = [candidate]

        result = _make_repurpose_result(target_type="blog_seed")
        MockRepurposer.return_value.expand_to_blog_seed.return_value = result

        comparison = _make_comparison(best_score=8.0)
        MockEvaluator.return_value.evaluate.return_value = comparison

        import repurpose_content
        repurpose_content.main()

        MockRepurposer.return_value.expand_to_blog_seed.assert_called_once_with(candidate)
        MockRepurposer.return_value.expand_post_to_thread.assert_not_called()
        # blog_seed should not be posted to X
        mock_parse_thread.assert_not_called()
        MockXClient.return_value.post_thread.assert_not_called()


class TestMainUnsupportedTargetType:
    @_repurpose_patches
    def test_errors_on_unsupported_target_type(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config()
        db = MagicMock()
        db.count_posts_today.return_value = 0
        mock_ctx.return_value = _mock_script_context(config, db)()

        candidate = _make_candidate(target_type="unknown_type")
        MockRepurposer.return_value.find_candidates.return_value = [candidate]

        import repurpose_content
        repurpose_content.main()

        # Should not call evaluator or post anything
        MockEvaluator.assert_not_called()
        mock_parse_thread.assert_not_called()
        MockXClient.return_value.post_thread.assert_not_called()


class TestMainBelowThreshold:
    @_repurpose_patches
    def test_does_not_post_when_below_threshold(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config()
        db = MagicMock()
        db.count_posts_today.return_value = 0
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        candidate = _make_candidate(target_type="x_thread")
        MockRepurposer.return_value.find_candidates.return_value = [candidate]

        result = _make_repurpose_result(target_type="x_thread")
        MockRepurposer.return_value.expand_post_to_thread.return_value = result

        # Score below threshold (0.7 * 10 = 7.0)
        comparison = _make_comparison(best_score=5.0)
        MockEvaluator.return_value.evaluate.return_value = comparison

        import repurpose_content
        repurpose_content.main()

        db.insert_repurposed_content.assert_called_once()
        mock_parse_thread.assert_not_called()
        MockXClient.return_value.post_thread.assert_not_called()
        db.mark_published.assert_not_called()
        mock_monitoring.assert_called_once_with("repurpose")


class TestMainRejected:
    @_repurpose_patches
    def test_does_not_post_when_rejected(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config()
        db = MagicMock()
        db.count_posts_today.return_value = 0
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        candidate = _make_candidate(target_type="x_thread")
        MockRepurposer.return_value.find_candidates.return_value = [candidate]

        result = _make_repurpose_result(target_type="x_thread")
        MockRepurposer.return_value.expand_post_to_thread.return_value = result

        comparison = _make_comparison(best_score=8.0, reject_reason="Too generic")
        MockEvaluator.return_value.evaluate.return_value = comparison

        import repurpose_content
        repurpose_content.main()

        db.insert_repurposed_content.assert_called_once()
        mock_parse_thread.assert_not_called()
        MockXClient.return_value.post_thread.assert_not_called()
        db.mark_published.assert_not_called()
        mock_monitoring.assert_called_once_with("repurpose")


class TestMainSuccessfulThreadPost:
    @_repurpose_patches
    def test_posts_thread_successfully(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config()
        db = MagicMock()
        db.count_posts_today.return_value = 0
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        candidate = _make_candidate(content_id=123, target_type="x_thread", engagement_score=15.0)
        MockRepurposer.return_value.find_candidates.return_value = [candidate]

        result = _make_repurpose_result(source_id=123, target_type="x_thread")
        MockRepurposer.return_value.expand_post_to_thread.return_value = result

        comparison = _make_comparison(best_score=8.0)
        MockEvaluator.return_value.evaluate.return_value = comparison

        mock_parse_thread.return_value = ["tweet1", "tweet2"]
        post_result = MagicMock(success=True, url="https://x.com/thread/789", tweet_id="tw789")
        MockXClient.return_value.post_thread.return_value = post_result

        import repurpose_content
        repurpose_content.main()

        db.insert_repurposed_content.assert_called_once_with(
            content_type="x_thread",
            source_content_id=123,
            content=result.content,
            eval_score=8.0,
            eval_feedback="Strong candidate",
        )
        mock_parse_thread.assert_called_once_with(result.content)
        MockXClient.return_value.post_thread.assert_called_once_with(["tweet1", "tweet2"])
        db.mark_published.assert_called_once_with(42, "https://x.com/thread/789", tweet_id="tw789")
        mock_monitoring.assert_called_once_with("repurpose")


class TestMainDailyCapReachedBeforePost:
    @_repurpose_patches
    def test_does_not_post_when_daily_cap_reached_on_recheck(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config(daily_post_cap=3)
        db = MagicMock()
        # First check returns 0, second check (before posting) returns 3
        db.count_posts_today.side_effect = [0, 3]
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        candidate = _make_candidate(target_type="x_thread")
        MockRepurposer.return_value.find_candidates.return_value = [candidate]

        result = _make_repurpose_result(target_type="x_thread")
        MockRepurposer.return_value.expand_post_to_thread.return_value = result

        comparison = _make_comparison(best_score=8.0)
        MockEvaluator.return_value.evaluate.return_value = comparison

        import repurpose_content
        repurpose_content.main()

        # Should check cap twice
        assert db.count_posts_today.call_count == 2
        db.insert_repurposed_content.assert_called_once()
        mock_parse_thread.assert_not_called()
        MockXClient.return_value.post_thread.assert_not_called()
        db.mark_published.assert_not_called()
        mock_monitoring.assert_called_once_with("repurpose")


class TestMainBlogSeedStoredForReview:
    @_repurpose_patches
    def test_blog_seed_stored_not_posted(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config()
        db = MagicMock()
        db.count_posts_today.return_value = 0
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        candidate = _make_candidate(target_type="blog_seed")
        MockRepurposer.return_value.find_candidates.return_value = [candidate]

        result = _make_repurpose_result(target_type="blog_seed")
        MockRepurposer.return_value.expand_to_blog_seed.return_value = result

        comparison = _make_comparison(best_score=8.0)
        MockEvaluator.return_value.evaluate.return_value = comparison

        import repurpose_content
        repurpose_content.main()

        db.insert_repurposed_content.assert_called_once_with(
            content_type="blog_seed",
            source_content_id=candidate.content_id,
            content=result.content,
            eval_score=8.0,
            eval_feedback="Strong candidate",
        )
        # Verify no X posting for blog_seed
        mock_parse_thread.assert_not_called()
        MockXClient.assert_not_called()
        db.mark_published.assert_not_called()
        mock_monitoring.assert_called_once_with("repurpose")

    def test_blog_seed_writes_markdown_draft_under_static_site_path(self, tmp_path):
        config = _make_config(static_site=str(tmp_path))
        db = MagicMock()
        db.count_posts_today.return_value = 0
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 42

        candidate = _make_candidate(content_id=123, target_type="blog_seed")
        result = RepurposeResult(
            source_id=123,
            target_type="blog_seed",
            content="TITLE: Reviewable Draft\n\n## Outline\n\nDraft opening.",
            generation_prompt="Test prompt",
        )

        import repurpose_content

        with patch("repurpose_content.script_context") as mock_ctx, \
             patch("repurpose_content.ContentRepurposer") as MockRepurposer, \
             patch("repurpose_content.CrossModelEvaluator") as MockEvaluator, \
             patch("repurpose_content.XClient") as MockXClient, \
             patch("repurpose_content.parse_thread_content") as mock_parse_thread, \
             patch("repurpose_content.update_monitoring") as mock_monitoring:
            mock_ctx.return_value = _mock_script_context(config, db)()
            MockRepurposer.return_value.find_candidates.return_value = [candidate]
            MockRepurposer.return_value.expand_to_blog_seed.return_value = result
            MockEvaluator.return_value.evaluate.return_value = _make_comparison(best_score=8.0)

            repurpose_content.main()

        draft_file = tmp_path / "drafts" / "reviewable-draft.md"
        assert draft_file.exists()
        draft = draft_file.read_text()
        assert 'title: "Reviewable Draft"' in draft
        assert "source_content_id: 123" in draft
        assert "generated_content_id: 42" in draft
        assert "status: draft" in draft
        assert "## Outline\n\nDraft opening." in draft
        manifest = json.loads((tmp_path / "drafts" / "manifest.json").read_text())
        assert manifest["drafts"][0]["slug"] == "reviewable-draft"
        assert manifest["drafts"][0]["title"] == "Reviewable Draft"
        assert manifest["drafts"][0]["source_content_id"] == 123
        assert manifest["drafts"][0]["generated_content_id"] == 42
        assert manifest["drafts"][0]["draft_path"] == "drafts/reviewable-draft.md"

        mock_parse_thread.assert_not_called()
        MockXClient.assert_not_called()
        db.mark_published.assert_not_called()
        mock_monitoring.assert_called_once_with("repurpose")

    def test_blog_seed_uses_configured_manifest_path(self, tmp_path):
        config = _make_config(static_site=str(tmp_path))
        config.blog.manifest_path = "data/blog-drafts.json"
        db = MagicMock()
        db.count_posts_today.return_value = 0
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 77

        candidate = _make_candidate(content_id=321, target_type="blog_seed")
        result = RepurposeResult(
            source_id=321,
            target_type="blog_seed",
            content="TITLE: Configured Manifest\n\nDraft body.",
            generation_prompt="Test prompt",
        )

        import repurpose_content

        with patch("repurpose_content.script_context") as mock_ctx, \
             patch("repurpose_content.ContentRepurposer") as MockRepurposer, \
             patch("repurpose_content.CrossModelEvaluator") as MockEvaluator, \
             patch("repurpose_content.XClient"), \
             patch("repurpose_content.parse_thread_content"), \
             patch("repurpose_content.update_monitoring"):
            mock_ctx.return_value = _mock_script_context(config, db)()
            MockRepurposer.return_value.find_candidates.return_value = [candidate]
            MockRepurposer.return_value.expand_to_blog_seed.return_value = result
            MockEvaluator.return_value.evaluate.return_value = _make_comparison(best_score=8.0)

            repurpose_content.main()

        manifest = json.loads((tmp_path / "data" / "blog-drafts.json").read_text())
        assert manifest["drafts"][0]["slug"] == "configured-manifest"
        assert manifest["drafts"][0]["source_content_id"] == 321
        assert manifest["drafts"][0]["generated_content_id"] == 77


class TestMainPostFailure:
    @_repurpose_patches
    def test_handles_post_failure(
        self, *, mock_ctx, MockRepurposer, MockEvaluator,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        config = _make_config()
        db = MagicMock()
        db.count_posts_today.return_value = 0
        db.get_top_performing_posts.return_value = [{"content": "ref1"}]
        db.get_all_classified_posts.return_value = {"resonated": [], "low_resonance": []}
        db.get_engagement_calibration_stats.return_value = {}
        db.insert_repurposed_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        candidate = _make_candidate(target_type="x_thread")
        MockRepurposer.return_value.find_candidates.return_value = [candidate]

        result = _make_repurpose_result(target_type="x_thread")
        MockRepurposer.return_value.expand_post_to_thread.return_value = result

        comparison = _make_comparison(best_score=8.0)
        MockEvaluator.return_value.evaluate.return_value = comparison

        mock_parse_thread.return_value = ["tweet1", "tweet2"]
        post_result = MagicMock(success=False, error="API error")
        MockXClient.return_value.post_thread.return_value = post_result

        import repurpose_content
        repurpose_content.main()

        MockXClient.return_value.post_thread.assert_called_once()
        db.mark_published.assert_not_called()
        mock_monitoring.assert_called_once_with("repurpose")
