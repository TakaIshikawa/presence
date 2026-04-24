"""Tests for scripts/weekly_digest.py script-level logic."""

import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from synthesis.evaluator_v2 import ComparisonResult
from synthesis.pipeline import PipelineResult

FIXED_NOW = datetime(2026, 4, 5, 12, 0, 0, tzinfo=timezone.utc)
FIXED_TODAY = FIXED_NOW.replace(hour=0, minute=0, second=0, microsecond=0)
FIXED_WEEK_AGO = FIXED_TODAY - timedelta(days=7)
PROMPT_TS = FIXED_WEEK_AGO + timedelta(days=3)


def _make_config(historical_enabled=False):
    config = MagicMock()
    config.paths.database = ":memory:"
    config.paths.claude_logs = "/tmp/fake_logs"
    config.paths.static_site = "/tmp/fake_site"
    config.blog.manifest_path = None
    config.anthropic.api_key = "test-key"
    config.synthesis.model = "gen-model"
    config.synthesis.eval_model = "eval-model"
    config.synthesis.num_candidates = 3
    config.synthesis.eval_threshold = 0.7
    config.timeouts.anthropic_seconds = 300
    config.embeddings = None
    if historical_enabled:
        config.historical.enabled = True
        config.historical.injection_frequency = 3
        config.historical.lookback_days = 180
        config.historical.min_age_days = 30
        config.historical.max_historical_commits = 5
    else:
        config.historical = None
    return config


def _mock_script_context(config, db):
    @contextmanager
    def _ctx():
        yield (config, db)

    return _ctx


def _make_comparison(best_score=8.0, reject_reason=None):
    return ComparisonResult(
        ranking=[0, 1, 2],
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


def _make_pipeline_result(final_score=8.0, reject_reason=None, content_format=None):
    comparison = _make_comparison(best_score=final_score, reject_reason=reject_reason)
    return PipelineResult(
        batch_id="abcd1234",
        candidates=["Candidate A", "Candidate B", "Candidate C"],
        comparison=comparison,
        refinement=None,
        final_content="TITLE: Weekly Recap\n\n## Section 1\nContent here.",
        final_score=final_score,
        source_prompts=["prompt1"],
        source_commits=["commit msg"],
        content_format=content_format,
    )


def _make_prompt_message():
    msg = MagicMock()
    msg.session_id = "session-456"
    msg.timestamp = PROMPT_TS
    msg.prompt_text = "Worked on error handling"
    msg.message_uuid = "uuid-456"
    msg.project_path = "/tmp/project"
    return msg


def _make_prompt_message_at(timestamp):
    msg = _make_prompt_message()
    msg.timestamp = timestamp
    return msg


def _make_commit_row():
    return {
        "repo_name": "my-project",
        "commit_message": "feat: add new feature",
        "commit_sha": "def456",
    }


def _weekly_patches(func):
    @patch("weekly_digest.datetime", wraps=datetime)
    @patch("weekly_digest.update_monitoring")
    @patch("weekly_digest.BlogWriter")
    @patch("weekly_digest.ClaudeLogParser")
    @patch("weekly_digest.SynthesisPipeline")
    @patch("weekly_digest.script_context")
    def wrapper(
        self,
        mock_ctx,
        MockPipeline,
        MockParser,
        MockBlogWriter,
        mock_monitoring,
        mock_dt,
        *args,
        **kwargs,
    ):
        mock_dt.now.return_value = FIXED_NOW
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        return func(
            self,
            mock_ctx=mock_ctx,
            MockPipeline=MockPipeline,
            MockParser=MockParser,
            MockBlogWriter=MockBlogWriter,
            mock_monitoring=mock_monitoring,
        )

    return wrapper


class TestMainExitsEarlyNoCommits:
    @_weekly_patches
    def test_exits_early_when_no_commits(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = []
        mock_ctx.return_value = _mock_script_context(config, db)()

        import weekly_digest

        weekly_digest.main()

        db.get_commits_in_range.assert_called_once()
        MockParser.assert_not_called()
        MockPipeline.return_value.run.assert_not_called()


class TestMainExitsEarlyNoPrompts:
    @_weekly_patches
    def test_exits_early_when_no_prompts(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        mock_ctx.return_value = _mock_script_context(config, db)()
        MockParser.return_value.parse_global_history.return_value = []

        import weekly_digest

        weekly_digest.main()

        MockPipeline.return_value.run.assert_not_called()


class TestMainPublishesWhenPassesThreshold:
    @_weekly_patches
    def test_runs_pipeline_and_publishes_blog(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.get_github_activity_in_range.return_value = [
            {
                "activity_id": "my-project#8:pull_request",
                "repo_name": "my-project",
                "activity_type": "pull_request",
                "number": 8,
            }
        ]
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=8.0)
        MockPipeline.return_value.run.return_value = result

        write_result = MagicMock(
            success=True,
            file_path="/tmp/fake_site/posts/weekly.md",
            url="https://blog.example.com/weekly",
        )
        MockBlogWriter.return_value.write_post.return_value = write_result
        MockBlogWriter.return_value.commit_and_push.return_value = True

        import weekly_digest

        weekly_digest.main()

        MockPipeline.return_value.run.assert_called_once()
        assert MockPipeline.return_value.run.call_args[1]["content_type"] == "blog_post"
        prompts_passed = MockPipeline.return_value.run.call_args[1]["prompts"]
        assert len(prompts_passed) == 1
        assert "Claude session session-456" in prompts_passed[0]
        assert "Worked on error handling" in prompts_passed[0]
        MockBlogWriter.assert_called_once_with("/tmp/fake_site", manifest_path=None)
        MockBlogWriter.return_value.write_post.assert_called_once_with(
            result.final_content,
            source_commits=["def456"],
            source_sessions=["uuid-456"],
            generated_content_id=42,
            canonical_social_post_url=None,
        )
        MockBlogWriter.return_value.commit_and_push.assert_called_once_with("Weekly Recap")
        db.mark_published.assert_called_once_with(42, "https://blog.example.com/weekly")

    @_weekly_patches
    def test_draft_mode_writes_draft_and_records_review_outcome(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        config.blog.manifest_path = "data/blog-drafts.json"
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.get_github_activity_in_range.return_value = []
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        prompt_ts = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)
        MockParser.return_value.parse_global_history.return_value = [
            _make_prompt_message_at(prompt_ts),
        ]

        result = _make_pipeline_result(final_score=8.0)
        result.claim_check_summary = {
            "supported_count": 1,
            "unsupported_count": 0,
            "annotation_text": None,
        }
        result.persona_guard_summary = {"passed": True}
        MockPipeline.return_value.run.return_value = result
        MockBlogWriter.return_value.write_draft.return_value = MagicMock(
            success=True,
            file_path="/tmp/fake_site/drafts/weekly-recap.md",
        )

        import weekly_digest

        weekly_digest.main(["--draft", "--week-start", "2026-04-13"])

        week_start = datetime(2026, 4, 13, tzinfo=timezone.utc)
        week_end = datetime(2026, 4, 20, tzinfo=timezone.utc)
        db.get_commits_in_range.assert_called_once_with(week_start, week_end)
        db.get_github_activity_in_range.assert_called_once_with(week_start, week_end)
        MockBlogWriter.assert_called_once_with(
            "/tmp/fake_site",
            manifest_path="data/blog-drafts.json",
        )
        db.insert_generated_content.assert_called_once()
        db.save_claim_check_summary.assert_called_once_with(
            42,
            supported_count=1,
            unsupported_count=0,
            annotation_text=None,
        )
        db.save_persona_guard_summary.assert_called_once_with(42, {"passed": True})
        MockBlogWriter.return_value.write_draft.assert_called_once_with(
            result.final_content,
            source_content_id=42,
            generated_content_id=42,
        )
        MockBlogWriter.return_value.write_post.assert_not_called()
        MockBlogWriter.return_value.commit_and_push.assert_not_called()
        db.mark_published.assert_not_called()

        run_kwargs = db.insert_pipeline_run.call_args.kwargs
        assert run_kwargs["content_id"] == 42
        assert run_kwargs["outcome"] == "draft"
        assert run_kwargs["rejection_reason"] == "Draft mode enabled"

    @_weekly_patches
    def test_uses_raw_prompt_fallback_when_no_session_summaries(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.get_github_activity_in_range.return_value = []
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]
        MockPipeline.return_value.run.return_value = _make_pipeline_result(final_score=5.0)

        import weekly_digest

        with patch("weekly_digest.build_session_summaries", return_value=[]):
            weekly_digest.main()

        prompts_passed = MockPipeline.return_value.run.call_args[1]["prompts"]
        assert prompts_passed == ["Worked on error handling"]

    @_weekly_patches
    def test_no_push_writes_post_without_commit_or_publish_mark(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.get_github_activity_in_range.return_value = []
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=8.0)
        MockPipeline.return_value.run.return_value = result
        MockBlogWriter.return_value.write_post.return_value = MagicMock(
            success=True,
            file_path="/tmp/fake_site/blog/weekly-recap.html",
            url="https://blog.example.com/weekly",
        )

        import weekly_digest

        weekly_digest.main(["--no-push"])

        MockBlogWriter.return_value.write_post.assert_called_once()
        MockBlogWriter.return_value.write_draft.assert_not_called()
        MockBlogWriter.return_value.commit_and_push.assert_not_called()
        db.mark_published.assert_not_called()
        run_kwargs = db.insert_pipeline_run.call_args.kwargs
        assert run_kwargs["outcome"] == "draft"
        assert run_kwargs["rejection_reason"] == "No-push mode enabled"


class TestMainDoesNotPublishBelowThreshold:
    @_weekly_patches
    def test_does_not_publish_below_threshold(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.get_github_activity_in_range.return_value = [
            {
                "activity_id": "my-project#8:pull_request",
                "repo_name": "my-project",
                "activity_type": "pull_request",
                "number": 8,
            }
        ]
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=5.0)
        MockPipeline.return_value.run.return_value = result

        import weekly_digest

        weekly_digest.main()

        MockBlogWriter.return_value.write_post.assert_not_called()
        MockBlogWriter.return_value.commit_and_push.assert_not_called()
        db.mark_published.assert_not_called()


class TestMainDoesNotPublishWhenRejected:
    @_weekly_patches
    def test_does_not_publish_when_reject_reason_set(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=5.0, reject_reason="All candidates too generic")
        MockPipeline.return_value.run.return_value = result

        import weekly_digest

        weekly_digest.main()

        MockBlogWriter.return_value.write_post.assert_not_called()
        db.mark_published.assert_not_called()


class TestCommitAndPushIntegration:
    @_weekly_patches
    def test_does_not_mark_published_when_push_fails(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=8.0)
        MockPipeline.return_value.run.return_value = result

        write_result = MagicMock(
            success=True,
            file_path="/tmp/fake_site/posts/weekly.md",
            url="https://blog.example.com/weekly",
        )
        MockBlogWriter.return_value.write_post.return_value = write_result
        MockBlogWriter.return_value.commit_and_push.return_value = False

        import weekly_digest

        weekly_digest.main()

        MockBlogWriter.return_value.write_post.assert_called_once()
        MockBlogWriter.return_value.commit_and_push.assert_called_once()
        db.mark_published.assert_not_called()

    @_weekly_patches
    def test_does_not_push_when_write_fails(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=8.0)
        MockPipeline.return_value.run.return_value = result

        write_result = MagicMock(success=False, error="disk full")
        MockBlogWriter.return_value.write_post.return_value = write_result

        import weekly_digest

        weekly_digest.main()

        MockBlogWriter.return_value.commit_and_push.assert_not_called()
        db.mark_published.assert_not_called()


class TestHistoricalContextInjection:
    @_weekly_patches
    def test_historical_context_injected_when_enabled(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config(historical_enabled=True)
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=5.0)
        MockPipeline.return_value.run.return_value = result

        mock_theme_ctx = MagicMock()
        mock_theme_ctx.theme_description = "Error handling patterns"
        mock_theme_ctx.commits = [{"sha": "old123", "message": "old commit", "repo_name": "old-repo"}]

        with patch("synthesis.theme_selector.ThemeSelector") as MockThemeSelector:
            mock_ts = MockThemeSelector.return_value
            mock_ts.should_inject.return_value = True
            mock_ts.select.return_value = mock_theme_ctx

            import weekly_digest

            weekly_digest.main()

            mock_ts.should_inject.assert_called_once_with("blog_post", 3)
            mock_ts.select.assert_called_once()

            run_call = MockPipeline.return_value.run.call_args
            commits_passed = run_call[1]["commits"]
            historical = [c for c in commits_passed if c.get("historical")]
            assert len(historical) == 1
            assert historical[0]["sha"] == "old123"


class TestContentFormatPersistence:
    @_weekly_patches
    def test_content_format_forwarded_to_db(
        self, *, mock_ctx, MockPipeline, MockParser, MockBlogWriter, mock_monitoring
    ):
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.get_github_activity_in_range.return_value = [
            {
                "activity_id": "my-project#8:pull_request",
                "repo_name": "my-project",
                "activity_type": "pull_request",
                "number": 8,
            }
        ]
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=8.0, content_format="surprising_result")
        MockPipeline.return_value.run.return_value = result

        MockBlogWriter.return_value.write_post.return_value = MagicMock(
            success=True,
            file_path="/tmp/fake_site/posts/weekly.md",
            url="https://blog.example.com/weekly",
        )
        MockBlogWriter.return_value.commit_and_push.return_value = True

        import weekly_digest

        weekly_digest.main()

        db.insert_generated_content.assert_called_once()
        kwargs = db.insert_generated_content.call_args.kwargs
        assert kwargs["content_format"] == "surprising_result"
        assert kwargs["source_activity_ids"] == ["my-project#8:pull_request"]
