"""Tests for scripts/daily_digest.py script-level logic."""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from synthesis.evaluator_v2 import ComparisonResult
from synthesis.pipeline import PipelineResult

# Fixed "now" for deterministic date ranges.
# daily_digest computes: today = midnight UTC, tomorrow = today + 1 day
# Prompt timestamps must satisfy: today <= ts < tomorrow
FIXED_NOW = datetime(2026, 4, 5, 12, 0, 0, tzinfo=timezone.utc)
FIXED_TODAY = FIXED_NOW.replace(hour=0, minute=0, second=0, microsecond=0)
PROMPT_TS = FIXED_TODAY + timedelta(hours=6)  # midday, inside [today, tomorrow)


# --- Helpers ---


def _make_config(historical_enabled=False):
    config = MagicMock()
    config.paths.database = ":memory:"
    config.paths.claude_logs = "/tmp/fake_logs"
    config.anthropic.api_key = "test-key"
    config.synthesis.model = "gen-model"
    config.synthesis.eval_model = "eval-model"
    config.synthesis.num_candidates = 3
    config.synthesis.eval_threshold = 0.7
    config.x.api_key = "xk"
    config.x.api_secret = "xs"
    config.x.access_token = "xt"
    config.x.access_token_secret = "xts"
    if historical_enabled:
        config.historical.enabled = True
        config.historical.injection_frequency = 3
        config.historical.lookback_days = 180
        config.historical.min_age_days = 30
        config.historical.max_historical_commits = 5
    else:
        config.historical = None
    return config


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


def _make_pipeline_result(final_score=8.0, reject_reason=None):
    comparison = _make_comparison(best_score=final_score, reject_reason=reject_reason)
    return PipelineResult(
        batch_id="abcd1234",
        candidates=["Candidate A", "Candidate B", "Candidate C"],
        comparison=comparison,
        refinement=None,
        final_content="TWEET 1: Some thread content\nTWEET 2: More content",
        final_score=final_score,
        source_prompts=["prompt1"],
        source_commits=["commit msg"],
    )


def _make_prompt_message():
    msg = MagicMock()
    msg.timestamp = PROMPT_TS
    msg.prompt_text = "Worked on error handling"
    msg.message_uuid = "uuid-123"
    return msg


def _make_commit_row():
    return {
        "repo_name": "my-project",
        "commit_message": "fix: handle timeout",
        "commit_sha": "abc123",
    }


# Shared decorator stack for patching daily_digest dependencies.
# Applied bottom-up: load_config, Database, SynthesisPipeline,
# ClaudeLogParser, XClient, parse_thread_content, _update_monitoring, datetime
def _daily_patches(func):
    @patch(f"daily_digest.datetime", wraps=datetime)
    @patch("daily_digest._update_monitoring")
    @patch("daily_digest.parse_thread_content")
    @patch("daily_digest.XClient")
    @patch("daily_digest.ClaudeLogParser")
    @patch("daily_digest.SynthesisPipeline")
    @patch("daily_digest.Database")
    @patch("daily_digest.load_config")
    def wrapper(self, mock_load_config, MockDB, MockPipeline, MockParser,
                MockXClient, mock_parse_thread, mock_monitoring, mock_dt,
                *args, **kwargs):
        # Pin datetime.now() so date ranges are deterministic
        mock_dt.now.return_value = FIXED_NOW
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
        return func(
            self,
            mock_load_config=mock_load_config,
            MockDB=MockDB,
            MockPipeline=MockPipeline,
            MockParser=MockParser,
            MockXClient=MockXClient,
            mock_parse_thread=mock_parse_thread,
            mock_monitoring=mock_monitoring,
        )
    return wrapper


# --- Tests ---


class TestMainExitsEarlyNoCommits:
    @_daily_patches
    def test_exits_early_when_no_commits(
        self, *, mock_load_config, MockDB, MockPipeline, MockParser,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        mock_load_config.return_value = _make_config()
        db = MockDB.return_value
        db.get_commits_in_range.return_value = []

        import daily_digest
        daily_digest.main()

        db.get_commits_in_range.assert_called_once()
        MockParser.assert_not_called()
        MockPipeline.return_value.run.assert_not_called()
        db.close.assert_called_once()


class TestMainExitsEarlyNoPrompts:
    @_daily_patches
    def test_exits_early_when_no_prompts(
        self, *, mock_load_config, MockDB, MockPipeline, MockParser,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        mock_load_config.return_value = _make_config()
        db = MockDB.return_value
        db.get_commits_in_range.return_value = [_make_commit_row()]

        # Parser returns no prompts in range
        MockParser.return_value.parse_global_history.return_value = []

        import daily_digest
        daily_digest.main()

        MockPipeline.return_value.run.assert_not_called()
        db.close.assert_called_once()


class TestMainPostsWhenPassesThreshold:
    @_daily_patches
    def test_runs_pipeline_and_posts(
        self, *, mock_load_config, MockDB, MockPipeline, MockParser,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        mock_load_config.return_value = _make_config()
        db = MockDB.return_value
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 42

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=8.0)
        MockPipeline.return_value.run.return_value = result

        mock_parse_thread.return_value = ["tweet1", "tweet2"]
        post_result = MagicMock(success=True, url="https://x.com/thread/123", tweet_id="tw123")
        MockXClient.return_value.post_thread.return_value = post_result

        import daily_digest
        daily_digest.main()

        MockPipeline.return_value.run.assert_called_once()
        assert MockPipeline.return_value.run.call_args[1]["content_type"] == "x_thread"
        mock_parse_thread.assert_called_once_with(result.final_content)
        MockXClient.return_value.post_thread.assert_called_once_with(["tweet1", "tweet2"])
        db.mark_published.assert_called_once_with(42, "https://x.com/thread/123", tweet_id="tw123")
        db.close.assert_called_once()


class TestMainDoesNotPostBelowThreshold:
    @_daily_patches
    def test_does_not_post_below_threshold(
        self, *, mock_load_config, MockDB, MockPipeline, MockParser,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        mock_load_config.return_value = _make_config()
        db = MockDB.return_value
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 42

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=5.0)
        MockPipeline.return_value.run.return_value = result

        import daily_digest
        daily_digest.main()

        mock_parse_thread.assert_not_called()
        MockXClient.return_value.post_thread.assert_not_called()
        db.mark_published.assert_not_called()
        db.close.assert_called_once()


class TestMainDoesNotPostWhenRejected:
    @_daily_patches
    def test_does_not_post_when_reject_reason_set(
        self, *, mock_load_config, MockDB, MockPipeline, MockParser,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        mock_load_config.return_value = _make_config()
        db = MockDB.return_value
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 42

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=5.0, reject_reason="All candidates too generic")
        MockPipeline.return_value.run.return_value = result

        import daily_digest
        daily_digest.main()

        mock_parse_thread.assert_not_called()
        MockXClient.return_value.post_thread.assert_not_called()
        db.mark_published.assert_not_called()


class TestHistoricalContextInjection:
    @_daily_patches
    def test_historical_context_injected_when_enabled(
        self, *, mock_load_config, MockDB, MockPipeline, MockParser,
        MockXClient, mock_parse_thread, mock_monitoring,
    ):
        mock_load_config.return_value = _make_config(historical_enabled=True)
        db = MockDB.return_value
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 42

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]

        result = _make_pipeline_result(final_score=5.0)
        MockPipeline.return_value.run.return_value = result

        mock_ctx = MagicMock()
        mock_ctx.theme_description = "Error handling patterns"
        mock_ctx.commits = [{"sha": "old123", "message": "old commit", "repo_name": "old-repo"}]

        with patch("synthesis.theme_selector.ThemeSelector") as MockThemeSelector:
            mock_ts = MockThemeSelector.return_value
            mock_ts.should_inject.return_value = True
            mock_ts.select.return_value = mock_ctx

            import daily_digest
            daily_digest.main()

            mock_ts.should_inject.assert_called_once_with("x_thread", 3)
            mock_ts.select.assert_called_once()

            run_call = MockPipeline.return_value.run.call_args
            commits_passed = run_call[1]["commits"]
            historical = [c for c in commits_passed if c.get("historical")]
            assert len(historical) == 1
            assert historical[0]["sha"] == "old123"


class TestUpdateMonitoring:
    def test_handles_missing_sync_script(self, tmp_path):
        """When the sync script doesn't exist, subprocess.run is not called."""
        import daily_digest

        with patch("daily_digest.Path") as MockPath:
            # Make Path(__file__).parent / "update_operations_state.py" return
            # a path whose .exists() is False
            mock_script = MagicMock()
            mock_script.exists.return_value = False
            MockPath.return_value.parent.__truediv__ = MagicMock(return_value=mock_script)

            with patch("daily_digest.subprocess.run") as mock_run:
                daily_digest._update_monitoring()
                mock_run.assert_not_called()

    def test_handles_exception_gracefully(self):
        """Exceptions inside _update_monitoring are swallowed."""
        import daily_digest

        with patch("daily_digest.Path") as MockPath:
            MockPath.side_effect = OSError("broken")
            # Should not raise
            daily_digest._update_monitoring()
