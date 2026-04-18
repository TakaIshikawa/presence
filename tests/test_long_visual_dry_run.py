"""Tests for long and visual post dry-run script behavior."""

import types
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# long_post imports Bluesky support eagerly; stub atproto for test environments
# that do not install that optional dependency.
fake_atproto = types.ModuleType("atproto")
fake_atproto.Client = object
fake_atproto_exceptions = types.ModuleType("atproto.exceptions")
fake_atproto_exceptions.AtProtocolError = Exception
sys.modules.setdefault("atproto", fake_atproto)
sys.modules.setdefault("atproto.exceptions", fake_atproto_exceptions)

from synthesis.evaluator_v2 import ComparisonResult
from synthesis.pipeline import PipelineResult
from synthesis.visual_pipeline import VisualPipelineResult
from synthesis.image_generator import GeneratedImage


FIXED_NOW = datetime(2026, 4, 18, 12, 0, 0, tzinfo=timezone.utc)
FIXED_TODAY = FIXED_NOW.replace(hour=0, minute=0, second=0, microsecond=0)
PROMPT_TS = FIXED_TODAY.replace(hour=3)


def _mock_script_context(config, db):
    @contextmanager
    def _ctx():
        yield (config, db)
    return _ctx


def _make_prompt_message():
    msg = MagicMock()
    msg.timestamp = PROMPT_TS
    msg.prompt_text = "Refined the content pipeline prompts"
    msg.message_uuid = "uuid-dry-run"
    return msg


def _make_commit_row():
    return {
        "repo_name": "presence",
        "commit_message": "feat: add dry-run support",
        "commit_sha": "abc123",
    }


def _make_comparison(best_score=8.2):
    return ComparisonResult(
        ranking=[0],
        best_score=best_score,
        groundedness=8.0,
        rawness=7.0,
        narrative_specificity=8.0,
        voice=7.0,
        engagement_potential=7.5,
        best_feedback="Strong candidate",
        improvement="",
        reject_reason=None,
        raw_response="",
    )


def _make_pipeline_result(content_type, final_content, final_score=8.4):
    return PipelineResult(
        batch_id="batch-dry-run",
        candidates=[final_content],
        comparison=_make_comparison(final_score),
        refinement=None,
        final_content=final_content,
        final_score=final_score,
        source_prompts=["prompt"],
        source_commits=["commit"],
        content_format="test_format",
    )


def _make_long_post_config():
    config = MagicMock()
    config.paths.database = ":memory:"
    config.paths.claude_logs = "/tmp/fake_logs"
    config.anthropic.api_key = "test-key"
    config.synthesis.model = "gen-model"
    config.synthesis.eval_model = "eval-model"
    config.synthesis.num_candidates = 3
    config.synthesis.eval_threshold = 0.7
    config.timeouts.anthropic_seconds = 300
    config.embeddings = None
    config.curated_sources = None
    config.bluesky = None
    config.historical = None
    config.x.api_key = "x-key"
    config.x.api_secret = "x-secret"
    config.x.access_token = "x-token"
    config.x.access_token_secret = "x-token-secret"
    return config


def _make_visual_post_config():
    config = _make_long_post_config()
    config.image_gen.provider = "pillow"
    config.image_gen.output_dir = "/tmp/presence-images"
    return config


class TestLongPostDryRun:
    @patch("long_post.datetime", wraps=datetime)
    @patch("long_post.update_monitoring")
    @patch("long_post.XClient")
    @patch("long_post.ClaudeLogParser")
    @patch("long_post.SynthesisPipeline")
    @patch("long_post.script_context")
    def test_skips_publish_with_dry_run(
        self, mock_ctx, MockPipeline, MockParser, MockXClient, mock_monitoring, mock_dt
    ):
        config = _make_long_post_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 42
        mock_ctx.return_value = _mock_script_context(config, db)()
        mock_dt.now.return_value = FIXED_NOW
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]
        MockPipeline.return_value.run.return_value = _make_pipeline_result(
            "x_long_post",
            "Long-form dry run content " * 20,
        )

        import long_post

        original_argv = sys.argv
        try:
            sys.argv = ["long_post.py", "--dry-run"]
            long_post.main()
        finally:
            sys.argv = original_argv

        MockXClient.return_value.post.assert_not_called()
        db.mark_published.assert_not_called()
        db.insert_generated_content.assert_called_once()
        assert db.insert_pipeline_run.call_args.kwargs["outcome"] == "dry_run"
        mock_monitoring.assert_called_once_with("run-long-post")


class TestVisualPostDryRun:
    @patch("visual_post.datetime", wraps=datetime)
    @patch("visual_post.update_monitoring")
    @patch("visual_post.XClient")
    @patch("visual_post.VisualPipeline")
    @patch("visual_post.ClaudeLogParser")
    @patch("visual_post.SynthesisPipeline")
    @patch("visual_post.script_context")
    def test_skips_publish_with_dry_run(
        self,
        mock_ctx,
        MockPipelineFactory,
        MockParser,
        MockVisualPipeline,
        MockXClient,
        mock_monitoring,
        mock_dt,
    ):
        config = _make_visual_post_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit_row()]
        db.insert_generated_content.return_value = 43
        mock_ctx.return_value = _mock_script_context(config, db)()
        mock_dt.now.return_value = FIXED_NOW
        mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)

        MockParser.return_value.parse_global_history.return_value = [_make_prompt_message()]
        MockVisualPipeline.return_value.run.return_value = VisualPipelineResult(
            pipeline_result=_make_pipeline_result("x_visual", "Visual dry run post"),
            image=GeneratedImage(
                path="/tmp/presence-images/test.png",
                prompt_used="Insight",
                provider="pillow",
                style="annotated",
            ),
            image_prompt="ANNOTATED | Insight | Visual dry run post",
        )

        import visual_post

        original_argv = sys.argv
        try:
            sys.argv = ["visual_post.py", "--dry-run"]
            visual_post.main()
        finally:
            sys.argv = original_argv

        MockXClient.return_value.post_with_media.assert_not_called()
        db.mark_published.assert_not_called()
        db.insert_generated_content.assert_called_once()
        assert db.insert_pipeline_run.call_args.kwargs["outcome"] == "dry_run"
        mock_monitoring.assert_called_once_with("run-visual-post")
