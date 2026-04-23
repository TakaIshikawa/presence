"""Tests for scripts/eval_pipeline.py — dry-run pipeline evaluation script."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


# --- Helpers ---


def _make_config(embeddings_enabled=True, curated_sources_enabled=True):
    """Create a mock config object with required attributes."""
    config = MagicMock()
    config.anthropic.api_key = "test-api-key"
    config.synthesis.model = "claude-sonnet-4.5"
    config.synthesis.eval_model = "claude-opus-4.6"
    config.synthesis.num_candidates = 5
    config.synthesis.eval_threshold = 0.7
    config.timeouts.anthropic_seconds = 300
    config.paths.claude_logs = "/path/to/logs"
    config.privacy.redaction_patterns = []

    if embeddings_enabled:
        config.embeddings = MagicMock()
        config.embeddings.api_key = "voyage-key"
        config.embeddings.model = "voyage-3"
        config.embeddings.semantic_dedup_threshold = 0.85
    else:
        config.embeddings = None

    if curated_sources_enabled:
        config.curated_sources = MagicMock()
    else:
        config.curated_sources = None

    return config


def _mock_script_context(config, db):
    """Create a mock script_context that yields (config, db)."""
    @contextmanager
    def _ctx():
        yield (config, db)
    return _ctx


def _make_commit(sha="abc123", repo="test-repo", message="feat: add feature"):
    """Create a mock commit dict."""
    return {
        "commit_sha": sha,
        "repo_name": repo,
        "commit_message": message,
    }


def _make_prompt(text="Test prompt"):
    """Create a mock prompt object."""
    prompt = MagicMock()
    prompt.prompt_text = text
    return prompt


def _make_comparison(best_score=8.0, reject_reason=None):
    """Create a mock ComparisonResult."""
    comp = MagicMock()
    comp.ranking = [0]
    comp.best_score = best_score
    comp.groundedness = 8
    comp.rawness = 7
    comp.narrative_specificity = 7
    comp.voice = 7
    comp.engagement_potential = 7
    comp.reject_reason = reject_reason
    comp.improvement = "Add more concrete examples"
    return comp


def _make_refinement(picked="refined", final_score=8.5):
    """Create a mock RefinementResult."""
    ref = MagicMock()
    ref.picked = picked
    ref.final_score = final_score
    return ref


def _make_pipeline_result(
    candidates=None,
    final_content="Test content",
    final_score=8.0,
    filter_stats=None,
    comparison=None,
    refinement=None,
):
    """Create a mock pipeline result."""
    result = MagicMock()
    result.candidates = candidates or ["Candidate 1", "Candidate 2"]
    result.final_content = final_content
    result.final_score = final_score
    result.filter_stats = filter_stats or {"repetition_rejected": 2, "stale_pattern_rejected": 1}
    result.comparison = comparison or _make_comparison()
    result.refinement = refinement
    return result


# --- Tests ---


class TestDefaultArguments:
    def test_default_runs_and_type(self):
        """Default args: --runs=3, --type='x_thread'."""
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        # Should run 3 times (default) with time slices [8, 16, 24]
        assert mock_pipeline.run.call_count == 3

        # All runs should use content_type="x_thread"
        for call in mock_pipeline.run.call_args_list:
            assert call[1]["content_type"] == "x_thread"


class TestCustomArguments:
    def test_custom_runs_argument(self):
        """Custom --runs=2 limits to first 2 time slices."""
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "2"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        # Should run only 2 times (8h, 16h)
        assert mock_pipeline.run.call_count == 2

    def test_custom_type_argument(self):
        """Custom --type=x_post uses x_post content type."""
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--type", "x_post"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        # All runs should use content_type="x_post"
        for call in mock_pipeline.run.call_args_list:
            assert call[1]["content_type"] == "x_post"


class TestSkipsEmptyData:
    def test_skips_when_no_commits(self, capsys):
        """Skips time slice when no commits found."""
        config = _make_config()
        db = MagicMock()
        # First slice: no commits, second slice: has commits
        db.get_commits_in_range.side_effect = [[], [_make_commit()]]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "2"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        # Should only run once (second slice)
        assert mock_pipeline.run.call_count == 1

        # Check output shows skip message
        captured = capsys.readouterr()
        assert "skipped (commits=0" in captured.out

    def test_skips_when_no_prompts(self, capsys):
        """Skips time slice when no prompts found."""
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        # First slice: no prompts, second slice: has prompts
        mock_parser.get_messages_since.side_effect = [[], [_make_prompt()]]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "2"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        # Should only run once (second slice)
        assert mock_pipeline.run.call_count == 1

        # Check output shows skip message
        captured = capsys.readouterr()
        assert "skipped" in captured.out
        assert "prompts=0" in captured.out


class TestPipelineOutput:
    def test_prints_run_header(self, capsys):
        """Prints run header with commits/prompts counts."""
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit(), _make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt(), _make_prompt(), _make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "1"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        captured = capsys.readouterr()
        assert "Run 1/1 — last 8h" in captured.out
        assert "Commits: 2, Prompts: 3" in captured.out
        assert "Content type: x_thread" in captured.out

    def test_prints_avoidance_context(self, capsys):
        """Prints avoidance context when available."""
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = "Recent topic: AI safety\nRecent topic: Testing"
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "1"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        captured = capsys.readouterr()
        assert "AVOIDANCE CONTEXT:" in captured.out
        assert "Recent topic: AI safety" in captured.out
        assert "Recent topic: Testing" in captured.out

    def test_prints_summary_table(self, capsys):
        """Prints summary table when results exist."""
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit(), _make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result(final_score=8.0)

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "2"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        captured = capsys.readouterr()
        assert "SUMMARY" in captured.out
        assert "Run" in captured.out
        assert "Window" in captured.out
        assert "Commits" in captured.out
        assert "Score" in captured.out
        assert "Status" in captured.out
        assert "PASS" in captured.out  # Score 8.0 >= threshold 7.0


class TestEmbedderInitialization:
    def test_initializes_embedder_when_configured(self):
        """Initializes VoyageEmbeddings when config.embeddings is set."""
        config = _make_config(embeddings_enabled=True)
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("knowledge.embeddings.VoyageEmbeddings") as MockEmbeddings, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "1"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

            # Verify embedder was initialized
            MockEmbeddings.assert_called_once_with(
                api_key="voyage-key",
                model="voyage-3"
            )

            # Verify pipeline was initialized with embedder and threshold
            call_kwargs = MockPipeline.call_args[1]
            assert call_kwargs["embedder"] is not None
            assert call_kwargs["semantic_threshold"] == 0.85

    def test_no_embedder_when_not_configured(self):
        """Does not initialize embedder when config.embeddings is None."""
        config = _make_config(embeddings_enabled=False)
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "1"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

            # Verify pipeline was initialized without embedder
            call_kwargs = MockPipeline.call_args[1]
            assert call_kwargs["embedder"] is None
            assert call_kwargs["semantic_threshold"] == 0.82  # Default


class TestKnowledgeStoreInitialization:
    def test_initializes_knowledge_store_when_configured(self):
        """Initializes KnowledgeStore when embedder and curated_sources configured."""
        config = _make_config(embeddings_enabled=True, curated_sources_enabled=True)
        db = MagicMock()
        db.conn = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("knowledge.embeddings.VoyageEmbeddings") as MockEmbeddings, \
             patch("knowledge.store.KnowledgeStore") as MockKnowledgeStore, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "1"]):

            mock_embedder = MagicMock()
            MockEmbeddings.return_value = mock_embedder

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

            # Verify knowledge store was initialized
            MockKnowledgeStore.assert_called_once_with(db.conn, mock_embedder)

            # Verify pipeline was initialized with knowledge_store
            call_kwargs = MockPipeline.call_args[1]
            assert call_kwargs["knowledge_store"] is not None

    def test_no_knowledge_store_without_embedder(self):
        """Does not initialize KnowledgeStore when embedder is None."""
        config = _make_config(embeddings_enabled=False, curated_sources_enabled=True)
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "1"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

            # Verify pipeline was initialized without knowledge_store
            call_kwargs = MockPipeline.call_args[1]
            assert call_kwargs["knowledge_store"] is None


class TestPipelineInitialization:
    def test_pipeline_run_called_with_correct_data(self):
        """Pipeline.run called with commits, prompts, content_type, threshold."""
        config = _make_config()
        db = MagicMock()
        commits = [
            _make_commit(sha="abc123", message="feat: add feature"),
            _make_commit(sha="def456", message="fix: bug fix"),
        ]
        db.get_commits_in_range.return_value = commits

        mock_parser = MagicMock()
        prompts = [_make_prompt("Prompt 1"), _make_prompt("Prompt 2")]
        mock_parser.get_messages_since.return_value = prompts

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "1", "--type", "x_post"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

            # Verify pipeline.run was called correctly
            mock_pipeline.run.assert_called_once()
            call_kwargs = mock_pipeline.run.call_args[1]

            assert call_kwargs["prompts"] == ["Prompt 1", "Prompt 2"]
            assert call_kwargs["commits"] == [
                {"sha": "abc123", "repo_name": "test-repo", "message": "feat: add feature"},
                {"sha": "def456", "repo_name": "test-repo", "message": "fix: bug fix"},
            ]
            assert call_kwargs["content_type"] == "x_post"
            assert call_kwargs["threshold"] == 0.7


class TestRecording:
    def test_default_does_not_record(self):
        """Default dry run preserves no-write behavior."""
        config = _make_config()
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result()

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "1"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        db.create_eval_batch.assert_not_called()
        db.add_eval_result.assert_not_called()

    def test_record_creates_batch_and_per_window_result(self):
        """--record stores batch metadata and window result."""
        config = _make_config()
        config.privacy.redaction_patterns = [
            {"name": "secret", "pattern": "SECRET123", "placeholder": "[REDACTED]"}
        ]
        db = MagicMock()
        db.create_eval_batch.return_value = 42
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result(
            final_content="Generated content with SECRET123",
            final_score=8.4,
            filter_stats={"repetition_rejected": 1},
        )

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--runs", "1", "--record", "--label", "baseline"]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        db.create_eval_batch.assert_called_once_with(
            label="baseline",
            content_type="x_thread",
            generator_model="claude-sonnet-4.5",
            evaluator_model="claude-opus-4.6",
            threshold=0.7,
        )
        db.add_eval_result.assert_called_once()
        kwargs = db.add_eval_result.call_args[1]
        assert kwargs["batch_id"] == 42
        assert kwargs["source_window_hours"] == 8
        assert kwargs["prompt_count"] == 1
        assert kwargs["commit_count"] == 1
        assert kwargs["candidate_count"] == 2
        assert kwargs["final_score"] == 8.4
        assert kwargs["filter_stats"] == {"repetition_rejected": 1}
        assert kwargs["final_content"] == "Generated content with [REDACTED]"


class TestJsonArtifactOutput:
    def test_single_run_writes_json_artifact_with_redacted_content(self, tmp_path):
        """--out writes a machine-readable summary and redacts final content."""
        config = _make_config()
        config.privacy.redaction_patterns = [
            {"name": "secret", "pattern": "SECRET123", "placeholder": "[REDACTED]"}
        ]
        db = MagicMock()
        db.get_commits_in_range.return_value = [_make_commit()]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.return_value = [_make_prompt()]

        mock_pipeline = MagicMock()
        mock_pipeline._build_avoidance_context.return_value = ""
        mock_pipeline.run.return_value = _make_pipeline_result(
            final_content="Generated content with SECRET123",
            final_score=8.4,
            filter_stats={"repetition_rejected": 1},
        )

        artifact_path = tmp_path / "eval_artifact.json"

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", [
                 "eval_pipeline.py",
                 "--runs",
                 "1",
                 "--out",
                 str(artifact_path),
             ]):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.return_value = mock_pipeline

            import eval_pipeline
            eval_pipeline.main()

        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        assert payload["schema_version"] == 1
        assert payload["mode"] == "single"
        assert payload["content_type"] == "x_thread"
        assert payload["source_windows"] == [
            {"run": 1, "hours": 8, "commit_count": 1, "prompt_count": 1}
        ]
        assert payload["aggregate"]["run_count"] == 1
        assert payload["aggregate"]["average_score"] == 8.4
        assert payload["runs"][0]["final_content"] == "Generated content with [REDACTED]"
        assert "SECRET123" not in artifact_path.read_text(encoding="utf-8")

    def test_matrix_writes_json_artifact_with_variant_aggregates(self, tmp_path):
        """--matrix writes ranked variant summaries and per-run details."""
        config = _make_config(embeddings_enabled=False)
        config.synthesis.num_candidates = 4
        db = MagicMock()
        db.create_eval_batch.side_effect = [101, 102]
        db.get_commits_in_range.side_effect = [
            [_make_commit(sha="a", message="feat: first window")],
            [_make_commit(sha="b", message="feat: second window")],
        ]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.side_effect = [
            [_make_prompt("Prompt window 1")],
            [_make_prompt("Prompt window 2")],
        ]

        prompt_a = tmp_path / "prompt_a.txt"
        prompt_b = tmp_path / "prompt_b.txt"
        prompt_a.write_text("Prompt A {prompts} {commits} {commit_count}")
        prompt_b.write_text("Prompt B {prompts} {commits} {commit_count}")

        pipeline_a = MagicMock()
        pipeline_b = MagicMock()
        for pipeline, score, secret in (
            (pipeline_a, 9.0, "SECRET_A"),
            (pipeline_b, 7.0, "SECRET_B"),
        ):
            pipeline.generator.CONTENT_TYPE_CONFIG = {
                "x_thread": {"template": "x_thread_v2", "max_tokens": 2000},
                "x_post": {"template": "x_post_v2", "max_tokens": 150},
            }
            pipeline.run.return_value = _make_pipeline_result(
                candidates=["Candidate 1", "Candidate 2"],
                final_content=f"Final {secret}",
                final_score=score,
            )

        config.privacy.redaction_patterns = [
            {"name": "secret", "pattern": r"SECRET_[AB]", "placeholder": "[REDACTED]"}
        ]

        artifact_path = tmp_path / "matrix.json"

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch(
                 "sys.argv",
                 [
                     "eval_pipeline.py",
                     "--matrix",
                     "--runs",
                     "2",
                     "--label",
                     "parent",
                     "--out",
                     str(artifact_path),
                     "--variant",
                     f"a:{prompt_a}:gen-a:eval-a",
                     "--variant",
                     f"b:{prompt_b}:gen-b:eval-b",
                 ],
             ):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.side_effect = [pipeline_a, pipeline_b]

            import eval_pipeline
            eval_pipeline.main()

        payload = json.loads(artifact_path.read_text(encoding="utf-8"))
        assert payload["schema_version"] == 1
        assert payload["mode"] == "matrix"
        assert payload["aggregate"]["variant_count"] == 2
        assert payload["aggregate"]["run_count"] == 4
        assert payload["variants"][0]["name"] == "a"
        assert payload["variants"][0]["runs"][0]["final_content"].endswith("[REDACTED]")
        assert payload["variants"][0]["prompt_file"] == str(prompt_a)
        assert payload["variants"][0]["aggregate"]["run_count"] == 2
        assert payload["variants"][0]["aggregate"]["average_score"] == 9.0
        assert payload["variants"][1]["aggregate"]["average_score"] == 7.0
        assert "SECRET_A" not in artifact_path.read_text(encoding="utf-8")
        assert "SECRET_B" not in artifact_path.read_text(encoding="utf-8")

    def test_list_batches_reads_without_pipeline(self, capsys):
        """--list prints recent batches without running synthesis."""
        config = _make_config()
        db = MagicMock()
        db.list_eval_batches.return_value = [
            {
                "id": 7,
                "created_at": "2026-04-23 10:00:00",
                "content_type": "x_post",
                "label": "baseline",
                "result_count": 2,
                "average_score": 7.5,
                "best_score": 8.1,
            }
        ]

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--list"]):

            mock_ctx.return_value = _mock_script_context(config, db)()

            import eval_pipeline
            eval_pipeline.main()

        captured = capsys.readouterr()
        assert "baseline" in captured.out
        assert "x_post" in captured.out
        db.list_eval_batches.assert_called_once()
        MockPipeline.assert_not_called()

    def test_show_batch_reads_results_without_pipeline(self, capsys):
        """--show prints one batch with stored results."""
        config = _make_config()
        db = MagicMock()
        db.get_eval_batch.return_value = {
            "batch": {
                "id": 7,
                "label": "baseline",
                "created_at": "2026-04-23 10:00:00",
                "content_type": "x_thread",
                "generator_model": "gen",
                "evaluator_model": "eval",
                "threshold": 0.7,
            },
            "results": [
                {
                    "id": 9,
                    "source_window_hours": 8,
                    "prompt_count": 3,
                    "commit_count": 2,
                    "candidate_count": 4,
                    "final_score": 8.0,
                    "threshold": 0.7,
                    "rejection_reason": None,
                    "filter_stats": {"stale_pattern_rejected": 1},
                    "final_content": "Stored content",
                }
            ],
        }

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch("sys.argv", ["eval_pipeline.py", "--show", "7"]):

            mock_ctx.return_value = _mock_script_context(config, db)()

            import eval_pipeline
            eval_pipeline.main()

        captured = capsys.readouterr()
        assert "Batch 7" in captured.out
        assert "Stored content" in captured.out
        assert "stale_pattern_rejected" in captured.out
        db.get_eval_batch.assert_called_once_with(7)
        MockPipeline.assert_not_called()


class TestMatrixMode:
    def test_matrix_reuses_identical_source_windows_for_each_variant(self, tmp_path, capsys):
        """--matrix selects source windows once, then reuses them for every variant."""
        config = _make_config(embeddings_enabled=False)
        config.synthesis.num_candidates = 4
        db = MagicMock()
        db.create_eval_batch.side_effect = [101, 102]
        db.get_commits_in_range.side_effect = [
            [_make_commit(sha="a", message="feat: first window")],
            [_make_commit(sha="b", message="feat: second window")],
        ]

        mock_parser = MagicMock()
        mock_parser.get_messages_since.side_effect = [
            [_make_prompt("Prompt window 1")],
            [_make_prompt("Prompt window 2")],
        ]

        prompt_a = tmp_path / "prompt_a.txt"
        prompt_b = tmp_path / "prompt_b.txt"
        prompt_a.write_text("Prompt A {prompts} {commits} {commit_count}")
        prompt_b.write_text("Prompt B {prompts} {commits} {commit_count}")

        pipeline_a = MagicMock()
        pipeline_b = MagicMock()
        for pipeline in (pipeline_a, pipeline_b):
            pipeline.generator.CONTENT_TYPE_CONFIG = {
                "x_thread": {"template": "x_thread_v2", "max_tokens": 2000},
                "x_post": {"template": "x_post_v2", "max_tokens": 150},
            }
            pipeline.run.return_value = _make_pipeline_result(
                candidates=["Candidate 1", "Candidate 2"],
                final_score=8.0,
            )

        with patch("eval_pipeline.script_context") as mock_ctx, \
             patch("eval_pipeline.ClaudeLogParser") as MockLogParser, \
             patch("eval_pipeline.SynthesisPipeline") as MockPipeline, \
             patch(
                 "sys.argv",
                 [
                     "eval_pipeline.py",
                     "--matrix",
                     "--runs",
                     "2",
                     "--label",
                     "parent",
                     "--variant",
                     f"a:{prompt_a}:gen-a:eval-a",
                     "--variant",
                     f"b:{prompt_b}:gen-b:eval-b",
                 ],
             ):

            mock_ctx.return_value = _mock_script_context(config, db)()
            MockLogParser.return_value = mock_parser
            MockPipeline.side_effect = [pipeline_a, pipeline_b]

            import eval_pipeline
            eval_pipeline.main()

        assert db.get_commits_in_range.call_count == 2
        assert mock_parser.get_messages_since.call_count == 2
        assert pipeline_a.run.call_count == 2
        assert pipeline_b.run.call_count == 2

        for idx in range(2):
            assert pipeline_a.run.call_args_list[idx][1]["prompts"] == (
                pipeline_b.run.call_args_list[idx][1]["prompts"]
            )
            assert pipeline_a.run.call_args_list[idx][1]["commits"] == (
                pipeline_b.run.call_args_list[idx][1]["commits"]
            )

        pipeline_a.generator.set_prompt_file_override.assert_called_once_with(
            "x_thread_v2", prompt_a
        )
        pipeline_b.generator.set_prompt_file_override.assert_called_once_with(
            "x_thread_v2", prompt_b
        )
        assert db.create_eval_batch.call_args_list[0][1]["label"] == "parent/a"
        assert db.create_eval_batch.call_args_list[1][1]["label"] == "parent/b"
        assert db.add_eval_result.call_count == 4

        captured = capsys.readouterr()
        assert "MATRIX SUMMARY" in captured.out
        assert "Survive" in captured.out
