"""Tests for the content refiner (synthesis.refiner)."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from synthesis.refiner import ContentRefiner, RefinementResult


# --- RefinementResult dataclass ---


class TestRefinementResult:
    def test_fields_stored(self):
        result = RefinementResult(
            original="orig",
            refined="ref",
            picked="REFINED",
            final_score=8.5,
            final_content="ref",
        )
        assert result.original == "orig"
        assert result.refined == "ref"
        assert result.picked == "REFINED"
        assert result.final_score == 8.5
        assert result.final_content == "ref"

    def test_picked_original(self):
        result = RefinementResult(
            original="orig",
            refined="ref",
            picked="ORIGINAL",
            final_score=6.0,
            final_content="orig",
        )
        assert result.picked == "ORIGINAL"
        assert result.final_content == "orig"


# --- FORMAT_CONSTRAINTS ---


class TestFormatConstraints:
    def test_x_post_constraints(self):
        constraints, max_tokens = ContentRefiner.FORMAT_CONSTRAINTS["x_post"]
        assert "280 characters" in constraints
        assert max_tokens == 500

    def test_x_thread_constraints(self):
        constraints, max_tokens = ContentRefiner.FORMAT_CONSTRAINTS["x_thread"]
        assert "280 characters" in constraints
        assert "TWEET N:" in constraints
        assert max_tokens == 2000

    def test_blog_post_constraints(self):
        constraints, max_tokens = ContentRefiner.FORMAT_CONSTRAINTS["blog_post"]
        assert "800-1200 words" in constraints
        assert max_tokens == 4000

    def test_unknown_type_falls_back_to_x_post(self):
        fallback = ContentRefiner.FORMAT_CONSTRAINTS.get(
            "unknown_type", ContentRefiner.FORMAT_CONSTRAINTS["x_post"]
        )
        expected = ContentRefiner.FORMAT_CONSTRAINTS["x_post"]
        assert fallback == expected


# --- Helpers ---


def _make_refiner():
    """Create a ContentRefiner with mocked Anthropic clients."""
    with patch("synthesis.refiner.anthropic.Anthropic") as mock_cls:
        refine_client = MagicMock()
        gate_client = MagicMock()
        mock_cls.side_effect = [refine_client, gate_client]
        refiner = ContentRefiner(
            refine_api_key="refine-key",
            refine_model="claude-sonnet",
            gate_api_key="gate-key",
            gate_model="claude-opus",
        )
    return refiner, refine_client, gate_client


def _mock_response(text: str) -> MagicMock:
    """Build a mock Anthropic message response."""
    resp = MagicMock()
    resp.content = [MagicMock(text=text)]
    return resp


# --- _final_gate parsing ---


class TestFinalGateParsing:
    @pytest.fixture
    def refiner(self):
        refiner, _, _ = _make_refiner()
        return refiner

    def test_pick_refined(self, refiner):
        refiner.gate_client.messages.create.return_value = _mock_response(
            "PICK: REFINED\nSCORE: 8.0"
        )
        result = refiner._final_gate("original text", "refined text")
        assert result.picked == "REFINED"
        assert result.final_content == "refined text"

    def test_pick_original(self, refiner):
        refiner.gate_client.messages.create.return_value = _mock_response(
            "PICK: ORIGINAL\nSCORE: 6.5"
        )
        result = refiner._final_gate("original text", "refined text")
        assert result.picked == "ORIGINAL"
        assert result.final_content == "original text"

    def test_missing_pick_defaults_to_refined(self, refiner):
        refiner.gate_client.messages.create.return_value = _mock_response(
            "SCORE: 7.0\nThe refined version is better."
        )
        result = refiner._final_gate("original text", "refined text")
        assert result.picked == "REFINED"
        assert result.final_content == "refined text"

    def test_score_integer(self, refiner):
        refiner.gate_client.messages.create.return_value = _mock_response(
            "PICK: REFINED\nSCORE: 9"
        )
        result = refiner._final_gate("orig", "ref")
        assert result.final_score == 9.0

    def test_score_float(self, refiner):
        refiner.gate_client.messages.create.return_value = _mock_response(
            "PICK: REFINED\nSCORE: 7.5"
        )
        result = refiner._final_gate("orig", "ref")
        assert result.final_score == 7.5

    def test_missing_score_defaults_to_5(self, refiner):
        refiner.gate_client.messages.create.return_value = _mock_response(
            "PICK: ORIGINAL\nThe original is better."
        )
        result = refiner._final_gate("orig", "ref")
        assert result.final_score == 5.0

    def test_case_insensitive_pick(self, refiner):
        refiner.gate_client.messages.create.return_value = _mock_response(
            "PICK: original\nSCORE: 6.0"
        )
        result = refiner._final_gate("orig", "ref")
        assert result.picked == "ORIGINAL"
        assert result.final_content == "orig"

    def test_result_preserves_original_and_refined(self, refiner):
        refiner.gate_client.messages.create.return_value = _mock_response(
            "PICK: REFINED\nSCORE: 8.0"
        )
        result = refiner._final_gate("the original", "the refined")
        assert result.original == "the original"
        assert result.refined == "the refined"


# --- _refine ---


class TestRefine:
    def test_refine_calls_api_with_template(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("  improved content  ")

        with patch.object(
            type(refiner).PROMPTS_DIR.__class__,
            "__truediv__",
            wraps=refiner.PROMPTS_DIR.__truediv__,
        ):
            # Mock the template read
            template = (
                "Refine this:\n{content}\n{best_feedback}\n"
                "{improvement}\n{format_constraints}"
            )
            with patch("pathlib.Path.read_text", return_value=template):
                result = refiner._refine(
                    content="draft post",
                    best_feedback="good hook",
                    improvement="add specifics",
                    content_type="x_post",
                )

        assert result == "improved content"  # stripped
        call_args = refine_client.messages.create.call_args
        assert call_args[1]["model"] == "claude-sonnet"
        assert call_args[1]["max_tokens"] == 500

    def test_refine_uses_content_type_max_tokens(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("refined blog")

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine("draft", "feedback", "improve", "blog_post")

        call_args = refine_client.messages.create.call_args
        assert call_args[1]["max_tokens"] == 4000

    def test_refine_unknown_content_type_uses_x_post_tokens(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("refined")

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine("draft", "feedback", "improve", "unknown_type")

        call_args = refine_client.messages.create.call_args
        assert call_args[1]["max_tokens"] == 500  # x_post fallback


# --- refine_and_gate end-to-end ---


class TestRefineAndGate:
    def test_calls_refine_then_final_gate(self):
        refiner, refine_client, gate_client = _make_refiner()

        # _refine returns refined text
        refine_client.messages.create.return_value = _mock_response("refined post")
        # _final_gate returns pick + score
        gate_client.messages.create.return_value = _mock_response(
            "PICK: REFINED\nSCORE: 8.5"
        )

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        gate_template = "ORIGINAL:\n{original}\nREFINED:\n{refined}"

        with patch("pathlib.Path.read_text", side_effect=[template, gate_template]):
            result = refiner.refine_and_gate(
                content="original post",
                best_feedback="strong opening",
                improvement="tighten ending",
                content_type="x_post",
            )

        assert isinstance(result, RefinementResult)
        assert result.original == "original post"
        assert result.refined == "refined post"
        assert result.picked == "REFINED"
        assert result.final_score == 8.5
        assert result.final_content == "refined post"

        # Both clients called exactly once
        refine_client.messages.create.assert_called_once()
        gate_client.messages.create.assert_called_once()

    def test_gate_picks_original_over_refined(self):
        refiner, refine_client, gate_client = _make_refiner()

        refine_client.messages.create.return_value = _mock_response("worse version")
        gate_client.messages.create.return_value = _mock_response(
            "PICK: ORIGINAL\nSCORE: 5.5"
        )

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        gate_template = "{original}\n{refined}"

        with patch("pathlib.Path.read_text", side_effect=[template, gate_template]):
            result = refiner.refine_and_gate(
                content="good original",
                best_feedback="feedback",
                improvement="improve",
            )

        assert result.picked == "ORIGINAL"
        assert result.final_content == "good original"
        assert result.final_score == 5.5


# --- Prompt template loading ---


class TestPromptLoading:
    def test_refine_loads_refiner_template(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("output")

        original_read_text = Path.read_text
        read_paths = []

        def tracking_read_text(self_path, *args, **kwargs):
            read_paths.append(str(self_path))
            return "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"

        with patch.object(Path, "read_text", tracking_read_text):
            refiner._refine("c", "f", "i", "x_post")

        assert any("refiner.txt" in p for p in read_paths)

    def test_final_gate_loads_final_gate_template(self):
        refiner, _, gate_client = _make_refiner()
        gate_client.messages.create.return_value = _mock_response(
            "PICK: REFINED\nSCORE: 7.0"
        )

        read_paths = []

        def tracking_read_text(self_path, *args, **kwargs):
            read_paths.append(str(self_path))
            return "{original}\n{refined}"

        with patch.object(Path, "read_text", tracking_read_text):
            refiner._final_gate("orig", "ref")

        assert any("final_gate.txt" in p for p in read_paths)
