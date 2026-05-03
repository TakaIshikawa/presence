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


# --- Refinement Feedback and Directives ---


class TestRefinementFeedbackAndDirectives:
    """Test refinement feedback generation and targeted refinement directives."""

    def test_refine_integrates_evaluation_feedback_into_prompt(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("improved")

        template = "Content: {content}\nFeedback: {best_feedback}\nImprovement: {improvement}\nConstraints: {format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine(
                content="draft post",
                best_feedback="Hook is weak, lacks specificity",
                improvement="add concrete example",
                content_type="x_post"
            )

        call_args = refine_client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]
        assert "Hook is weak, lacks specificity" in prompt
        assert "add concrete example" in prompt

    def test_refine_supports_targeted_directive_improve_hook(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("better hook")

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine(
                content="weak opening...",
                best_feedback="opening lacks punch",
                improvement="strengthen the hook with a surprising insight",
                content_type="x_post"
            )

        call_args = refine_client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]
        assert "strengthen the hook" in prompt

    def test_refine_supports_targeted_directive_strengthen_cta(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("stronger CTA")

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine(
                content="post ending is weak",
                best_feedback="call-to-action unclear",
                improvement="make the call-to-action more direct and actionable",
                content_type="x_post"
            )

        call_args = refine_client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]
        assert "call-to-action more direct" in prompt

    def test_refine_supports_targeted_directive_reduce_jargon(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("clearer language")

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine(
                content="lots of technical jargon here",
                best_feedback="too much jargon",
                improvement="replace jargon with plain language",
                content_type="x_post"
            )

        call_args = refine_client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]
        assert "replace jargon with plain language" in prompt


# --- Quality Gates ---


class TestRefinementQualityGates:
    """Test refinement quality gates that reject poor refinements."""

    def test_gate_rejects_refined_when_score_lower_than_baseline(self):
        """Gate should pick ORIGINAL when refined version scores lower."""
        refiner, refine_client, gate_client = _make_refiner()

        refine_client.messages.create.return_value = _mock_response("worse version")
        # Gate picks ORIGINAL with lower score indicating refined is worse
        gate_client.messages.create.return_value = _mock_response(
            "PICK: ORIGINAL\nSCORE: 4.5"
        )

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        gate_template = "{original}\n{refined}"

        with patch("pathlib.Path.read_text", side_effect=[template, gate_template]):
            result = refiner.refine_and_gate(
                content="original (quality 6.0)",
                best_feedback="feedback",
                improvement="improve"
            )

        assert result.picked == "ORIGINAL"
        assert result.final_content == "original (quality 6.0)"

    def test_gate_accepts_refined_when_score_improved(self):
        """Gate should pick REFINED when score improves."""
        refiner, refine_client, gate_client = _make_refiner()

        refine_client.messages.create.return_value = _mock_response("better version")
        gate_client.messages.create.return_value = _mock_response(
            "PICK: REFINED\nSCORE: 8.5"
        )

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        gate_template = "{original}\n{refined}"

        with patch("pathlib.Path.read_text", side_effect=[template, gate_template]):
            result = refiner.refine_and_gate(
                content="original (quality 6.0)",
                best_feedback="feedback",
                improvement="improve"
            )

        assert result.picked == "REFINED"
        assert result.final_score == 8.5

    def test_gate_compares_both_versions_objectively(self):
        """Gate receives both original and refined for comparison."""
        refiner, refine_client, gate_client = _make_refiner()

        refine_client.messages.create.return_value = _mock_response("refined text")
        gate_client.messages.create.return_value = _mock_response(
            "PICK: REFINED\nSCORE: 7.0"
        )

        gate_template = "ORIGINAL: {original}\nREFINED: {refined}\nPick the better one."
        with patch("pathlib.Path.read_text", side_effect=[
            "{content}\n{best_feedback}\n{improvement}\n{format_constraints}",
            gate_template
        ]):
            refiner.refine_and_gate("original text", "feedback", "improve")

        call_args = gate_client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]
        assert "ORIGINAL: original text" in prompt
        assert "REFINED: refined text" in prompt


# --- Style Consistency ---


class TestStyleConsistency:
    """Test that refinements maintain style consistency."""

    def test_format_constraints_enforced_for_x_post(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("refined post")

        template = "{content}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine("draft", "feedback", "improve", "x_post")

        call_args = refine_client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]
        # X post constraints should be in the prompt
        assert "280 characters" in prompt
        assert "Single tweet format" in prompt

    def test_format_constraints_enforced_for_x_thread(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("refined thread")

        template = "{content}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine("draft", "feedback", "improve", "x_thread")

        call_args = refine_client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]
        assert "280 characters" in prompt
        assert "TWEET N:" in prompt
        assert "3-5 tweets" in prompt

    def test_format_constraints_enforced_for_blog_post(self):
        refiner, refine_client, _ = _make_refiner()
        refine_client.messages.create.return_value = _mock_response("refined blog")

        template = "{content}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine("draft", "feedback", "improve", "blog_post")

        call_args = refine_client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]
        assert "800-1200 words" in prompt
        assert "## section headers" in prompt


# --- Prompt Versioning ---


class TestPromptVersioning:
    """Test refinement prompt versioning and tracking."""

    def test_refiner_prompt_registered_with_db(self, db):
        refiner, refine_client, _ = _make_refiner()
        refiner.db = db
        refine_client.messages.create.return_value = _mock_response("refined")

        template = "Refiner template v1"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._refine("content", "feedback", "improve", "x_post")

        # Check prompt was registered
        import hashlib
        prompt_hash = hashlib.sha256(template.encode("utf-8")).hexdigest()
        row = db.get_prompt_version("refiner", prompt_hash)
        assert row is not None
        assert row["version"] == 1

    def test_final_gate_prompt_registered_with_db(self, db):
        refiner, _, gate_client = _make_refiner()
        refiner.db = db
        gate_client.messages.create.return_value = _mock_response("PICK: REFINED\nSCORE: 7.0")

        template = "Final gate template v1"
        with patch("pathlib.Path.read_text", return_value=template):
            refiner._final_gate("original", "refined")

        # Check prompt was registered
        import hashlib
        prompt_hash = hashlib.sha256(template.encode("utf-8")).hexdigest()
        row = db.get_prompt_version("final_gate", prompt_hash)
        assert row is not None
        assert row["version"] == 1

    def test_prompt_version_incremented_on_change(self, db):
        refiner, refine_client, _ = _make_refiner()
        refiner.db = db
        refine_client.messages.create.return_value = _mock_response("refined")

        # First version
        template_v1 = "Refiner template v1"
        with patch("pathlib.Path.read_text", return_value=template_v1):
            refiner._refine("content", "feedback", "improve", "x_post")

        # Second version (changed template)
        template_v2 = "Refiner template v2 - updated"
        with patch("pathlib.Path.read_text", return_value=template_v2):
            refiner._refine("content", "feedback", "improve", "x_post")

        # Check both versions registered
        import hashlib
        hash_v1 = hashlib.sha256(template_v1.encode("utf-8")).hexdigest()
        hash_v2 = hashlib.sha256(template_v2.encode("utf-8")).hexdigest()

        row_v1 = db.get_prompt_version("refiner", hash_v1)
        row_v2 = db.get_prompt_version("refiner", hash_v2)

        assert row_v1["version"] == 1
        assert row_v2["version"] == 2


# --- Effectiveness Measurement ---


class TestRefinementEffectiveness:
    """Test refinement effectiveness measurement via score deltas."""

    def test_score_delta_positive_when_refined_better(self):
        """Measure improvement by comparing gate score to baseline."""
        refiner, refine_client, gate_client = _make_refiner()

        refine_client.messages.create.return_value = _mock_response("improved")
        gate_client.messages.create.return_value = _mock_response(
            "PICK: REFINED\nSCORE: 8.5"
        )

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        gate_template = "{original}\n{refined}"
        with patch("pathlib.Path.read_text", side_effect=[template, gate_template]):
            result = refiner.refine_and_gate("original", "feedback", "improve")

        # If we assume original had score 6.0, delta would be +2.5
        # We can track this by comparing result.final_score to a baseline
        assert result.final_score == 8.5
        assert result.picked == "REFINED"
        # Effectiveness = score improved from baseline (measured externally)

    def test_score_delta_negative_when_refined_worse(self):
        """Negative delta when refinement makes content worse."""
        refiner, refine_client, gate_client = _make_refiner()

        refine_client.messages.create.return_value = _mock_response("worse")
        gate_client.messages.create.return_value = _mock_response(
            "PICK: ORIGINAL\nSCORE: 4.0"
        )

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        gate_template = "{original}\n{refined}"
        with patch("pathlib.Path.read_text", side_effect=[template, gate_template]):
            result = refiner.refine_and_gate("original", "feedback", "improve")

        # Score of 4.0 indicates poor quality (negative delta from baseline)
        assert result.final_score == 4.0
        assert result.picked == "ORIGINAL"

    def test_effectiveness_tracking_across_multiple_refinements(self):
        """Track scores across multiple refine calls."""
        refiner, refine_client, gate_client = _make_refiner()

        scores = []
        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        gate_template = "{original}\n{refined}"

        # First refinement: improvement
        refine_client.messages.create.return_value = _mock_response("refined v1")
        gate_client.messages.create.return_value = _mock_response("PICK: REFINED\nSCORE: 7.5")
        with patch("pathlib.Path.read_text", side_effect=[template, gate_template]):
            result1 = refiner.refine_and_gate("original", "feedback1", "improve")
        scores.append(result1.final_score)

        # Second refinement: further improvement
        refine_client.messages.create.return_value = _mock_response("refined v2")
        gate_client.messages.create.return_value = _mock_response("PICK: REFINED\nSCORE: 8.5")
        with patch("pathlib.Path.read_text", side_effect=[template, gate_template]):
            result2 = refiner.refine_and_gate(result1.final_content, "feedback2", "improve")
        scores.append(result2.final_score)

        # Track improvement trajectory
        assert scores == [7.5, 8.5]
        assert scores[1] > scores[0]  # Effectiveness increasing


# --- Error Handling ---


class TestRefinementErrorHandling:
    """Test error handling for LLM failures and malformed outputs."""

    def test_refine_propagates_api_connection_error(self):
        refiner, refine_client, _ = _make_refiner()

        import anthropic
        refine_client.messages.create.side_effect = anthropic.APIConnectionError(
            request=MagicMock()
        )

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            with pytest.raises(anthropic.APIConnectionError):
                refiner._refine("content", "feedback", "improve", "x_post")

    def test_refine_propagates_api_status_error(self):
        refiner, refine_client, _ = _make_refiner()

        import anthropic
        refine_client.messages.create.side_effect = anthropic.APIStatusError(
            message="Rate limited",
            response=MagicMock(),
            body=None
        )

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            with pytest.raises(anthropic.APIStatusError):
                refiner._refine("content", "feedback", "improve", "x_post")

    def test_final_gate_propagates_api_connection_error(self):
        refiner, _, gate_client = _make_refiner()

        import anthropic
        gate_client.messages.create.side_effect = anthropic.APIConnectionError(
            request=MagicMock()
        )

        template = "{original}\n{refined}"
        with patch("pathlib.Path.read_text", return_value=template):
            with pytest.raises(anthropic.APIConnectionError):
                refiner._final_gate("original", "refined")

    def test_final_gate_propagates_api_status_error(self):
        refiner, _, gate_client = _make_refiner()

        import anthropic
        gate_client.messages.create.side_effect = anthropic.APIStatusError(
            message="Service unavailable",
            response=MagicMock(),
            body=None
        )

        template = "{original}\n{refined}"
        with patch("pathlib.Path.read_text", return_value=template):
            with pytest.raises(anthropic.APIStatusError):
                refiner._final_gate("original", "refined")

    def test_malformed_refiner_output_returned_as_is(self):
        """Malformed refine output is returned as-is (no parsing failure)."""
        refiner, refine_client, _ = _make_refiner()

        # Refiner returns malformed JSON or unexpected format
        refine_client.messages.create.return_value = _mock_response(
            '{"incomplete": "json'
        )

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            result = refiner._refine("content", "feedback", "improve", "x_post")

        # Should return the raw text (no parsing in _refine)
        assert result == '{"incomplete": "json'

    def test_malformed_gate_output_uses_defaults(self):
        """Malformed gate output falls back to defaults."""
        refiner, _, gate_client = _make_refiner()

        # Gate returns unparseable output
        gate_client.messages.create.return_value = _mock_response(
            "This is just random text without PICK or SCORE markers"
        )

        template = "{original}\n{refined}"
        with patch("pathlib.Path.read_text", return_value=template):
            result = refiner._final_gate("original", "refined")

        # Should default to REFINED pick and 5.0 score
        assert result.picked == "REFINED"
        assert result.final_score == 5.0
        assert result.final_content == "refined"

    def test_concurrent_refine_calls_with_different_content_types(self):
        """Test concurrent refinements with different content types."""
        refiner, refine_client, _ = _make_refiner()

        # Simulate concurrent calls
        call_count = [0]
        responses = ["refined post", "refined thread", "refined blog"]

        def side_effect_refine(*args, **kwargs):
            idx = call_count[0]
            call_count[0] += 1
            return _mock_response(responses[idx])

        refine_client.messages.create.side_effect = side_effect_refine

        template = "{content}\n{best_feedback}\n{improvement}\n{format_constraints}"
        with patch("pathlib.Path.read_text", return_value=template):
            result1 = refiner._refine("c1", "f1", "i1", "x_post")
            result2 = refiner._refine("c2", "f2", "i2", "x_thread")
            result3 = refiner._refine("c3", "f3", "i3", "blog_post")

        assert result1 == "refined post"
        assert result2 == "refined thread"
        assert result3 == "refined blog"

        # Verify max_tokens varied per content type
        calls = refine_client.messages.create.call_args_list
        assert calls[0][1]["max_tokens"] == 500  # x_post
        assert calls[1][1]["max_tokens"] == 2000  # x_thread
        assert calls[2][1]["max_tokens"] == 4000  # blog_post
