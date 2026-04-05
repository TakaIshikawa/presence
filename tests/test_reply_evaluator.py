"""Tests for ReplyEvaluator — reply quality evaluation."""

import json
from unittest.mock import MagicMock, patch

import pytest

from engagement.reply_evaluator import ReplyEvaluator, ReplyEvalResult
from engagement.cultivate_bridge import PersonContext


@pytest.fixture
def evaluator():
    """ReplyEvaluator with mocked Anthropic client."""
    with patch("engagement.reply_evaluator.anthropic") as mock_anthropic:
        eval_ = ReplyEvaluator(api_key="test-key", model="test-model")
        eval_._mock_anthropic = mock_anthropic
        yield eval_


def _mock_response(evaluator, score, feedback, flags=None):
    """Set up mock LLM response."""
    response_data = {"score": score, "feedback": feedback, "flags": flags or []}
    mock_msg = MagicMock()
    mock_msg.content = [MagicMock(text=json.dumps(response_data))]
    evaluator.client.messages.create.return_value = mock_msg


# -- Fast check tests --------------------------------------------------------


class TestFastChecks:
    def test_detects_sycophantic_great_point(self, evaluator):
        result = evaluator.evaluate(
            draft="Great point! I totally agree.",
            our_post="Building in public",
            their_reply="I like this approach",
        )
        assert "sycophantic" in result.flags
        assert result.score == 2.0
        assert not result.passes

    def test_detects_sycophantic_love_this(self, evaluator):
        result = evaluator.evaluate(
            draft="Love this! So true.",
            our_post="AI agents",
            their_reply="Interesting take",
        )
        assert "sycophantic" in result.flags

    def test_detects_sycophantic_couldnt_agree_more(self, evaluator):
        result = evaluator.evaluate(
            draft="Couldn't agree more with this take.",
            our_post="Testing patterns",
            their_reply="We do this too",
        )
        assert "sycophantic" in result.flags

    def test_detects_hashtags(self, evaluator):
        result = evaluator.evaluate(
            draft="Interesting approach #AI #agents",
            our_post="Building agents",
            their_reply="Nice work",
        )
        assert "hashtags" in result.flags
        assert result.score == 2.0

    def test_clean_draft_passes_fast_checks(self, evaluator):
        _mock_response(evaluator, 7.5, "Good reply")
        result = evaluator.evaluate(
            draft="That shipping speed improvement matches what we saw too — curious if you ran into caching edge cases?",
            our_post="Building in public",
            their_reply="We shipped faster after",
        )
        # Should NOT be fast-flagged, goes to LLM
        assert "sycophantic" not in result.flags
        assert "hashtags" not in result.flags


# -- LLM evaluation tests ----------------------------------------------------


class TestLLMEvaluation:
    def test_good_reply_passes(self, evaluator):
        _mock_response(evaluator, 7.5, "Engaging reply that adds value")
        result = evaluator.evaluate(
            draft="That's a pattern we hit too — the latency dropped 40% after moving to streaming.",
            our_post="Optimizing agent responses",
            their_reply="We noticed similar issues",
        )
        assert result.score == 7.5
        assert result.passes
        assert result.flags == []

    def test_generic_reply_flagged(self, evaluator):
        _mock_response(evaluator, 3.5, "Too generic", ["generic"])
        result = evaluator.evaluate(
            draft="Interesting, will check it out.",
            our_post="AI evaluation",
            their_reply="We built something similar",
        )
        assert result.score == 3.5
        assert not result.passes
        assert "generic" in result.flags

    def test_stage_mismatch_flagged(self, evaluator):
        _mock_response(evaluator, 4.0, "Too familiar for stage 0", ["stage_mismatch"])
        ctx = PersonContext(
            x_handle="new_user",
            display_name="New User",
            bio=None,
            relationship_strength=None,
            engagement_stage=0,
            dunbar_tier=4,
            authenticity_score=None,
            content_quality_score=None,
            content_relevance_score=None,
            is_known=True,
        )
        result = evaluator.evaluate(
            draft="Haha yeah we always run into that! Remember when we chatted about this last time?",
            our_post="Testing AI agents",
            their_reply="This is new to me",
            person_context=ctx,
        )
        assert "stage_mismatch" in result.flags

    def test_threshold_customizable(self, evaluator):
        _mock_response(evaluator, 5.5, "Decent but not great")
        result = evaluator.evaluate(
            draft="Interesting approach",
            our_post="Test",
            their_reply="Test",
            threshold=5.0,
        )
        assert result.passes  # 5.5 >= 5.0

    def test_api_error_returns_safe_default(self, evaluator):
        evaluator.client.messages.create.side_effect = Exception("API error")
        result = evaluator.evaluate(
            draft="Some reply",
            our_post="Our post",
            their_reply="Their reply",
        )
        assert result.score == 5.0
        assert "eval_error" in result.flags


# -- Response parsing tests ---------------------------------------------------


class TestResponseParsing:
    def test_parses_clean_json(self, evaluator):
        result = evaluator._parse_response(
            '{"score": 8.0, "feedback": "Good", "flags": []}', 6.0
        )
        assert result.score == 8.0
        assert result.passes
        assert result.flags == []

    def test_parses_json_with_code_fences(self, evaluator):
        raw = '```json\n{"score": 7.0, "feedback": "OK", "flags": ["generic"]}\n```'
        result = evaluator._parse_response(raw, 6.0)
        assert result.score == 7.0
        assert "generic" in result.flags

    def test_handles_malformed_json(self, evaluator):
        result = evaluator._parse_response("not json at all", 6.0)
        assert result.score == 5.0
        assert "parse_error" in result.flags

    def test_clamps_score_to_range(self, evaluator):
        result = evaluator._parse_response(
            '{"score": 15.0, "feedback": "Over", "flags": []}', 6.0
        )
        assert result.score == 10.0

    def test_handles_embedded_json(self, evaluator):
        raw = 'Here is my evaluation:\n{"score": 6.5, "feedback": "OK", "flags": []}\nDone.'
        result = evaluator._parse_response(raw, 6.0)
        assert result.score == 6.5


# -- Serialization tests ------------------------------------------------------


class TestEvalResultSerialization:
    def test_to_json_round_trip(self):
        result = ReplyEvalResult(score=7.5, passes=True, feedback="Good", flags=["clean"])
        data = json.loads(result.to_json())
        assert data["score"] == 7.5
        assert data["passes"] is True
        assert data["flags"] == ["clean"]
