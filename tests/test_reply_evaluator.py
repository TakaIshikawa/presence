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


# -- Fast check edge cases ----------------------------------------------------


class TestFastCheckEdgeCases:
    def test_sycophantic_case_insensitive_uppercase(self, evaluator):
        """Sycophantic pattern matching should be case-insensitive."""
        result = evaluator.evaluate(
            draft="GREAT POINT! I totally agree.",
            our_post="Test post",
            their_reply="Test reply",
        )
        assert "sycophantic" in result.flags

    def test_sycophantic_case_insensitive_mixed(self, evaluator):
        """Sycophantic pattern matching should work with mixed case."""
        result = evaluator.evaluate(
            draft="GrEaT pOiNt! Totally.",
            our_post="Test post",
            their_reply="Test reply",
        )
        assert "sycophantic" in result.flags

    def test_sycophantic_case_insensitive_love_this(self, evaluator):
        """'Love this' should match regardless of case."""
        result = evaluator.evaluate(
            draft="LOVE THIS! Amazing.",
            our_post="Test post",
            their_reply="Test reply",
        )
        assert "sycophantic" in result.flags

    def test_multiple_flags_sycophantic_and_hashtags(self, evaluator):
        """Both sycophantic and hashtags flags should be returned."""
        result = evaluator.evaluate(
            draft="Great point! #AI #agents",
            our_post="Test post",
            their_reply="Test reply",
        )
        assert "sycophantic" in result.flags
        assert "hashtags" in result.flags
        assert len(result.flags) == 2

    def test_hashtag_not_in_url_fragment(self, evaluator):
        """Hashtag pattern should match # in URLs (regression check)."""
        # Note: The current implementation WILL match #bar in URLs
        # This test documents the current behavior
        result = evaluator.evaluate(
            draft="Check this out http://example.com#bar",
            our_post="Test post",
            their_reply="Test reply",
        )
        # Current regex matches # followed by \w+, so #bar will match
        assert "hashtags" in result.flags

    def test_hashtag_in_middle_of_text(self, evaluator):
        """Hashtag in the middle should be detected."""
        result = evaluator.evaluate(
            draft="This approach is interesting #thoughtful and useful",
            our_post="Test post",
            their_reply="Test reply",
        )
        assert "hashtags" in result.flags

    def test_empty_string_no_flags(self, evaluator):
        """Empty string should return no flags."""
        flags = evaluator._fast_check("")
        assert flags == []

    def test_whitespace_only_no_flags(self, evaluator):
        """Whitespace-only string should return no flags."""
        flags = evaluator._fast_check("   \n\t  ")
        assert flags == []

    def test_almost_sycophantic_great_question(self, evaluator):
        """'Great question' is not in patterns, should not flag."""
        _mock_response(evaluator, 7.0, "OK")
        result = evaluator.evaluate(
            draft="Great question — we found the same issue.",
            our_post="Test post",
            their_reply="Test reply",
        )
        # Should NOT be fast-flagged
        assert "sycophantic" not in result.flags

    def test_almost_sycophantic_excellent_work(self, evaluator):
        """'Excellent work' is not in patterns (only 'excellent point/take/etc')."""
        _mock_response(evaluator, 7.0, "OK")
        result = evaluator.evaluate(
            draft="Excellent work on this project.",
            our_post="Test post",
            their_reply="Test reply",
        )
        # Should NOT be fast-flagged
        assert "sycophantic" not in result.flags

    def test_sycophantic_must_be_at_start(self, evaluator):
        """Sycophantic patterns require ^ anchor (start of string)."""
        _mock_response(evaluator, 7.0, "OK")
        result = evaluator.evaluate(
            draft="I think this is a great point you made.",
            our_post="Test post",
            their_reply="Test reply",
        )
        # 'great point' is not at start, should not fast-flag
        assert "sycophantic" not in result.flags

    def test_multiple_hashtags(self, evaluator):
        """Multiple hashtags should still result in single 'hashtags' flag."""
        result = evaluator.evaluate(
            draft="Interesting #AI #ML #agents #tools",
            our_post="Test post",
            their_reply="Test reply",
        )
        assert "hashtags" in result.flags
        # Should only appear once
        assert result.flags.count("hashtags") == 1

    def test_hashtag_with_numbers(self, evaluator):
        """Hashtag with numbers should be detected."""
        result = evaluator.evaluate(
            draft="Looking forward to 2026 #AI2026",
            our_post="Test post",
            their_reply="Test reply",
        )
        assert "hashtags" in result.flags

    def test_hashtag_with_underscore(self, evaluator):
        """Hashtag with underscore should be detected (\\w includes _)."""
        result = evaluator.evaluate(
            draft="Great insights #ai_agents",
            our_post="Test post",
            their_reply="Test reply",
        )
        assert "hashtags" in result.flags


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


# -- LLM evaluation with person_context tests ---------------------------------


class TestLLMEvaluationWithPersonContext:
    def test_context_note_includes_stage_and_tier(self, evaluator):
        """Verify context_note includes stage name and tier when person_context is provided."""
        ctx = PersonContext(
            x_handle="known_user",
            display_name="Known User",
            bio="Bio",
            relationship_strength=0.7,
            engagement_stage=2,
            dunbar_tier=2,
            authenticity_score=0.8,
            content_quality_score=0.9,
            content_relevance_score=0.85,
            is_known=True,
        )

        _mock_response(evaluator, 7.0, "Good")

        # Call evaluate to trigger _llm_evaluate
        evaluator.evaluate(
            draft="Nice work on this",
            our_post="Test post",
            their_reply="Test reply",
            person_context=ctx,
        )

        # Check the prompt sent to the API includes context
        call_args = evaluator.client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]

        # Should include stage and tier information
        assert "stage 2" in prompt
        assert "tier 2" in prompt
        assert "stage_mismatch" in prompt

    def test_context_note_empty_when_is_known_false(self, evaluator):
        """Verify context_note is empty when person_context.is_known is False."""
        ctx = PersonContext(
            x_handle="unknown_user",
            display_name="Unknown User",
            bio=None,
            relationship_strength=None,
            engagement_stage=None,
            dunbar_tier=None,
            authenticity_score=None,
            content_quality_score=None,
            content_relevance_score=None,
            is_known=False,
        )

        _mock_response(evaluator, 7.0, "Good")

        evaluator.evaluate(
            draft="Interesting point",
            our_post="Test post",
            their_reply="Test reply",
            person_context=ctx,
        )

        # Check the prompt does NOT include context
        call_args = evaluator.client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]

        # Should NOT include stage/tier information
        assert "Relationship context:" not in prompt
        assert "stage_mismatch" not in prompt

    def test_context_note_empty_when_no_person_context(self, evaluator):
        """Verify context_note is empty when person_context is None."""
        _mock_response(evaluator, 7.0, "Good")

        evaluator.evaluate(
            draft="Good insight",
            our_post="Test post",
            their_reply="Test reply",
            person_context=None,
        )

        call_args = evaluator.client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]

        # Should NOT include context
        assert "Relationship context:" not in prompt

    def test_context_note_empty_when_stage_is_none(self, evaluator):
        """Verify context_note is empty when engagement_stage is None."""
        ctx = PersonContext(
            x_handle="user",
            display_name="User",
            bio=None,
            relationship_strength=None,
            engagement_stage=None,  # None
            dunbar_tier=4,
            authenticity_score=None,
            content_quality_score=None,
            content_relevance_score=None,
            is_known=True,
        )

        _mock_response(evaluator, 7.0, "Good")

        evaluator.evaluate(
            draft="Interesting",
            our_post="Test post",
            their_reply="Test reply",
            person_context=ctx,
        )

        call_args = evaluator.client.messages.create.call_args
        prompt = call_args[1]["messages"][0]["content"]

        # Should NOT include context when stage is None
        assert "Relationship context:" not in prompt

    def test_exception_fallback_returns_correct_values(self, evaluator):
        """Verify Exception handler returns score=5.0, passes=False, flags=['eval_error']."""
        evaluator.client.messages.create.side_effect = RuntimeError("Network timeout")

        result = evaluator.evaluate(
            draft="Some draft",
            our_post="Some post",
            their_reply="Some reply",
        )

        assert result.score == 5.0
        assert result.passes is False
        assert result.flags == ["eval_error"]
        assert "Network timeout" in result.feedback

    def test_exception_with_person_context_still_fails_safely(self, evaluator):
        """Exception during LLM call with person_context should still fail safely."""
        ctx = PersonContext(
            x_handle="user",
            display_name="User",
            bio=None,
            relationship_strength=None,
            engagement_stage=1,
            dunbar_tier=3,
            authenticity_score=None,
            content_quality_score=None,
            content_relevance_score=None,
            is_known=True,
        )

        evaluator.client.messages.create.side_effect = Exception("API down")

        result = evaluator.evaluate(
            draft="Test",
            our_post="Post",
            their_reply="Reply",
            person_context=ctx,
        )

        assert result.score == 5.0
        assert not result.passes
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

    def test_parses_json_with_plain_code_fences(self, evaluator):
        """Test code fences without language specifier."""
        raw = '```\n{"score": 6.0, "feedback": "Decent", "flags": []}\n```'
        result = evaluator._parse_response(raw, 6.0)
        assert result.score == 6.0
        assert result.passes

    def test_missing_score_key_defaults_to_5(self, evaluator):
        """Missing score key should default to 5.0."""
        result = evaluator._parse_response(
            '{"feedback": "No score", "flags": []}', 6.0
        )
        assert result.score == 5.0
        assert not result.passes  # 5.0 < 6.0

    def test_flags_as_non_list_normalizes_to_empty(self, evaluator):
        """Non-list flags should normalize to empty list."""
        # flags as string
        result = evaluator._parse_response(
            '{"score": 7.0, "feedback": "OK", "flags": "generic"}', 6.0
        )
        assert result.flags == []

        # flags as int
        result = evaluator._parse_response(
            '{"score": 7.0, "feedback": "OK", "flags": 123}', 6.0
        )
        assert result.flags == []

    def test_score_clamps_below_zero(self, evaluator):
        """Score below 0.0 should clamp to 0.0."""
        result = evaluator._parse_response(
            '{"score": -5.0, "feedback": "Negative", "flags": []}', 6.0
        )
        assert result.score == 0.0
        assert not result.passes

    def test_score_clamps_above_ten(self, evaluator):
        """Score above 10.0 should clamp to 10.0."""
        result = evaluator._parse_response(
            '{"score": 20.0, "feedback": "Too high", "flags": []}', 6.0
        )
        assert result.score == 10.0
        assert result.passes

    def test_threshold_boundary_exact_match_passes(self, evaluator):
        """Score exactly equal to threshold should pass (>=)."""
        result = evaluator._parse_response(
            '{"score": 6.0, "feedback": "Exactly at threshold", "flags": []}', 6.0
        )
        assert result.score == 6.0
        assert result.passes  # 6.0 >= 6.0

    def test_threshold_boundary_just_below_fails(self, evaluator):
        """Score just below threshold should fail."""
        result = evaluator._parse_response(
            '{"score": 5.99, "feedback": "Just below", "flags": []}', 6.0
        )
        assert result.score == 5.99
        assert not result.passes

    def test_json_embedded_with_prefix_text(self, evaluator):
        """JSON embedded after prefix text should be extracted."""
        raw = 'Sure, here is the evaluation: {"score": 7.5, "feedback": "Good", "flags": []}'
        result = evaluator._parse_response(raw, 6.0)
        assert result.score == 7.5
        assert result.passes

    def test_json_embedded_with_suffix_text(self, evaluator):
        """JSON embedded before suffix text should be extracted."""
        raw = '{"score": 8.0, "feedback": "Solid", "flags": []} - that is my assessment.'
        result = evaluator._parse_response(raw, 6.0)
        assert result.score == 8.0

    def test_completely_unparseable_no_json(self, evaluator):
        """Response with no JSON at all should return parse_error."""
        result = evaluator._parse_response(
            "This is just plain text without any JSON whatsoever.", 6.0
        )
        assert result.score == 5.0
        assert not result.passes
        assert "parse_error" in result.flags

    def test_empty_json_object(self, evaluator):
        """Empty JSON object should use defaults."""
        result = evaluator._parse_response('{}', 6.0)
        assert result.score == 5.0  # default
        assert result.flags == []
        assert result.feedback == ""

    def test_missing_feedback_defaults_to_empty(self, evaluator):
        """Missing feedback should default to empty string."""
        result = evaluator._parse_response(
            '{"score": 7.0, "flags": []}', 6.0
        )
        assert result.feedback == ""


# -- Serialization tests ------------------------------------------------------


class TestEvalResultSerialization:
    def test_to_json_round_trip(self):
        result = ReplyEvalResult(score=7.5, passes=True, feedback="Good", flags=["clean"])
        data = json.loads(result.to_json())
        assert data["score"] == 7.5
        assert data["passes"] is True
        assert data["flags"] == ["clean"]
