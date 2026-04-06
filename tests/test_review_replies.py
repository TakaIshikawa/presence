"""Tests for review_replies.py display formatting functions."""

import json
import sys
from pathlib import Path

import pytest

# Add scripts/ to path so we can import the module under test
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from review_replies import _format_context_line, _format_quality_line


# --- _format_context_line ---


class TestFormatContextLine:
    def test_full_context(self):
        ctx = {
            "engagement_stage": 3,
            "stage_name": "Active",
            "dunbar_tier": 2,
            "tier_name": "Key Network",
            "relationship_strength": 0.42,
        }
        result = _format_context_line(json.dumps(ctx))
        assert result == "Active (stage 3) | Key Network (tier 2) | strength: 0.42"

    def test_partial_context_stage_only(self):
        ctx = {"engagement_stage": 1, "stage_name": "Ambient"}
        result = _format_context_line(json.dumps(ctx))
        assert result == "Ambient (stage 1)"

    def test_missing_stage_name_shows_question_mark(self):
        ctx = {"engagement_stage": 3}
        result = _format_context_line(json.dumps(ctx))
        assert result == "? (stage 3)"

    def test_none_input_returns_none(self):
        assert _format_context_line(None) is None

    def test_empty_string_returns_none(self):
        assert _format_context_line("") is None

    def test_malformed_json_returns_none(self):
        assert _format_context_line("not json{") is None

    def test_empty_object_returns_none(self):
        assert _format_context_line("{}") is None


# --- _format_quality_line ---


class TestFormatQualityLine:
    def test_passing_score_no_flags(self):
        result = _format_quality_line(7.5, None)
        assert result == "Quality: 7.5/10"

    def test_flagged_score(self):
        result = _format_quality_line(3.0, '["sycophantic"]')
        assert result == "Quality: 3.0/10 ⚠ sycophantic"

    def test_multiple_flags(self):
        result = _format_quality_line(2.0, '["sycophantic", "generic"]')
        assert "sycophantic" in result
        assert "generic" in result

    def test_none_score_returns_none(self):
        assert _format_quality_line(None, None) is None

    def test_score_with_empty_flags(self):
        result = _format_quality_line(8.0, "[]")
        assert result == "Quality: 8.0/10"

    def test_score_with_malformed_flags(self):
        result = _format_quality_line(6.0, "not json")
        assert result == "Quality: 6.0/10"
