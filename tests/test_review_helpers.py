"""Tests for review_helpers.py shared utility functions."""

import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

# Add scripts/ to path so we can import the module under test
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from review_helpers import truncate, read_char, format_relationship_context


# --- truncate ---


class TestTruncate:
    def test_empty_string(self):
        result = truncate("", 50)
        assert result == ""

    def test_none_returns_empty(self):
        result = truncate(None, 50)
        assert result == ""

    def test_short_string_unchanged(self):
        text = "Hello world"
        result = truncate(text, 50)
        assert result == "Hello world"

    def test_exact_length_unchanged(self):
        text = "x" * 50
        result = truncate(text, 50)
        assert result == text
        assert len(result) == 50

    def test_long_string_with_ellipsis(self):
        text = "This is a long string that needs to be truncated"
        result = truncate(text, 20)
        assert len(result) == 20
        assert result.endswith("...")
        assert result == "This is a long st..."

    def test_truncate_at_max_len_minus_3(self):
        text = "abcdefghij"
        result = truncate(text, 7)
        assert result == "abcd..."
        assert len(result) == 7


# --- read_char ---


class TestReadChar:
    @patch("sys.stdin")
    def test_tty_mode_reads_single_char(self, mock_stdin):
        """Test normal tty mode reads a single character."""
        with patch("tty.setraw"), patch("termios.tcgetattr") as mock_getattr, patch(
            "termios.tcsetattr"
        ) as mock_setattr:
            mock_stdin.fileno.return_value = 0
            mock_stdin.read.return_value = "a"
            mock_getattr.return_value = ["old", "settings"]

            result = read_char()

            assert result == "a"
            mock_getattr.assert_called_once_with(0)
            mock_setattr.assert_called_once()

    def test_fallback_on_import_error(self):
        """Test fallback to input() when tty is not available."""
        with patch("builtins.input", return_value="abc"):
            with patch("sys.stdin.fileno", side_effect=AttributeError):
                result = read_char()
                assert result == "a"

    @patch("sys.stdin")
    def test_fallback_on_termios_error(self, mock_stdin):
        """Test fallback when termios raises an error."""
        import termios

        with patch("builtins.input", return_value="xyz"):
            mock_stdin.fileno.return_value = 0
            with patch("termios.tcgetattr", side_effect=termios.error):
                result = read_char()
                assert result == "x"

    def test_fallback_empty_input(self):
        """Test fallback handles empty input."""
        with patch("builtins.input", return_value=""):
            with patch("sys.stdin.fileno", side_effect=AttributeError):
                result = read_char()
                assert result == ""


# --- format_relationship_context ---


class TestFormatRelationshipContext:
    def test_none_input_returns_none(self):
        result = format_relationship_context(None)
        assert result is None

    def test_empty_string_returns_none(self):
        result = format_relationship_context("")
        assert result is None

    def test_invalid_json_returns_none(self):
        result = format_relationship_context("not valid json{")
        assert result is None

    def test_empty_object_returns_none(self):
        result = format_relationship_context("{}")
        assert result is None

    def test_full_context_all_fields(self):
        ctx = {
            "engagement_stage": 3,
            "stage_name": "Active",
            "dunbar_tier": 2,
            "tier_name": "Key Network",
            "relationship_strength": 0.42,
        }
        result = format_relationship_context(json.dumps(ctx))
        assert result == "Active (stage 3) | Key Network (tier 2) | strength: 0.42"

    def test_partial_context_stage_only(self):
        ctx = {"engagement_stage": 1, "stage_name": "Ambient"}
        result = format_relationship_context(json.dumps(ctx))
        assert result == "Ambient (stage 1)"

    def test_partial_context_tier_only(self):
        ctx = {"dunbar_tier": 4, "tier_name": "Outer Circle"}
        result = format_relationship_context(json.dumps(ctx))
        assert result == "Outer Circle (tier 4)"

    def test_partial_context_strength_only(self):
        ctx = {"relationship_strength": 0.75}
        result = format_relationship_context(json.dumps(ctx))
        assert result == "strength: 0.75"

    def test_missing_stage_name_shows_question_mark(self):
        ctx = {"engagement_stage": 3}
        result = format_relationship_context(json.dumps(ctx))
        assert result == "? (stage 3)"

    def test_missing_tier_name_shows_question_mark(self):
        ctx = {"dunbar_tier": 2}
        result = format_relationship_context(json.dumps(ctx))
        assert result == "? (tier 2)"

    def test_zero_values_are_included(self):
        ctx = {
            "engagement_stage": 0,
            "stage_name": "New",
            "dunbar_tier": 0,
            "tier_name": "Core",
            "relationship_strength": 0.0,
        }
        result = format_relationship_context(json.dumps(ctx))
        assert result == "New (stage 0) | Core (tier 0) | strength: 0.00"

    def test_strength_formatting_two_decimals(self):
        ctx = {"relationship_strength": 0.123456}
        result = format_relationship_context(json.dumps(ctx))
        assert result == "strength: 0.12"

    def test_extra_fields_ignored(self):
        ctx = {
            "engagement_stage": 2,
            "stage_name": "Growing",
            "extra_field": "ignored",
            "another_field": 123,
        }
        result = format_relationship_context(json.dumps(ctx))
        assert result == "Growing (stage 2)"
