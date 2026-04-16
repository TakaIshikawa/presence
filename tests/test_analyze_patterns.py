"""Tests for scripts/analyze_patterns.py script-level logic."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from synthesis.pattern_analyzer import PatternAnalysis


# --- Helpers ---


def _make_config():
    """Create a mock config object with required attributes."""
    config = MagicMock()
    config.anthropic.api_key = "test-api-key"
    config.synthesis.eval_model = "claude-opus-4-20250514"
    return config


def _mock_script_context(config, db):
    """Create a mock script_context that yields (config, db)."""
    @contextmanager
    def _ctx():
        yield (config, db)
    return _ctx


def _make_classified_posts(resonated_count=5, low_resonance_count=10):
    """Create mock classified posts data."""
    resonated = [
        {
            "content": f"Resonated post {i} with specific details",
            "engagement_score": 10 + i,
            "post_id": f"res-{i}",
        }
        for i in range(resonated_count)
    ]
    low_resonance = [
        {
            "content": f"Low resonance post {i}",
            "engagement_score": 0,
            "post_id": f"low-{i}",
        }
        for i in range(low_resonance_count)
    ]
    return {"resonated": resonated, "low_resonance": low_resonance}


def _make_pattern_analysis():
    """Create a mock PatternAnalysis object."""
    return PatternAnalysis(
        positive_patterns=[
            "Opens with specific moments",
            "Uses concrete examples",
            "Shows vulnerability",
        ],
        negative_patterns=[
            "Generic opening statements",
            "Abstract descriptions",
            "Lacks specificity",
        ],
        key_differences=[
            "Resonated posts use specific details vs generic statements",
            "Engaging posts show personal struggle",
        ],
        actionable_rules=[
            "Always open with a specific moment",
            "Never use generic statements",
            "Include concrete technical details",
        ],
        analyzed_at=datetime(2026, 4, 17, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        raw_response="<json>{...}</json>",
        confidence="medium",
    )


# Shared decorator stack for patching analyze_patterns dependencies.
def _pattern_patches(func):
    @patch("analyze_patterns.PatternAnalyzer")
    @patch("analyze_patterns.script_context")
    def wrapper(self, mock_ctx, MockPatternAnalyzer, *args, **kwargs):
        # Set MIN_RESONATED on the mock class to avoid comparison errors
        MockPatternAnalyzer.MIN_RESONATED = 3
        return func(
            self,
            mock_ctx=mock_ctx,
            MockPatternAnalyzer=MockPatternAnalyzer,
        )
    return wrapper


# --- Tests ---


class TestMainHappyPath:
    @_pattern_patches
    def test_analyzes_and_stores_results(
        self, *, mock_ctx, MockPatternAnalyzer
    ):
        """Test happy path: sufficient data, analysis succeeds, results stored."""
        config = _make_config()
        db = MagicMock()

        # Mock classified posts with sufficient resonated posts
        classified = _make_classified_posts(resonated_count=5, low_resonance_count=10)
        db.get_all_classified_posts.return_value = classified

        mock_ctx.return_value = _mock_script_context(config, db)()

        # Mock successful pattern analysis
        analysis = _make_pattern_analysis()
        MockPatternAnalyzer.return_value.analyze.return_value = analysis

        import analyze_patterns
        analyze_patterns.main()

        # Verify DB calls
        db.get_all_classified_posts.assert_called_once_with(content_type="x_post")

        # Verify PatternAnalyzer initialization
        MockPatternAnalyzer.assert_called_once_with(
            api_key="test-api-key",
            model="claude-opus-4-20250514",
        )

        # Verify analyze was called with correct data
        MockPatternAnalyzer.return_value.analyze.assert_called_once_with(
            resonated=classified["resonated"],
            low_resonance=classified["low_resonance"],
        )

        # Verify set_meta was called
        db.set_meta.assert_called_once()
        call_args = db.set_meta.call_args
        assert call_args[0][0] == "pattern_analysis"

        # Verify JSON structure
        stored_json = json.loads(call_args[0][1])
        assert "positive_patterns" in stored_json
        assert "negative_patterns" in stored_json
        assert "key_differences" in stored_json
        assert "actionable_rules" in stored_json
        assert "analyzed_at" in stored_json
        assert "resonated_count" in stored_json
        assert "low_resonance_count" in stored_json
        assert "confidence" in stored_json

        # Verify values
        assert stored_json["positive_patterns"] == analysis.positive_patterns
        assert stored_json["negative_patterns"] == analysis.negative_patterns
        assert stored_json["key_differences"] == analysis.key_differences
        assert stored_json["actionable_rules"] == analysis.actionable_rules
        assert stored_json["analyzed_at"] == analysis.analyzed_at
        assert stored_json["resonated_count"] == 5
        assert stored_json["low_resonance_count"] == 10
        assert stored_json["confidence"] == "medium"


class TestMainExitsEarlyInsufficientData:
    @_pattern_patches
    def test_exits_when_below_min_resonated_threshold(
        self, *, mock_ctx, MockPatternAnalyzer
    ):
        """Test early exit when resonated count < MIN_RESONATED (3)."""
        config = _make_config()
        db = MagicMock()

        # Only 2 resonated posts (below MIN_RESONATED = 3)
        classified = _make_classified_posts(resonated_count=2, low_resonance_count=10)
        db.get_all_classified_posts.return_value = classified

        mock_ctx.return_value = _mock_script_context(config, db)()

        import analyze_patterns
        analyze_patterns.main()

        # Verify early exit - no analysis performed
        MockPatternAnalyzer.assert_not_called()
        db.set_meta.assert_not_called()


class TestMainExitsWhenAnalysisReturnsNone:
    @_pattern_patches
    def test_exits_when_analyzer_returns_none(
        self, *, mock_ctx, MockPatternAnalyzer
    ):
        """Test early exit when pattern analysis returns None."""
        config = _make_config()
        db = MagicMock()

        # Sufficient resonated posts
        classified = _make_classified_posts(resonated_count=5, low_resonance_count=10)
        db.get_all_classified_posts.return_value = classified

        mock_ctx.return_value = _mock_script_context(config, db)()

        # Mock analyzer returning None (could happen in edge cases)
        MockPatternAnalyzer.return_value.analyze.return_value = None

        import analyze_patterns
        analyze_patterns.main()

        # Verify analyzer was called but no results stored
        MockPatternAnalyzer.return_value.analyze.assert_called_once()
        db.set_meta.assert_not_called()


class TestEdgeCases:
    @_pattern_patches
    def test_handles_zero_low_resonance_posts(
        self, *, mock_ctx, MockPatternAnalyzer
    ):
        """Test behavior when there are resonated posts but no low_resonance posts."""
        config = _make_config()
        db = MagicMock()

        # Resonated posts but no low_resonance
        classified = _make_classified_posts(resonated_count=5, low_resonance_count=0)
        db.get_all_classified_posts.return_value = classified

        mock_ctx.return_value = _mock_script_context(config, db)()

        analysis = _make_pattern_analysis()
        MockPatternAnalyzer.return_value.analyze.return_value = analysis

        import analyze_patterns
        analyze_patterns.main()

        # Should still proceed with analysis
        MockPatternAnalyzer.return_value.analyze.assert_called_once_with(
            resonated=classified["resonated"],
            low_resonance=[],
        )
        db.set_meta.assert_called_once()

    @_pattern_patches
    def test_handles_exactly_min_resonated_threshold(
        self, *, mock_ctx, MockPatternAnalyzer
    ):
        """Test behavior when resonated count equals MIN_RESONATED (3)."""
        config = _make_config()
        db = MagicMock()

        # Exactly MIN_RESONATED = 3
        classified = _make_classified_posts(resonated_count=3, low_resonance_count=10)
        db.get_all_classified_posts.return_value = classified

        mock_ctx.return_value = _mock_script_context(config, db)()

        analysis = _make_pattern_analysis()
        analysis.confidence = "low"  # Low confidence for small sample
        MockPatternAnalyzer.return_value.analyze.return_value = analysis

        import analyze_patterns
        analyze_patterns.main()

        # Should proceed with analysis (>= threshold)
        MockPatternAnalyzer.return_value.analyze.assert_called_once()
        db.set_meta.assert_called_once()

        # Verify confidence is included
        call_args = db.set_meta.call_args
        stored_json = json.loads(call_args[0][1])
        assert stored_json["confidence"] == "low"


class TestAnalysisDataStructure:
    @_pattern_patches
    def test_stored_json_includes_all_required_fields(
        self, *, mock_ctx, MockPatternAnalyzer
    ):
        """Test that stored JSON includes all required fields with correct structure."""
        config = _make_config()
        db = MagicMock()

        classified = _make_classified_posts(resonated_count=10, low_resonance_count=15)
        db.get_all_classified_posts.return_value = classified

        mock_ctx.return_value = _mock_script_context(config, db)()

        analysis = _make_pattern_analysis()
        analysis.confidence = "high"  # High confidence for larger sample
        MockPatternAnalyzer.return_value.analyze.return_value = analysis

        import analyze_patterns
        analyze_patterns.main()

        call_args = db.set_meta.call_args
        stored_json = json.loads(call_args[0][1])

        # Verify all required fields exist
        required_fields = [
            "positive_patterns",
            "negative_patterns",
            "key_differences",
            "actionable_rules",
            "analyzed_at",
            "resonated_count",
            "low_resonance_count",
            "confidence",
        ]
        for field in required_fields:
            assert field in stored_json, f"Missing required field: {field}"

        # Verify field types
        assert isinstance(stored_json["positive_patterns"], list)
        assert isinstance(stored_json["negative_patterns"], list)
        assert isinstance(stored_json["key_differences"], list)
        assert isinstance(stored_json["actionable_rules"], list)
        assert isinstance(stored_json["analyzed_at"], str)
        assert isinstance(stored_json["resonated_count"], int)
        assert isinstance(stored_json["low_resonance_count"], int)
        assert isinstance(stored_json["confidence"], str)

        # Verify counts match
        assert stored_json["resonated_count"] == 10
        assert stored_json["low_resonance_count"] == 15
