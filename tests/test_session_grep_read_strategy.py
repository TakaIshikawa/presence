"""Tests for session Grep vs Read strategy analyzer."""

import pytest

from synthesis.session_grep_read_strategy import (
    analyze_session_grep_read_strategy,
    _calculate_efficiency_score,
    _calculate_token_correlation,
    _percentage,
    _average,
)


class TestAnalyzeSessionGrepReadStrategy:
    """Test main analyzer function."""

    def test_empty_input_returns_zeroed_metrics(self):
        """Verify empty input returns zero metrics."""
        result = analyze_session_grep_read_strategy([])

        assert result["total_sequences"] == 0
        assert result["total_grep_calls"] == 0
        assert result["total_read_calls"] == 0
        assert result["grep_guided_read_ratio"] == 0.0
        assert result["direct_read_ratio"] == 0.0
        assert result["avg_pattern_specificity"] == 0.0
        assert result["optimal_sequence_ratio"] == 0.0
        assert result["inefficient_pattern_ratio"] == 0.0
        assert result["search_efficiency_score"] == 0.0
        assert result["token_correlation"] == 0.0
        assert result["avg_tokens_grep_guided"] == 0.0
        assert result["avg_tokens_direct"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_grep_read_strategy(None)
        assert result["total_sequences"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_grep_read_strategy("not a list")

    def test_grep_guided_read_detected(self):
        """Verify Grep-guided read is detected."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py", "offset": 0, "limit": 50}},
                ],
                "is_grep_guided": True,
                "pattern_specificity": 0.6,
                "total_tokens": 100,
            }
        ])

        assert result["total_sequences"] == 1
        assert result["total_grep_calls"] == 1
        assert result["total_read_calls"] == 1
        assert result["grep_guided_read_ratio"] == 100.0
        assert result["direct_read_ratio"] == 0.0
        assert result["avg_pattern_specificity"] == 0.6
        assert result["optimal_sequence_ratio"] == 100.0  # Grep → targeted Read

    def test_direct_read_detected(self):
        """Verify direct read (without Grep) is detected."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": 1,
                "tool_calls": [
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                ],
                "is_grep_guided": False,
                "total_tokens": 200,
            }
        ])

        assert result["grep_guided_read_ratio"] == 0.0
        assert result["direct_read_ratio"] == 100.0
        assert result["avg_tokens_direct"] == 200.0

    def test_pattern_specificity_tracking(self):
        """Verify pattern specificity is tracked."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": 1,
                "tool_calls": [{"tool_name": "Grep"}],
                "pattern_specificity": 0.5,
            },
            {
                "sequence_index": 2,
                "tool_calls": [{"tool_name": "Grep"}],
                "pattern_specificity": 0.7,
            }
        ])

        assert result["avg_pattern_specificity"] == 0.6  # (0.5 + 0.7) / 2

    def test_inefficient_pattern_detection(self):
        """Verify inefficient patterns (too broad or narrow) are detected."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": 1,
                "tool_calls": [{"tool_name": "Grep"}],
                "pattern_specificity": 0.2,  # Too broad
            },
            {
                "sequence_index": 2,
                "tool_calls": [{"tool_name": "Grep"}],
                "pattern_specificity": 0.95,  # Too narrow
            },
            {
                "sequence_index": 3,
                "tool_calls": [{"tool_name": "Grep"}],
                "pattern_specificity": 0.5,  # Good
            }
        ])

        # 2 out of 3 are inefficient
        assert result["inefficient_pattern_ratio"] == pytest.approx(66.67, abs=0.01)

    def test_optimal_sequence_with_targeted_read(self):
        """Verify optimal Grep→targeted-Read sequence is detected."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                    {"tool_name": "Read", "parameters": {
                        "file_path": "/a.py",
                        "offset": 100,
                        "limit": 50
                    }},
                ],
                "is_grep_guided": True,
                "pattern_specificity": 0.6,
            }
        ])

        assert result["optimal_sequence_ratio"] == 100.0

    def test_grep_guided_read_without_offset_limit(self):
        """Verify Grep-guided read without offset/limit is not optimal."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep", "parameters": {"pattern": "test"}},
                    {"tool_name": "Read", "parameters": {"file_path": "/a.py"}},
                ],
                "is_grep_guided": True,
                "pattern_specificity": 0.6,
            }
        ])

        assert result["grep_guided_read_ratio"] == 100.0
        assert result["optimal_sequence_ratio"] == 0.0  # No offset/limit

    def test_token_correlation_negative(self):
        """Verify negative correlation when Grep-guided uses fewer tokens."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep"},
                    {"tool_name": "Read"},
                ],
                "is_grep_guided": True,
                "total_tokens": 100,
            },
            {
                "sequence_index": 2,
                "tool_calls": [{"tool_name": "Read"}],
                "is_grep_guided": False,
                "total_tokens": 200,
            }
        ])

        # Grep-guided: 100, Direct: 200
        assert result["avg_tokens_grep_guided"] == 100.0
        assert result["avg_tokens_direct"] == 200.0
        assert result["token_correlation"] < 0  # Negative correlation

    def test_token_correlation_positive(self):
        """Verify positive correlation when direct read uses fewer tokens."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep"},
                    {"tool_name": "Read"},
                ],
                "is_grep_guided": True,
                "total_tokens": 300,
            },
            {
                "sequence_index": 2,
                "tool_calls": [{"tool_name": "Read"}],
                "is_grep_guided": False,
                "total_tokens": 150,
            }
        ])

        # Grep-guided: 300, Direct: 150
        assert result["token_correlation"] > 0  # Positive correlation

    def test_mixed_strategies(self):
        """Verify mixed Grep and direct read strategies."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": 1,
                "tool_calls": [
                    {"tool_name": "Grep"},
                    {"tool_name": "Read", "parameters": {"offset": 0, "limit": 50}},
                ],
                "is_grep_guided": True,
                "pattern_specificity": 0.5,
            },
            {
                "sequence_index": 2,
                "tool_calls": [
                    {"tool_name": "Read"},
                ],
                "is_grep_guided": False,
            },
            {
                "sequence_index": 3,
                "tool_calls": [
                    {"tool_name": "Grep"},
                    {"tool_name": "Read", "parameters": {"limit": 30}},
                ],
                "is_grep_guided": True,
                "pattern_specificity": 0.6,
            }
        ])

        # 2 Grep-guided, 1 direct = 66.67% Grep-guided
        assert result["grep_guided_read_ratio"] == pytest.approx(66.67, abs=0.01)
        assert result["direct_read_ratio"] == pytest.approx(33.33, abs=0.01)
        assert result["avg_pattern_specificity"] == 0.55  # (0.5 + 0.6) / 2

    def test_efficiency_score_high(self):
        """Verify high efficiency score for good strategy."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": i,
                "tool_calls": [
                    {"tool_name": "Grep"},
                    {"tool_name": "Read", "parameters": {"offset": 0, "limit": 50}},
                ],
                "is_grep_guided": True,
                "pattern_specificity": 0.5,
            }
            for i in range(10)
        ])

        # 100% Grep-guided, optimal patterns, all optimal sequences
        assert result["search_efficiency_score"] > 80.0

    def test_efficiency_score_low(self):
        """Verify low efficiency score for poor strategy."""
        result = analyze_session_grep_read_strategy([
            {
                "sequence_index": i,
                "tool_calls": [{"tool_name": "Read"}],
                "is_grep_guided": False,
            }
            for i in range(10)
        ])

        # All direct reads, no patterns
        assert result["search_efficiency_score"] < 20.0


class TestCalculateEfficiencyScore:
    """Test efficiency score calculation."""

    def test_perfect_score(self):
        """Verify perfect score for ideal strategy."""
        # 100% Grep-guided, 0.5 specificity (ideal), 100% optimal sequences
        score = _calculate_efficiency_score(100.0, 0.0, 0.5, 100.0)
        assert score == 100.0

    def test_zero_score(self):
        """Verify zero score for worst strategy."""
        # 0% Grep-guided, 0.0 specificity, 0% optimal sequences
        score = _calculate_efficiency_score(0.0, 100.0, 0.0, 0.0)
        assert score < 20.0

    def test_moderate_score(self):
        """Verify moderate score for mixed strategy."""
        # 50% Grep-guided, 0.5 specificity, 50% optimal sequences
        score = _calculate_efficiency_score(50.0, 50.0, 0.5, 50.0)
        assert 40.0 < score < 70.0


class TestCalculateTokenCorrelation:
    """Test token correlation calculation."""

    def test_negative_correlation(self):
        """Verify negative correlation when Grep-guided is more efficient."""
        corr = _calculate_token_correlation([100, 100], [200, 200])
        assert corr < 0

    def test_positive_correlation(self):
        """Verify positive correlation when direct is more efficient."""
        corr = _calculate_token_correlation([300, 300], [150, 150])
        assert corr > 0

    def test_zero_correlation(self):
        """Verify zero correlation when equal."""
        corr = _calculate_token_correlation([100, 100], [100, 100])
        assert corr == 0.0

    def test_insufficient_data(self):
        """Verify insufficient data returns 0.0."""
        assert _calculate_token_correlation([], []) == 0.0
        assert _calculate_token_correlation([100], []) == 0.0
        assert _calculate_token_correlation([], [100]) == 0.0


class TestHelperFunctions:
    """Test helper functions."""

    def test_percentage_calculation(self):
        """Verify percentage calculation."""
        assert _percentage(50, 100) == 50.0
        assert _percentage(1, 3) == 33.33
        assert _percentage(0, 100) == 0.0

    def test_percentage_zero_denominator(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(50, 0) == 0.0

    def test_average_calculation(self):
        """Verify average calculation."""
        assert _average([1.0, 2.0, 3.0]) == 2.0
        assert _average([10.0, 20.0]) == 15.0
        assert _average([100.0]) == 100.0

    def test_average_empty_list(self):
        """Verify empty list returns 0.0."""
        assert _average([]) == 0.0
