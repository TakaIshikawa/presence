"""Tests for analyze_backtest.py pure statistics functions."""

import sys
from pathlib import Path

import pytest

# Add scripts/ to path
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from analyze_backtest import (
    _is_link_only,
    pearson_correlation,
    quartile_precision,
    spearman_rank_correlation,
)


# --- _is_link_only ---


class TestIsLinkOnly:
    def test_url_only(self):
        assert _is_link_only("https://example.com/article") is True

    def test_url_with_few_words(self):
        assert _is_link_only("Check this https://example.com/article") is True

    def test_url_with_three_words(self):
        # 3 words = boundary (≤3 → True)
        assert _is_link_only("Check this out https://example.com") is True

    def test_url_with_substantive_text(self):
        assert _is_link_only(
            "This is a really interesting article about AI https://example.com"
        ) is False

    def test_no_url_substantive_text(self):
        assert _is_link_only("Building a new feature for the dashboard today") is False

    def test_multiple_urls_stripped(self):
        assert _is_link_only(
            "https://one.com https://two.com check"
        ) is True

    def test_no_url_few_words(self):
        # Few words, no URL — still ≤3 words
        assert _is_link_only("Hello world") is True

    def test_empty_string(self):
        assert _is_link_only("") is True


# --- spearman_rank_correlation ---


class TestSpearmanRankCorrelation:
    def test_perfect_positive(self):
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert spearman_rank_correlation(x, y) == pytest.approx(1.0)

    def test_perfect_negative(self):
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [5.0, 4.0, 3.0, 2.0, 1.0]
        assert spearman_rank_correlation(x, y) == pytest.approx(-1.0)

    def test_no_correlation(self):
        # Orthogonal-ish ordering
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [3.0, 5.0, 1.0, 4.0, 2.0]
        result = spearman_rank_correlation(x, y)
        assert -0.5 < result < 0.5

    def test_tied_values(self):
        x = [1.0, 2.0, 2.0, 4.0, 5.0]
        y = [1.0, 3.0, 2.0, 4.0, 5.0]
        result = spearman_rank_correlation(x, y)
        assert 0.5 < result < 1.0

    def test_fewer_than_three_returns_zero(self):
        assert spearman_rank_correlation([1.0, 2.0], [2.0, 1.0]) == 0.0

    def test_exactly_three_elements(self):
        x = [1.0, 2.0, 3.0]
        y = [1.0, 2.0, 3.0]
        assert spearman_rank_correlation(x, y) == pytest.approx(1.0)

    def test_empty_lists(self):
        assert spearman_rank_correlation([], []) == 0.0


# --- pearson_correlation ---


class TestPearsonCorrelation:
    def test_perfect_positive(self):
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [2.0, 4.0, 6.0, 8.0, 10.0]
        assert pearson_correlation(x, y) == pytest.approx(1.0)

    def test_perfect_negative(self):
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [10.0, 8.0, 6.0, 4.0, 2.0]
        assert pearson_correlation(x, y) == pytest.approx(-1.0)

    def test_zero_variance_returns_zero(self):
        x = [3.0, 3.0, 3.0, 3.0]
        y = [1.0, 2.0, 3.0, 4.0]
        assert pearson_correlation(x, y) == 0.0

    def test_fewer_than_three_returns_zero(self):
        assert pearson_correlation([1.0, 2.0], [2.0, 4.0]) == 0.0

    def test_known_dataset(self):
        # Known: x=[1,2,3,4,5], y=[2,3,5,4,6]
        x = [1.0, 2.0, 3.0, 4.0, 5.0]
        y = [2.0, 3.0, 5.0, 4.0, 6.0]
        result = pearson_correlation(x, y)
        assert 0.8 < result < 1.0  # strong positive

    def test_empty_lists(self):
        assert pearson_correlation([], []) == 0.0


# --- quartile_precision ---


class TestQuartilePrecision:
    def test_perfect_prediction(self):
        predicted = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        actual = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        assert quartile_precision(predicted, actual, "top") == pytest.approx(1.0)
        assert quartile_precision(predicted, actual, "bottom") == pytest.approx(1.0)

    def test_inverted_prediction(self):
        predicted = [8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0]
        actual = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0]
        # Top quartile: predicted says [8,7] are top → actual says those are bottom → 0 overlap
        assert quartile_precision(predicted, actual, "top") == pytest.approx(0.0)

    def test_top_vs_bottom(self):
        predicted = [5.0, 1.0, 3.0, 7.0]
        actual = [4.0, 2.0, 3.0, 8.0]
        # q_size = max(1, 4//4) = 1
        # Top: predicted top-1 = index 3 (7.0), actual top-1 = index 3 (8.0) → overlap=1 → 1.0
        assert quartile_precision(predicted, actual, "top") == pytest.approx(1.0)

    def test_small_n(self):
        predicted = [1.0, 2.0, 3.0, 4.0]
        actual = [1.0, 2.0, 3.0, 4.0]
        # q_size = max(1, 4//4) = 1
        result = quartile_precision(predicted, actual, "top")
        assert result == pytest.approx(1.0)
