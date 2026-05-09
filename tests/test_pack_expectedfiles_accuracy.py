"""Tests for pack expectedFiles accuracy analyzer."""

import pytest

from synthesis.pack_expectedfiles_accuracy import analyze_pack_expectedfiles_accuracy


class TestAnalyzePackExpectedFilesAccuracy:
    """Test main analyzer function."""

    def test_empty_pack(self):
        result = analyze_pack_expectedfiles_accuracy([])
        assert result["total_sessions"] == 0
        assert result["accuracy_score"] == 0.0

    def test_perfect_prediction(self):
        result = analyze_pack_expectedfiles_accuracy([
            {
                "session_id": "s1",
                "declared_files_count": 5,
                "actually_changed_count": 5,
                "correctly_predicted": 5,
                "unexpected_modifications": 0,
                "missing_test_companions": 0,
                "total_source_files": 5,
            }
        ])

        assert result["precision"] == 100.0
        assert result["recall"] == 100.0
        assert result["f1_score"] == 100.0
        assert result["test_coverage_ratio"] == 100.0
        assert result["accuracy_score"] >= 0.9

    def test_over_prediction(self):
        result = analyze_pack_expectedfiles_accuracy([
            {
                "declared_files_count": 10,
                "actually_changed_count": 5,
                "correctly_predicted": 5,
            }
        ])

        assert result["precision"] == 50.0
        assert result["recall"] == 100.0

    def test_under_prediction(self):
        result = analyze_pack_expectedfiles_accuracy([
            {
                "declared_files_count": 3,
                "actually_changed_count": 6,
                "correctly_predicted": 3,
                "unexpected_modifications": 3,
            }
        ])

        assert result["precision"] == 100.0
        assert result["recall"] == 50.0
        assert result["unexpected_ratio"] == 50.0
