"""Tests for pack code complexity analyzer."""

import pytest

from synthesis.pack_code_complexity import analyze_pack_code_complexity


class TestAnalyzePackCodeComplexity:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_code_complexity([])

        assert result["total_packs"] == 0
        assert result["avg_total_lines_changed"] == 0.0
        assert result["avg_lines_added_ratio"] == 0.0
        assert result["avg_abstraction_count"] == 0.0
        assert result["avg_helper_utils_ratio"] == 0.0
        assert result["packs_with_premature_optimization"] == 0
        assert result["avg_unnecessary_additions_ratio"] == 0.0
        assert result["packs_with_feature_creep"] == 0
        assert result["complexity_score"] == 0.0
        assert result["simple_implementation_packs"] == 0
        assert result["over_engineered_packs"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_code_complexity(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_code_complexity("not a list")

    def test_simple_implementation_minimal_changes(self):
        """Verify simple implementation with minimal changes scores high."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "simple",
                "total_lines_changed": 100,
                "lines_added": 20,
                "new_functions": 1,
                "new_classes": 0,
                "helper_utils_created": 0,
                "feature_flags_added": 0,
                "compatibility_shims": 0,
                "unused_params_added": 0,
                "comments_on_unchanged": 5,
                "types_on_unchanged": 3,
                "defensive_validation": 2,
                "features_beyond_request": 0,
            }
        ])

        # 20 / 100 = 20%
        assert result["avg_lines_added_ratio"] == 20.0
        # 1 function
        assert result["avg_abstraction_count"] == 1.0
        assert result["packs_with_premature_optimization"] == 0
        # 10 / 100 = 10%
        assert result["avg_unnecessary_additions_ratio"] == 10.0
        assert result["packs_with_feature_creep"] == 0
        assert result["complexity_score"] > 80.0
        assert result["simple_implementation_packs"] == 1

    def test_over_engineered_excessive_abstractions(self):
        """Verify over-engineered implementation scores low."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "over_engineered",
                "total_lines_changed": 100,
                "lines_added": 90,
                "new_functions": 8,
                "new_classes": 3,
                "helper_utils_created": 6,
                "feature_flags_added": 2,
                "compatibility_shims": 1,
                "unused_params_added": 3,
                "comments_on_unchanged": 20,
                "types_on_unchanged": 15,
                "defensive_validation": 25,
                "features_beyond_request": 2,
            }
        ])

        # 90 / 100 = 90%
        assert result["avg_lines_added_ratio"] == 90.0
        # 8 + 3 = 11 abstractions
        assert result["avg_abstraction_count"] == 11.0
        # 6 / 11 = 54.55%
        assert 54.0 <= result["avg_helper_utils_ratio"] <= 55.0
        assert result["packs_with_premature_optimization"] == 1
        # 60 / 100 = 60%
        assert result["avg_unnecessary_additions_ratio"] == 60.0
        assert result["packs_with_feature_creep"] == 1
        assert result["complexity_score"] < 50.0
        assert result["over_engineered_packs"] == 1

    def test_premature_optimization_detection(self):
        """Verify detection of premature optimization patterns."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "flags",
                "total_lines_changed": 50,
                "feature_flags_added": 1,
            },
            {
                "pack_id": "shims",
                "total_lines_changed": 50,
                "compatibility_shims": 1,
            },
            {
                "pack_id": "unused",
                "total_lines_changed": 50,
                "unused_params_added": 1,
            },
            {
                "pack_id": "clean",
                "total_lines_changed": 50,
                "feature_flags_added": 0,
            },
        ])

        assert result["packs_with_premature_optimization"] == 3

    def test_feature_creep_detection(self):
        """Verify feature creep detection."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "creep",
                "total_lines_changed": 100,
                "features_beyond_request": 3,
            },
            {
                "pack_id": "clean",
                "total_lines_changed": 100,
                "features_beyond_request": 0,
            },
        ])

        assert result["packs_with_feature_creep"] == 1

    def test_abstraction_count_calculation(self):
        """Verify abstraction count includes functions and classes."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "pack1",
                "total_lines_changed": 200,
                "new_functions": 5,
                "new_classes": 2,
            }
        ])

        # 5 + 2 = 7
        assert result["avg_abstraction_count"] == 7.0

    def test_helper_utils_ratio_calculation(self):
        """Verify helper utils ratio calculated correctly."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "pack1",
                "total_lines_changed": 100,
                "new_functions": 10,
                "helper_utils_created": 6,
            }
        ])

        # 6 / 10 = 60%
        assert result["avg_helper_utils_ratio"] == 60.0

    def test_unnecessary_additions_ratio(self):
        """Verify unnecessary additions ratio calculated correctly."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "pack1",
                "total_lines_changed": 100,
                "comments_on_unchanged": 10,
                "types_on_unchanged": 5,
                "defensive_validation": 15,
            }
        ])

        # (10 + 5 + 15) / 100 = 30%
        assert result["avg_unnecessary_additions_ratio"] == 30.0

    def test_multiple_packs_averaged(self):
        """Verify metrics averaged across multiple packs."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "pack1",
                "total_lines_changed": 100,
                "lines_added": 30,
            },
            {
                "pack_id": "pack2",
                "total_lines_changed": 200,
                "lines_added": 50,
            },
        ])

        assert result["total_packs"] == 2
        # (100 + 200) / 2 = 150
        assert result["avg_total_lines_changed"] == 150.0
        # (30% + 25%) / 2 = 27.5%
        assert result["avg_lines_added_ratio"] == 27.5

    def test_complexity_score_excellent_metrics(self):
        """Verify complexity score with excellent simplicity metrics."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "excellent",
                "total_lines_changed": 100,
                "lines_added": 25,  # 25% (30pts)
                "new_functions": 1,
                "new_classes": 0,  # 1 abstraction (25pts)
                "feature_flags_added": 0,  # 20pts
                "comments_on_unchanged": 10,
                "types_on_unchanged": 0,
                "defensive_validation": 0,  # 10% (15pts)
                "features_beyond_request": 0,  # 10pts
            }
        ])

        # Should score: 30 + 25 + 20 + 15 + 10 = 100
        assert result["complexity_score"] == 100.0
        assert result["simple_implementation_packs"] == 1

    def test_complexity_score_poor_metrics(self):
        """Verify complexity score with poor metrics."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "poor",
                "total_lines_changed": 100,
                "lines_added": 90,  # 90% (0pts)
                "new_functions": 10,
                "new_classes": 5,  # 15 abstractions (0pts)
                "feature_flags_added": 2,  # 0pts
                "comments_on_unchanged": 30,
                "types_on_unchanged": 25,
                "defensive_validation": 20,  # 75% (0pts)
                "features_beyond_request": 3,  # 0pts
            }
        ])

        # Should score: 0 + 0 + 0 + 0 + 0 = 0
        assert result["complexity_score"] == 0.0
        assert result["over_engineered_packs"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_code_complexity([
            "not a dict",
            {
                "pack_id": "pack1",
                "total_lines_changed": 50,
            },
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "pack1",
                "total_lines_changed": True,
                "lines_added": False,
            }
        ])

        assert result["avg_total_lines_changed"] == 0.0

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_code_complexity([
            {
                "pack_id": "comprehensive",
                "pack_title": "Test Pack",
                "total_lines_changed": 250,
                "lines_added": 60,
                "lines_modified": 190,
                "new_functions": 3,
                "new_classes": 1,
                "helper_utils_created": 1,
                "feature_flags_added": 0,
                "compatibility_shims": 0,
                "unused_params_added": 0,
                "comments_on_unchanged": 15,
                "types_on_unchanged": 10,
                "defensive_validation": 5,
                "features_beyond_request": 0,
            }
        ])

        assert result["avg_total_lines_changed"] == 250.0
        # 60 / 250 = 24%
        assert result["avg_lines_added_ratio"] == 24.0
        # 3 + 1 = 4
        assert result["avg_abstraction_count"] == 4.0
        # 1 / 4 = 25%
        assert result["avg_helper_utils_ratio"] == 25.0
        assert result["packs_with_premature_optimization"] == 0
        # 30 / 250 = 12%
        assert result["avg_unnecessary_additions_ratio"] == 12.0
        assert result["packs_with_feature_creep"] == 0
        # Should score high for simple implementation
        assert result["complexity_score"] > 75.0
