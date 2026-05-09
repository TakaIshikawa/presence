"""Tests for session Glob pattern analyzer."""

import pytest

from synthesis.session_glob_patterns import analyze_session_glob_patterns


class TestAnalyzeSessionGlobPatterns:
    """Test main analyzer function."""

    def test_empty_records_returns_zeroed_metrics(self):
        """Verify empty record list returns zero metrics."""
        result = analyze_session_glob_patterns([])

        assert result["total_glob_calls"] == 0
        assert result["avg_specificity_score"] == 0.0
        assert result["inefficient_searches"] == 0
        assert result["inefficient_search_ratio"] == 0.0
        assert result["avg_files_returned"] == 0.0
        assert result["avg_files_used"] == 0.0
        assert result["pattern_to_action_ratio"] == 0.0
        assert result["duplicate_patterns"] == 0
        assert result["duplicate_pattern_ratio"] == 0.0
        assert result["search_efficiency_score"] == 0.0
        assert result["high_efficiency_searches"] == 0
        assert result["low_efficiency_searches"] == 0
        assert result["patterns_by_type"] == {}
        assert result["inefficient_patterns"] == []
        assert result["recommendations"] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_glob_patterns(None)
        assert result["total_glob_calls"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_glob_patterns("not a list")

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_glob_patterns([
            "not a dict",
            {
                "glob_id": "glob1",
                "pattern": "*.py",
                "files_returned": 10,
            },
        ])

        assert result["total_glob_calls"] == 1

    def test_high_efficiency_targeted_pattern(self):
        """Verify high efficiency with specific pattern and high usage."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "src/synthesis/pack_*.py",
                "files_returned": 8,
                "files_used": 6,
                "specificity_score": 0.95,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "exact",
            }
        ])

        assert result["total_glob_calls"] == 1
        assert result["avg_specificity_score"] == 0.95
        assert result["inefficient_searches"] == 0
        assert result["avg_files_returned"] == 8.0
        assert result["avg_files_used"] == 6.0
        # 6 / 8 = 75%
        assert result["pattern_to_action_ratio"] == 75.0
        assert result["duplicate_patterns"] == 0
        # High specificity (40) + low files (25) + high ratio (25) + no dup (10) = 100
        assert result["search_efficiency_score"] == 100.0
        assert result["high_efficiency_searches"] == 1
        assert result["low_efficiency_searches"] == 0
        assert result["patterns_by_type"]["exact"] == 1

    def test_low_efficiency_broad_pattern(self):
        """Verify low efficiency with broad pattern and low usage."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "**/*",
                "files_returned": 150,
                "files_used": 5,
                "specificity_score": 0.1,
                "is_inefficient": True,
                "is_duplicate": False,
                "pattern_type": "broad",
            }
        ])

        assert result["total_glob_calls"] == 1
        assert result["avg_specificity_score"] == 0.1
        assert result["inefficient_searches"] == 1
        assert result["inefficient_search_ratio"] == 100.0
        assert result["avg_files_returned"] == 150.0
        assert result["avg_files_used"] == 5.0
        # 5 / 150 = 3.33%
        assert 3.0 <= result["pattern_to_action_ratio"] <= 3.5
        # Low specificity (0) + high files (0) + low ratio (0) + no dup (10) = 10
        assert result["search_efficiency_score"] == 10.0
        assert result["low_efficiency_searches"] == 1
        assert result["high_efficiency_searches"] == 0
        assert result["patterns_by_type"]["broad"] == 1
        assert len(result["inefficient_patterns"]) == 1
        assert result["inefficient_patterns"][0]["pattern"] == "**/*"
        assert result["inefficient_patterns"][0]["files_returned"] == 150

    def test_medium_specificity_pattern(self):
        """Verify medium specificity pattern scoring."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "**/*.py",
                "files_returned": 35,
                "files_used": 15,
                "specificity_score": 0.6,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "medium",
            }
        ])

        assert result["avg_specificity_score"] == 0.6
        assert result["inefficient_searches"] == 0
        # 15 / 35 = 42.86%
        assert 42.0 <= result["pattern_to_action_ratio"] <= 43.0
        # Medium specificity (20) + acceptable files (15) + good ratio (20) + no dup (10) = 65
        assert result["search_efficiency_score"] == 65.0
        assert result["patterns_by_type"]["medium"] == 1

    def test_duplicate_pattern_penalty(self):
        """Verify duplicate pattern reduces efficiency score."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "src/**/*.py",
                "files_returned": 10,
                "files_used": 8,
                "specificity_score": 0.9,
                "is_inefficient": False,
                "is_duplicate": True,
                "pattern_type": "exact",
            }
        ])

        assert result["duplicate_patterns"] == 1
        assert result["duplicate_pattern_ratio"] == 100.0
        # High specificity (40) + low files (25) + high ratio (25) + IS dup (0) = 90
        assert result["search_efficiency_score"] == 90.0

    def test_multiple_glob_calls_averaged(self):
        """Verify metrics averaged across multiple Glob calls."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "src/synthesis/*.py",
                "files_returned": 20,
                "files_used": 15,
                "specificity_score": 0.9,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "exact",
            },
            {
                "glob_id": "glob2",
                "pattern": "**/*.ts",
                "files_returned": 40,
                "files_used": 20,
                "specificity_score": 0.5,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "medium",
            },
        ])

        assert result["total_glob_calls"] == 2
        # (0.9 + 0.5) / 2 = 0.7
        assert result["avg_specificity_score"] == 0.7
        # (20 + 40) / 2 = 30
        assert result["avg_files_returned"] == 30.0
        # (15 + 20) / 2 = 17.5
        assert result["avg_files_used"] == 17.5
        # (75% + 50%) / 2 = 62.5%
        assert result["pattern_to_action_ratio"] == 62.5
        assert result["patterns_by_type"]["exact"] == 1
        assert result["patterns_by_type"]["medium"] == 1

    def test_inefficient_patterns_list_limited(self):
        """Verify inefficient patterns list limited to 10 entries."""
        records = [
            {
                "glob_id": f"glob{i}",
                "pattern": f"pattern{i}/**/*",
                "files_returned": 100 + i,
                "files_used": 5,
                "specificity_score": 0.1,
                "is_inefficient": True,
                "is_duplicate": False,
                "pattern_type": "broad",
            }
            for i in range(15)
        ]

        result = analyze_session_glob_patterns(records)

        assert result["total_glob_calls"] == 15
        assert result["inefficient_searches"] == 15
        # Inefficient patterns list should be limited to 10
        assert len(result["inefficient_patterns"]) == 10

    def test_pattern_to_action_ratio_zero_files_returned(self):
        """Verify pattern-to-action ratio handles zero files returned."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "nonexistent/*.foo",
                "files_returned": 0,
                "files_used": 0,
                "specificity_score": 0.8,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "exact",
            }
        ])

        # No ratio calculated when files_returned = 0
        assert result["pattern_to_action_ratio"] == 0.0
        assert result["avg_files_returned"] == 0.0
        assert result["avg_files_used"] == 0.0

    def test_pattern_to_action_ratio_capped_at_100(self):
        """Verify pattern-to-action ratio capped at 100%."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "exact_file.py",
                "files_returned": 1,
                "files_used": 1,
                "specificity_score": 1.0,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "exact",
            }
        ])

        # 1 / 1 = 100%
        assert result["pattern_to_action_ratio"] == 100.0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "*.py",
                # Missing most fields
            }
        ])

        assert result["total_glob_calls"] == 1
        assert result["avg_specificity_score"] == 0.0
        assert result["patterns_by_type"]["unknown"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "*.py",
                "files_returned": True,
                "files_used": False,
                "specificity_score": True,
            }
        ])

        assert result["total_glob_calls"] == 1
        assert result["avg_specificity_score"] == 0.0
        assert result["avg_files_returned"] == 0.0
        assert result["avg_files_used"] == 0.0

    def test_boundary_efficiency_classification(self):
        """Verify boundary cases for efficiency classification."""
        result = analyze_session_glob_patterns([
            # Exactly 75: specificity (30) + files (20) + ratio (15) + no dup (10) = 75
            {
                "glob_id": "s1",
                "pattern": "src/**/*.py",
                "files_returned": 25,
                "files_used": 6,  # 24% ratio
                "specificity_score": 0.75,
                "is_inefficient": False,
                "is_duplicate": False,
            },
            # Just above 75: specificity (40) + files (25) + ratio (15) + no dup (10) = 90
            {
                "glob_id": "s2",
                "pattern": "src/synthesis/*.py",
                "files_returned": 8,
                "files_used": 2,  # 25% ratio
                "specificity_score": 0.9,
                "is_inefficient": False,
                "is_duplicate": False,
            },
            # Exactly 40: specificity (20) + files (10) + ratio (0) + no dup (10) = 40
            {
                "glob_id": "s3",
                "pattern": "**/*.js",
                "files_returned": 80,
                "files_used": 5,  # 6.25% ratio
                "specificity_score": 0.5,
                "is_inefficient": False,
                "is_duplicate": False,
            },
            # Below 40: specificity (10) + files (0) + ratio (0) + no dup (10) = 20
            {
                "glob_id": "s4",
                "pattern": "**/*",
                "files_returned": 120,
                "files_used": 3,  # 2.5% ratio
                "specificity_score": 0.35,
                "is_inefficient": True,
                "is_duplicate": False,
            },
        ])

        # >75 means strictly greater
        assert result["high_efficiency_searches"] == 1
        # <40 means strictly less
        assert result["low_efficiency_searches"] == 1

    def test_efficiency_score_excellent_all_metrics(self):
        """Verify efficiency score with excellent all metrics."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "excellent",
                "pattern": "src/synthesis/pack_error_recovery.py",
                "files_returned": 1,
                "files_used": 1,
                "specificity_score": 1.0,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "exact",
            }
        ])

        # Should score: 40 + 25 + 25 + 10 = 100
        assert result["search_efficiency_score"] == 100.0
        assert result["high_efficiency_searches"] == 1

    def test_efficiency_score_poor_all_metrics(self):
        """Verify efficiency score with poor all metrics."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "poor",
                "pattern": "**/*",
                "files_returned": 200,
                "files_used": 5,
                "specificity_score": 0.05,
                "is_inefficient": True,
                "is_duplicate": True,
                "pattern_type": "broad",
            }
        ])

        # Should score: 0 + 0 + 0 + 0 = 0
        assert result["search_efficiency_score"] == 0.0
        assert result["low_efficiency_searches"] == 1

    def test_efficiency_score_mixed_metrics(self):
        """Verify efficiency score with mixed quality metrics."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "mixed",
                "pattern": "src/**/*.ts",
                "files_returned": 45,
                "files_used": 20,
                "specificity_score": 0.65,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "medium",
            }
        ])

        # Should score: 20 (specificity) + 15 (files) + 20 (ratio ~44%) + 10 (no dup) = 65
        assert result["search_efficiency_score"] == 65.0

    def test_recommendations_low_specificity(self):
        """Verify recommendation for low specificity."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "**/*",
                "files_returned": 100,
                "files_used": 10,
                "specificity_score": 0.3,
                "is_inefficient": True,
                "is_duplicate": False,
            }
        ])

        recommendations = result["recommendations"]
        assert any("more specific patterns" in r for r in recommendations)
        assert any("overly broad searches" in r for r in recommendations)

    def test_recommendations_high_inefficient_ratio(self):
        """Verify recommendation for high inefficient search ratio."""
        records = [
            {
                "glob_id": f"glob{i}",
                "pattern": f"**/*.{i}",
                "files_returned": 100,
                "files_used": 10,
                "specificity_score": 0.5,
                "is_inefficient": True,
                "is_duplicate": False,
            }
            for i in range(5)
        ]

        result = analyze_session_glob_patterns(records)
        recommendations = result["recommendations"]
        assert any("overly broad searches" in r for r in recommendations)

    def test_recommendations_low_pattern_to_action(self):
        """Verify recommendation for low pattern-to-action ratio."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "src/**/*.py",
                "files_returned": 50,
                "files_used": 5,
                "specificity_score": 0.6,
                "is_inefficient": False,
                "is_duplicate": False,
            }
        ])

        recommendations = result["recommendations"]
        assert any("pattern-to-action ratio" in r for r in recommendations)

    def test_recommendations_high_duplicate_ratio(self):
        """Verify recommendation for high duplicate ratio."""
        records = [
            {
                "glob_id": f"glob{i}",
                "pattern": "src/**/*.py",
                "files_returned": 20,
                "files_used": 10,
                "specificity_score": 0.7,
                "is_inefficient": False,
                "is_duplicate": True,
            }
            for i in range(5)
        ]

        result = analyze_session_glob_patterns(records)
        recommendations = result["recommendations"]
        assert any("similar patterns" in r for r in recommendations)

    def test_recommendations_good_efficiency(self):
        """Verify recommendation for good overall efficiency."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "glob1",
                "pattern": "src/synthesis/*.py",
                "files_returned": 10,
                "files_used": 8,
                "specificity_score": 0.9,
                "is_inefficient": False,
                "is_duplicate": False,
            }
        ])

        recommendations = result["recommendations"]
        assert any("efficient" in r and "maintain" in r for r in recommendations)

    def test_comprehensive_glob_call_all_fields(self):
        """Verify comprehensive Glob call with all fields populated."""
        result = analyze_session_glob_patterns([
            {
                "glob_id": "comprehensive",
                "session_id": "session123",
                "pattern": "src/synthesis/session_*.py",
                "files_returned": 12,
                "files_used": 9,
                "specificity_score": 0.85,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "exact",
            }
        ])

        assert result["total_glob_calls"] == 1
        assert result["avg_specificity_score"] == 0.85
        assert result["inefficient_searches"] == 0
        assert result["avg_files_returned"] == 12.0
        assert result["avg_files_used"] == 9.0
        # 9 / 12 = 75%
        assert result["pattern_to_action_ratio"] == 75.0
        assert result["duplicate_patterns"] == 0
        # Good specificity (30) + good files (20) + excellent ratio (25) + no dup (10) = 85
        assert result["search_efficiency_score"] == 85.0
        assert result["high_efficiency_searches"] == 1
        assert result["patterns_by_type"]["exact"] == 1

    def test_mixed_efficiency_sessions(self):
        """Verify sessions with mixed efficiency searches."""
        result = analyze_session_glob_patterns([
            # High efficiency
            {
                "glob_id": "glob1",
                "pattern": "src/synthesis/*.py",
                "files_returned": 8,
                "files_used": 7,
                "specificity_score": 0.95,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "exact",
            },
            # Low efficiency
            {
                "glob_id": "glob2",
                "pattern": "**/*",
                "files_returned": 150,
                "files_used": 3,
                "specificity_score": 0.1,
                "is_inefficient": True,
                "is_duplicate": False,
                "pattern_type": "broad",
            },
            # Medium efficiency
            {
                "glob_id": "glob3",
                "pattern": "**/*.ts",
                "files_returned": 40,
                "files_used": 15,
                "specificity_score": 0.5,
                "is_inefficient": False,
                "is_duplicate": False,
                "pattern_type": "medium",
            },
        ])

        assert result["total_glob_calls"] == 3
        assert result["high_efficiency_searches"] == 1
        assert result["low_efficiency_searches"] == 1
        assert result["inefficient_searches"] == 1
        assert result["patterns_by_type"]["exact"] == 1
        assert result["patterns_by_type"]["broad"] == 1
        assert result["patterns_by_type"]["medium"] == 1

    def test_specificity_score_boundaries(self):
        """Verify specificity score boundaries in efficiency calculation."""
        test_cases = [
            (1.0, 40.0),   # >=0.9 excellent
            (0.9, 40.0),   # >=0.9 excellent
            (0.89, 30.0),  # >=0.7 good
            (0.7, 30.0),   # >=0.7 good
            (0.69, 20.0),  # >=0.5 acceptable
            (0.5, 20.0),   # >=0.5 acceptable
            (0.49, 10.0),  # >=0.3 poor
            (0.3, 10.0),   # >=0.3 poor
            (0.29, 0.0),   # <0.3 very broad
        ]

        for specificity, expected_points in test_cases:
            result = analyze_session_glob_patterns([
                {
                    "glob_id": "test",
                    "pattern": "test",
                    "specificity_score": specificity,
                    "is_duplicate": False,
                }
            ])
            # Specificity component + no duplication (10) = expected + 10
            assert result["search_efficiency_score"] == expected_points + 10.0

    def test_files_returned_boundaries(self):
        """Verify files returned boundaries in efficiency calculation."""
        test_cases = [
            (5, 25.0),    # <=10 excellent
            (10, 25.0),   # <=10 excellent
            (11, 20.0),   # <=30 good
            (30, 20.0),   # <=30 good
            (31, 15.0),   # <=50 acceptable
            (50, 15.0),   # <=50 acceptable
            (51, 10.0),   # <=100 poor
            (100, 10.0),  # <=100 poor
            (101, 0.0),   # >100 very broad
        ]

        for files, expected_points in test_cases:
            result = analyze_session_glob_patterns([
                {
                    "glob_id": "test",
                    "pattern": "test",
                    "files_returned": files,
                    "is_duplicate": False,
                }
            ])
            # Files component + no duplication (10) = expected + 10
            assert result["search_efficiency_score"] == expected_points + 10.0

    def test_pattern_to_action_boundaries(self):
        """Verify pattern-to-action ratio boundaries in efficiency calculation."""
        test_cases = [
            (60, 100, 25.0),  # 60% >=60% excellent
            (70, 100, 25.0),  # 70% >=60% excellent
            (59, 100, 20.0),  # 59% >=40% good
            (40, 100, 20.0),  # 40% >=40% good
            (39, 100, 15.0),  # 39% >=20% acceptable
            (20, 100, 15.0),  # 20% >=20% acceptable
            (19, 100, 10.0),  # 19% >=10% poor
            (10, 100, 10.0),  # 10% >=10% poor
            (9, 100, 0.0),    # 9% <10% very low
        ]

        for files_used, files_returned, expected_points in test_cases:
            result = analyze_session_glob_patterns([
                {
                    "glob_id": "test",
                    "pattern": "test",
                    "files_returned": files_returned,
                    "files_used": files_used,
                    "is_duplicate": False,
                }
            ])
            # Ratio component + files_returned component (10 for <=100) + no duplication (10)
            assert result["search_efficiency_score"] == expected_points + 10.0 + 10.0
