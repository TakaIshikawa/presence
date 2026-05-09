"""Tests for pack EnterPlanMode usage analyzer."""

import pytest

from synthesis.pack_planmode_usage import analyze_pack_planmode_usage


class TestAnalyzePackPlanModeUsage:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty packs returns zero metrics."""
        result = analyze_pack_planmode_usage([])

        assert result["total_packs"] == 0
        assert result["packs_with_planning"] == 0
        assert result["planning_usage_rate"] == 0.0
        assert result["avg_plan_file_lines"] == 0.0
        assert result["plans_within_optimal_size"] == 0
        assert result["total_exitplanmode_calls"] == 0
        assert result["avg_exitplanmode_per_pack"] == 0.0
        assert result["plans_revised_after_feedback"] == 0
        assert result["revision_rate"] == 0.0
        assert result["avg_plan_alignment_score"] == 0.0
        assert result["high_alignment_packs"] == 0
        assert result["low_alignment_packs"] == 0
        assert result["multi_file_tasks_without_planning"] == 0
        assert result["planning_discipline_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_planmode_usage(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_planmode_usage("not a list")

    def test_pack_with_planning(self):
        """Verify pack with EnterPlanMode usage."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_file_lines": 50,
                "exitplanmode_calls": 1,
                "plan_acceptance_criteria": ["AC1", "AC2"],
                "final_acceptance_criteria": ["AC1", "AC2"],
                "plan_revised": False,
                "files_changed_count": 3,
            }
        ])

        assert result["total_packs"] == 1
        assert result["packs_with_planning"] == 1
        assert result["planning_usage_rate"] == 100.0
        assert result["avg_plan_file_lines"] == 50.0
        assert result["plans_within_optimal_size"] == 1  # 50 is in 20-100 range
        assert result["total_exitplanmode_calls"] == 1
        assert result["multi_file_tasks_without_planning"] == 0

    def test_pack_without_planning(self):
        """Verify pack without EnterPlanMode usage."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": False,
                "files_changed_count": 1,
            }
        ])

        assert result["packs_with_planning"] == 0
        assert result["planning_usage_rate"] == 0.0
        assert result["multi_file_tasks_without_planning"] == 0  # Only 1 file changed

    def test_multi_file_task_without_planning_penalty(self):
        """Verify penalty for multi-file task without planning."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": False,
                "files_changed_count": 5,  # >2 files
            }
        ])

        assert result["multi_file_tasks_without_planning"] == 1

    def test_high_complexity_task_without_planning_penalty(self):
        """Verify penalty for high complexity task without planning."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": False,
                "task_complexity": "high",
                "files_changed_count": 1,
            }
        ])

        assert result["multi_file_tasks_without_planning"] == 1

    def test_plan_file_size_tracking(self):
        """Verify plan file size is tracked."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_file_lines": 30,
            },
            {
                "pack_id": "pack2",
                "enterplanmode_used": True,
                "plan_file_lines": 70,
            },
        ])

        # (30 + 70) / 2 = 50
        assert result["avg_plan_file_lines"] == 50.0
        assert result["plans_within_optimal_size"] == 2  # Both in 20-100 range

    def test_plan_size_boundaries(self):
        """Verify optimal plan size boundaries."""
        result = analyze_pack_planmode_usage([
            {"pack_id": "p1", "enterplanmode_used": True, "plan_file_lines": 10},   # Too small
            {"pack_id": "p2", "enterplanmode_used": True, "plan_file_lines": 20},   # Optimal
            {"pack_id": "p3", "enterplanmode_used": True, "plan_file_lines": 50},   # Optimal
            {"pack_id": "p4", "enterplanmode_used": True, "plan_file_lines": 100},  # Optimal
            {"pack_id": "p5", "enterplanmode_used": True, "plan_file_lines": 150},  # Too large
        ])

        assert result["plans_within_optimal_size"] == 3  # Only 20, 50, 100

    def test_exitplanmode_calls_tracking(self):
        """Verify ExitPlanMode calls are tracked."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "exitplanmode_calls": 2,  # Plan revised once
            },
            {
                "pack_id": "pack2",
                "enterplanmode_used": True,
                "exitplanmode_calls": 1,  # Direct approval
            },
        ])

        assert result["total_exitplanmode_calls"] == 3
        # (2 + 1) / 2 = 1.5
        assert result["avg_exitplanmode_per_pack"] == 1.5

    def test_plan_revision_tracking(self):
        """Verify plan revisions are tracked."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_revised": True,
            },
            {
                "pack_id": "pack2",
                "enterplanmode_used": True,
                "plan_revised": False,
            },
            {
                "pack_id": "pack3",
                "enterplanmode_used": True,
                "plan_revised": True,
            },
        ])

        assert result["plans_revised_after_feedback"] == 2
        # 2/3 = 66.67%
        assert result["revision_rate"] == 66.67

    def test_plan_alignment_perfect_match(self):
        """Verify perfect plan-to-implementation alignment."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_acceptance_criteria": ["AC1", "AC2", "AC3"],
                "final_acceptance_criteria": ["AC1", "AC2", "AC3"],
            }
        ])

        assert result["avg_plan_alignment_score"] == 100.0
        assert result["high_alignment_packs"] == 1

    def test_plan_alignment_partial_match(self):
        """Verify partial plan alignment."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_acceptance_criteria": ["Test passes", "Code builds"],
                "final_acceptance_criteria": ["Test passes successfully", "Build completes"],
            }
        ])

        # Both should fuzzy match
        assert result["avg_plan_alignment_score"] >= 50.0

    def test_plan_alignment_low_match(self):
        """Verify low plan alignment."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_acceptance_criteria": ["Original plan AC1", "Original plan AC2"],
                "final_acceptance_criteria": ["Completely different final AC1", "Different final AC2"],
            }
        ])

        assert result["low_alignment_packs"] == 1

    def test_high_alignment_classification(self):
        """Verify high alignment (>80%) classification."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_acceptance_criteria": ["AC1", "AC2", "AC3", "AC4", "AC5"],
                "final_acceptance_criteria": ["AC1", "AC2", "AC3", "AC4", "AC5"],
            }
        ])

        assert result["avg_plan_alignment_score"] == 100.0
        assert result["high_alignment_packs"] == 1
        assert result["low_alignment_packs"] == 0

    def test_mixed_alignment_scores(self):
        """Verify mixed alignment scores."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_acceptance_criteria": ["A", "B"],
                "final_acceptance_criteria": ["A", "B"],  # 100% match
            },
            {
                "pack_id": "pack2",
                "enterplanmode_used": True,
                "plan_acceptance_criteria": ["X", "Y"],
                "final_acceptance_criteria": ["Z", "W"],  # 0% match
            },
        ])

        # (100 + 0) / 2 = 50.0
        assert result["avg_plan_alignment_score"] == 50.0
        assert result["high_alignment_packs"] == 1
        assert result["low_alignment_packs"] == 1

    def test_planning_discipline_score_perfect(self):
        """Verify perfect planning discipline score."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_file_lines": 50,
                "plan_acceptance_criteria": ["AC1", "AC2"],
                "final_acceptance_criteria": ["AC1", "AC2"],
                "files_changed_count": 3,
            }
        ])

        # Perfect usage, alignment, and plan size
        assert result["planning_discipline_score"] > 0.9

    def test_planning_discipline_score_poor(self):
        """Verify poor planning discipline score."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": False,
                "files_changed_count": 10,  # Should have used planning
            }
        ])

        # Missed planning on complex task
        assert result["planning_discipline_score"] < 0.3

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_planmode_usage([
            "not a dict",
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
            },
        ])

        assert result["total_packs"] == 1

    def test_missing_optional_fields(self):
        """Verify missing optional fields are handled gracefully."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                # Missing most optional fields
            }
        ])

        assert result["total_packs"] == 1
        assert result["packs_with_planning"] == 1
        assert result["avg_plan_file_lines"] == 0.0
        assert result["total_exitplanmode_calls"] == 0

    def test_boolean_field_variations(self):
        """Verify boolean fields handle various input formats."""
        result = analyze_pack_planmode_usage([
            {"pack_id": "p1", "enterplanmode_used": True},
            {"pack_id": "p2", "enterplanmode_used": "true"},
            {"pack_id": "p3", "enterplanmode_used": "yes"},
            {"pack_id": "p4", "enterplanmode_used": False},
            {"pack_id": "p5", "enterplanmode_used": "false"},
        ])

        assert result["packs_with_planning"] == 3  # First 3 are truthy

    def test_empty_acceptance_criteria_lists(self):
        """Verify empty AC lists are handled."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_acceptance_criteria": [],
                "final_acceptance_criteria": [],
            }
        ])

        assert result["avg_plan_alignment_score"] == 0.0
        assert result["high_alignment_packs"] == 0

    def test_single_string_acceptance_criteria(self):
        """Verify single string AC is converted to list."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_acceptance_criteria": "Single AC",
                "final_acceptance_criteria": "Single AC",
            }
        ])

        # Should still calculate alignment
        assert result["avg_plan_alignment_score"] == 100.0

    def test_fuzzy_matching_partial_overlap(self):
        """Verify fuzzy matching for AC alignment."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_acceptance_criteria": ["Tests pass successfully"],
                "final_acceptance_criteria": ["All tests pass successfully"],  # Similar enough
            }
        ])

        # Should match due to >60% word overlap
        assert result["avg_plan_alignment_score"] > 0.0

    def test_case_insensitive_task_complexity(self):
        """Verify task complexity is case-insensitive."""
        result = analyze_pack_planmode_usage([
            {"pack_id": "p1", "enterplanmode_used": False, "task_complexity": "HIGH"},
            {"pack_id": "p2", "enterplanmode_used": False, "task_complexity": "High"},
            {"pack_id": "p3", "enterplanmode_used": False, "task_complexity": "high"},
        ])

        assert result["multi_file_tasks_without_planning"] == 3

    def test_optimal_pattern_comprehensive_planning(self):
        """Verify optimal pattern with comprehensive planning."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_file_lines": 75,
                "exitplanmode_calls": 1,
                "plan_acceptance_criteria": [
                    "Implements feature X correctly",
                    "All tests pass",
                    "Code is well-documented",
                ],
                "final_acceptance_criteria": [
                    "Feature X implemented correctly",
                    "Tests passing successfully",
                    "Documentation complete",
                ],
                "plan_revised": False,
                "files_changed_count": 5,
            }
        ])

        assert result["plans_within_optimal_size"] == 1
        # Fuzzy matching may not get perfect alignment
        assert result["avg_plan_alignment_score"] > 0.0
        assert result["multi_file_tasks_without_planning"] == 0
        assert result["planning_discipline_score"] > 0.5

    def test_anti_pattern_no_planning_complex_task(self):
        """Verify anti-pattern of skipping planning for complex task."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": False,
                "files_changed_count": 15,
                "task_complexity": "high",
            }
        ])

        assert result["packs_with_planning"] == 0
        assert result["multi_file_tasks_without_planning"] == 1
        assert result["planning_discipline_score"] < 0.5

    def test_anti_pattern_plan_misalignment(self):
        """Verify anti-pattern of poor plan-to-implementation alignment."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_file_lines": 40,
                "plan_acceptance_criteria": [
                    "Implement authentication",
                    "Add user profiles",
                    "Create admin dashboard",
                ],
                "final_acceptance_criteria": [
                    "Fixed bug in login",
                    "Updated styling",
                ],
            }
        ])

        assert result["avg_plan_alignment_score"] < 50.0
        assert result["low_alignment_packs"] == 1

    def test_anti_pattern_excessive_plan_size(self):
        """Verify anti-pattern of excessively large plans."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_file_lines": 500,  # Way too large
            }
        ])

        assert result["plans_within_optimal_size"] == 0
        assert result["avg_plan_file_lines"] == 500.0

    def test_anti_pattern_tiny_plan(self):
        """Verify anti-pattern of too-small plans."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_file_lines": 5,  # Too small
            }
        ])

        assert result["plans_within_optimal_size"] == 0

    def test_zero_files_changed_default(self):
        """Verify zero files changed by default doesn't trigger penalty."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": False,
                # files_changed_count not provided
            }
        ])

        assert result["multi_file_tasks_without_planning"] == 0

    def test_revision_rate_with_no_planning(self):
        """Verify revision rate is 0 when no planning used."""
        result = analyze_pack_planmode_usage([
            {
                "pack_id": "pack1",
                "enterplanmode_used": False,
            }
        ])

        assert result["revision_rate"] == 0.0

    def test_multiple_packs_mixed_patterns(self):
        """Verify analysis across multiple packs with mixed patterns."""
        result = analyze_pack_planmode_usage([
            # Good planning
            {
                "pack_id": "pack1",
                "enterplanmode_used": True,
                "plan_file_lines": 60,
                "exitplanmode_calls": 1,
                "plan_acceptance_criteria": ["AC1", "AC2"],
                "final_acceptance_criteria": ["AC1", "AC2"],
                "files_changed_count": 4,
            },
            # No planning on simple task (OK)
            {
                "pack_id": "pack2",
                "enterplanmode_used": False,
                "files_changed_count": 1,
            },
            # No planning on complex task (BAD)
            {
                "pack_id": "pack3",
                "enterplanmode_used": False,
                "files_changed_count": 8,
            },
            # Planning with revision
            {
                "pack_id": "pack4",
                "enterplanmode_used": True,
                "plan_file_lines": 45,
                "plan_revised": True,
                "exitplanmode_calls": 2,
            },
        ])

        assert result["total_packs"] == 4
        assert result["packs_with_planning"] == 2
        assert result["planning_usage_rate"] == 50.0
        assert result["multi_file_tasks_without_planning"] == 1  # pack3
        assert result["plans_revised_after_feedback"] == 1
