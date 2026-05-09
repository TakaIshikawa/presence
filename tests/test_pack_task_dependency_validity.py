"""Tests for pack task dependency validity analyzer."""

import pytest

from synthesis.pack_task_dependency_validity import (
    analyze_pack_task_dependency_validity,
)


class TestAnalyzePackTaskDependencyValidity:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_task_dependency_validity([])

        assert result["total_packs"] == 0
        assert result["avg_total_tasks"] == 0.0
        assert result["avg_independence_ratio"] == 0.0
        assert result["avg_valid_reference_rate"] == 0.0
        assert result["avg_circular_dependency_rate"] == 0.0
        assert result["avg_max_chain_depth"] == 0.0
        assert result["avg_deep_chain_ratio"] == 0.0
        assert result["avg_execution_alignment_rate"] == 0.0
        assert result["packs_with_circular_deps"] == 0
        assert result["packs_with_invalid_refs"] == 0
        assert result["high_independence_packs"] == 0
        assert result["low_independence_packs"] == 0
        assert result["deep_chain_packs"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_task_dependency_validity(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_task_dependency_validity("not a list")

    def test_pack_with_all_independent_tasks(self):
        """Verify pack where all tasks are independent."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "total_tasks": 10,
                "tasks_independent": 10,
                "tasks_with_dependencies": 0,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_tasks"] == 10.0
        assert result["avg_independence_ratio"] == 100.0
        assert result["high_independence_packs"] == 1

    def test_pack_with_dependencies(self):
        """Verify pack with task dependencies."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "total_tasks": 10,
                "tasks_independent": 6,
                "tasks_with_dependencies": 4,
            }
        ])

        # 6 / 10 = 60%
        assert result["avg_independence_ratio"] == 60.0

    def test_high_independence_classification(self):
        """Verify detection of high independence packs."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "total_tasks": 10,
                "tasks_independent": 8,
            }
        ])

        # 8 / 10 = 80%
        assert result["avg_independence_ratio"] == 80.0
        assert result["high_independence_packs"] == 1
        assert result["low_independence_packs"] == 0

    def test_low_independence_classification(self):
        """Verify detection of low independence packs."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "total_tasks": 10,
                "tasks_independent": 2,
            }
        ])

        # 2 / 10 = 20%
        assert result["avg_independence_ratio"] == 20.0
        assert result["high_independence_packs"] == 0
        assert result["low_independence_packs"] == 1

    def test_valid_dependency_references(self):
        """Verify tracking of valid dependency references."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "valid_dependency_references": 15,
                "invalid_dependency_references": 0,
            }
        ])

        assert result["avg_valid_reference_rate"] == 100.0
        assert result["packs_with_invalid_refs"] == 0

    def test_invalid_dependency_references(self):
        """Verify detection of invalid dependency references."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "valid_dependency_references": 12,
                "invalid_dependency_references": 3,
            }
        ])

        # 12 / 15 = 80%
        assert result["avg_valid_reference_rate"] == 80.0
        assert result["packs_with_invalid_refs"] == 1

    def test_circular_dependency_detection(self):
        """Verify detection of circular dependencies."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "circular_dependencies_detected": 2,
            }
        ])

        assert result["avg_circular_dependency_rate"] == 100.0
        assert result["packs_with_circular_deps"] == 1

    def test_no_circular_dependencies(self):
        """Verify packs with no circular dependencies."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "circular_dependencies_detected": 0,
            }
        ])

        assert result["avg_circular_dependency_rate"] == 0.0
        assert result["packs_with_circular_deps"] == 0

    def test_dependency_chain_depth(self):
        """Verify tracking of dependency chain depth."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "max_dependency_chain_depth": 2,
            }
        ])

        assert result["avg_max_chain_depth"] == 2.0
        assert result["deep_chain_packs"] == 0

    def test_deep_dependency_chains(self):
        """Verify detection of deep dependency chains."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "max_dependency_chain_depth": 5,
                "total_tasks": 10,
                "deep_dependency_chains": 3,
            }
        ])

        assert result["avg_max_chain_depth"] == 5.0
        # 3 / 10 = 30%
        assert result["avg_deep_chain_ratio"] == 30.0
        assert result["deep_chain_packs"] == 1

    def test_execution_order_alignment(self):
        """Verify tracking of execution order alignment."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "execution_order_aligned": 18,
                "execution_order_misaligned": 2,
            }
        ])

        # 18 / 20 = 90%
        assert result["avg_execution_alignment_rate"] == 90.0

    def test_perfect_alignment(self):
        """Verify packs with perfect execution alignment."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "execution_order_aligned": 20,
                "execution_order_misaligned": 0,
            }
        ])

        assert result["avg_execution_alignment_rate"] == 100.0

    def test_multiple_packs_averaged(self):
        """Verify metrics averaged across multiple packs."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "total_tasks": 10,
                "tasks_independent": 7,
            },
            {
                "pack_id": "pack2",
                "total_tasks": 10,
                "tasks_independent": 9,
            },
        ])

        assert result["total_packs"] == 2
        # (10 + 10) / 2 = 10
        assert result["avg_total_tasks"] == 10.0
        # (70% + 90%) / 2 = 80%
        assert result["avg_independence_ratio"] == 80.0

    def test_boundary_independence_classification(self):
        """Verify boundary cases for independence classification."""
        result = analyze_pack_task_dependency_validity([
            # Exactly 70% (should not be high)
            {
                "pack_id": "p1",
                "total_tasks": 10,
                "tasks_independent": 7,
            },
            # Just above 70% (should be high)
            {
                "pack_id": "p2",
                "total_tasks": 10,
                "tasks_independent": 8,
            },
            # Exactly 30% (should not be low)
            {
                "pack_id": "p3",
                "total_tasks": 10,
                "tasks_independent": 3,
            },
            # Just below 30% (should be low)
            {
                "pack_id": "p4",
                "total_tasks": 10,
                "tasks_independent": 2,
            },
        ])

        # >70% means strictly greater
        assert result["high_independence_packs"] == 1
        # <30% means strictly less
        assert result["low_independence_packs"] == 1

    def test_boundary_chain_depth_classification(self):
        """Verify boundary cases for chain depth classification."""
        result = analyze_pack_task_dependency_validity([
            # Exactly 3 (should not be deep)
            {
                "pack_id": "p1",
                "max_dependency_chain_depth": 3,
            },
            # Just above 3 (should be deep)
            {
                "pack_id": "p2",
                "max_dependency_chain_depth": 4,
            },
        ])

        # >3 means strictly greater
        assert result["deep_chain_packs"] == 1

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_task_dependency_validity([
            "not a dict",
            {
                "pack_id": "pack1",
                "total_tasks": 10,
            },
        ])

        assert result["total_packs"] == 1

    def test_boolean_values_ignored(self):
        """Verify boolean values are ignored for numeric fields."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "total_tasks": True,
                "tasks_independent": False,
            }
        ])

        assert result["avg_total_tasks"] == 0.0

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "total_tasks": 10,
                # Missing most fields
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_tasks"] == 10.0

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "comprehensive",
                "pack_title": "Test Pack",
                "total_tasks": 20,
                "tasks_with_dependencies": 6,
                "tasks_independent": 14,
                "valid_dependency_references": 18,
                "invalid_dependency_references": 2,
                "circular_dependencies_detected": 0,
                "max_dependency_chain_depth": 2,
                "avg_dependency_chain_depth": 1.5,
                "deep_dependency_chains": 0,
                "execution_order_aligned": 17,
                "execution_order_misaligned": 3,
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_total_tasks"] == 20.0
        # 14 / 20 = 70%
        assert result["avg_independence_ratio"] == 70.0
        # 18 / 20 = 90%
        assert result["avg_valid_reference_rate"] == 90.0
        assert result["avg_circular_dependency_rate"] == 0.0
        assert result["avg_max_chain_depth"] == 2.0
        assert result["avg_deep_chain_ratio"] == 0.0
        # 17 / 20 = 85%
        assert result["avg_execution_alignment_rate"] == 85.0
        assert result["packs_with_circular_deps"] == 0
        assert result["packs_with_invalid_refs"] == 1
        assert result["high_independence_packs"] == 0  # Exactly 70%, not >70%
        assert result["deep_chain_packs"] == 0

    def test_mixed_pack_quality(self):
        """Verify mixed pack quality across multiple packs."""
        result = analyze_pack_task_dependency_validity([
            # High quality: high independence, no issues
            {
                "pack_id": "p1",
                "total_tasks": 10,
                "tasks_independent": 9,
                "circular_dependencies_detected": 0,
            },
            # Medium quality
            {
                "pack_id": "p2",
                "total_tasks": 10,
                "tasks_independent": 5,
                "circular_dependencies_detected": 0,
            },
            # Low quality: low independence, circular deps
            {
                "pack_id": "p3",
                "total_tasks": 10,
                "tasks_independent": 2,
                "circular_dependencies_detected": 1,
            },
        ])

        assert result["total_packs"] == 3
        # (90% + 50% + 20%) / 3 = 53.33%
        assert 53.0 <= result["avg_independence_ratio"] <= 54.0
        # 1 pack with circular deps
        assert result["packs_with_circular_deps"] == 1
        assert result["high_independence_packs"] == 1
        assert result["low_independence_packs"] == 1

    def test_float_values_accepted(self):
        """Verify float values are accepted for numeric fields."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "total_tasks": 10.5,
                "tasks_independent": 7.5,
            }
        ])

        # 7.5 / 10.5 = 71.43%
        assert 71.0 <= result["avg_independence_ratio"] <= 72.0

    def test_zero_tasks_no_division_error(self):
        """Verify zero total tasks doesn't cause division errors."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "total_tasks": 0,
                "tasks_independent": 0,
            }
        ])

        assert result["avg_independence_ratio"] == 0.0

    def test_zero_references_no_division_error(self):
        """Verify zero dependency references doesn't cause division errors."""
        result = analyze_pack_task_dependency_validity([
            {
                "pack_id": "pack1",
                "valid_dependency_references": 0,
                "invalid_dependency_references": 0,
            }
        ])

        assert result["avg_valid_reference_rate"] == 0.0
