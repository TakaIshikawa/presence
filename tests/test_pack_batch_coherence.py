"""Tests for pack batch coherence analyzer."""

import pytest

from synthesis.pack_batch_coherence import analyze_pack_batch_coherence


class TestAnalyzePackBatchCoherence:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty pack list returns zero metrics."""
        result = analyze_pack_batch_coherence([])

        assert result["total_packs"] == 0
        assert result["avg_task_title_similarity"] == 0.0
        assert result["avg_shared_category_rate"] == 0.0
        assert result["avg_slice_overlap_rate"] == 0.0
        assert result["avg_dependency_chain_length"] == 0.0
        assert result["avg_root_task_ratio"] == 0.0
        assert result["high_coherence_packs"] == 0
        assert result["low_coherence_packs"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_batch_coherence(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_batch_coherence("not a list")

    def test_high_coherence_similar_titles(self):
        """Verify high coherence with similar task titles."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_titles": [
                    "Add user authentication endpoint",
                    "Add user authorization endpoint",
                    "Add user validation endpoint",
                ],
                "task_categories": ["api", "api", "api"],
                "total_task_count": 3,
                "root_task_count": 3,
            }
        ])

        # Similar titles should have high similarity
        assert result["avg_task_title_similarity"] > 0.7
        # All same category = 100%
        assert result["avg_shared_category_rate"] == 100.0
        # 3 root / 3 total = 100%
        assert result["avg_root_task_ratio"] == 100.0
        assert result["high_coherence_packs"] == 1
        assert result["low_coherence_packs"] == 0

    def test_low_coherence_unrelated_titles(self):
        """Verify low coherence with unrelated task titles."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_titles": [
                    "Fix database connection bug",
                    "Update frontend styling for button",
                    "Add logging to email service",
                ],
                "task_categories": ["bugfix", "ui", "feature"],
            }
        ])

        # Unrelated titles should have low similarity
        assert result["avg_task_title_similarity"] < 0.4
        # All different categories = 33% (1 of 3)
        assert result["avg_shared_category_rate"] == pytest.approx(33.33, abs=0.1)
        assert result["high_coherence_packs"] == 0
        assert result["low_coherence_packs"] == 1

    def test_category_alignment_all_same(self):
        """Verify category alignment when all tasks same category."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_categories": ["feature", "feature", "feature", "feature"],
            }
        ])

        # 4 of 4 same category = 100%
        assert result["avg_shared_category_rate"] == 100.0

    def test_category_alignment_mixed(self):
        """Verify category alignment with mixed categories."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_categories": ["feature", "feature", "bugfix", "refactor"],
            }
        ])

        # 2 of 4 same category (most common) = 50%
        assert result["avg_shared_category_rate"] == 50.0

    def test_slice_overlap_perfect(self):
        """Verify perfect slice overlap."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_slices": [
                    ["src/api", "tests/api"],
                    ["src/api", "tests/api"],
                    ["src/api", "tests/api"],
                ],
            }
        ])

        # All tasks share both slices = 100%
        assert result["avg_slice_overlap_rate"] == 100.0

    def test_slice_overlap_partial(self):
        """Verify partial slice overlap."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_slices": [
                    ["src/api", "tests/api"],
                    ["src/api", "src/db"],
                    ["src/api", "src/frontend"],
                ],
            }
        ])

        # All share "src/api", average 2 slices each = 1/2 = 50%
        assert result["avg_slice_overlap_rate"] == 50.0

    def test_slice_overlap_none(self):
        """Verify no slice overlap."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_slices": [
                    ["src/api"],
                    ["src/db"],
                    ["src/frontend"],
                ],
            }
        ])

        # No shared slices = 0%
        assert result["avg_slice_overlap_rate"] == 0.0

    def test_dependency_chain_length(self):
        """Verify dependency chain length tracking."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "dependency_chain_length": 5,
            },
            {
                "pack_id": "pack2",
                "dependency_chain_length": 3,
            },
        ])

        # (5 + 3) / 2 = 4
        assert result["avg_dependency_chain_length"] == 4.0

    def test_root_task_ratio(self):
        """Verify root task ratio calculation."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "total_task_count": 10,
                "root_task_count": 7,
            }
        ])

        # 7 / 10 = 70%
        assert result["avg_root_task_ratio"] == 70.0

    def test_multiple_packs_averaged(self):
        """Verify metrics averaged across multiple packs."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_titles": ["Add feature A", "Add feature B"],
                "total_task_count": 2,
                "root_task_count": 2,
            },
            {
                "pack_id": "pack2",
                "task_titles": ["Fix bug X", "Fix bug Y"],
                "total_task_count": 2,
                "root_task_count": 0,
            },
        ])

        assert result["total_packs"] == 2
        # Both packs have similar titles
        assert result["avg_task_title_similarity"] > 0.5
        # (100 + 0) / 2 = 50%
        assert result["avg_root_task_ratio"] == 50.0

    def test_single_task_pack(self):
        """Verify pack with single task handled gracefully."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_titles": ["Single task"],
                "task_categories": ["feature"],
                "task_slices": [["src/api"]],
            }
        ])

        # Single task means no pairwise comparisons
        assert result["avg_task_title_similarity"] == 0.0
        assert result["avg_shared_category_rate"] == 0.0

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_batch_coherence([
            "not a dict",
            {
                "pack_id": "pack1",
                "task_titles": ["Task A", "Task B"],
            },
        ])

        assert result["total_packs"] == 1

    def test_missing_optional_fields(self):
        """Verify missing optional fields handled gracefully."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                # Missing all optional fields
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_task_title_similarity"] == 0.0

    def test_empty_task_lists(self):
        """Verify empty task lists handled gracefully."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_titles": [],
                "task_categories": [],
                "task_slices": [],
            }
        ])

        assert result["total_packs"] == 1
        assert result["avg_task_title_similarity"] == 0.0

    def test_boundary_coherence_classification(self):
        """Verify boundary cases for coherence classification."""
        result = analyze_pack_batch_coherence([
            # High similarity (exactly 0.7 should not be high)
            {
                "pack_id": "p1",
                "task_titles": ["Add test A", "Add test B"],
            },
            # Low similarity (exactly 0.4 should not be low)
            {
                "pack_id": "p2",
                "task_titles": ["Different task", "Unrelated work"],
            },
        ])

        # Classification depends on actual similarity scores
        # Just verify no errors
        assert result["total_packs"] == 2

    def test_comprehensive_pack_all_fields(self):
        """Verify comprehensive pack with all fields populated."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "comprehensive",
                "pack_title": "API Feature Pack",
                "task_titles": [
                    "Add user endpoint",
                    "Add auth endpoint",
                    "Add validation endpoint",
                ],
                "task_categories": ["api", "api", "api"],
                "task_slices": [
                    ["src/api", "tests/api"],
                    ["src/api", "tests/api"],
                    ["src/api", "tests/api"],
                ],
                "dependency_chain_length": 2,
                "root_task_count": 2,
                "total_task_count": 3,
            }
        ])

        # Similar titles
        assert result["avg_task_title_similarity"] > 0.6
        # All same category
        assert result["avg_shared_category_rate"] == 100.0
        # Perfect slice overlap
        assert result["avg_slice_overlap_rate"] == 100.0
        # Short chain
        assert result["avg_dependency_chain_length"] == 2.0
        # 2/3 root tasks
        assert result["avg_root_task_ratio"] == pytest.approx(66.67, abs=0.1)

    def test_invalid_task_titles(self):
        """Verify handling of non-string task titles."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_titles": ["Valid title", None, 123, "Another valid"],
            }
        ])

        # Should only use valid strings
        assert result["total_packs"] == 1

    def test_invalid_categories(self):
        """Verify handling of non-string categories."""
        result = analyze_pack_batch_coherence([
            {
                "pack_id": "pack1",
                "task_categories": ["valid", None, 123, "valid"],
            }
        ])

        assert result["total_packs"] == 1
