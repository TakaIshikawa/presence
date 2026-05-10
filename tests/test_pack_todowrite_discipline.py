"""Tests for pack TodoWrite discipline analyzer."""

import pytest

from synthesis.pack_todowrite_discipline import analyze_pack_todowrite_discipline


class TestAnalyzePackTodowriteDiscipline:
    """Test main analyzer function."""

    def test_empty_records_returns_zero_metrics(self):
        """Verify empty records returns zero metrics."""
        result = analyze_pack_todowrite_discipline([])
        assert result["total_sessions"] == 0
        assert result["sessions_with_tasks"] == 0
        assert result["avg_tasks_per_session"] == 0.0
        assert result["discipline_score"] == 0.0

    def test_invalid_input_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="must be a list"):
            analyze_pack_todowrite_discipline("not a list")

    def test_high_discipline_session(self):
        """Verify high discipline session yields high score."""
        records = [
            {
                "total_tasks_created": 5,
                "total_tasks_completed": 5,
                "immediate_completions": 4,
                "batch_completions": 0,
                "tasks_skipping_in_progress": 0,
                "tasks_stuck_in_progress": 0,
                "tasks_with_activeform": 5,
                "tasks_missing_activeform": 0,
                "tasks_never_updated": 0,
                "avg_task_description_length": 35.0,
                "avg_task_lifespan_turns": 2.5,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["sessions_with_tasks"] == 1
        assert result["avg_tasks_per_session"] == 5.0
        assert result["immediate_completion_rate"] == 80.0
        assert result["batch_completion_rate"] == 0.0
        assert result["activeform_presence_rate"] == 100.0
        assert result["discipline_score"] > 0.80

    def test_task_granularity_score_optimal_range(self):
        """Verify task granularity score rewards optimal task count."""
        records = [
            {
                "total_tasks_created": 5,
                "avg_task_description_length": 40.0,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["task_granularity_score"] == 1.0

    def test_task_granularity_score_too_few_tasks(self):
        """Verify granularity score penalizes too few tasks."""
        records = [
            {
                "total_tasks_created": 1,
                "avg_task_description_length": 40.0,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        # 1 task = 0.30 (task component) + 0.4 (optimal description) = 0.70
        assert result["task_granularity_score"] == 0.70

    def test_task_granularity_score_too_many_tasks(self):
        """Verify granularity score penalizes over-fragmentation."""
        records = [
            {
                "total_tasks_created": 20,
                "avg_task_description_length": 40.0,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["task_granularity_score"] < 0.60

    def test_immediate_completion_rate_calculation(self):
        """Verify immediate completion rate is calculated correctly."""
        records = [
            {
                "total_tasks_created": 10,
                "total_tasks_completed": 10,
                "immediate_completions": 7,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["immediate_completion_rate"] == 70.0

    def test_batch_completion_anti_pattern_detection(self):
        """Verify batch completion anti-pattern is detected."""
        records = [
            {
                "total_tasks_created": 10,
                "total_tasks_completed": 10,
                "batch_completions": 5,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["batch_completion_rate"] == 50.0

    def test_activeform_presence_rate(self):
        """Verify activeForm presence rate calculation."""
        records = [
            {
                "total_tasks_created": 10,
                "tasks_with_activeform": 9,
                "tasks_missing_activeform": 1,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["activeform_presence_rate"] == 90.0

    def test_tasks_stuck_in_progress_detection(self):
        """Verify stuck in_progress tasks are tracked."""
        records = [
            {
                "total_tasks_created": 10,
                "tasks_stuck_in_progress": 2,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["tasks_stuck_rate"] == 20.0

    def test_tasks_never_updated_detection(self):
        """Verify never-updated tasks are tracked."""
        records = [
            {
                "total_tasks_created": 10,
                "tasks_never_updated": 3,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["tasks_never_updated_rate"] == 30.0

    def test_multiple_sessions_aggregation(self):
        """Verify metrics aggregate across sessions."""
        records = [
            {
                "total_tasks_created": 5,
                "total_tasks_completed": 5,
                "immediate_completions": 4,
                "tasks_with_activeform": 5,
            },
            {
                "total_tasks_created": 8,
                "total_tasks_completed": 8,
                "immediate_completions": 6,
                "tasks_with_activeform": 7,
            },
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["total_sessions"] == 2
        assert result["sessions_with_tasks"] == 2
        assert result["avg_tasks_per_session"] == 6.5
        # (4+6)/(5+8) = 10/13 = 76.92%
        assert result["immediate_completion_rate"] == 76.92
        # (5+7)/(5+8) = 12/13 = 92.31%
        assert result["activeform_presence_rate"] == 92.31

    def test_high_discipline_sessions_count(self):
        """Verify high discipline sessions are counted correctly."""
        records = [
            {
                "total_tasks_created": 5,
                "total_tasks_completed": 5,
                "immediate_completions": 4,
                "batch_completions": 0,
                "tasks_with_activeform": 5,
                "tasks_missing_activeform": 0,
                "tasks_stuck_in_progress": 0,
                "tasks_never_updated": 0,
                "avg_task_description_length": 40.0,
            },
            {
                "total_tasks_created": 6,
                "total_tasks_completed": 6,
                "immediate_completions": 5,
                "batch_completions": 0,
                "tasks_with_activeform": 6,
                "tasks_missing_activeform": 0,
                "tasks_stuck_in_progress": 0,
                "tasks_never_updated": 0,
                "avg_task_description_length": 45.0,
            },
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["high_discipline_sessions"] == 2

    def test_low_discipline_sessions_count(self):
        """Verify low discipline sessions are counted correctly."""
        records = [
            {
                "total_tasks_created": 10,
                "total_tasks_completed": 10,
                "immediate_completions": 1,
                "batch_completions": 7,
                "tasks_with_activeform": 3,
                "tasks_missing_activeform": 7,
                "tasks_stuck_in_progress": 4,
                "tasks_never_updated": 5,
                "avg_task_description_length": 10.0,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["low_discipline_sessions"] == 1

    def test_tasks_skipping_in_progress_rate(self):
        """Verify tasks skipping in_progress state are tracked."""
        records = [
            {
                "total_tasks_created": 10,
                "tasks_skipping_in_progress": 3,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["tasks_skipping_in_progress_rate"] == 30.0

    def test_avg_task_lifespan_calculation(self):
        """Verify average task lifespan is calculated correctly."""
        records = [
            {"avg_task_lifespan_turns": 2.5},
            {"avg_task_lifespan_turns": 3.5},
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["avg_task_lifespan_turns"] == 3.0

    def test_description_length_optimal_range(self):
        """Verify optimal description length range."""
        records = [
            {
                "total_tasks_created": 5,
                "avg_task_description_length": 40.0,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        # Optimal length (20-60) + optimal count (3-8) = 1.0
        assert result["task_granularity_score"] == 1.0

    def test_description_length_too_short(self):
        """Verify short descriptions reduce granularity score."""
        records = [
            {
                "total_tasks_created": 5,
                "avg_task_description_length": 8.0,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        # Optimal count (0.6) + short description (0.1) = 0.7
        assert result["task_granularity_score"] == 0.7

    def test_description_length_too_long(self):
        """Verify verbose descriptions reduce granularity score."""
        records = [
            {
                "total_tasks_created": 5,
                "avg_task_description_length": 120.0,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        # Optimal count (0.6) + long description (0.1) = 0.7
        assert result["task_granularity_score"] == 0.7

    def test_mixed_discipline_pack(self):
        """Verify pack with mixed discipline levels."""
        records = [
            {
                "total_tasks_created": 5,
                "total_tasks_completed": 5,
                "immediate_completions": 4,
                "batch_completions": 0,
                "tasks_with_activeform": 5,
                "tasks_missing_activeform": 0,
                "tasks_stuck_in_progress": 0,
                "tasks_never_updated": 0,
                "avg_task_description_length": 40.0,
            },
            {
                "total_tasks_created": 10,
                "total_tasks_completed": 10,
                "immediate_completions": 2,
                "batch_completions": 6,
                "tasks_with_activeform": 4,
                "tasks_missing_activeform": 6,
                "tasks_stuck_in_progress": 3,
                "tasks_never_updated": 4,
                "avg_task_description_length": 15.0,
            },
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["total_sessions"] == 2
        assert result["high_discipline_sessions"] == 1
        assert result["low_discipline_sessions"] == 1
        # Mixed discipline should yield moderate score
        assert 0.3 < result["discipline_score"] < 0.6

    def test_zero_tasks_session_ignored(self):
        """Verify sessions with zero tasks don't affect task averages."""
        records = [
            {"total_tasks_created": 0},
            {
                "total_tasks_created": 5,
                "avg_task_description_length": 40.0,
            },
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["total_sessions"] == 2
        assert result["sessions_with_tasks"] == 1
        assert result["avg_tasks_per_session"] == 5.0

    def test_none_values_handled_gracefully(self):
        """Verify None values are handled without errors."""
        records = [
            {
                "total_tasks_created": 5,
                "immediate_completions": None,
                "batch_completions": None,
                "tasks_with_activeform": None,
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["sessions_with_tasks"] == 1
        assert result["immediate_completion_rate"] == 0.0

    def test_discipline_score_components(self):
        """Verify discipline score combines all components correctly."""
        records = [
            {
                "total_tasks_created": 5,
                "total_tasks_completed": 5,
                "immediate_completions": 4,  # 80% -> 0.25
                "batch_completions": 0,      # 0% -> 0.20
                "tasks_with_activeform": 5,  # 100% -> 0.15
                "tasks_missing_activeform": 0,
                "tasks_stuck_in_progress": 0,  # 0% -> 0.10
                "tasks_never_updated": 0,      # 0% -> 0.10
                "avg_task_description_length": 40.0,  # granularity 1.0 -> 0.20
            }
        ]
        result = analyze_pack_todowrite_discipline(records)
        # 0.20 + 0.25 + 0.20 + 0.15 + 0.10 + 0.10 = 1.00
        assert result["discipline_score"] == 1.0

    def test_non_mapping_records_skipped(self):
        """Verify non-mapping records are skipped gracefully."""
        records = [
            "invalid",
            {
                "total_tasks_created": 5,
                "tasks_with_activeform": 5,
            },
            123,
        ]
        result = analyze_pack_todowrite_discipline(records)
        assert result["total_sessions"] == 1
        assert result["sessions_with_tasks"] == 1
