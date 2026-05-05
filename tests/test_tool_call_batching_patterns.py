"""Tests for tool call batching patterns analysis."""

import pytest

from synthesis.tool_call_batching_patterns import (
    ToolBatch,
    BatchingStats,
    IdleAnalysis,
    BatchingRecommendations,
    analyze_tool_call_batching_patterns,
    _calculate_batching_stats,
    _build_batch_size_distribution,
    _calculate_parallelization_ratio,
    _calculate_consistency_score,
    _analyze_idle_time,
    PARALLELIZATION_LOW,
    PARALLELIZATION_HIGH,
    OPTIMAL_BATCH_SIZE_MIN,
    OPTIMAL_BATCH_SIZE_MAX,
)


class TestToolBatch:
    """Test ToolBatch dataclass."""

    def test_create_tool_batch(self):
        """Verify tool batch can be created."""
        batch = ToolBatch(
            batch_number=0,
            batch_size=3,
            tool_names=("Read", "Edit", "Write"),
            timestamp_seconds=10.5,
        )
        assert batch.batch_number == 0
        assert batch.batch_size == 3
        assert batch.tool_names == ("Read", "Edit", "Write")
        assert batch.timestamp_seconds == 10.5

    def test_tool_batch_frozen(self):
        """Verify tool batch is immutable."""
        batch = ToolBatch(0, 1, ("Read",), 0.0)
        with pytest.raises(AttributeError):
            batch.batch_size = 2


class TestCalculateBatchingStats:
    """Test batching statistics calculation."""

    def test_empty_batches(self):
        """Verify empty batches returns zero stats."""
        stats = _calculate_batching_stats([])
        assert stats.total_batches == 0
        assert stats.total_tool_calls == 0
        assert stats.avg_batch_size == 0.0

    def test_single_sequential_batch(self):
        """Verify single sequential (size 1) batch."""
        batches = [ToolBatch(0, 1, ("Read",), 0.0)]
        stats = _calculate_batching_stats(batches)
        assert stats.total_batches == 1
        assert stats.total_tool_calls == 1
        assert stats.avg_batch_size == 1.0
        assert stats.max_batch_size == 1
        assert stats.min_batch_size == 1
        assert stats.single_call_batches == 1
        assert stats.parallel_batches == 0

    def test_single_parallel_batch(self):
        """Verify single parallel (size > 1) batch."""
        batches = [ToolBatch(0, 3, ("Read", "Edit", "Write"), 0.0)]
        stats = _calculate_batching_stats(batches)
        assert stats.total_batches == 1
        assert stats.total_tool_calls == 3
        assert stats.avg_batch_size == 3.0
        assert stats.single_call_batches == 0
        assert stats.parallel_batches == 1

    def test_mixed_batch_sizes(self):
        """Verify mixed sequential and parallel batches."""
        batches = [
            ToolBatch(0, 1, ("Read",), 0.0),
            ToolBatch(1, 3, ("Edit", "Write", "Bash"), 5.0),
            ToolBatch(2, 2, ("Read", "Grep"), 10.0),
            ToolBatch(3, 1, ("Edit",), 15.0),
        ]
        stats = _calculate_batching_stats(batches)
        assert stats.total_batches == 4
        assert stats.total_tool_calls == 7  # 1 + 3 + 2 + 1
        assert stats.avg_batch_size == 1.75  # 7 / 4
        assert stats.max_batch_size == 3
        assert stats.min_batch_size == 1
        assert stats.single_call_batches == 2
        assert stats.parallel_batches == 2

    def test_uniform_batch_sizes(self):
        """Verify uniform batch sizes have zero variance."""
        batches = [
            ToolBatch(0, 2, ("Read", "Edit"), 0.0),
            ToolBatch(1, 2, ("Write", "Bash"), 5.0),
            ToolBatch(2, 2, ("Grep", "Glob"), 10.0),
        ]
        stats = _calculate_batching_stats(batches)
        assert stats.avg_batch_size == 2.0
        assert stats.batch_size_variance == 0.0


class TestBuildBatchSizeDistribution:
    """Test batch size distribution construction."""

    def test_empty_batches(self):
        """Verify empty batches returns empty distribution."""
        distribution = _build_batch_size_distribution([])
        assert distribution == {}

    def test_single_size(self):
        """Verify distribution for single batch size."""
        batches = [
            ToolBatch(0, 2, ("Read", "Edit"), 0.0),
            ToolBatch(1, 2, ("Write", "Bash"), 5.0),
            ToolBatch(2, 2, ("Grep", "Glob"), 10.0),
        ]
        distribution = _build_batch_size_distribution(batches)
        assert distribution == {2: 3}

    def test_multiple_sizes(self):
        """Verify distribution for multiple batch sizes."""
        batches = [
            ToolBatch(0, 1, ("Read",), 0.0),
            ToolBatch(1, 1, ("Edit",), 5.0),
            ToolBatch(2, 2, ("Write", "Bash"), 10.0),
            ToolBatch(3, 3, ("Grep", "Glob", "Read"), 15.0),
            ToolBatch(4, 2, ("Edit", "Write"), 20.0),
        ]
        distribution = _build_batch_size_distribution(batches)
        assert distribution == {1: 2, 2: 2, 3: 1}


class TestCalculateParallelizationRatio:
    """Test parallelization ratio calculation."""

    def test_empty_stats(self):
        """Verify empty stats returns zero ratio."""
        stats = BatchingStats(0, 0, 0.0, 0, 0, 0.0, 0, 0)
        ratio = _calculate_parallelization_ratio(stats)
        assert ratio == 0.0

    def test_all_sequential(self):
        """Verify all sequential batches."""
        stats = BatchingStats(5, 5, 1.0, 1, 1, 0.0, 5, 0)
        ratio = _calculate_parallelization_ratio(stats)
        assert ratio == 0.0

    def test_all_parallel(self):
        """Verify all parallel batches."""
        stats = BatchingStats(5, 15, 3.0, 3, 3, 0.0, 0, 5)
        ratio = _calculate_parallelization_ratio(stats)
        assert ratio == 1.0

    def test_mixed_execution(self):
        """Verify mixed sequential and parallel."""
        stats = BatchingStats(10, 20, 2.0, 5, 1, 1.5, 3, 7)
        ratio = _calculate_parallelization_ratio(stats)
        assert ratio == 0.7  # 7 / 10


class TestCalculateConsistencyScore:
    """Test consistency score calculation."""

    def test_empty_stats(self):
        """Verify empty stats returns zero score."""
        stats = BatchingStats(0, 0, 0.0, 0, 0, 0.0, 0, 0)
        score = _calculate_consistency_score(stats)
        assert score == 0.0

    def test_perfect_consistency(self):
        """Verify zero variance gives perfect score."""
        stats = BatchingStats(5, 10, 2.0, 2, 2, 0.0, 0, 5)
        score = _calculate_consistency_score(stats)
        assert score == 100.0

    def test_moderate_variance(self):
        """Verify moderate variance calculation."""
        # Variance of 5.0 should give score of 50
        stats = BatchingStats(5, 10, 2.0, 5, 1, 5.0, 2, 3)
        score = _calculate_consistency_score(stats)
        assert score == 50.0

    def test_high_variance(self):
        """Verify high variance gives low score."""
        # Variance of 10+ should give score near 0
        stats = BatchingStats(5, 10, 2.0, 10, 1, 15.0, 2, 3)
        score = _calculate_consistency_score(stats)
        assert score <= 10.0


class TestAnalyzeIdleTime:
    """Test idle time analysis."""

    def test_empty_batches(self):
        """Verify empty batches returns zero analysis."""
        analysis = _analyze_idle_time([])
        assert analysis.avg_idle_time_seconds == 0.0
        assert analysis.max_idle_time_seconds == 0.0
        assert analysis.long_idle_periods == 0

    def test_single_batch(self):
        """Verify single batch returns zero idle time."""
        batches = [ToolBatch(0, 1, ("Read",), 0.0)]
        analysis = _analyze_idle_time(batches)
        assert analysis.avg_idle_time_seconds == 0.0
        assert analysis.max_idle_time_seconds == 0.0

    def test_uniform_idle_times(self):
        """Verify uniform idle times."""
        batches = [
            ToolBatch(0, 1, ("Read",), 0.0),
            ToolBatch(1, 1, ("Edit",), 10.0),
            ToolBatch(2, 1, ("Write",), 20.0),
        ]
        analysis = _analyze_idle_time(batches)
        assert analysis.avg_idle_time_seconds == 10.0
        assert analysis.max_idle_time_seconds == 10.0
        assert analysis.idle_time_variance == 0.0

    def test_varying_idle_times(self):
        """Verify varying idle times."""
        batches = [
            ToolBatch(0, 1, ("Read",), 0.0),
            ToolBatch(1, 1, ("Edit",), 5.0),   # 5s idle
            ToolBatch(2, 1, ("Write",), 10.0),  # 5s idle
            ToolBatch(3, 1, ("Bash",), 50.0),  # 40s idle (long)
        ]
        analysis = _analyze_idle_time(batches)
        # Average: (5 + 5 + 40) / 3 = 16.67
        assert 16.0 < analysis.avg_idle_time_seconds < 17.0
        assert analysis.max_idle_time_seconds == 40.0
        assert analysis.long_idle_periods == 1  # One > 30s gap


class TestAnalyzeToolCallBatchingPatterns:
    """Test complete batching pattern analysis."""

    def test_empty_batches(self):
        """Verify analysis of empty batches."""
        result = analyze_tool_call_batching_patterns([])
        assert result["batching_stats"]["total_batches"] == 0
        assert result["parallelization_ratio"] == 0.0
        assert result["consistency_score"] == 0.0
        assert result["batch_size_distribution"] == {}

    def test_fully_sequential_execution(self):
        """Verify analysis of fully sequential execution."""
        batches = [
            ToolBatch(0, 1, ("Read",), 0.0),
            ToolBatch(1, 1, ("Edit",), 5.0),
            ToolBatch(2, 1, ("Write",), 10.0),
        ]
        result = analyze_tool_call_batching_patterns(batches)
        assert result["batching_stats"]["total_batches"] == 3
        assert result["batching_stats"]["total_tool_calls"] == 3
        assert result["batching_stats"]["avg_batch_size"] == 1.0
        assert result["parallelization_ratio"] == 0.0  # All sequential
        assert result["batch_size_distribution"] == {1: 3}

    def test_fully_parallel_execution(self):
        """Verify analysis of fully parallel execution."""
        batches = [
            ToolBatch(0, 3, ("Read", "Edit", "Write"), 0.0),
            ToolBatch(1, 2, ("Bash", "Grep"), 10.0),
            ToolBatch(2, 4, ("Glob", "Read", "Edit", "Write"), 20.0),
        ]
        result = analyze_tool_call_batching_patterns(batches)
        assert result["batching_stats"]["total_batches"] == 3
        assert result["batching_stats"]["total_tool_calls"] == 9
        assert result["parallelization_ratio"] == 1.0  # All parallel
        assert result["batch_size_distribution"] == {2: 1, 3: 1, 4: 1}

    def test_mixed_batching_patterns(self):
        """Verify analysis of mixed batching patterns."""
        batches = [
            ToolBatch(0, 1, ("Read",), 0.0),
            ToolBatch(1, 3, ("Edit", "Write", "Bash"), 5.0),
            ToolBatch(2, 1, ("Grep",), 10.0),
            ToolBatch(3, 2, ("Glob", "Read"), 15.0),
        ]
        result = analyze_tool_call_batching_patterns(batches)
        assert result["batching_stats"]["total_batches"] == 4
        assert result["batching_stats"]["total_tool_calls"] == 7
        assert result["batching_stats"]["single_call_batches"] == 2
        assert result["batching_stats"]["parallel_batches"] == 2
        assert result["parallelization_ratio"] == 0.5
        assert result["batch_size_distribution"] == {1: 2, 2: 1, 3: 1}

    def test_consistent_batching(self):
        """Verify high consistency score for uniform batches."""
        batches = [
            ToolBatch(0, 2, ("Read", "Edit"), 0.0),
            ToolBatch(1, 2, ("Write", "Bash"), 5.0),
            ToolBatch(2, 2, ("Grep", "Glob"), 10.0),
        ]
        result = analyze_tool_call_batching_patterns(batches)
        assert result["consistency_score"] == 100.0  # Perfect consistency

    def test_recommendations_for_sequential(self):
        """Verify recommendations for sequential execution."""
        batches = [
            ToolBatch(0, 1, ("Read",), 0.0),
            ToolBatch(1, 1, ("Edit",), 5.0),
            ToolBatch(2, 1, ("Write",), 10.0),
        ]
        result = analyze_tool_call_batching_patterns(batches)
        recommendations = result["recommendations"]
        assert recommendations["parallelization_opportunities"] == 3
        assert len(recommendations["recommendations"]) > 0

    def test_invalid_batches_not_sequence(self):
        """Verify error on non-sequence input."""
        with pytest.raises(ValueError, match="batches must be a sequence"):
            analyze_tool_call_batching_patterns("not a sequence")

    def test_invalid_batches_wrong_type(self):
        """Verify error on wrong element type."""
        with pytest.raises(ValueError, match="must contain ToolBatch instances"):
            analyze_tool_call_batching_patterns([{"batch": 0}])

    def test_invalid_negative_batch_number(self):
        """Verify error on negative batch number."""
        batches = [ToolBatch(-1, 1, ("Read",), 0.0)]
        with pytest.raises(ValueError, match="batch_number must be non-negative"):
            analyze_tool_call_batching_patterns(batches)

    def test_invalid_zero_batch_size(self):
        """Verify error on zero batch size."""
        batches = [ToolBatch(0, 0, (), 0.0)]
        with pytest.raises(ValueError, match="batch_size must be at least 1"):
            analyze_tool_call_batching_patterns(batches)

    def test_invalid_negative_timestamp(self):
        """Verify error on negative timestamp."""
        batches = [ToolBatch(0, 1, ("Read",), -1.0)]
        with pytest.raises(ValueError, match="timestamp_seconds must be non-negative"):
            analyze_tool_call_batching_patterns(batches)

    def test_invalid_tool_names_length_mismatch(self):
        """Verify error when tool_names length doesn't match batch_size."""
        batches = [ToolBatch(0, 3, ("Read", "Edit"), 0.0)]  # Size 3 but only 2 tools
        with pytest.raises(ValueError, match="tool_names length must equal batch_size"):
            analyze_tool_call_batching_patterns(batches)

    def test_result_structure(self):
        """Verify result structure contains all required fields."""
        batches = [
            ToolBatch(0, 2, ("Read", "Edit"), 0.0),
            ToolBatch(1, 1, ("Write",), 5.0),
        ]
        result = analyze_tool_call_batching_patterns(batches)

        # Check top-level keys
        assert "batching_stats" in result
        assert "parallelization_ratio" in result
        assert "consistency_score" in result
        assert "idle_analysis" in result
        assert "recommendations" in result
        assert "batch_size_distribution" in result

        # Check batching_stats structure
        stats = result["batching_stats"]
        assert "total_batches" in stats
        assert "total_tool_calls" in stats
        assert "avg_batch_size" in stats
        assert "max_batch_size" in stats
        assert "min_batch_size" in stats
        assert "batch_size_variance" in stats
        assert "single_call_batches" in stats
        assert "parallel_batches" in stats

        # Check idle_analysis structure
        idle = result["idle_analysis"]
        assert "avg_idle_time_seconds" in idle
        assert "max_idle_time_seconds" in idle
        assert "idle_time_variance" in idle
        assert "long_idle_periods" in idle

        # Check recommendations structure
        rec = result["recommendations"]
        assert "recommended_batch_size" in rec
        assert "parallelization_opportunities" in rec
        assert "consistency_improvement_potential" in rec
        assert "recommendations" in rec
