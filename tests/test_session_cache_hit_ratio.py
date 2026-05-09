"""Tests for session cache hit ratio analyzer."""

import pytest

from synthesis.session_cache_hit_ratio import (
    analyze_session_cache_hit_ratio,
    _calculate_cache_effectiveness_score,
    _number,
    _percentage,
)


class TestAnalyzeSessionCacheHitRatio:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_cache_hit_ratio([])

        assert result['total_cache_queries'] == 0
        assert result['cache_hits'] == 0
        assert result['cache_misses'] == 0
        assert result['cache_snapshots'] == 0
        assert result['cache_clears'] == 0
        assert result['cache_hit_rate'] == 0.0
        assert result['total_file_reads'] == 0
        assert result['reads_of_cached_files'] == 0
        assert result['reads_of_uncached_files'] == 0
        assert result['repeated_full_reads'] == 0
        assert result['missed_caching_opportunities'] == 0
        assert result['avg_bytes_per_cached_read'] == 0.0
        assert result['avg_bytes_per_uncached_read'] == 0.0
        assert result['cache_effectiveness_score'] == 0.0
        assert result['examples'] == []

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_cache_hit_ratio(None)
        assert result['total_cache_queries'] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match='records must be a list'):
            analyze_session_cache_hit_ratio('not a list')

    def test_cache_hit_counted_correctly(self):
        """Verify cache hit is counted correctly."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'cache_query',
                'file_path': 'main.py',
                'cache_hit': True,
                'turn_index': 0,
            }
        ])

        assert result['total_cache_queries'] == 1
        assert result['cache_hits'] == 1
        assert result['cache_misses'] == 0
        assert result['cache_hit_rate'] == 100.0

    def test_cache_miss_counted_correctly(self):
        """Verify cache miss is counted correctly."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'cache_query',
                'file_path': 'main.py',
                'cache_hit': False,
                'turn_index': 0,
            }
        ])

        assert result['total_cache_queries'] == 1
        assert result['cache_hits'] == 0
        assert result['cache_misses'] == 1
        assert result['cache_hit_rate'] == 0.0

    def test_cache_snapshot_adds_file_to_cache(self):
        """Verify cache snapshot operation adds file to cache."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'cache_snapshot',
                'file_path': 'utils.py',
                'turn_index': 0,
            },
            {
                'operation_type': 'read',
                'file_path': 'utils.py',
                'bytes_read': 1000,
                'turn_index': 1,
            },
        ])

        assert result['cache_snapshots'] == 1
        assert result['reads_of_cached_files'] == 1
        assert result['reads_of_uncached_files'] == 0

    def test_cache_clear_removes_cached_files(self):
        """Verify cache clear operation removes all cached files."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'cache_snapshot',
                'file_path': 'utils.py',
                'turn_index': 0,
            },
            {
                'operation_type': 'cache_clear',
                'turn_index': 1,
            },
            {
                'operation_type': 'read',
                'file_path': 'utils.py',
                'bytes_read': 1000,
                'turn_index': 2,
            },
        ])

        assert result['cache_clears'] == 1
        assert result['reads_of_cached_files'] == 0
        assert result['reads_of_uncached_files'] == 1

    def test_read_of_uncached_file(self):
        """Verify read of uncached file is counted correctly."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'bytes_read': 500,
                'turn_index': 0,
            }
        ])

        assert result['total_file_reads'] == 1
        assert result['reads_of_cached_files'] == 0
        assert result['reads_of_uncached_files'] == 1

    def test_repeated_reads_without_caching_detected(self):
        """Verify repeated reads without caching are detected as missed opportunities."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'bytes_read': 500,
                'turn_index': 0,
            },
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'bytes_read': 500,
                'turn_index': 1,
            },
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'bytes_read': 500,
                'turn_index': 2,
            },
        ])

        assert result['total_file_reads'] == 3
        assert result['repeated_full_reads'] == 1  # One file read multiple times
        assert result['missed_caching_opportunities'] == 2  # 2nd and 3rd reads

    def test_cache_usage_reduces_missed_opportunities(self):
        """Verify caching a file prevents missed opportunities."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'bytes_read': 500,
                'turn_index': 0,
            },
            {
                'operation_type': 'cache_snapshot',
                'file_path': 'main.py',
                'turn_index': 1,
            },
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'bytes_read': 500,
                'turn_index': 2,
            },
        ])

        assert result['missed_caching_opportunities'] == 0
        assert result['reads_of_cached_files'] == 1
        assert result['reads_of_uncached_files'] == 1

    def test_avg_bytes_per_cached_read_calculated(self):
        """Verify average bytes per cached read is calculated correctly."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'cache_snapshot',
                'file_path': 'file1.py',
                'turn_index': 0,
            },
            {
                'operation_type': 'read',
                'file_path': 'file1.py',
                'bytes_read': 1000,
                'turn_index': 1,
            },
            {
                'operation_type': 'read',
                'file_path': 'file1.py',
                'bytes_read': 1500,
                'turn_index': 2,
            },
        ])

        # Average of 1000 and 1500 is 1250
        assert result['avg_bytes_per_cached_read'] == 1250.0

    def test_avg_bytes_per_uncached_read_calculated(self):
        """Verify average bytes per uncached read is calculated correctly."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'read',
                'file_path': 'file1.py',
                'bytes_read': 800,
                'turn_index': 0,
            },
            {
                'operation_type': 'read',
                'file_path': 'file2.py',
                'bytes_read': 1200,
                'turn_index': 1,
            },
        ])

        # Average of 800 and 1200 is 1000
        assert result['avg_bytes_per_uncached_read'] == 1000.0

    def test_mixed_cache_operations(self):
        """Verify mixed cache operations are handled correctly."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'cache_query',
                'file_path': 'main.py',
                'cache_hit': False,
                'turn_index': 0,
            },
            {
                'operation_type': 'cache_snapshot',
                'file_path': 'main.py',
                'turn_index': 1,
            },
            {
                'operation_type': 'cache_query',
                'file_path': 'main.py',
                'cache_hit': True,
                'turn_index': 2,
            },
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'bytes_read': 500,
                'turn_index': 3,
            },
        ])

        assert result['total_cache_queries'] == 2
        assert result['cache_hits'] == 1
        assert result['cache_misses'] == 1
        assert result['cache_hit_rate'] == 50.0
        assert result['cache_snapshots'] == 1
        assert result['reads_of_cached_files'] == 1

    def test_examples_limited_to_five(self):
        """Verify examples are limited to 5."""
        records = [
            {
                'operation_type': 'cache_query',
                'file_path': f'file{i}.py',
                'cache_hit': True,
                'turn_index': i,
            }
            for i in range(10)
        ]

        result = analyze_session_cache_hit_ratio(records)
        assert len(result['examples']) == 5

    def test_cache_hit_example_structure(self):
        """Verify cache hit example contains expected fields."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'cache_query',
                'file_path': 'utils.py',
                'cache_hit': True,
                'turn_index': 5,
            }
        ])

        example = result['examples'][0]
        assert example['turn_index'] == 5
        assert example['operation'] == 'cache_hit'
        assert example['file_path'] == 'utils.py'
        assert 'description' in example

    def test_cache_snapshot_example_structure(self):
        """Verify cache snapshot example contains expected fields."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'cache_snapshot',
                'file_path': 'main.py',
                'turn_index': 3,
            }
        ])

        example = result['examples'][0]
        assert example['turn_index'] == 3
        assert example['operation'] == 'cache_snapshot'
        assert example['file_path'] == 'main.py'

    def test_missed_opportunity_example_structure(self):
        """Verify missed opportunity example contains expected fields."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'read',
                'file_path': 'test.py',
                'bytes_read': 100,
                'turn_index': 0,
            },
            {
                'operation_type': 'read',
                'file_path': 'test.py',
                'bytes_read': 100,
                'turn_index': 1,
            },
        ])

        # First (and only) example should be missed opportunity from second read
        assert len(result['examples']) == 1
        example = result['examples'][0]
        assert example['turn_index'] == 1
        assert example['operation'] == 'missed_opportunity'
        assert example['file_path'] == 'test.py'
        assert '2 times' in example['description']

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_cache_hit_ratio([
            'not a dict',
            {
                'operation_type': 'cache_query',
                'file_path': 'main.py',
                'cache_hit': True,
                'turn_index': 0,
            },
        ])

        assert result['total_cache_queries'] == 1

    def test_missing_turn_index_uses_record_index(self):
        """Verify missing turn_index uses record index."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'cache_query',
                'file_path': 'main.py',
                'cache_hit': True,
            },
        ])

        assert result['examples'][0]['turn_index'] == 0

    def test_missing_bytes_read_handled(self):
        """Verify missing bytes_read is handled gracefully."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'turn_index': 0,
            },
        ])

        assert result['total_file_reads'] == 1
        assert result['avg_bytes_per_uncached_read'] == 0.0

    def test_bytes_read_as_string_converted(self):
        """Verify bytes_read as string is converted to int."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'bytes_read': '1000',
                'turn_index': 0,
            },
        ])

        assert result['avg_bytes_per_uncached_read'] == 1000.0

    def test_bytes_read_as_float_converted(self):
        """Verify bytes_read as float is converted to int."""
        result = analyze_session_cache_hit_ratio([
            {
                'operation_type': 'read',
                'file_path': 'main.py',
                'bytes_read': 1000.5,
                'turn_index': 0,
            },
        ])

        assert result['avg_bytes_per_uncached_read'] == 1000.0


class TestNumber:
    """Test number extraction helper."""

    def test_integer_returned_as_is(self):
        """Verify integer is returned as-is."""
        assert _number(42) == 42

    def test_float_converted_to_int(self):
        """Verify float is converted to int."""
        assert _number(42.7) == 42

    def test_string_number_converted(self):
        """Verify string number is converted to int."""
        assert _number('42') == 42

    def test_invalid_string_returns_none(self):
        """Verify invalid string returns None."""
        assert _number('not a number') is None

    def test_boolean_returns_none(self):
        """Verify boolean returns None."""
        assert _number(True) is None
        assert _number(False) is None

    def test_none_returns_none(self):
        """Verify None returns None."""
        assert _number(None) is None


class TestPercentage:
    """Test percentage calculation helper."""

    def test_zero_denominator_returns_zero(self):
        """Verify zero denominator returns 0.0."""
        assert _percentage(10, 0) == 0.0

    def test_negative_denominator_returns_zero(self):
        """Verify negative denominator returns 0.0."""
        assert _percentage(10, -5) == 0.0

    def test_zero_numerator_returns_zero(self):
        """Verify zero numerator returns 0.0."""
        assert _percentage(0, 10) == 0.0

    def test_equal_values_returns_100(self):
        """Verify equal values return 100.0."""
        assert _percentage(10, 10) == 100.0

    def test_half_returns_50(self):
        """Verify half returns 50.0."""
        assert _percentage(5, 10) == 50.0

    def test_result_rounded_to_two_decimals(self):
        """Verify result is rounded to 2 decimal places."""
        assert _percentage(1, 3) == 33.33


class TestCalculateCacheEffectivenessScore:
    """Test cache effectiveness score calculation helper."""

    def test_perfect_cache_usage_scores_100(self):
        """Verify perfect cache usage (100% hit rate, high usage, no misses) scores high."""
        score = _calculate_cache_effectiveness_score(
            cache_hit_rate=100.0,
            cache_usage_rate=100.0,
            missed_opportunity_rate=0.0,
        )
        assert score == 75.0  # 100*0.4 + 100*0.35 - 0*0.25 = 75

    def test_no_cache_usage_scores_zero(self):
        """Verify no cache usage scores 0."""
        score = _calculate_cache_effectiveness_score(
            cache_hit_rate=0.0,
            cache_usage_rate=0.0,
            missed_opportunity_rate=0.0,
        )
        assert score == 0.0

    def test_high_missed_opportunities_penalized(self):
        """Verify high missed opportunities are heavily penalized."""
        score = _calculate_cache_effectiveness_score(
            cache_hit_rate=50.0,
            cache_usage_rate=30.0,
            missed_opportunity_rate=80.0,
        )
        # 50*0.4 + 30*0.35 - 80*0.25 = 20 + 10.5 - 20 = 10.5
        assert score == 10.5

    def test_mixed_effectiveness(self):
        """Verify mixed effectiveness calculates correctly."""
        score = _calculate_cache_effectiveness_score(
            cache_hit_rate=60.0,
            cache_usage_rate=40.0,
            missed_opportunity_rate=20.0,
        )
        # 60*0.4 + 40*0.35 - 20*0.25 = 24 + 14 - 5 = 33
        assert score == 33.0

    def test_score_clamped_to_zero(self):
        """Verify score doesn't go below 0."""
        score = _calculate_cache_effectiveness_score(
            cache_hit_rate=0.0,
            cache_usage_rate=0.0,
            missed_opportunity_rate=100.0,
        )
        # 0*0.4 + 0*0.35 - 100*0.25 = -25, clamped to 0
        assert score == 0.0

    def test_score_clamped_to_100(self):
        """Verify score doesn't exceed 100."""
        # This shouldn't happen in practice with real percentages
        score = _calculate_cache_effectiveness_score(
            cache_hit_rate=200.0,
            cache_usage_rate=200.0,
            missed_opportunity_rate=0.0,
        )
        assert score == 100.0


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_session_with_no_cache_usage(self):
        """Verify session with no cache usage at all."""
        result = analyze_session_cache_hit_ratio([
            {'operation_type': 'read', 'file_path': 'main.py', 'bytes_read': 500, 'turn_index': 0},
            {'operation_type': 'read', 'file_path': 'utils.py', 'bytes_read': 300, 'turn_index': 1},
        ])

        assert result['total_cache_queries'] == 0
        assert result['cache_snapshots'] == 0
        assert result['total_file_reads'] == 2
        assert result['reads_of_uncached_files'] == 2
        assert result['cache_effectiveness_score'] == 0.0

    def test_session_with_high_cache_usage(self):
        """Verify session with effective cache usage."""
        result = analyze_session_cache_hit_ratio([
            {'operation_type': 'read', 'file_path': 'main.py', 'bytes_read': 1000, 'turn_index': 0},
            {'operation_type': 'cache_snapshot', 'file_path': 'main.py', 'turn_index': 1},
            {'operation_type': 'cache_query', 'file_path': 'main.py', 'cache_hit': True, 'turn_index': 2},
            {'operation_type': 'read', 'file_path': 'main.py', 'bytes_read': 100, 'turn_index': 3},
        ])

        assert result['cache_hit_rate'] == 100.0
        assert result['cache_snapshots'] == 1
        assert result['reads_of_cached_files'] == 1
        assert result['missed_caching_opportunities'] == 0
        assert result['cache_effectiveness_score'] > 30.0

    def test_session_with_missed_opportunities(self):
        """Verify session with many missed caching opportunities."""
        result = analyze_session_cache_hit_ratio([
            {'operation_type': 'read', 'file_path': 'config.py', 'bytes_read': 500, 'turn_index': 0},
            {'operation_type': 'read', 'file_path': 'config.py', 'bytes_read': 500, 'turn_index': 1},
            {'operation_type': 'read', 'file_path': 'config.py', 'bytes_read': 500, 'turn_index': 2},
            {'operation_type': 'read', 'file_path': 'config.py', 'bytes_read': 500, 'turn_index': 3},
        ])

        assert result['total_file_reads'] == 4
        assert result['repeated_full_reads'] == 1
        assert result['missed_caching_opportunities'] == 3
        # Score should be low due to many missed opportunities
        assert result['cache_effectiveness_score'] < 10.0

    def test_typical_optimized_session(self):
        """Verify typical session with good cache practices."""
        result = analyze_session_cache_hit_ratio([
            # First read, then cache
            {'operation_type': 'read', 'file_path': 'utils.py', 'bytes_read': 2000, 'turn_index': 0},
            {'operation_type': 'cache_snapshot', 'file_path': 'utils.py', 'turn_index': 1},
            # Query cache before reading
            {'operation_type': 'cache_query', 'file_path': 'utils.py', 'cache_hit': True, 'turn_index': 2},
            {'operation_type': 'read', 'file_path': 'utils.py', 'bytes_read': 50, 'turn_index': 3},
            # Another file
            {'operation_type': 'read', 'file_path': 'main.py', 'bytes_read': 1500, 'turn_index': 4},
            {'operation_type': 'cache_snapshot', 'file_path': 'main.py', 'turn_index': 5},
            {'operation_type': 'cache_query', 'file_path': 'main.py', 'cache_hit': True, 'turn_index': 6},
        ])

        assert result['cache_hit_rate'] == 100.0
        assert result['cache_snapshots'] == 2
        assert result['reads_of_cached_files'] == 1
        assert result['missed_caching_opportunities'] == 0
        assert result['cache_effectiveness_score'] > 40.0

    def test_cache_clear_resets_tracking(self):
        """Verify cache clear properly resets cache tracking."""
        result = analyze_session_cache_hit_ratio([
            {'operation_type': 'cache_snapshot', 'file_path': 'test.py', 'turn_index': 0},
            {'operation_type': 'read', 'file_path': 'test.py', 'bytes_read': 100, 'turn_index': 1},
            {'operation_type': 'cache_clear', 'turn_index': 2},
            {'operation_type': 'read', 'file_path': 'test.py', 'bytes_read': 100, 'turn_index': 3},
        ])

        # First read should be cached, second should be uncached after clear
        assert result['reads_of_cached_files'] == 1
        assert result['reads_of_uncached_files'] == 1
        assert result['cache_clears'] == 1

    def test_bytes_saved_calculation(self):
        """Verify that cached reads use fewer bytes on average."""
        result = analyze_session_cache_hit_ratio([
            # Uncached: full file reads
            {'operation_type': 'read', 'file_path': 'large.py', 'bytes_read': 5000, 'turn_index': 0},
            {'operation_type': 'read', 'file_path': 'large.py', 'bytes_read': 5000, 'turn_index': 1},
            # Now cache and use targeted reads
            {'operation_type': 'cache_snapshot', 'file_path': 'large.py', 'turn_index': 2},
            {'operation_type': 'read', 'file_path': 'large.py', 'bytes_read': 100, 'turn_index': 3},
            {'operation_type': 'read', 'file_path': 'large.py', 'bytes_read': 150, 'turn_index': 4},
        ])

        # Uncached reads: 5000 average
        # Cached reads: (100 + 150) / 2 = 125 average
        assert result['avg_bytes_per_uncached_read'] == 5000.0
        assert result['avg_bytes_per_cached_read'] == 125.0
        # Demonstrates significant token savings with caching

    def test_multiple_files_with_varying_cache_patterns(self):
        """Verify session with multiple files and different caching patterns."""
        result = analyze_session_cache_hit_ratio([
            # File 1: well cached
            {'operation_type': 'read', 'file_path': 'a.py', 'bytes_read': 1000, 'turn_index': 0},
            {'operation_type': 'cache_snapshot', 'file_path': 'a.py', 'turn_index': 1},
            {'operation_type': 'read', 'file_path': 'a.py', 'bytes_read': 100, 'turn_index': 2},
            # File 2: missed opportunity
            {'operation_type': 'read', 'file_path': 'b.py', 'bytes_read': 1000, 'turn_index': 3},
            {'operation_type': 'read', 'file_path': 'b.py', 'bytes_read': 1000, 'turn_index': 4},
            # File 3: single read (no opportunity)
            {'operation_type': 'read', 'file_path': 'c.py', 'bytes_read': 500, 'turn_index': 5},
        ])

        assert result['total_file_reads'] == 5  # 5 read operations (not counting snapshot)
        assert result['cache_snapshots'] == 1
        assert result['reads_of_cached_files'] == 1
        assert result['repeated_full_reads'] == 2  # Files a and b
        assert result['missed_caching_opportunities'] == 1  # Second read of b.py
