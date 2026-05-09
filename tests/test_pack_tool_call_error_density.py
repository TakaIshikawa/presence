"""Tests for pack tool call error density analyzer."""

import pytest

from synthesis.pack_tool_call_error_density import analyze_pack_tool_call_error_density


class TestAnalyzePackToolCallErrorDensity:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty packs returns zero metrics."""
        result = analyze_pack_tool_call_error_density([])

        assert result["total_packs"] == 0
        assert result["total_tool_calls"] == 0
        assert result["failed_tool_calls"] == 0
        assert result["error_density"] == 0.0
        assert result["high_error_density_packs"] == 0
        assert result["error_rate_by_tool"] == {}
        assert result["tool_with_highest_error_rate"] == ""
        assert result["error_clustering_score"] == 0.0
        assert result["clustered_error_packs"] == 0
        assert result["retry_attempts"] == 0
        assert result["successful_retries"] == 0
        assert result["failed_retries"] == 0
        assert result["retry_resolution_rate"] == 0.0
        assert result["error_cascade_count"] == 0
        assert result["fatal_error_count"] == 0
        assert result["recoverable_error_count"] == 0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_tool_call_error_density(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_tool_call_error_density("not a list")

    def test_pack_with_zero_errors(self):
        """Verify pack with all successful tool calls."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Write", "success": True},
                            {"tool_name": "Edit", "success": True},
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_tool_calls"] == 3
        assert result["failed_tool_calls"] == 0
        assert result["error_density"] == 0.0
        assert result["high_error_density_packs"] == 0

    def test_pack_with_low_error_density(self):
        """Verify pack with acceptable error density (<15%)."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Write", "success": True},
                            {"tool_name": "Edit", "success": False},  # 1 failure
                            {"tool_name": "Bash", "success": True},
                            {"tool_name": "Grep", "success": True},
                            {"tool_name": "Glob", "success": True},
                            {"tool_name": "Task", "success": True},
                            {"tool_name": "Read", "success": True},
                        ]
                    }
                ]
            }
        ])

        assert result["total_tool_calls"] == 8
        assert result["failed_tool_calls"] == 1
        # 1/8 = 12.5%
        assert result["error_density"] == 12.5
        assert result["high_error_density_packs"] == 0  # Below 15% threshold

    def test_pack_with_high_error_density(self):
        """Verify pack with high error density (>15%)."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Write", "success": False},
                            {"tool_name": "Edit", "success": True},
                            {"tool_name": "Bash", "success": True},
                            {"tool_name": "Grep", "success": True},
                        ]
                    }
                ]
            }
        ])

        # 2 failures / 5 total = 40%
        assert result["error_density"] == 40.0
        assert result["high_error_density_packs"] == 1

    def test_error_rate_by_tool_type(self):
        """Verify error rates are calculated per tool type."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Write", "success": False},
                            {"tool_name": "Write", "success": False},
                            {"tool_name": "Edit", "success": True},
                            {"tool_name": "Edit", "success": True},
                            {"tool_name": "Edit", "success": False},
                        ]
                    }
                ]
            }
        ])

        # Read: 0/2 = 0% error rate
        # Write: 2/2 = 100% error rate
        # Edit: 1/3 = 33.33% error rate
        assert result["error_rate_by_tool"]["Read"] == 0.0
        assert result["error_rate_by_tool"]["Write"] == 100.0
        assert result["error_rate_by_tool"]["Edit"] == 33.33
        assert result["tool_with_highest_error_rate"] == "Write"

    def test_error_clustering_distributed_errors(self):
        """Verify distributed errors result in low clustering score."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": False},  # 1 error
                        ]
                    },
                    {
                        "session_id": "session2",
                        "tool_calls": [
                            {"tool_name": "Write", "success": False},  # 1 error
                        ]
                    },
                    {
                        "session_id": "session3",
                        "tool_calls": [
                            {"tool_name": "Edit", "success": False},  # 1 error
                        ]
                    },
                ]
            }
        ])

        # Errors evenly distributed across 3 sessions
        assert result["error_clustering_score"] < 0.3
        assert result["clustered_error_packs"] == 0

    def test_error_clustering_concentrated_errors(self):
        """Verify concentrated errors result in high clustering score."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Write", "success": False},
                            {"tool_name": "Edit", "success": False},
                            {"tool_name": "Bash", "success": False},
                            {"tool_name": "Grep", "success": False},
                        ]
                    },
                    {
                        "session_id": "session2",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                        ]
                    },
                    {
                        "session_id": "session3",
                        "tool_calls": [
                            {"tool_name": "Write", "success": True},
                        ]
                    },
                ]
            }
        ])

        # All errors in one session
        assert result["error_clustering_score"] > 0.7
        assert result["clustered_error_packs"] == 1

    def test_retry_pattern_successful_retry(self):
        """Verify successful retry pattern detection."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {
                                "tool_name": "Read",
                                "file_path": "test.py",
                                "success": False,
                                "turn_index": 1
                            },
                            {
                                "tool_name": "Read",
                                "file_path": "test.py",
                                "success": True,
                                "turn_index": 2
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["successful_retries"] == 1
        assert result["retry_resolution_rate"] > 0.0

    def test_retry_pattern_failed_retry(self):
        """Verify failed retry pattern detection."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {
                                "tool_name": "Write",
                                "file_path": "test.py",
                                "success": False,
                                "turn_index": 1
                            },
                            {
                                "tool_name": "Write",
                                "file_path": "test.py",
                                "success": False,
                                "turn_index": 2
                            },
                            {
                                "tool_name": "Write",
                                "file_path": "test.py",
                                "success": False,
                                "turn_index": 3
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["retry_attempts"] == 2  # 3 attempts - 1 initial
        assert result["failed_retries"] == 1
        assert result["retry_resolution_rate"] == 0.0

    def test_error_cascade_detection(self):
        """Verify error cascade detection (3+ consecutive failures)."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Write", "success": False},
                            {"tool_name": "Edit", "success": False},
                            {"tool_name": "Bash", "success": False},
                        ]
                    }
                ]
            }
        ])

        # 4 consecutive failures = error cascade
        assert result["error_cascade_count"] == 1

    def test_no_error_cascade_with_successes_interspersed(self):
        """Verify no cascade when successes break failure chain."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Write", "success": True},  # Breaks chain
                            {"tool_name": "Edit", "success": False},
                            {"tool_name": "Bash", "success": False},
                        ]
                    }
                ]
            }
        ])

        # No cascade (success breaks the chain)
        assert result["error_cascade_count"] == 0

    def test_fatal_vs_recoverable_errors(self):
        """Verify fatal and recoverable error classification."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {
                                "tool_name": "Read",
                                "success": False,
                                "error_type": "fatal"
                            },
                            {
                                "tool_name": "Write",
                                "success": False,
                                "error_type": "recoverable"
                            },
                            {
                                "tool_name": "Edit",
                                "success": False,
                                "error_type": "FATAL"  # Case insensitive
                            },
                            {
                                "tool_name": "Bash",
                                "success": False,
                                "error_type": "recoverable"
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["fatal_error_count"] == 2
        assert result["recoverable_error_count"] == 2

    def test_multiple_packs_aggregation(self):
        """Verify metrics are aggregated across multiple packs."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Write", "success": False},
                        ]
                    }
                ]
            },
            {
                "pack_id": "pack2",
                "sessions": [
                    {
                        "session_id": "session2",
                        "tool_calls": [
                            {"tool_name": "Edit", "success": True},
                            {"tool_name": "Bash", "success": False},
                        ]
                    }
                ]
            },
        ])

        assert result["total_packs"] == 2
        assert result["total_tool_calls"] == 4
        assert result["failed_tool_calls"] == 2
        assert result["error_density"] == 50.0

    def test_multiple_sessions_per_pack(self):
        """Verify multiple sessions are aggregated within pack."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Write", "success": False},
                        ]
                    },
                    {
                        "session_id": "session2",
                        "tool_calls": [
                            {"tool_name": "Edit", "success": True},
                            {"tool_name": "Bash", "success": False},
                        ]
                    },
                ]
            }
        ])

        assert result["total_tool_calls"] == 4
        assert result["failed_tool_calls"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_tool_call_error_density([
            "not a dict",
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                        ]
                    }
                ]
            },
        ])

        assert result["total_packs"] == 1
        assert result["total_tool_calls"] == 1

    def test_missing_sessions_handled_gracefully(self):
        """Verify pack without sessions is handled."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                # Missing sessions
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_tool_calls"] == 0

    def test_empty_sessions_list(self):
        """Verify pack with empty sessions list."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": []
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_tool_calls"] == 0

    def test_missing_tool_calls_in_session(self):
        """Verify session without tool_calls is handled."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        # Missing tool_calls
                    }
                ]
            }
        ])

        assert result["total_tool_calls"] == 0

    def test_malformed_tool_call_skipped(self):
        """Verify non-dict tool calls are skipped."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            "not a dict",
                            {"tool_name": "Read", "success": True},
                        ]
                    }
                ]
            }
        ])

        assert result["total_tool_calls"] == 1

    def test_default_success_value(self):
        """Verify tool calls default to success=True when not specified."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read"},  # No success field
                        ]
                    }
                ]
            }
        ])

        assert result["failed_tool_calls"] == 0

    def test_unknown_tool_name_default(self):
        """Verify unknown tool names are handled."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"success": False},  # No tool_name
                        ]
                    }
                ]
            }
        ])

        assert result["failed_tool_calls"] == 1
        assert "unknown" in result["error_rate_by_tool"]

    def test_boolean_field_variations(self):
        """Verify boolean success field handles various input formats."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Write", "success": "true"},
                            {"tool_name": "Edit", "success": False},
                            {"tool_name": "Bash", "success": "false"},
                        ]
                    }
                ]
            }
        ])

        assert result["total_tool_calls"] == 4
        assert result["failed_tool_calls"] == 2

    def test_error_density_boundary_exactly_15_percent(self):
        """Verify 15% error density is not considered high."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": True},
                        ]
                    }
                ]
            }
        ])

        # 3/20 = 15.0%
        assert result["error_density"] == 15.0
        assert result["high_error_density_packs"] == 0

    def test_clustering_with_single_session(self):
        """Verify clustering is not calculated for single session."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Write", "success": False},
                        ]
                    }
                ]
            }
        ])

        # No clustering score for single session
        assert result["error_clustering_score"] == 0.0

    def test_retry_without_file_path(self):
        """Verify retry detection works without file_path."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {
                                "tool_name": "Bash",
                                "success": False,
                                "turn_index": 1
                            },
                            {
                                "tool_name": "Bash",
                                "success": True,
                                "turn_index": 2
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["successful_retries"] == 1

    def test_optimal_pattern_low_errors_distributed(self):
        """Verify optimal pattern with low error density and distribution."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Write", "success": True},
                            {"tool_name": "Edit", "success": True},
                            {"tool_name": "Bash", "success": True},
                            {"tool_name": "Grep", "success": False},  # 1 error
                        ]
                    },
                    {
                        "session_id": "session2",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Edit", "success": False},  # 1 error
                        ]
                    },
                    {
                        "session_id": "session3",
                        "tool_calls": [
                            {"tool_name": "Glob", "success": True},
                            {"tool_name": "Task", "success": True},
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Write", "success": False},  # 1 error
                        ]
                    }
                ]
            }
        ])

        # 3/11 = 27.27% error density (but evenly distributed)
        assert result["error_density"] < 30.0
        assert result["error_clustering_score"] < 0.5  # Evenly distributed across sessions

    def test_anti_pattern_high_error_density_clustered(self):
        """Verify anti-pattern with high error density and clustering."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Write", "success": False},
                            {"tool_name": "Edit", "success": False},
                            {"tool_name": "Bash", "success": False},
                            {"tool_name": "Grep", "success": False},
                        ]
                    },
                    {
                        "session_id": "session2",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                        ]
                    }
                ]
            }
        ])

        # 5/6 = 83.33% error density
        assert result["error_density"] > 15.0
        assert result["high_error_density_packs"] == 1
        assert result["error_clustering_score"] > 0.7

    def test_anti_pattern_failed_retries_without_resolution(self):
        """Verify anti-pattern of repeated failures without resolution."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {
                                "tool_name": "Write",
                                "file_path": "test.py",
                                "success": False,
                                "turn_index": 1
                            },
                            {
                                "tool_name": "Write",
                                "file_path": "test.py",
                                "success": False,
                                "turn_index": 2
                            },
                            {
                                "tool_name": "Write",
                                "file_path": "test.py",
                                "success": False,
                                "turn_index": 3
                            },
                            {
                                "tool_name": "Write",
                                "file_path": "test.py",
                                "success": False,
                                "turn_index": 4
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["failed_retries"] >= 1
        assert result["retry_resolution_rate"] == 0.0

    def test_anti_pattern_error_cascade(self):
        """Verify anti-pattern of error cascade."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Edit", "success": False},
                            {"tool_name": "Write", "success": False},
                            {"tool_name": "Bash", "success": False},
                            {"tool_name": "Grep", "success": False},
                        ]
                    }
                ]
            }
        ])

        assert result["error_cascade_count"] >= 1

    def test_mixed_error_types_across_tools(self):
        """Verify mixed error types are tracked correctly."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            {"tool_name": "Read", "success": True},
                            {"tool_name": "Read", "success": False},
                            {"tool_name": "Write", "success": False},
                            {"tool_name": "Write", "success": False},
                            {"tool_name": "Edit", "success": True},
                            {"tool_name": "Bash", "success": False},
                        ]
                    }
                ]
            }
        ])

        # Read: 1/2 = 50%
        # Write: 2/2 = 100%
        # Edit: 0/1 = 0%
        # Bash: 1/1 = 100%
        assert result["error_rate_by_tool"]["Read"] == 50.0
        assert result["error_rate_by_tool"]["Write"] == 100.0
        assert result["error_rate_by_tool"]["Edit"] == 0.0
        assert result["error_rate_by_tool"]["Bash"] == 100.0

    def test_complex_retry_pattern_with_multiple_files(self):
        """Verify complex retry patterns with different files."""
        result = analyze_pack_tool_call_error_density([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "tool_calls": [
                            # File A: failure then success
                            {
                                "tool_name": "Read",
                                "file_path": "a.py",
                                "success": False,
                                "turn_index": 1
                            },
                            {
                                "tool_name": "Read",
                                "file_path": "a.py",
                                "success": True,
                                "turn_index": 2
                            },
                            # File B: multiple failures
                            {
                                "tool_name": "Write",
                                "file_path": "b.py",
                                "success": False,
                                "turn_index": 3
                            },
                            {
                                "tool_name": "Write",
                                "file_path": "b.py",
                                "success": False,
                                "turn_index": 4
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["successful_retries"] >= 1
        assert result["failed_retries"] >= 1
