"""Tests for pack parallel tool execution efficiency analyzer."""

import pytest

from synthesis.pack_parallel_tool_execution import analyze_pack_parallel_tool_execution


class TestAnalyzePackParallelToolExecution:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty packs returns zero metrics."""
        result = analyze_pack_parallel_tool_execution([])

        assert result["total_packs"] == 0
        assert result["total_batches"] == 0
        assert result["parallel_batches"] == 0
        assert result["sequential_batches"] == 0
        assert result["parallelization_rate"] == 0.0
        assert result["total_tools_in_parallel"] == 0
        assert result["avg_tools_per_parallel_batch"] == 0.0
        assert result["max_tools_per_parallel_batch"] == 0
        assert result["potential_parallel_opportunities"] == 0
        assert result["missed_read_parallelization"] == 0
        assert result["missed_search_parallelization"] == 0
        assert result["missed_bash_parallelization"] == 0
        assert result["parallelization_efficiency"] == 0.0
        assert result["common_parallel_patterns"] == []
        assert result["example_good_parallel"] == {}
        assert result["example_missed_opportunity"] == {}

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_parallel_tool_execution(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_parallel_tool_execution("not a list")

    def test_no_parallel_calls_all_sequential(self):
        """Verify pack with only sequential tool calls."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "b.py"}
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {"tool_name": "Edit", "file_path": "c.py"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_batches"] == 3
        assert result["parallel_batches"] == 0
        assert result["sequential_batches"] == 3
        assert result["parallelization_rate"] == 0.0
        assert result["avg_tools_per_parallel_batch"] == 0.0

    def test_all_parallel_calls_no_sequential(self):
        """Verify pack with all parallel tool calls."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"},
                                    {"tool_name": "Read", "file_path": "b.py"},
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Grep", "pattern": "foo"},
                                    {"tool_name": "Glob", "pattern": "*.py"},
                                    {"tool_name": "Read", "file_path": "c.py"},
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["total_batches"] == 2
        assert result["parallel_batches"] == 2
        assert result["sequential_batches"] == 0
        assert result["parallelization_rate"] == 100.0
        assert result["total_tools_in_parallel"] == 5
        assert result["avg_tools_per_parallel_batch"] == 2.5
        assert result["max_tools_per_parallel_batch"] == 3

    def test_mixed_parallel_and_sequential(self):
        """Verify pack with mix of parallel and sequential calls."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"},
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "b.py"},
                                    {"tool_name": "Read", "file_path": "c.py"},
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {"tool_name": "Edit", "file_path": "a.py"},
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["total_batches"] == 3
        assert result["parallel_batches"] == 1
        assert result["sequential_batches"] == 2
        assert result["parallelization_rate"] == 33.33

    def test_missed_read_parallelization_opportunity(self):
        """Verify detection of sequential Read calls for different files."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "b.py"}
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "c.py"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["potential_parallel_opportunities"] == 2
        assert result["missed_read_parallelization"] == 2
        assert result["example_missed_opportunity"]["type"] == "sequential_reads"

    def test_no_missed_read_opportunity_same_file(self):
        """Verify sequential reads of same file not flagged as missed opportunity."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        # Same file reads might be intentional re-verification
        assert result["potential_parallel_opportunities"] == 0
        assert result["missed_read_parallelization"] == 0

    def test_missed_search_parallelization_opportunity(self):
        """Verify detection of sequential Grep/Glob calls."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Grep", "pattern": "foo"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Glob", "pattern": "*.py"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["potential_parallel_opportunities"] == 1
        assert result["missed_search_parallelization"] == 1
        assert result["example_missed_opportunity"]["type"] == "sequential_searches"

    def test_missed_bash_parallelization_opportunity(self):
        """Verify detection of sequential independent Bash calls."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "ls src/"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "cat README.md"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["potential_parallel_opportunities"] == 1
        assert result["missed_bash_parallelization"] == 1

    def test_bash_chaining_not_flagged_as_missed_opportunity(self):
        """Verify chained Bash commands with && are not flagged."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "cd src && ls"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "pwd"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        # Chained commands should not be flagged
        assert result["potential_parallel_opportunities"] == 0

    def test_common_parallel_patterns_tracked(self):
        """Verify common parallel patterns are identified."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"},
                                    {"tool_name": "Read", "file_path": "b.py"},
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "c.py"},
                                    {"tool_name": "Read", "file_path": "d.py"},
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {"tool_name": "Grep", "pattern": "foo"},
                                    {"tool_name": "Glob", "pattern": "*.py"},
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert len(result["common_parallel_patterns"]) == 2
        # Most common should be Read+Read (appears twice)
        assert result["common_parallel_patterns"][0]["count"] == 2
        assert "Read" in result["common_parallel_patterns"][0]["tools"]

    def test_example_good_parallel_captured(self):
        """Verify good parallel execution example is captured."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"},
                                    {"tool_name": "Read", "file_path": "b.py"},
                                    {"tool_name": "Read", "file_path": "c.py"},
                                    {"tool_name": "Read", "file_path": "d.py"},
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["example_good_parallel"]["count"] == 4
        assert len(result["example_good_parallel"]["tools"]) == 4

    def test_parallelization_efficiency_score_high(self):
        """Verify high efficiency score for good parallelization."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            # High parallelization rate (50%)
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"},
                                    {"tool_name": "Read", "file_path": "b.py"},
                                    {"tool_name": "Read", "file_path": "c.py"},
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Edit", "file_path": "a.py"}
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {"tool_name": "Grep", "pattern": "foo"},
                                    {"tool_name": "Glob", "pattern": "*.py"},
                                    {"tool_name": "Read", "file_path": "d.py"},
                                ]
                            },
                            {
                                "message_index": 3,
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "e.py"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        # 50% parallelization rate, avg 3 tools per batch, no missed opportunities
        assert result["parallelization_rate"] == 50.0
        assert result["avg_tools_per_parallel_batch"] == 3.0
        assert result["parallelization_efficiency"] >= 0.7

    def test_parallelization_efficiency_score_low(self):
        """Verify low efficiency score for poor parallelization."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "b.py"}
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "c.py"}
                                ]
                            },
                            {
                                "message_index": 3,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "d.py"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        # 0% parallelization rate, many missed opportunities
        assert result["parallelization_rate"] == 0.0
        assert result["potential_parallel_opportunities"] == 3
        assert result["parallelization_efficiency"] < 0.3

    def test_multiple_packs_aggregation(self):
        """Verify metrics are aggregated across multiple packs."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"},
                                    {"tool_name": "Read", "file_path": "b.py"},
                                ]
                            },
                        ]
                    }
                ]
            },
            {
                "pack_id": "pack2",
                "sessions": [
                    {
                        "session_id": "session2",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Grep", "pattern": "foo"},
                                    {"tool_name": "Glob", "pattern": "*.py"},
                                ]
                            },
                        ]
                    }
                ]
            },
        ])

        assert result["total_packs"] == 2
        assert result["total_batches"] == 2
        assert result["parallel_batches"] == 2

    def test_multiple_sessions_per_pack(self):
        """Verify multiple sessions are aggregated within pack."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"},
                                ]
                            },
                        ]
                    },
                    {
                        "session_id": "session2",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "b.py"},
                                ]
                            },
                        ]
                    },
                ]
            }
        ])

        assert result["total_batches"] == 2
        assert result["sequential_batches"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_parallel_tool_execution([
            "not a dict",
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"},
                                ]
                            },
                        ]
                    }
                ]
            },
        ])

        assert result["total_packs"] == 1
        assert result["total_batches"] == 1

    def test_missing_sessions_handled_gracefully(self):
        """Verify pack without sessions is handled."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                # Missing sessions
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_batches"] == 0

    def test_missing_messages_handled_gracefully(self):
        """Verify session without messages is handled."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        # Missing messages
                    }
                ]
            }
        ])

        assert result["total_batches"] == 0

    def test_missing_tool_calls_in_message(self):
        """Verify message without tool_calls is handled."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                # Missing tool_calls
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_batches"] == 0

    def test_empty_tool_calls_list(self):
        """Verify message with empty tool_calls list is handled."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": []
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_batches"] == 0

    def test_malformed_tool_call_skipped(self):
        """Verify non-dict tool calls are skipped."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    "not a dict",
                                    {"tool_name": "Read", "file_path": "a.py"},
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        # Should count as sequential (1 valid tool call)
        assert result["total_batches"] == 1
        assert result["sequential_batches"] == 1

    def test_dependency_chain_justifies_sequential(self):
        """Verify dependency chains (Edit→Read) don't count as missed opportunity."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Edit", "file_path": "a.py"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        # Edit→Read of same file is a valid dependency
        assert result["potential_parallel_opportunities"] == 0

    def test_write_then_bash_dependency_not_flagged(self):
        """Verify Write→Bash dependency chain not flagged."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "test.py"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "python test.py"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        # Different tool types, not flagged as parallelizable
        assert result["potential_parallel_opportunities"] == 0

    def test_realistic_optimized_pattern(self):
        """Verify realistic optimized execution pattern."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            # Parallel reads for exploration
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "src/main.py"},
                                    {"tool_name": "Read", "file_path": "src/utils.py"},
                                    {"tool_name": "Read", "file_path": "tests/test_main.py"},
                                ]
                            },
                            # Edit (depends on previous reads)
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Edit", "file_path": "src/main.py"}
                                ]
                            },
                            # Parallel verification
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "pytest tests/"},
                                    {"tool_name": "Bash", "command": "mypy src/"},
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["parallelization_rate"] == 66.67
        assert result["avg_tools_per_parallel_batch"] == 2.5
        assert result["parallelization_efficiency"] >= 0.6

    def test_realistic_unoptimized_pattern(self):
        """Verify realistic unoptimized execution pattern."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            # Sequential reads (could be parallel)
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "src/main.py"}
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "src/utils.py"}
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "tests/test_main.py"}
                                ]
                            },
                            # Edit
                            {
                                "message_index": 3,
                                "tool_calls": [
                                    {"tool_name": "Edit", "file_path": "src/main.py"}
                                ]
                            },
                            # Sequential verification (could be parallel)
                            {
                                "message_index": 4,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "pytest tests/"}
                                ]
                            },
                            {
                                "message_index": 5,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "mypy src/"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["parallelization_rate"] == 0.0
        assert result["potential_parallel_opportunities"] >= 3
        assert result["missed_read_parallelization"] == 2
        assert result["missed_bash_parallelization"] == 1
        assert result["parallelization_efficiency"] < 0.4

    def test_edge_case_single_batch_with_many_tools(self):
        """Verify single parallel batch with many tools."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": f"file{i}.py"}
                                    for i in range(10)
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["parallel_batches"] == 1
        assert result["max_tools_per_parallel_batch"] == 10
        assert result["avg_tools_per_parallel_batch"] == 10.0
        assert result["parallelization_rate"] == 100.0

    def test_edge_case_alternating_parallel_sequential(self):
        """Verify alternating pattern of parallel and sequential."""
        result = analyze_pack_parallel_tool_execution([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {"tool_name": "Read", "file_path": "a.py"},
                                    {"tool_name": "Read", "file_path": "b.py"},
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {"tool_name": "Edit", "file_path": "a.py"}
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {"tool_name": "Bash", "command": "pytest"},
                                    {"tool_name": "Bash", "command": "mypy"},
                                ]
                            },
                            {
                                "message_index": 3,
                                "tool_calls": [
                                    {"tool_name": "Write", "file_path": "c.py"}
                                ]
                            },
                        ]
                    }
                ]
            }
        ])

        assert result["total_batches"] == 4
        assert result["parallel_batches"] == 2
        assert result["sequential_batches"] == 2
        assert result["parallelization_rate"] == 50.0
