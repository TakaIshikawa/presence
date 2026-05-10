"""Tests for pack Bash command composition analyzer."""

import pytest

from synthesis.pack_bash_command_composition import analyze_pack_bash_command_composition


class TestAnalyzePackBashCommandComposition:
    """Test main analyzer function."""

    def test_empty_packs_returns_zeroed_metrics(self):
        """Verify empty packs returns zero metrics."""
        result = analyze_pack_bash_command_composition([])

        assert result["total_packs"] == 0
        assert result["total_bash_calls"] == 0
        assert result["sequential_chaining_and_usage"] == 0.0
        assert result["sequential_chaining_semicolon_usage"] == 0.0
        assert result["parallel_bash_opportunities"] == 0
        assert result["parallel_bash_opportunities_taken"] == 0
        assert result["parallel_execution_rate"] == 0.0
        assert result["total_paths_in_commands"] == 0
        assert result["properly_quoted_paths"] == 0
        assert result["path_quoting_compliance"] == 0.0
        assert result["unquoted_space_paths"] == 0
        assert result["heredoc_usage_count"] == 0
        assert result["multi_line_opportunities"] == 0
        assert result["heredoc_usage_rate"] == 0.0
        assert result["total_descriptions"] == 0
        assert result["avg_description_length"] == 0.0
        assert result["overly_verbose_descriptions"] == 0
        assert result["missing_descriptions"] == 0
        assert result["tool_preference_violations"] == 0
        assert result["tool_violation_rate"] == 0.0
        assert result["example_good_composition"] == {}
        assert result["example_missed_parallel"] == {}
        assert result["example_quoting_violation"] == {}
        assert result["example_tool_violation"] == {}

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_pack_bash_command_composition(None)
        assert result["total_packs"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_pack_bash_command_composition("not a list")

    def test_sequential_chaining_with_and_operator(self):
        """Verify detection of && chaining in commands."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git add . && git commit -m 'fix'",
                                        "description": "Stage and commit changes"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_bash_calls"] == 1
        assert result["sequential_chaining_and_usage"] == 100.0

    def test_sequential_chaining_with_semicolon(self):
        """Verify detection of ; chaining in commands."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "ls src; ls tests",
                                        "description": "List directories"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_bash_calls"] == 1
        assert result["sequential_chaining_semicolon_usage"] == 100.0

    def test_parallel_bash_execution_in_single_message(self):
        """Verify detection of parallel Bash calls in one message."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git status",
                                        "description": "Check git status"
                                    },
                                    {
                                        "tool_name": "Bash",
                                        "command": "git diff",
                                        "description": "Check git diff"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_bash_calls"] == 2
        assert result["parallel_bash_opportunities_taken"] == 1

    def test_missed_parallel_bash_opportunity(self):
        """Verify detection of sequential independent Bash calls."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "ls src/",
                                        "description": "List src"
                                    }
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "ls tests/",
                                        "description": "List tests"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["parallel_bash_opportunities"] == 1
        assert result["example_missed_parallel"]["prev_command"] == "ls src/"
        assert result["example_missed_parallel"]["current_command"] == "ls tests/"

    def test_dependent_commands_not_flagged_as_parallel_opportunity(self):
        """Verify commands with && chaining not flagged as parallel opportunity."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "cd src && ls",
                                        "description": "Change to src and list"
                                    }
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "pwd",
                                        "description": "Print working directory"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        # Commands with && are complex, not flagged
        assert result["parallel_bash_opportunities"] == 0

    def test_path_quoting_properly_quoted_path(self):
        """Verify detection of properly quoted paths."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": 'cd "/Users/name/My Documents"',
                                        "description": "Change directory"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_paths_in_commands"] == 1
        assert result["properly_quoted_paths"] == 1
        assert result["path_quoting_compliance"] == 100.0

    def test_path_quoting_unquoted_path_with_spaces(self):
        """Verify detection of unquoted paths with spaces."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "cd /Users/name/My Documents",
                                        "description": "Change directory"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["unquoted_space_paths"] >= 1
        assert result["example_quoting_violation"]["command"] == "cd /Users/name/My Documents"

    def test_heredoc_usage_for_git_commit(self):
        """Verify detection of heredoc usage in git commit."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git commit -m \"$(cat <<'EOF'\\nFix bug in module\\n\\nCo-Authored-By: Claude\\nEOF\\n)\"",
                                        "description": "Commit with multi-line message"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["heredoc_usage_count"] == 1
        assert result["multi_line_opportunities"] == 1
        assert result["heredoc_usage_rate"] == 100.0

    def test_multi_line_opportunity_without_heredoc(self):
        """Verify detection of git commit without heredoc."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": 'git commit -m "Fix bug"',
                                        "description": "Commit changes"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["multi_line_opportunities"] == 1
        assert result["heredoc_usage_count"] == 0
        assert result["heredoc_usage_rate"] == 0.0

    def test_description_quality_concise(self):
        """Verify tracking of concise descriptions."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "ls src/",
                                        "description": "List source files"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_descriptions"] == 1
        assert result["avg_description_length"] == 3.0  # "List source files"
        assert result["overly_verbose_descriptions"] == 0

    def test_description_quality_overly_verbose(self):
        """Verify detection of overly verbose descriptions."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "ls",
                                        "description": "This command will execute the ls utility to list all files and directories in the current working directory, providing a comprehensive view of the file system structure"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["overly_verbose_descriptions"] == 1

    def test_missing_description(self):
        """Verify detection of missing command descriptions."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "ls src/"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["missing_descriptions"] == 1

    def test_tool_preference_violation_grep(self):
        """Verify detection of grep usage (should use Grep tool)."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "grep -r 'TODO' src/",
                                        "description": "Search for TODOs"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["tool_preference_violations"] == 1
        assert result["tool_violation_rate"] == 100.0
        assert result["example_tool_violation"]["violation_type"] == "should_use_grep_tool"

    def test_tool_preference_violation_cat(self):
        """Verify detection of cat usage (should use Read tool)."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "cat README.md",
                                        "description": "Read README"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["tool_preference_violations"] == 1
        assert result["example_tool_violation"]["violation_type"] == "should_use_read_tool"

    def test_tool_preference_violation_find(self):
        """Verify detection of find usage (should use Glob tool)."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "find . -name '*.py'",
                                        "description": "Find Python files"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["tool_preference_violations"] == 1
        assert result["example_tool_violation"]["violation_type"] == "should_use_glob_tool"

    def test_tool_preference_no_violation_for_valid_commands(self):
        """Verify valid commands not flagged as violations."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git status",
                                        "description": "Check git status"
                                    },
                                    {
                                        "tool_name": "Bash",
                                        "command": "pytest tests/",
                                        "description": "Run tests"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["tool_preference_violations"] == 0
        assert result["tool_violation_rate"] == 0.0

    def test_example_good_composition_captured(self):
        """Verify good composition example is captured."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git add . && git commit -m 'fix'",
                                        "description": "Stage and commit"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["example_good_composition"]["command"] == "git add . && git commit -m 'fix'"
        assert result["example_good_composition"]["description"] == "Stage and commit"

    def test_multiple_packs_aggregation(self):
        """Verify metrics are aggregated across multiple packs."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "ls src/",
                                        "description": "List src"
                                    }
                                ]
                            }
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
                                    {
                                        "tool_name": "Bash",
                                        "command": "ls tests/",
                                        "description": "List tests"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 2
        assert result["total_bash_calls"] == 2

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_pack_bash_command_composition([
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
                                    {
                                        "tool_name": "Bash",
                                        "command": "ls",
                                        "description": "List files"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_packs"] == 1
        assert result["total_bash_calls"] == 1

    def test_realistic_optimized_bash_pattern(self):
        """Verify realistic optimized Bash usage pattern."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git status",
                                        "description": "Check git status"
                                    },
                                    {
                                        "tool_name": "Bash",
                                        "command": "git diff",
                                        "description": "Check git diff"
                                    }
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git add src/main.py && git commit -m \"$(cat <<'EOF'\nFix bug\n\nCo-Authored-By: Claude\nEOF\n)\"",
                                        "description": "Stage and commit changes"
                                    }
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "pytest tests/",
                                        "description": "Run tests"
                                    },
                                    {
                                        "tool_name": "Bash",
                                        "command": "mypy src/",
                                        "description": "Run type checking"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_bash_calls"] == 5
        assert result["parallel_bash_opportunities_taken"] == 2  # Two pairs of parallel calls
        assert result["sequential_chaining_and_usage"] > 0  # git add && git commit
        assert result["heredoc_usage_count"] == 1
        assert result["tool_preference_violations"] == 0

    def test_realistic_unoptimized_bash_pattern(self):
        """Verify realistic unoptimized Bash usage pattern."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git status"
                                        # Missing description
                                    }
                                ]
                            },
                            {
                                "message_index": 1,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git diff"
                                        # Missing description
                                    }
                                ]
                            },
                            {
                                "message_index": 2,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "grep -r TODO src/",
                                        "description": "Find TODOs"  # Should use Grep tool
                                    }
                                ]
                            },
                            {
                                "message_index": 3,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "cat README.md",
                                        "description": "Read file"  # Should use Read tool
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        assert result["total_bash_calls"] == 4
        assert result["parallel_bash_opportunities"] >= 1  # git status and git diff could be parallel
        assert result["missing_descriptions"] == 2
        assert result["tool_preference_violations"] == 2  # grep and cat

    def test_cat_in_heredoc_not_violation(self):
        """Verify cat in heredoc context is not flagged as violation."""
        result = analyze_pack_bash_command_composition([
            {
                "pack_id": "pack1",
                "sessions": [
                    {
                        "session_id": "session1",
                        "messages": [
                            {
                                "message_index": 0,
                                "tool_calls": [
                                    {
                                        "tool_name": "Bash",
                                        "command": "git commit -m \"$(cat <<'EOF'\nMessage\nEOF\n)\"",
                                        "description": "Commit with heredoc"
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }
        ])

        # cat << is for heredoc, not a violation
        assert result["tool_preference_violations"] == 0
