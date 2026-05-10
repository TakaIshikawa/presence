"""Tests for pack tool call sequencing and dependency violation analyzer."""

import pytest

from synthesis.pack_tool_call_sequencing import PackToolCallSequencingAnalyzer


@pytest.fixture
def analyzer():
    return PackToolCallSequencingAnalyzer()


# --- Input validation ---


def test_none_input_returns_clean(analyzer):
    result = analyzer.analyze(None)
    assert result["violation_count"] == 0
    assert result["sequencing_score"] == 1.0


def test_empty_list_returns_clean(analyzer):
    result = analyzer.analyze([])
    assert result["violation_count"] == 0


def test_non_list_input_raises(analyzer):
    with pytest.raises(ValueError, match="records must be a list"):
        analyzer.analyze({"sessions": []})


# --- Clean session ---


def test_proper_read_then_edit_no_violations(analyzer):
    """Read → Edit sequence has no violations, score 1.0."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {"tool_name": "Read", "file_path": "/src/foo.py"},
                            ]
                        },
                        {
                            "tool_calls": [
                                {"tool_name": "Edit", "file_path": "/src/foo.py"},
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["violation_count"] == 0
    assert result["sequencing_score"] == 1.0


# --- Edit without Read ---


def test_edit_without_read_violation(analyzer):
    """Edit without prior Read → violation detected."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {"tool_name": "Edit", "file_path": "/src/foo.py"},
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["violation_count"] == 1
    assert result["violations"][0]["tool"] == "Edit"
    assert result["violations"][0]["file"] == "/src/foo.py"
    assert "Read" in result["violations"][0]["rule_violated"]


# --- Parallelization opportunities ---


def test_sequential_independent_reads_flagged(analyzer):
    """Two sequential Read calls on different files → parallelization opportunity."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {"tool_name": "Read", "file_path": "/src/a.py"},
                            ]
                        },
                        {
                            "tool_calls": [
                                {"tool_name": "Read", "file_path": "/src/b.py"},
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["parallelization_opportunities"] >= 1


# --- Proper parallel calls ---


def test_parallel_independent_calls_no_violations(analyzer):
    """Multiple independent calls in one message → no violations."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {"tool_name": "Read", "file_path": "/src/a.py"},
                                {"tool_name": "Read", "file_path": "/src/b.py"},
                            ]
                        },
                        {
                            "tool_calls": [
                                {"tool_name": "Edit", "file_path": "/src/a.py"},
                                {"tool_name": "Edit", "file_path": "/src/b.py"},
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["violation_count"] == 0
    assert result["sequencing_score"] == 1.0


# --- Multiple violations ---


def test_multiple_violations_all_captured(analyzer):
    """Multiple violations in one session → all captured."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {"tool_name": "Edit", "file_path": "/a.py"},
                            ]
                        },
                        {
                            "tool_calls": [
                                {"tool_name": "Write", "file_path": "/b.py"},
                            ]
                        },
                        {
                            "tool_calls": [
                                {"tool_name": "Bash", "command": "git commit -m 'x'"},
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["violation_count"] == 3
    tools_violated = [v["tool"] for v in result["violations"]]
    assert "Edit" in tools_violated
    assert "Write" in tools_violated
    assert "Bash" in tools_violated
    assert result["sequencing_score"] < 1.0
