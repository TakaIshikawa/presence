"""Tests for pack file read efficiency and cache utilization analyzer."""

import pytest

from synthesis.pack_file_read_efficiency import PackFileReadEfficiencyAnalyzer


@pytest.fixture
def analyzer():
    return PackFileReadEfficiencyAnalyzer()


# --- Input validation ---


def test_none_input_returns_neutral(analyzer):
    result = analyzer.analyze(None)
    assert result["efficiency_score"] == 1.0
    assert result["targeted_read_rate"] == 0.0
    assert result["reread_count"] == 0
    assert result["recommendations"] == []


def test_empty_list_returns_neutral(analyzer):
    result = analyzer.analyze([])
    assert result["efficiency_score"] == 1.0
    assert result["avg_lines_per_read"] == 0.0


def test_non_list_input_raises(analyzer):
    with pytest.raises(ValueError, match="records must be a list"):
        analyzer.analyze({"sessions": []})


def test_non_mapping_records_are_skipped(analyzer):
    result = analyzer.analyze(["not_a_dict", 42, None])
    assert result["efficiency_score"] == 1.0


# --- Highly efficient session ---


def test_highly_efficient_session_scores_above_09(analyzer):
    """90% targeted reads, avg 50 lines → score > 0.9."""
    tool_calls = []
    # 9 targeted reads (with offset/limit)
    for i in range(9):
        tool_calls.append({
            "tool_name": "Read",
            "file_path": f"/src/file_{i}.py",
            "offset": 10,
            "limit": 50,
        })
    # 1 full read (no offset/limit)
    tool_calls.append({
        "tool_name": "Read",
        "file_path": "/src/file_full.py",
    })

    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {"tool_calls": tool_calls},
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["targeted_read_rate"] == 0.9
    assert result["avg_lines_per_read"] < 250
    assert result["reread_count"] == 0
    assert result["efficiency_score"] > 0.8


# --- Wasteful session ---


def test_wasteful_session_scores_below_04(analyzer):
    """0% targeted reads, avg 2000 lines (full file), many re-reads → score < 0.4."""
    tool_calls = []
    # 10 full reads of the same 3 files (heavy re-reads)
    files = ["/src/big_a.py", "/src/big_b.py", "/src/big_c.py"]
    for i in range(10):
        tool_calls.append({
            "tool_name": "Read",
            "file_path": files[i % 3],
        })

    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {"tool_calls": tool_calls},
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["targeted_read_rate"] == 0.0
    assert result["avg_lines_per_read"] == 2000.0
    assert result["reread_count"] == 7  # 10 reads - 3 unique files
    assert result["efficiency_score"] < 0.4


# --- Mixed session ---


def test_mixed_session_mid_range_score(analyzer):
    """Some targeted, some full reads → mid-range score."""
    tool_calls = []
    # 5 targeted reads
    for i in range(5):
        tool_calls.append({
            "tool_name": "Read",
            "file_path": f"/src/targeted_{i}.py",
            "offset": 100,
            "limit": 80,
        })
    # 5 full reads (different files)
    for i in range(5):
        tool_calls.append({
            "tool_name": "Read",
            "file_path": f"/src/full_{i}.py",
        })

    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {"tool_calls": tool_calls},
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["targeted_read_rate"] == 0.5
    assert 0.4 <= result["efficiency_score"] <= 0.8
    assert result["reread_count"] == 0


# --- No reads session ---


def test_session_with_no_reads_returns_neutral(analyzer):
    """Session with no Read tool calls → neutral score."""
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
                                {"tool_name": "Bash", "command": "pytest"},
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["efficiency_score"] == 1.0
    assert result["targeted_read_rate"] == 0.0
    assert result["avg_lines_per_read"] == 0.0
    assert result["reread_count"] == 0
    assert result["recommendations"] == []


# --- Recommendations ---


def test_recommendations_populated_for_inefficient_session(analyzer):
    """Inefficient session should produce actionable recommendations."""
    tool_calls = []
    # All full reads, lots of re-reads
    for _ in range(8):
        tool_calls.append({
            "tool_name": "Read",
            "file_path": "/src/same_file.py",
        })

    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {"tool_calls": tool_calls},
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert len(result["recommendations"]) > 0
    # Should recommend offset/limit usage
    assert any("offset/limit" in r for r in result["recommendations"])
    # Should recommend caching
    assert any("cache" in r.lower() or "re-read" in r.lower() for r in result["recommendations"])


# --- Edge cases ---


def test_only_limit_counts_as_targeted(analyzer):
    """Read with only limit (no offset) still counts as targeted."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {
                                    "tool_name": "Read",
                                    "file_path": "/src/x.py",
                                    "limit": 30,
                                }
                            ]
                        }
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["targeted_read_rate"] == 1.0


def test_multiple_sessions_aggregated(analyzer):
    """Reads across multiple sessions and packs are aggregated."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {"tool_name": "Read", "file_path": "/a.py", "offset": 1, "limit": 20},
                            ]
                        }
                    ],
                }
            ],
        },
        {
            "pack_id": "p2",
            "sessions": [
                {
                    "session_id": "s2",
                    "messages": [
                        {
                            "tool_calls": [
                                {"tool_name": "Read", "file_path": "/b.py"},
                            ]
                        }
                    ],
                }
            ],
        },
    ]

    result = analyzer.analyze(records)

    assert result["targeted_read_rate"] == 0.5
    assert result["reread_count"] == 0
