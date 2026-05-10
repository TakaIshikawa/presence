"""Tests for pack WebFetch redirect chain and URL validation analyzer."""

import pytest

from synthesis.pack_webfetch_redirect_handling import PackWebFetchRedirectAnalyzer


@pytest.fixture
def analyzer():
    return PackWebFetchRedirectAnalyzer()


# --- Input validation ---


def test_none_input_returns_neutral(analyzer):
    result = analyzer.analyze(None)
    assert result["overall_score"] == 1.0
    assert result["issues"] == []


def test_empty_list_returns_neutral(analyzer):
    result = analyzer.analyze([])
    assert result["overall_score"] == 1.0


def test_non_list_input_raises(analyzer):
    with pytest.raises(ValueError, match="records must be a list"):
        analyzer.analyze({"sessions": []})


# --- Proper redirect following ---


def test_proper_redirect_following_scores_full(analyzer):
    """Session properly follows redirects → redirect_follow_rate 1.0."""
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
                                    "tool_name": "WebFetch",
                                    "url": "https://example.com/page",
                                    "result": "Redirect to a different host detected. URL: https://other.com/page",
                                }
                            ]
                        },
                        {
                            "tool_calls": [
                                {
                                    "tool_name": "WebFetch",
                                    "url": "https://other.com/page",
                                    "result": "Page content here.",
                                }
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["redirect_follow_rate"] == 1.0
    assert result["overall_score"] == 1.0
    assert result["issues"] == []


# --- Missed redirects ---


def test_missed_redirect_lowers_score(analyzer):
    """Session doesn't follow redirect → lower score."""
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
                                    "tool_name": "WebFetch",
                                    "url": "https://example.com/page",
                                    "result": "Redirect to a different host detected. URL: https://other.com/page",
                                }
                            ]
                        },
                        {
                            "tool_calls": [
                                {
                                    "tool_name": "Bash",
                                    "command": "echo done",
                                }
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["redirect_follow_rate"] == 0.0
    assert result["overall_score"] < 1.0
    assert any("redirect" in issue.lower() for issue in result["issues"])


# --- GitHub URL penalty ---


def test_github_url_penalizes_tool_selection(analyzer):
    """Using WebFetch for GitHub URLs penalizes tool_selection_score."""
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
                                    "tool_name": "WebFetch",
                                    "url": "https://github.com/owner/repo/pulls/123",
                                    "result": "PR content",
                                }
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["tool_selection_score"] == 0.0
    assert result["overall_score"] < 1.0
    assert any("github" in issue.lower() for issue in result["issues"])


# --- No WebFetch calls ---


def test_no_webfetch_calls_neutral(analyzer):
    """Session with no WebFetch calls → neutral score."""
    records = [
        {
            "pack_id": "p1",
            "sessions": [
                {
                    "session_id": "s1",
                    "messages": [
                        {
                            "tool_calls": [
                                {"tool_name": "Read", "file_path": "/foo.py"},
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["overall_score"] == 1.0
    assert result["issues"] == []


# --- Multiple redirects in chain ---


def test_multiple_redirects_in_chain(analyzer):
    """Multiple redirect responses all properly followed."""
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
                                    "tool_name": "WebFetch",
                                    "url": "https://a.com/1",
                                    "result": "Redirect to a different host detected. URL: https://b.com/1",
                                }
                            ]
                        },
                        {
                            "tool_calls": [
                                {
                                    "tool_name": "WebFetch",
                                    "url": "https://b.com/1",
                                    "result": "Redirect to a different host detected. URL: https://c.com/1",
                                }
                            ]
                        },
                        {
                            "tool_calls": [
                                {
                                    "tool_name": "WebFetch",
                                    "url": "https://c.com/1",
                                    "result": "Final content",
                                }
                            ]
                        },
                    ],
                }
            ],
        }
    ]

    result = analyzer.analyze(records)

    assert result["redirect_follow_rate"] == 1.0
    assert result["overall_score"] == 1.0
