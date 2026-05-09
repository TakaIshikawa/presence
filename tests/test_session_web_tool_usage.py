"""Tests for session web tool usage analyzer."""

import pytest

from synthesis.session_web_tool_usage import analyze_session_web_tool_usage


class TestAnalyzeSessionWebToolUsage:
    """Test main analyzer function."""

    def test_empty_session_returns_zeroed_metrics(self):
        """Verify empty session returns zero metrics."""
        result = analyze_session_web_tool_usage([])

        assert result["total_turns"] == 0
        assert result["total_tool_calls"] == 0
        assert result["web_fetch_count"] == 0
        assert result["web_search_count"] == 0
        assert result["total_web_calls"] == 0
        assert result["web_tool_ratio"] == 0.0
        assert result["web_calls_with_citations"] == 0
        assert result["sources_cited_ratio"] == 0.0
        assert result["redirect_handling_count"] == 0
        assert result["redirect_handling_ratio"] == 0.0
        assert result["authentication_error_count"] == 0
        assert result["authentication_error_ratio"] == 0.0
        assert result["web_research_effectiveness_score"] == 0.0

    def test_none_input_treated_as_empty_list(self):
        """Verify None input is treated as empty list."""
        result = analyze_session_web_tool_usage(None)
        assert result["total_turns"] == 0

    def test_invalid_input_type_raises_error(self):
        """Verify non-list input raises ValueError."""
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_web_tool_usage("not a list")

    def test_session_with_no_web_calls(self):
        """Verify session with only non-web tools."""
        result = analyze_session_web_tool_usage([
            {"turn_index": 0, "tool_name": "Read", "total_tool_calls": 1},
            {"turn_index": 1, "tool_name": "Edit", "total_tool_calls": 1},
            {"turn_index": 2, "tool_name": "Bash", "total_tool_calls": 1},
        ])

        assert result["total_turns"] == 3
        assert result["total_tool_calls"] == 3
        assert result["web_fetch_count"] == 0
        assert result["web_search_count"] == 0
        assert result["total_web_calls"] == 0
        assert result["web_tool_ratio"] == 0.0

    def test_single_webfetch_call(self):
        """Verify single WebFetch call tracking."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_params": {"url": "https://example.com"},
                "assistant_response": "The page shows...",
            }
        ])

        assert result["web_fetch_count"] == 1
        assert result["web_search_count"] == 0
        assert result["total_web_calls"] == 1
        assert result["total_tool_calls"] == 1
        assert result["web_tool_ratio"] == 100.0

    def test_single_websearch_call(self):
        """Verify single WebSearch call tracking."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebSearch",
                "total_tool_calls": 1,
                "tool_params": {"query": "test query"},
                "assistant_response": "Search results show...",
            }
        ])

        assert result["web_fetch_count"] == 0
        assert result["web_search_count"] == 1
        assert result["total_web_calls"] == 1
        assert result["web_tool_ratio"] == 100.0

    def test_mixed_web_and_non_web_tools(self):
        """Verify mixed web and non-web tool usage."""
        result = analyze_session_web_tool_usage([
            {"turn_index": 0, "tool_name": "Read", "total_tool_calls": 1},
            {"turn_index": 1, "tool_name": "WebFetch", "total_tool_calls": 1},
            {"turn_index": 2, "tool_name": "Edit", "total_tool_calls": 1},
            {"turn_index": 3, "tool_name": "WebSearch", "total_tool_calls": 1},
            {"turn_index": 4, "tool_name": "Write", "total_tool_calls": 1},
        ])

        assert result["total_tool_calls"] == 5
        assert result["web_fetch_count"] == 1
        assert result["web_search_count"] == 1
        assert result["total_web_calls"] == 2
        # 2/5 = 40%
        assert result["web_tool_ratio"] == 40.0

    def test_citation_tracking_with_sources_section(self):
        """Verify citation tracking detects Sources section."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebSearch",
                "total_tool_calls": 1,
                "assistant_response": """
Based on the search results, here's what I found.

Sources:
- [Example Article](https://example.com/article)
- [Another Source](https://example.com/source)
                """,
            }
        ])

        assert result["web_calls_with_citations"] == 1
        assert result["sources_cited_ratio"] == 100.0

    def test_citation_tracking_without_sources_section(self):
        """Verify citation tracking detects missing Sources section."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "assistant_response": "I found some information about this topic.",
            }
        ])

        assert result["web_calls_with_citations"] == 0
        assert result["sources_cited_ratio"] == 0.0

    def test_citation_in_same_turn(self):
        """Verify citation tracking checks same turn response."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebSearch",
                "total_tool_calls": 1,
                "assistant_response": """
Searching for information...

Sources:
- [Source 1](https://example.com)
                """,
            },
            {
                "turn_index": 1,
                "tool_name": "Read",
                "total_tool_calls": 1,
                "assistant_response": "Reading files...",
            }
        ])

        assert result["web_calls_with_citations"] == 1
        assert result["sources_cited_ratio"] == 100.0

    def test_multiple_web_calls_partial_citations(self):
        """Verify partial citation tracking across multiple web calls."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "assistant_response": "Sources:\n- [Link](https://example.com)",
            },
            {
                "turn_index": 1,
                "tool_name": "WebSearch",
                "total_tool_calls": 1,
                "assistant_response": "No citation here",
            },
            {
                "turn_index": 2,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "assistant_response": "## Sources\n- [Article](https://test.com)",
            },
        ])

        assert result["total_web_calls"] == 3
        assert result["web_calls_with_citations"] == 2
        # 2/3 = 66.67%
        assert result["sources_cited_ratio"] == 66.67

    def test_redirect_handling_detected(self):
        """Verify redirect detection and handling tracking."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "The URL redirected to a different host: https://new.example.com",
            },
            {
                "turn_index": 1,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_params": {"url": "https://new.example.com"},
            },
        ])

        assert result["redirect_handling_count"] == 1
        assert result["redirect_handling_ratio"] == 100.0

    def test_redirect_not_handled(self):
        """Verify redirect without follow-up is not counted as handled."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Redirect to different host detected",
            },
            {
                "turn_index": 1,
                "tool_name": "Read",  # Not a WebFetch follow-up
                "total_tool_calls": 1,
            },
        ])

        assert result["redirect_handling_count"] == 0
        assert result["redirect_handling_ratio"] == 0.0

    def test_multiple_redirects_partial_handling(self):
        """Verify partial redirect handling tracking."""
        result = analyze_session_web_tool_usage([
            # First redirect - handled
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Redirected to different host",
            },
            {
                "turn_index": 1,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
            },
            # Second redirect - not handled
            {
                "turn_index": 2,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Redirect URL: https://other.com",
            },
            {
                "turn_index": 3,
                "tool_name": "Edit",
                "total_tool_calls": 1,
            },
        ])

        assert result["redirect_handling_count"] == 1
        # 1/2 = 50%
        assert result["redirect_handling_ratio"] == 50.0

    def test_authentication_error_detection(self):
        """Verify authentication error detection."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Error: This is an authenticated Google Docs URL",
            }
        ])

        assert result["authentication_error_count"] == 1
        assert result["authentication_error_ratio"] == 100.0

    def test_multiple_authentication_errors(self):
        """Verify multiple authentication error tracking."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Authentication required for Confluence",
            },
            {
                "turn_index": 1,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Access denied - login required",
            },
            {
                "turn_index": 2,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Success: Page fetched",
            },
        ])

        assert result["total_web_calls"] == 3
        assert result["authentication_error_count"] == 2
        # 2/3 = 66.67%
        assert result["authentication_error_ratio"] == 66.67

    def test_case_insensitive_tool_matching(self):
        """Verify tool name matching is case-insensitive."""
        result = analyze_session_web_tool_usage([
            {"turn_index": 0, "tool_name": "WEBFETCH", "total_tool_calls": 1},
            {"turn_index": 1, "tool_name": "websearch", "total_tool_calls": 1},
            {"turn_index": 2, "tool_name": "WebFetch", "total_tool_calls": 1},
        ])

        assert result["web_fetch_count"] == 2
        assert result["web_search_count"] == 1
        assert result["total_web_calls"] == 3

    def test_malformed_record_skipped(self):
        """Verify non-dict records are skipped."""
        result = analyze_session_web_tool_usage([
            "not a dict",
            {"turn_index": 0, "tool_name": "WebFetch", "total_tool_calls": 1},
        ])

        assert result["total_turns"] == 1
        assert result["web_fetch_count"] == 1

    def test_record_without_tool_name(self):
        """Verify records without tool_name are handled."""
        result = analyze_session_web_tool_usage([
            {"turn_index": 0},
            {"turn_index": 1, "tool_name": "WebSearch", "total_tool_calls": 1},
        ])

        assert result["total_turns"] == 2
        assert result["web_search_count"] == 1

    def test_whitespace_handling_in_tool_names(self):
        """Verify whitespace in tool names is stripped."""
        result = analyze_session_web_tool_usage([
            {"turn_index": 0, "tool_name": "  WebFetch  ", "total_tool_calls": 1}
        ])

        assert result["web_fetch_count"] == 1

    def test_optimal_pattern_high_effectiveness(self):
        """Verify optimal web research pattern scores highly."""
        result = analyze_session_web_tool_usage([
            # Good ratio: 2 web calls out of 20 total (10%)
            *[{"turn_index": i, "tool_name": "Read", "total_tool_calls": 1} for i in range(18)],
            {
                "turn_index": 18,
                "tool_name": "WebSearch",
                "total_tool_calls": 1,
                "assistant_response": "Results found.\n\nSources:\n- [Link](https://example.com)",
            },
            {
                "turn_index": 19,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "assistant_response": "Page content.\n\n## Sources\n- [Article](https://test.com)",
            },
        ])

        assert result["total_tool_calls"] == 20
        assert result["total_web_calls"] == 2
        assert result["web_tool_ratio"] == 10.0
        assert result["sources_cited_ratio"] == 100.0
        assert result["authentication_error_ratio"] == 0.0
        # High effectiveness due to citations and appropriate usage
        assert result["web_research_effectiveness_score"] > 0.6

    def test_anti_pattern_missing_citations(self):
        """Verify anti-pattern of web calls without citations."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebSearch",
                "total_tool_calls": 1,
                "assistant_response": "Found some info but no citation",
            },
            {
                "turn_index": 1,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "assistant_response": "Got the page content",
            },
        ])

        assert result["sources_cited_ratio"] == 0.0
        # Low effectiveness due to missing citations
        assert result["web_research_effectiveness_score"] < 0.3

    def test_anti_pattern_excessive_auth_errors(self):
        """Verify anti-pattern of excessive authentication errors."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Google Docs authentication required",
            },
            {
                "turn_index": 1,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Jira login required",
            },
        ])

        assert result["authentication_error_ratio"] == 100.0
        # Low effectiveness due to auth errors
        assert result["web_research_effectiveness_score"] < 0.5

    def test_anti_pattern_excessive_web_usage(self):
        """Verify anti-pattern of excessive web tool usage."""
        # 80% web calls (16 out of 20)
        records = [
            {"turn_index": i, "tool_name": "WebFetch", "total_tool_calls": 1}
            for i in range(16)
        ]
        records.extend([
            {"turn_index": i + 16, "tool_name": "Read", "total_tool_calls": 1}
            for i in range(4)
        ])

        result = analyze_session_web_tool_usage(records)

        assert result["web_tool_ratio"] == 80.0
        # Penalized for excessive web usage
        assert result["web_research_effectiveness_score"] < 0.6

    def test_effectiveness_score_components(self):
        """Verify effectiveness score calculation components."""
        result = analyze_session_web_tool_usage([
            # 10 total tools, 1 web call = 10% (optimal)
            *[{"turn_index": i, "tool_name": "Edit", "total_tool_calls": 1} for i in range(9)],
            {
                "turn_index": 9,
                "tool_name": "WebSearch",
                "total_tool_calls": 1,
                "assistant_response": "Great results.\n\nSources:\n- [Example](https://example.com)",
            },
        ])

        assert result["web_tool_ratio"] == 10.0  # Optimal range (5-20%)
        assert result["sources_cited_ratio"] == 100.0  # Perfect citations
        assert result["authentication_error_ratio"] == 0.0  # No errors
        # High overall effectiveness
        assert result["web_research_effectiveness_score"] > 0.5

    def test_tool_count_from_total_tool_calls_field(self):
        """Verify tool counting uses total_tool_calls when available."""
        result = analyze_session_web_tool_usage([
            {"turn_index": 0, "tool_name": "Read", "total_tool_calls": 3},
            {"turn_index": 1, "tool_name": "WebFetch", "total_tool_calls": 1},
        ])

        assert result["total_tool_calls"] == 4
        assert result["web_tool_ratio"] == 25.0

    def test_tool_count_defaults_to_one_when_missing(self):
        """Verify tool counting defaults to 1 when total_tool_calls is missing."""
        result = analyze_session_web_tool_usage([
            {"turn_index": 0, "tool_name": "Read"},  # No total_tool_calls
            {"turn_index": 1, "tool_name": "WebFetch"},
        ])

        assert result["total_tool_calls"] == 2
        assert result["web_tool_ratio"] == 50.0

    def test_redirect_within_two_turns(self):
        """Verify redirect handling checks next two turns."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "tool_result": "Redirect to different host",
            },
            {"turn_index": 1, "tool_name": "Read"},
            {"turn_index": 2, "tool_name": "WebFetch"},  # Follow-up in turn 2
        ])

        assert result["redirect_handling_count"] == 1
        assert result["redirect_handling_ratio"] == 100.0

    def test_sources_section_variants(self):
        """Verify various Sources section formats are detected."""
        test_cases = [
            "Sources:\n- [Link](https://example.com)",
            "## Sources\n- [Article](https://test.com)",
            "Source: [Page](https://example.org)",
            "**Sources:**\n- [Doc](https://doc.com)",
        ]

        for response in test_cases:
            result = analyze_session_web_tool_usage([
                {
                    "turn_index": 0,
                    "tool_name": "WebSearch",
                    "total_tool_calls": 1,
                    "assistant_response": response,
                }
            ])
            assert result["sources_cited_ratio"] == 100.0, f"Failed for: {response}"

    def test_no_false_positive_citations(self):
        """Verify no false positive citation detection."""
        false_positive_cases = [
            "The sources of this data are unknown",  # No markdown link
            "Check the source code",  # Not a citation
            "Multiple sources indicate this",  # No link
        ]

        for response in false_positive_cases:
            result = analyze_session_web_tool_usage([
                {
                    "turn_index": 0,
                    "tool_name": "WebFetch",
                    "total_tool_calls": 1,
                    "assistant_response": response,
                }
            ])
            assert result["sources_cited_ratio"] == 0.0, f"False positive for: {response}"

    def test_empty_assistant_response(self):
        """Verify empty assistant responses are handled."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebSearch",
                "total_tool_calls": 1,
                "assistant_response": "",
            }
        ])

        assert result["web_calls_with_citations"] == 0
        assert result["sources_cited_ratio"] == 0.0

    def test_missing_assistant_response_field(self):
        """Verify missing assistant_response field is handled."""
        result = analyze_session_web_tool_usage([
            {
                "turn_index": 0,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
            }
        ])

        assert result["web_calls_with_citations"] == 0

    def test_comprehensive_session_scenario(self):
        """Verify comprehensive session with mixed patterns."""
        result = analyze_session_web_tool_usage([
            # Turn 0: Regular read
            {"turn_index": 0, "tool_name": "Read", "total_tool_calls": 1},
            # Turn 1: WebSearch with citation
            {
                "turn_index": 1,
                "tool_name": "WebSearch",
                "total_tool_calls": 1,
                "assistant_response": "Results.\n\nSources:\n- [Link](https://example.com)",
            },
            # Turn 2: Edit
            {"turn_index": 2, "tool_name": "Edit", "total_tool_calls": 1},
            # Turn 3: WebFetch with redirect
            {
                "turn_index": 3,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Redirected to different host",
            },
            # Turn 4: WebFetch follow-up (handles redirect)
            {
                "turn_index": 4,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "assistant_response": "Content found.\n\n## Sources\n- [Page](https://test.com)",
            },
            # Turn 5: WebFetch with auth error
            {
                "turn_index": 5,
                "tool_name": "WebFetch",
                "total_tool_calls": 1,
                "tool_result": "Authentication required for Google Docs",
            },
            # Turn 6-9: More edits
            *[{"turn_index": i, "tool_name": "Write", "total_tool_calls": 1} for i in range(6, 10)],
        ])

        assert result["total_turns"] == 10
        assert result["total_tool_calls"] == 10
        assert result["web_fetch_count"] == 3
        assert result["web_search_count"] == 1
        assert result["total_web_calls"] == 4
        assert result["web_tool_ratio"] == 40.0
        # 2 citations out of 4 web calls
        assert result["sources_cited_ratio"] == 50.0
        # 1 redirect handled
        assert result["redirect_handling_count"] == 1
        assert result["redirect_handling_ratio"] == 100.0
        # 1 auth error
        assert result["authentication_error_count"] == 1
        assert result["authentication_error_ratio"] == 25.0
        # Moderate effectiveness (good redirect handling, partial citations)
        assert 0.3 < result["web_research_effectiveness_score"] <= 0.7
