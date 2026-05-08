"""Tests for final answer tool coverage analyzer."""

import pytest

from synthesis.final_answer_tool_coverage import (
    FinalAnswerToolCoverage,
    SessionToolCall,
    analyze_final_answer_tool_coverage,
)


def test_empty_tool_calls_returns_zero_state():
    result = analyze_final_answer_tool_coverage([], "Some final answer")

    assert result.metrics.total_tools == 0
    assert result.metrics.critical_tools == 0
    assert result.metrics.tools_mentioned == 0
    assert result.metrics.coverage_rate == 0.0
    assert "No tool calls" in result.insights[0]


def test_empty_final_answer_shows_zero_coverage():
    tool_calls = [
        SessionToolCall("read", "File contains data", is_critical=True),
    ]

    result = analyze_final_answer_tool_coverage(tool_calls, "")

    assert result.metrics.total_tools == 1
    assert result.metrics.tools_mentioned == 0
    assert result.metrics.coverage_rate == 0.0
    assert "No final answer" in result.insights[0]


def test_tool_result_mentioned_in_final_answer():
    tool_calls = [
        SessionToolCall(
            "grep",
            "Found 5 error messages in the log file",
            is_critical=True,
        ),
    ]
    final_answer = "I found 5 error messages in the log file that need attention."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.total_tools == 1
    assert result.metrics.tools_mentioned == 1
    assert result.metrics.coverage_rate == 100.0


def test_tool_result_not_mentioned_in_final_answer():
    tool_calls = [
        SessionToolCall(
            "read",
            "Configuration file contains database settings",
            is_critical=True,
        ),
    ]
    final_answer = "I've completed the task successfully."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.total_tools == 1
    assert result.metrics.tools_mentioned == 0
    assert result.metrics.coverage_rate == 0.0
    assert len(result.examples) == 1
    assert result.examples[0].mentioned_in_final_answer is False


def test_critical_vs_non_critical_tools():
    tool_calls = [
        SessionToolCall("read", "File A contents", is_critical=True),
        SessionToolCall("write", "Created file B", is_critical=False),
        SessionToolCall("grep", "Found pattern X", is_critical=True),
    ]
    final_answer = "I read file A and found pattern X in it."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.total_tools == 3
    assert result.metrics.critical_tools == 2
    assert result.metrics.tools_mentioned == 2
    assert result.metrics.critical_tools_mentioned == 2
    assert result.metrics.critical_coverage_rate == 100.0


def test_partial_tool_coverage():
    tool_calls = [
        SessionToolCall("bash", "Tests passed: 25/25", is_critical=True),
        SessionToolCall("read", "README.md content", is_critical=True),
        SessionToolCall("grep", "Found TODO items", is_critical=True),
    ]
    final_answer = "All 25 tests passed successfully. I also found TODO items."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.total_tools == 3
    assert result.metrics.tools_mentioned == 2
    assert result.metrics.coverage_rate == pytest.approx(66.67, rel=0.01)


def test_by_tool_type_metrics():
    tool_calls = [
        SessionToolCall("read", "File 1", is_critical=True),
        SessionToolCall("read", "File 2", is_critical=True),
        SessionToolCall("bash", "Test output", is_critical=True),
    ]
    final_answer = "I read file 1 and ran tests."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert "read" in result.metrics.by_tool_type
    assert "bash" in result.metrics.by_tool_type
    assert result.metrics.by_tool_type["read"]["total"] == 2
    assert result.metrics.by_tool_type["bash"]["total"] == 1


def test_tool_type_mention_with_result_overlap():
    tool_calls = [
        SessionToolCall(
            "grep",
            "Found 3 instances of deprecated function usage",
            is_critical=True,
        ),
    ]
    final_answer = "The grep search found 3 deprecated function instances."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.tools_mentioned == 1


def test_verification_tool_mention():
    tool_calls = [
        SessionToolCall(
            "bash",
            "pytest: All tests passed successfully",
            is_critical=True,
        ),
    ]
    final_answer = "The tests all passed."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.tools_mentioned == 1


def test_word_overlap_detection():
    tool_calls = [
        SessionToolCall(
            "webfetch",
            "Documentation shows three different authentication methods available",
            is_critical=True,
        ),
    ]
    final_answer = "The documentation shows three authentication methods are available."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.tools_mentioned == 1


def test_common_words_filtered_from_overlap():
    tool_calls = [
        SessionToolCall(
            "read",
            "The file is in the directory with the code",
            is_critical=True,
        ),
    ]
    final_answer = "The system is working as expected."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    # Should not match due to only common words overlapping
    assert result.metrics.tools_mentioned == 0


def test_examples_capped_at_five():
    tool_calls = [
        SessionToolCall(f"read", f"Content {i}", is_critical=True)
        for i in range(10)
    ]
    final_answer = "Task completed."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert len(result.examples) <= 5


def test_low_coverage_insight():
    tool_calls = [
        SessionToolCall("read", f"Content {i}", is_critical=True)
        for i in range(5)
    ]
    final_answer = "Done."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert any("low coverage" in insight.lower() for insight in result.insights)


def test_critical_findings_missing_insight():
    tool_calls = [
        SessionToolCall("bash", "Test result 1", is_critical=True),
        SessionToolCall("grep", "Search result 2", is_critical=True),
    ]
    final_answer = "Everything is good."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert any("critical findings missing" in insight.lower() for insight in result.insights)


def test_worst_tool_type_coverage_insight():
    tool_calls = [
        SessionToolCall("read", "File A", is_critical=True),
        SessionToolCall("read", "File B", is_critical=True),
        SessionToolCall("read", "File C", is_critical=True),
        SessionToolCall("bash", "Test passed", is_critical=True),
    ]
    final_answer = "Tests passed successfully."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    # 'read' should be worst with 0% coverage
    assert any("read" in insight.lower() for insight in result.insights)


def test_full_coverage_no_warnings():
    tool_calls = [
        SessionToolCall("read", "Configuration data", is_critical=True),
        SessionToolCall("grep", "Found errors", is_critical=True),
    ]
    final_answer = "I read the configuration data and found errors in the logs."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.coverage_rate == 100.0
    assert not any("low coverage" in insight.lower() for insight in result.insights)


def test_result_summary_truncated_in_examples():
    long_summary = "x" * 200
    tool_calls = [
        SessionToolCall("read", long_summary, is_critical=True),
    ]
    final_answer = "Done."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert len(result.examples) == 1
    assert len(result.examples[0].result_summary) <= 100


@pytest.mark.parametrize(
    ("tool_calls", "message"),
    [
        ("not_a_list", "list or tuple"),
        ([{"tool": "read"}], "SessionToolCall"),
        ([SessionToolCall("", "result", True)], "not be empty"),
    ],
)
def test_invalid_tool_calls_raise_errors(tool_calls, message):
    with pytest.raises(ValueError, match=message):
        analyze_final_answer_tool_coverage(tool_calls, "Final answer")


@pytest.mark.parametrize(
    ("final_answer", "message"),
    [
        (None, "must be a string"),
        (123, "must be a string"),
        ([], "must be a string"),
    ],
)
def test_invalid_final_answer_raises_errors(final_answer, message):
    tool_calls = [SessionToolCall("read", "content", True)]
    with pytest.raises(ValueError, match=message):
        analyze_final_answer_tool_coverage(tool_calls, final_answer)


def test_non_critical_tools_tracked():
    tool_calls = [
        SessionToolCall("write", "Created file.txt", is_critical=False),
        SessionToolCall("edit", "Updated config", is_critical=False),
    ]
    final_answer = "I created file.txt and updated the config."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.total_tools == 2
    assert result.metrics.critical_tools == 0
    assert result.metrics.tools_mentioned == 2


def test_mixed_critical_and_non_critical():
    tool_calls = [
        SessionToolCall("read", "Data from config file loaded", is_critical=True),
        SessionToolCall("write", "Created output file successfully", is_critical=False),
        SessionToolCall("grep", "Found matching pattern in logs", is_critical=True),
    ]
    final_answer = "I created the output file and found matching pattern in logs."

    result = analyze_final_answer_tool_coverage(tool_calls, final_answer)

    assert result.metrics.total_tools == 3
    assert result.metrics.critical_tools == 2
    assert result.metrics.tools_mentioned == 2
    # Only grep is mentioned among critical tools
    assert result.metrics.critical_tools_mentioned == 1
