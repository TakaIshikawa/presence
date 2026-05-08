"""Tests for branch verification coverage analyzer."""

import pytest

from synthesis.branch_verification_coverage import (
    BranchVerificationCoverage,
    SessionSummary,
    analyze_branch_verification_coverage,
)


def test_empty_sessions_returns_zero_state():
    result = analyze_branch_verification_coverage([])

    assert result.metrics.total_sessions == 0
    assert result.metrics.sessions_with_verification == 0
    assert result.metrics.total_files_changed == 0
    assert result.metrics.session_verification_rate == 0.0
    assert "No sessions" in result.insights[0]


def test_single_session_with_verification():
    sessions = [
        SessionSummary(
            session_id="session-1",
            verification_commands=["pytest tests/"],
            files_changed=["src/main.py", "tests/test_main.py"],
        )
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert result.metrics.total_sessions == 1
    assert result.metrics.sessions_with_verification == 1
    assert result.metrics.total_files_changed == 2
    assert result.metrics.files_with_verification == 2
    assert result.metrics.session_verification_rate == 100.0
    assert result.metrics.file_verification_rate == 100.0


def test_single_session_without_verification():
    sessions = [
        SessionSummary(
            session_id="session-1",
            verification_commands=[],
            files_changed=["src/main.py"],
        )
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert result.metrics.total_sessions == 1
    assert result.metrics.sessions_with_verification == 0
    assert result.metrics.sessions_without_verification == 1
    assert result.metrics.total_files_changed == 1
    assert result.metrics.files_without_verification == 1
    assert result.metrics.session_verification_rate == 0.0
    assert len(result.examples) == 1
    assert result.examples[0].missing_verification is True


def test_multiple_sessions_mixed_verification():
    sessions = [
        SessionSummary("s1", ["pytest"], ["file1.py"]),
        SessionSummary("s2", [], ["file2.py"]),
        SessionSummary("s3", ["mypy src/"], ["file3.py"]),
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert result.metrics.total_sessions == 3
    assert result.metrics.sessions_with_verification == 2
    assert result.metrics.sessions_without_verification == 1
    assert result.metrics.session_verification_rate == pytest.approx(66.67, rel=0.01)


def test_files_changed_deduplicated_across_sessions():
    sessions = [
        SessionSummary("s1", ["pytest"], ["main.py", "util.py"]),
        SessionSummary("s2", ["mypy"], ["main.py"]),  # main.py appears again
    ]

    result = analyze_branch_verification_coverage(sessions)

    # main.py and util.py = 2 unique files
    assert result.metrics.total_files_changed == 2
    assert result.metrics.files_with_verification == 2


def test_file_verification_rate_calculation():
    sessions = [
        SessionSummary("s1", ["pytest"], ["file1.py", "file2.py"]),
        SessionSummary("s2", [], ["file3.py", "file4.py"]),
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert result.metrics.total_files_changed == 4
    assert result.metrics.files_with_verification == 2
    assert result.metrics.files_without_verification == 2
    assert result.metrics.file_verification_rate == 50.0


def test_verification_command_categorization():
    sessions = [
        SessionSummary("s1", ["pytest tests/", "mypy src/", "eslint .", "npm run build"], []),
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert "test" in result.verification_commands_by_type
    assert "typecheck" in result.verification_commands_by_type
    assert "lint" in result.verification_commands_by_type
    assert "build" in result.verification_commands_by_type


def test_command_diversity_score():
    # All 4 types present = 1.0
    sessions_full = [
        SessionSummary("s1", ["pytest", "mypy", "eslint", "npm run build"], []),
    ]
    result_full = analyze_branch_verification_coverage(sessions_full)
    assert result_full.metrics.command_diversity_score == 1.0

    # 2 types present = 0.5
    sessions_partial = [
        SessionSummary("s1", ["pytest", "mypy"], []),
    ]
    result_partial = analyze_branch_verification_coverage(sessions_partial)
    assert result_partial.metrics.command_diversity_score == 0.5


def test_verification_to_change_ratio():
    sessions = [
        SessionSummary("s1", ["pytest"], ["f1.py", "f2.py", "f3.py"]),
        SessionSummary("s2", ["mypy"], ["f4.py", "f5.py"]),
    ]

    result = analyze_branch_verification_coverage(sessions)

    # 2 sessions with verification / 5 files changed = 0.4
    assert result.metrics.verification_to_change_ratio == 0.4


def test_examples_capped_at_five():
    sessions = [
        SessionSummary(f"s{i}", [], [f"file{i}.py"])
        for i in range(10)
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert len(result.examples) <= 5


def test_examples_truncate_long_file_lists():
    sessions = [
        SessionSummary("s1", [], [f"file{i}.py" for i in range(20)]),
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert len(result.examples) == 1
    assert len(result.examples[0].files_changed) <= 5


def test_missing_command_types_insight():
    sessions = [
        SessionSummary("s1", ["pytest"], ["file.py"]),  # Only test, missing others
    ]

    result = analyze_branch_verification_coverage(sessions)

    missing_insight = [i for i in result.insights if "missing verification types" in i.lower()]
    assert len(missing_insight) > 0


def test_low_command_diversity_insight():
    sessions = [
        SessionSummary("s1", ["pytest"], ["file.py"]),  # Only 1 type
    ]

    result = analyze_branch_verification_coverage(sessions)

    diversity_insight = [i for i in result.insights if "diversity" in i.lower()]
    assert len(diversity_insight) > 0


def test_low_session_coverage_warning():
    sessions = [
        SessionSummary("s1", ["pytest"], ["f1.py"]),
        SessionSummary("s2", [], ["f2.py"]),
        SessionSummary("s3", [], ["f3.py"]),
        SessionSummary("s4", [], ["f4.py"]),
    ]

    result = analyze_branch_verification_coverage(sessions)

    # 1/4 = 25% < 50%
    coverage_warning = [i for i in result.insights if "low session coverage" in i.lower()]
    assert len(coverage_warning) > 0


def test_low_file_coverage_warning():
    sessions = [
        SessionSummary("s1", ["pytest"], ["f1.py"]),
        SessionSummary("s2", [], ["f2.py", "f3.py", "f4.py", "f5.py", "f6.py"]),
    ]

    result = analyze_branch_verification_coverage(sessions)

    # 1/6 files < 50%
    file_warning = [i for i in result.insights if "low file coverage" in i.lower()]
    assert len(file_warning) > 0


def test_full_coverage_no_warnings():
    sessions = [
        SessionSummary("s1", ["pytest", "mypy", "eslint", "npm run build"], ["f1.py"]),
        SessionSummary("s2", ["pytest"], ["f2.py"]),
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert result.metrics.session_verification_rate == 100.0
    assert result.metrics.file_verification_rate == 100.0
    assert not any("low" in insight.lower() for insight in result.insights)


def test_other_verification_command_category():
    sessions = [
        SessionSummary("s1", ["some-custom-check"], []),
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert "other" in result.verification_commands_by_type


def test_multiple_verification_commands_per_session():
    sessions = [
        SessionSummary("s1", ["pytest", "pytest --cov", "mypy"], ["file.py"]),
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert result.verification_commands_by_type["test"] == 2  # Two pytest commands
    assert result.verification_commands_by_type["typecheck"] == 1


@pytest.mark.parametrize(
    ("sessions", "message"),
    [
        ("not_a_list", "list or tuple"),
        ([{"session_id": "s1"}], "SessionSummary"),
    ],
)
def test_invalid_sessions_raise_errors(sessions, message):
    with pytest.raises(ValueError, match=message):
        analyze_branch_verification_coverage(sessions)


def test_session_with_empty_files():
    sessions = [
        SessionSummary("s1", ["pytest"], []),
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert result.metrics.total_sessions == 1
    assert result.metrics.sessions_with_verification == 1
    assert result.metrics.total_files_changed == 0


def test_verification_patterns_case_insensitive():
    sessions = [
        SessionSummary("s1", ["PYTEST tests/", "Mypy src/"], []),
    ]

    result = analyze_branch_verification_coverage(sessions)

    assert "test" in result.verification_commands_by_type
    assert "typecheck" in result.verification_commands_by_type
