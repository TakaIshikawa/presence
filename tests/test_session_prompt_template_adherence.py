"""Tests for session prompt template adherence analyzer."""

import pytest

from synthesis.session_prompt_template_adherence import (
    analyze_session_prompt_template_adherence,
)


def _turn(
    turn_index: int = 0,
    tool_name: str = "Bash",
    tool_params: dict | None = None,
    tool_result: str = "",
    assistant_response: str = "",
    is_error: bool = False,
    is_last_turn: bool = False,
) -> dict:
    return {
        "turn_index": turn_index,
        "tool_name": tool_name,
        "tool_params": tool_params or {},
        "tool_result": tool_result,
        "assistant_response": assistant_response,
        "is_error": is_error,
        "is_last_turn": is_last_turn,
    }


class TestInputValidation:
    def test_none_returns_empty(self):
        result = analyze_session_prompt_template_adherence(None)
        assert result["total_template_operations"] == 0
        assert result["template_adherence_score"] == 1.0

    def test_empty_list_returns_empty(self):
        result = analyze_session_prompt_template_adherence([])
        assert result["total_template_operations"] == 0
        assert result["template_adherence_score"] == 1.0

    def test_non_list_raises(self):
        with pytest.raises(ValueError, match="records must be a list"):
            analyze_session_prompt_template_adherence("not a list")


class TestCommitFormatDetection:
    def test_heredoc_commit_detected(self):
        records = [
            _turn(
                tool_params={
                    "command": (
                        'git commit -m "$(cat <<\'EOF\'\n'
                        "fix: resolve auth bug\n\n"
                        "Co-Authored-By: Claude <noreply@anthropic.com>\n"
                        "EOF\n"
                        ')"'
                    )
                },
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["total_commits"] == 1
        assert result["commit_format_adherence"] == 1
        assert result["heredoc_usage_count"] == 1

    def test_inline_commit_detected(self):
        records = [
            _turn(
                tool_params={
                    "command": 'git commit -m "feat: add new feature"'
                },
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["total_commits"] == 1
        assert result["commit_format_adherence"] == 1

    def test_coauthored_by_detected(self):
        records = [
            _turn(
                tool_params={
                    "command": (
                        'git commit -m "some message\n\n'
                        'Co-Authored-By: Claude <noreply@anthropic.com>"'
                    )
                },
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["total_commits"] == 1
        assert result["commit_format_adherence"] == 1


class TestPRTemplateDetection:
    def test_pr_with_summary_and_test_plan(self):
        records = [
            _turn(
                tool_params={
                    "command": (
                        'gh pr create --title "Add feature" --body "$(cat <<\'EOF\'\n'
                        "## Summary\n"
                        "- Added feature X\n\n"
                        "## Test plan\n"
                        "- Run tests\n"
                        "EOF\n"
                        ')"'
                    )
                },
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["total_prs"] == 1
        assert result["pr_template_usage"] == 1

    def test_pr_without_template(self):
        records = [
            _turn(
                tool_params={
                    "command": 'gh pr create --title "quick fix" --body "just a fix"'
                },
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["total_prs"] == 1
        assert result["pr_template_usage"] == 0


class TestFormatInconsistency:
    def test_mixed_commit_formats_flagged(self):
        records = [
            _turn(
                tool_params={
                    "command": (
                        'git commit -m "$(cat <<\'EOF\'\n'
                        "feat: add feature\n"
                        "EOF\n"
                        ')"'
                    )
                },
            ),
            _turn(
                turn_index=1,
                tool_params={
                    "command": 'git commit -m "fix: quick fix"'
                },
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["format_inconsistencies"] >= 1

    def test_consistent_formats_no_flag(self):
        records = [
            _turn(
                tool_params={
                    "command": 'git commit -m "feat: add feature A"'
                },
            ),
            _turn(
                turn_index=1,
                tool_params={
                    "command": 'git commit -m "fix: fix bug B"'
                },
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["format_inconsistencies"] == 0


class TestVersionReferences:
    def test_version_in_params(self):
        records = [
            _turn(
                tool_name="Task",
                tool_params={"prompt": "Use prompt_v3 for generation"},
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["version_references"] >= 1

    def test_version_in_assistant_response(self):
        records = [
            _turn(assistant_response="Switching to v2 of the template"),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["version_references"] >= 1


class TestTemplateVariables:
    def test_template_variables_detected(self):
        records = [
            _turn(
                tool_name="Task",
                tool_params={
                    "prompt": "Generate {title} for {audience}"
                },
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["template_variable_count"] == 2


class TestScoring:
    def test_perfect_adherence(self):
        records = [
            _turn(
                tool_params={
                    "command": (
                        'git commit -m "$(cat <<\'EOF\'\n'
                        "feat: add new feature\n\n"
                        "Co-Authored-By: Claude <noreply@anthropic.com>\n"
                        "EOF\n"
                        ')"'
                    )
                },
            ),
            _turn(
                turn_index=1,
                tool_params={
                    "command": (
                        'gh pr create --title "Add feature" --body "$(cat <<\'EOF\'\n'
                        "## Summary\n"
                        "- Added feature\n\n"
                        "## Test plan\n"
                        "- Tests pass\n"
                        "EOF\n"
                        ')"'
                    )
                },
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["commit_format_adherence"] == 1
        assert result["pr_template_usage"] == 1
        assert result["format_inconsistencies"] == 0
        assert result["template_adherence_score"] >= 0.9

    def test_no_template_operations_perfect_score(self):
        records = [
            _turn(
                tool_name="Read",
                tool_params={"file_path": "/some/file.py"},
            ),
        ]
        result = analyze_session_prompt_template_adherence(records)
        assert result["total_template_operations"] == 0
        # With no commits and no PRs the score formula yields:
        # 0.4*0 + 0.3*0 + 0.3*1.0 = 0.3
        # But empty result returns 1.0; with actual records but no
        # template operations, the formula applies.
        assert result["template_adherence_score"] == 0.3
