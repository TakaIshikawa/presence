"""Tests for session Task agent delegation appropriateness analyzer."""

from __future__ import annotations

import pytest

from src.synthesis.session_task_agent_delegation import SessionTaskAgentDelegationAnalyzer


@pytest.fixture
def analyzer():
    return SessionTaskAgentDelegationAnalyzer()


def _make_session(tool_calls: list[dict]) -> dict:
    """Helper to wrap tool calls into session_data format."""
    return {"messages": [{"tool_calls": tool_calls}]}


def _task_call(
    *,
    subagent_type: str = "general-purpose",
    model: str = "",
    prompt: str = "Do something useful",
    description: str = "task description",
    run_in_background: bool = False,
) -> dict:
    """Helper to create a Task tool call."""
    return {
        "tool_name": "Task",
        "subagent_type": subagent_type,
        "model": model,
        "prompt": prompt,
        "description": description,
        "run_in_background": run_in_background,
    }


class TestAnalyzerBasics:
    def test_empty_session(self, analyzer):
        result = analyzer.analyze({})
        assert result["total_task_calls"] == 0
        assert result["agent_selection_correctness"] == 1.0
        assert result["model_efficiency"] == 1.0
        assert result["delegation_appropriateness"] == 1.0

    def test_none_input(self, analyzer):
        result = analyzer.analyze(None)
        assert result["total_task_calls"] == 0

    def test_invalid_input_raises(self, analyzer):
        with pytest.raises(ValueError, match="session_data must be a dict"):
            analyzer.analyze([1, 2, 3])

    def test_no_messages_key(self, analyzer):
        result = analyzer.analyze({"other_key": "value"})
        assert result["total_task_calls"] == 0

    def test_empty_messages(self, analyzer):
        result = analyzer.analyze({"messages": []})
        assert result["total_task_calls"] == 0


class TestAgentSelectionCorrectness:
    def test_explore_for_search_task(self, analyzer):
        session = _make_session([
            _task_call(
                subagent_type="Explore",
                prompt="Search the codebase for authentication handlers",
                description="find auth handlers",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["agent_selection_correctness"] == 1.0

    def test_explore_for_non_search_task(self, analyzer):
        session = _make_session([
            _task_call(
                subagent_type="Explore",
                prompt="Implement a new database migration for users table",
                description="implement migration",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["agent_selection_correctness"] == 0.0

    def test_bash_for_command_task(self, analyzer):
        session = _make_session([
            _task_call(
                subagent_type="Bash",
                prompt="Run the test suite with pytest",
                description="run tests",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["agent_selection_correctness"] == 1.0

    def test_bash_for_non_command_task(self, analyzer):
        session = _make_session([
            _task_call(
                subagent_type="Bash",
                prompt="Analyze the architecture of the authentication module",
                description="analyze architecture",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["agent_selection_correctness"] == 0.0

    def test_plan_for_design_task(self, analyzer):
        session = _make_session([
            _task_call(
                subagent_type="Plan",
                prompt="Design the implementation approach for the new API",
                description="plan api design",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["agent_selection_correctness"] == 1.0

    def test_general_purpose_always_acceptable(self, analyzer):
        session = _make_session([
            _task_call(
                subagent_type="general-purpose",
                prompt="Do something complex that doesn't fit categories",
                description="misc task",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["agent_selection_correctness"] == 1.0

    def test_mixed_correct_and_incorrect(self, analyzer):
        session = _make_session([
            _task_call(
                subagent_type="Explore",
                prompt="Search for all API endpoints in the codebase",
                description="find endpoints",
            ),
            _task_call(
                subagent_type="Explore",
                prompt="Implement the new payment processor",
                description="implement payment",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["agent_selection_correctness"] == 0.5
        assert result["correct_agent_selection_rate"] == 0.5


class TestModelEfficiency:
    def test_opus_for_simple_task_is_inefficient(self, analyzer):
        session = _make_session([
            _task_call(
                model="opus",
                prompt="Quick search for the config file",
                description="simple find",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["model_efficiency"] == 0.0

    def test_haiku_for_simple_task_is_efficient(self, analyzer):
        session = _make_session([
            _task_call(
                model="haiku",
                prompt="Quick search for the config file",
                description="simple find",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["model_efficiency"] == 1.0

    def test_sonnet_default_is_efficient(self, analyzer):
        session = _make_session([
            _task_call(
                model="sonnet",
                prompt="Implement a complex authentication system with OAuth",
                description="implement auth",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["model_efficiency"] == 1.0

    def test_empty_model_is_efficient(self, analyzer):
        session = _make_session([
            _task_call(
                model="",
                prompt="Implement feature X",
                description="implement feature",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["model_efficiency"] == 1.0

    def test_haiku_usage_rate_tracking(self, analyzer):
        session = _make_session([
            _task_call(
                model="haiku",
                prompt="Quick check for files",
                description="simple check",
            ),
            _task_call(
                model="sonnet",
                prompt="Simple search for patterns",
                description="simple search",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["haiku_usage_for_simple_tasks_rate"] == 0.5


class TestDelegationAppropriateness:
    def test_trivial_task_is_over_delegation(self, analyzer):
        session = _make_session([
            _task_call(
                prompt="Read a file at src/main.py",
                description="read file",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["over_delegation_count"] == 1
        assert result["delegation_appropriateness"] < 1.0

    def test_complex_task_is_not_over_delegation(self, analyzer):
        session = _make_session([
            _task_call(
                prompt="Investigate the authentication module, understand the flow, and propose improvements",
                description="investigate auth",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["over_delegation_count"] == 0

    def test_under_delegation_detected(self, analyzer):
        """4+ consecutive Glob/Grep/Read calls without Task is under-delegation."""
        tool_calls = [
            {"tool_name": "Glob", "pattern": "**/*.py"},
            {"tool_name": "Grep", "pattern": "class Foo"},
            {"tool_name": "Read", "file_path": "src/foo.py"},
            {"tool_name": "Read", "file_path": "src/bar.py"},
        ]
        session = _make_session(tool_calls)
        result = analyzer.analyze(session)
        assert result["under_delegation_count"] == 1

    def test_no_under_delegation_with_task_break(self, analyzer):
        """Task call between searches resets the sequence."""
        tool_calls = [
            {"tool_name": "Glob", "pattern": "**/*.py"},
            {"tool_name": "Grep", "pattern": "class Foo"},
            _task_call(prompt="Search the codebase for related patterns", description="search"),
            {"tool_name": "Glob", "pattern": "**/*.ts"},
            {"tool_name": "Read", "file_path": "src/bar.ts"},
        ]
        session = _make_session(tool_calls)
        result = analyzer.analyze(session)
        assert result["under_delegation_count"] == 0

    def test_multiple_over_delegations(self, analyzer):
        session = _make_session([
            _task_call(prompt="Read a file at src/a.py", description="read file"),
            _task_call(prompt="Read a file at src/b.py", description="read file"),
            _task_call(prompt="Look at single file src/c.py", description="look at file"),
        ])
        result = analyzer.analyze(session)
        assert result["over_delegation_count"] == 3
        # Penalty caps at 1.0 so score floors at 0.0
        assert result["delegation_appropriateness"] <= 0.7

    def test_delegation_appropriateness_with_no_issues(self, analyzer):
        session = _make_session([
            _task_call(
                prompt="Investigate the complex authentication module architecture",
                description="investigate auth",
            ),
        ])
        result = analyzer.analyze(session)
        assert result["over_delegation_count"] == 0
        assert result["under_delegation_count"] == 0
        assert result["delegation_appropriateness"] == 1.0


class TestBackgroundUsage:
    def test_background_task_tracked(self, analyzer):
        session = _make_session([
            _task_call(
                prompt="Run the full test suite",
                description="run tests",
                run_in_background=True,
            ),
            _task_call(
                prompt="Build the project",
                description="build",
                run_in_background=False,
            ),
        ])
        result = analyzer.analyze(session)
        assert result["background_task_usage_rate"] == 0.5


class TestTaskInvocationCounts:
    def test_invocations_by_type(self, analyzer):
        session = _make_session([
            _task_call(subagent_type="Explore", prompt="Search codebase", description="search"),
            _task_call(subagent_type="Explore", prompt="Find files", description="find"),
            _task_call(subagent_type="Bash", prompt="Run tests", description="test"),
            _task_call(subagent_type="general-purpose", prompt="Do work", description="work"),
        ])
        result = analyzer.analyze(session)
        assert result["task_invocations"] == {
            "Explore": 2,
            "Bash": 1,
            "general-purpose": 1,
        }
        assert result["total_task_calls"] == 4


class TestPromptQuality:
    def test_short_prompt_low_quality(self, analyzer):
        """Verifies the internal prompt quality scoring works."""
        score = analyzer._score_prompt_quality("hi")
        assert score < 0.5

    def test_detailed_prompt_high_quality(self, analyzer):
        prompt = "Search the codebase for all authentication handlers in src/auth/"
        score = analyzer._score_prompt_quality(prompt)
        assert score >= 0.7

    def test_empty_prompt_zero_quality(self, analyzer):
        score = analyzer._score_prompt_quality("")
        assert score == 0.0


class TestEdgeCases:
    def test_non_task_tool_calls_ignored(self, analyzer):
        session = _make_session([
            {"tool_name": "Read", "file_path": "src/main.py"},
            {"tool_name": "Edit", "file_path": "src/main.py"},
            {"tool_name": "Glob", "pattern": "**/*.py"},
        ])
        result = analyzer.analyze(session)
        assert result["total_task_calls"] == 0

    def test_malformed_tool_calls_skipped(self, analyzer):
        session = {"messages": [{"tool_calls": [None, "invalid", 42, {}]}]}
        result = analyzer.analyze(session)
        assert result["total_task_calls"] == 0

    def test_messages_without_tool_calls(self, analyzer):
        session = {"messages": [{"content": "Hello"}, {"role": "assistant"}]}
        result = analyzer.analyze(session)
        assert result["total_task_calls"] == 0

    def test_multiple_messages(self, analyzer):
        session = {
            "messages": [
                {"tool_calls": [
                    _task_call(subagent_type="Explore", prompt="Search files", description="search"),
                ]},
                {"tool_calls": [
                    _task_call(subagent_type="Bash", prompt="Run build", description="build"),
                ]},
            ]
        }
        result = analyzer.analyze(session)
        assert result["total_task_calls"] == 2
        assert result["task_invocations"] == {"Explore": 1, "Bash": 1}
