"""Session Task agent delegation appropriateness and model selection analyzer.

Analyzes Task tool usage patterns at the raw message/tool_call level to evaluate:
1. subagent_type selection correctness (Explore for exploration, Bash for commands, etc.)
2. Model parameter efficiency (haiku for simple tasks vs unnecessary sonnet/opus)
3. Prompt quality (clear, detailed prompts vs vague instructions)
4. Delegation appropriateness (Task for multi-step work vs trivial operations)
5. run_in_background usage (long-running work vs blocking unnecessarily)

Metrics:
- task_invocations: Count by subagent_type
- correct_agent_selection_rate: % correct subagent_type choices
- haiku_usage_for_simple_tasks_rate: % of simple tasks using haiku
- background_task_usage_rate: % tasks using run_in_background
- over_delegation_count: Task used for trivial single-tool operations
- under_delegation_count: Complex multi-step sequences without Task

Scores (0-1):
- agent_selection_correctness: Weighted correctness of subagent_type choices
- model_efficiency: Appropriate model selection for task complexity
- delegation_appropriateness: Correct use of Task vs direct tool calls
"""

from __future__ import annotations

from typing import Any, Mapping


class SessionTaskAgentDelegationAnalyzer:
    """Analyzes Task tool delegation patterns at the session message level."""

    # Keywords indicating appropriate subagent_type selection
    EXPLORE_KEYWORDS = {
        "find", "search", "where", "locate", "explore", "codebase",
        "structure", "understand", "discover", "look for", "what files",
    }
    BASH_KEYWORDS = {
        "run", "execute", "install", "build", "test", "git", "npm",
        "command", "shell", "compile", "deploy",
    }
    PLAN_KEYWORDS = {
        "plan", "design", "architect", "strategy", "approach",
        "implementation plan", "how to implement",
    }
    GENERAL_PURPOSE_KEYWORDS = {
        "research", "investigate", "complex", "multi-step", "analyze",
        "implement", "create", "write code",
    }

    # Indicators of simple tasks (should use haiku or direct tools)
    SIMPLE_TASK_INDICATORS = {
        "quick", "simple", "find", "search", "list", "check", "grep",
        "look up", "what is", "count",
    }

    # Indicators of trivial operations (should NOT use Task at all)
    TRIVIAL_INDICATORS = {
        "read file", "read a file", "cat file", "single file",
        "one file", "check if", "look at",
    }

    # Minimum prompt length for quality (characters)
    MIN_PROMPT_LENGTH = 30

    def analyze(self, session_data: object) -> dict[str, Any]:
        """Analyze Task tool delegation patterns in session data.

        Args:
            session_data: Dict with 'messages' key containing list of messages,
                each with optional 'tool_calls' list. Each tool_call has:
                - tool_name: str
                - subagent_type: str (for Task calls)
                - model: str (for Task calls)
                - prompt: str (for Task calls)
                - description: str (for Task calls)
                - run_in_background: bool (for Task calls)

        Returns:
            Dict with task_invocations (by type), scores, and metrics.

        Raises:
            ValueError: If session_data is not a dict.
        """
        if session_data is None:
            session_data = {}
        if not isinstance(session_data, Mapping):
            raise ValueError("session_data must be a dict with 'messages' key")

        messages = session_data.get("messages")
        if not isinstance(messages, list):
            messages = []

        task_calls: list[dict[str, Any]] = []
        all_tool_calls: list[dict[str, Any]] = []

        for message in messages:
            if not isinstance(message, Mapping):
                continue
            tool_calls = message.get("tool_calls")
            if not isinstance(tool_calls, list):
                continue
            for tc in tool_calls:
                if not isinstance(tc, Mapping):
                    continue
                all_tool_calls.append(dict(tc))
                if tc.get("tool_name") == "Task":
                    task_calls.append(dict(tc))

        # Count invocations by subagent_type
        invocations_by_type: dict[str, int] = {}
        for tc in task_calls:
            agent_type = str(tc.get("subagent_type", "unknown"))
            invocations_by_type[agent_type] = invocations_by_type.get(agent_type, 0) + 1

        # Evaluate each Task call
        correct_agent_selections = 0
        model_efficient_count = 0
        simple_task_count = 0
        simple_tasks_with_haiku = 0
        background_count = 0
        over_delegation_count = 0
        prompt_quality_scores: list[float] = []

        for tc in task_calls:
            prompt = str(tc.get("prompt", "")).lower()
            description = str(tc.get("description", "")).lower()
            model = str(tc.get("model", ""))
            subagent_type = str(tc.get("subagent_type", ""))
            run_in_background = bool(tc.get("run_in_background", False))

            combined = prompt + " " + description

            # 1. Agent selection correctness
            if self._is_agent_selection_correct(combined, subagent_type):
                correct_agent_selections += 1

            # 2. Model efficiency
            is_simple = self._is_simple_task(combined)
            if is_simple:
                simple_task_count += 1
                if model == "haiku":
                    simple_tasks_with_haiku += 1

            if self._is_model_efficient(combined, model):
                model_efficient_count += 1

            # 3. Background usage
            if run_in_background:
                background_count += 1

            # 4. Over-delegation detection
            if self._is_trivial_task(combined):
                over_delegation_count += 1

            # 5. Prompt quality
            prompt_quality_scores.append(self._score_prompt_quality(prompt))

        # Detect under-delegation: sequences of 3+ direct tool calls that could be Task
        under_delegation_count = self._detect_under_delegation(all_tool_calls, task_calls)

        total_task_calls = len(task_calls)

        if total_task_calls == 0:
            return {
                "task_invocations": {},
                "total_task_calls": 0,
                "correct_agent_selection_rate": 1.0,
                "haiku_usage_for_simple_tasks_rate": 1.0,
                "background_task_usage_rate": 0.0,
                "over_delegation_count": 0,
                "under_delegation_count": under_delegation_count,
                "agent_selection_correctness": 1.0,
                "model_efficiency": 1.0,
                "delegation_appropriateness": 1.0 if under_delegation_count == 0 else 0.8,
            }

        correct_agent_selection_rate = correct_agent_selections / total_task_calls
        haiku_rate = (
            simple_tasks_with_haiku / simple_task_count
            if simple_task_count > 0
            else 1.0
        )
        background_rate = background_count / total_task_calls
        model_efficiency_rate = model_efficient_count / total_task_calls

        # Compute scores (0-1)
        agent_selection_correctness = round(correct_agent_selection_rate, 4)
        model_efficiency = round(model_efficiency_rate, 4)

        # Delegation appropriateness penalizes over- and under-delegation
        delegation_penalty = min(
            1.0,
            (over_delegation_count + under_delegation_count) * 0.1,
        )
        delegation_appropriateness = round(max(0.0, 1.0 - delegation_penalty), 4)

        return {
            "task_invocations": invocations_by_type,
            "total_task_calls": total_task_calls,
            "correct_agent_selection_rate": round(correct_agent_selection_rate, 4),
            "haiku_usage_for_simple_tasks_rate": round(haiku_rate, 4),
            "background_task_usage_rate": round(background_rate, 4),
            "over_delegation_count": over_delegation_count,
            "under_delegation_count": under_delegation_count,
            "agent_selection_correctness": agent_selection_correctness,
            "model_efficiency": model_efficiency,
            "delegation_appropriateness": delegation_appropriateness,
        }

    def _is_agent_selection_correct(self, combined: str, subagent_type: str) -> bool:
        """Check if the subagent_type matches the task content."""
        if subagent_type == "Explore":
            return any(kw in combined for kw in self.EXPLORE_KEYWORDS)
        if subagent_type == "Bash":
            return any(kw in combined for kw in self.BASH_KEYWORDS)
        if subagent_type == "Plan":
            return any(kw in combined for kw in self.PLAN_KEYWORDS)
        if subagent_type == "general-purpose":
            # General-purpose is acceptable for most tasks
            return True
        # Unknown types: acceptable
        return True

    def _is_simple_task(self, combined: str) -> bool:
        """Determine if a task is simple based on content indicators."""
        return any(kw in combined for kw in self.SIMPLE_TASK_INDICATORS)

    def _is_trivial_task(self, combined: str) -> bool:
        """Determine if a task is trivial and shouldn't use Task at all."""
        return any(kw in combined for kw in self.TRIVIAL_INDICATORS)

    def _is_model_efficient(self, combined: str, model: str) -> bool:
        """Check if model selection is efficient for task complexity."""
        is_simple = self._is_simple_task(combined)

        # Opus for simple tasks is wasteful
        if model == "opus" and is_simple:
            return False

        # Haiku is always efficient (cost-saving)
        if model == "haiku":
            return True

        # Sonnet is the default and acceptable for most tasks
        if model == "sonnet" or model == "":
            return True

        return True

    def _score_prompt_quality(self, prompt: str) -> float:
        """Score prompt quality from 0.0 to 1.0."""
        if not prompt:
            return 0.0

        score = 0.0

        # Length check
        if len(prompt) >= self.MIN_PROMPT_LENGTH:
            score += 0.4
        elif len(prompt) >= 15:
            score += 0.2

        # Has context (mentions files, paths, or specific details)
        if "/" in prompt or "." in prompt or "file" in prompt:
            score += 0.3

        # Has clear instruction (imperative verbs)
        imperative_verbs = {"find", "search", "create", "implement", "run", "check", "analyze"}
        if any(verb in prompt for verb in imperative_verbs):
            score += 0.3

        return min(1.0, score)

    def _detect_under_delegation(
        self, all_tool_calls: list[dict[str, Any]], task_calls: list[dict[str, Any]]  # noqa: ARG002
    ) -> int:
        """Detect sequences of 3+ related direct tool calls that should be a Task.

        Looks for consecutive Glob/Grep/Read sequences without Task delegation.
        """
        search_tools = {"Glob", "Grep", "Read"}
        consecutive_search = 0
        under_delegation_sequences = 0

        for tc in all_tool_calls:
            tool_name = tc.get("tool_name", "")
            if tool_name in search_tools:
                consecutive_search += 1
                if consecutive_search >= 4:
                    under_delegation_sequences += 1
                    consecutive_search = 0  # Reset after counting
            elif tool_name == "Task":
                consecutive_search = 0
            else:
                consecutive_search = 0

        return under_delegation_sequences
