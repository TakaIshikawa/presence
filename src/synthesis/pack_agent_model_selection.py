"""Pack Task agent model selection and subagent_type appropriateness analyzer.

Evaluates whether Task tool invocations use appropriate model and
subagent_type selections for the given task complexity.

Metrics:
- total_task_calls: Total Task tool invocations
- model_appropriateness_score: Whether model matches task complexity
- subagent_type_score: Whether subagent_type matches task description
- overall_score: Weighted composite (0-1)
- issues: List of detected mismatches
"""

from __future__ import annotations

from typing import Any, Mapping


class PackAgentModelSelectionAnalyzer:
    """Analyzes Task tool model and subagent_type selection patterns."""

    # Subagent types that are appropriate for different task patterns
    EXPLORE_KEYWORDS = {"find", "search", "where", "locate", "explore", "codebase", "structure"}
    BASH_KEYWORDS = {"run", "execute", "install", "build", "test", "git"}
    PLAN_KEYWORDS = {"plan", "design", "architect", "strategy", "approach"}

    def analyze(self, records: object) -> dict[str, Any]:
        """Analyze Task tool model/subagent selection across pack records.

        Args:
            records: List of pack dictionaries with sessions/messages/tool_calls.

        Returns:
            Dict with total_task_calls, model_appropriateness_score,
            subagent_type_score, overall_score, and issues.

        Raises:
            ValueError: If records is not a list.
        """
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError("records must be a list of pack dictionaries")

        total_task_calls = 0
        model_appropriate_count = 0
        subagent_appropriate_count = 0
        issues: list[str] = []

        for record in records:
            if not isinstance(record, Mapping):
                continue

            sessions = record.get("sessions")
            if not isinstance(sessions, list):
                continue

            for session in sessions:
                if not isinstance(session, Mapping):
                    continue

                messages = session.get("messages")
                if not isinstance(messages, list):
                    continue

                for message in messages:
                    if not isinstance(message, Mapping):
                        continue

                    tool_calls = message.get("tool_calls")
                    if not isinstance(tool_calls, list):
                        continue

                    for tc in tool_calls:
                        if not isinstance(tc, Mapping):
                            continue
                        if tc.get("tool_name") != "Task":
                            continue

                        total_task_calls += 1
                        prompt = str(tc.get("prompt", "")).lower()
                        model = tc.get("model", "")
                        subagent_type = tc.get("subagent_type", "")
                        description = str(tc.get("description", "")).lower()

                        # Evaluate model appropriateness
                        if self._is_model_appropriate(prompt, description, model):
                            model_appropriate_count += 1
                        else:
                            issues.append(
                                f"Model '{model}' may be overkill for simple task: "
                                f"{description[:50]}"
                            )

                        # Evaluate subagent_type appropriateness
                        if self._is_subagent_appropriate(prompt, description, subagent_type):
                            subagent_appropriate_count += 1
                        else:
                            issues.append(
                                f"subagent_type '{subagent_type}' may not match task: "
                                f"{description[:50]}"
                            )

        if total_task_calls == 0:
            return {
                "total_task_calls": 0,
                "model_appropriateness_score": 1.0,
                "subagent_type_score": 1.0,
                "overall_score": 1.0,
                "issues": [],
            }

        model_appropriateness_score = model_appropriate_count / total_task_calls
        subagent_type_score = subagent_appropriate_count / total_task_calls

        overall_score = 0.5 * model_appropriateness_score + 0.5 * subagent_type_score

        return {
            "total_task_calls": total_task_calls,
            "model_appropriateness_score": round(model_appropriateness_score, 4),
            "subagent_type_score": round(subagent_type_score, 4),
            "overall_score": round(overall_score, 4),
            "issues": issues[:10],
        }

    def _is_model_appropriate(self, prompt: str, description: str, model: str) -> bool:
        """Check if model selection matches task complexity."""
        combined = prompt + " " + description

        # Haiku is appropriate for simple/quick tasks
        simple_indicators = {"quick", "simple", "find", "search", "list", "check"}
        is_simple = any(word in combined for word in simple_indicators)

        if model == "haiku" and not is_simple:
            # Haiku for complex task — potentially inappropriate
            # But we're lenient: haiku is fine for cost savings
            return True  # Don't penalize haiku usage

        if model == "opus" and is_simple:
            # Opus for simple task — wasteful
            return False

        return True

    def _is_subagent_appropriate(
        self, prompt: str, description: str, subagent_type: str
    ) -> bool:
        """Check if subagent_type matches the task description."""
        combined = prompt + " " + description

        if subagent_type == "Explore":
            return any(kw in combined for kw in self.EXPLORE_KEYWORDS)
        if subagent_type == "Bash":
            return any(kw in combined for kw in self.BASH_KEYWORDS)
        if subagent_type == "Plan":
            return any(kw in combined for kw in self.PLAN_KEYWORDS)
        if subagent_type == "general-purpose":
            # General purpose is always acceptable
            return True

        return True  # Unknown types are acceptable
