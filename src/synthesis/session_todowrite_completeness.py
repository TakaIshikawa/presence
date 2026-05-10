"""Session TodoWrite completeness and status transition analyzer.

Measures whether TodoWrite tasks are properly completed vs abandoned,
and whether status transitions follow the correct lifecycle:
pending → in_progress → completed.

Metrics:
- completion_rate: Ratio of completed tasks to total
- transition_score: Proper lifecycle transitions
- single_active_compliance: Only one task in_progress at a time
- abandoned_tasks: Count of tasks left incomplete at session end
- overall_discipline_score: Weighted composite (0-1)
"""

from __future__ import annotations

from typing import Any, Mapping


class SessionTodoWriteCompletenessAnalyzer:
    """Analyzes TodoWrite task lifecycle discipline within sessions."""

    def analyze(self, records: object) -> dict[str, Any]:
        """Analyze TodoWrite completeness across session records.

        Args:
            records: List of session dictionaries with:
                - session_id: Session identifier
                - messages: List of message dictionaries with tool_calls
                  where TodoWrite calls contain a 'todos' list of
                  {content, status, activeForm} items.

        Returns:
            Dict with completion_rate, transition_score, single_active_compliance,
            abandoned_tasks, and overall_discipline_score.

        Raises:
            ValueError: If records is not a list.
        """
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError("records must be a list of session dictionaries")

        total_tasks_seen = 0
        completed_tasks = 0
        improper_transitions = 0
        total_transitions = 0
        multi_active_violations = 0
        total_todowrite_calls = 0
        abandoned_tasks = 0

        for record in records:
            if not isinstance(record, Mapping):
                continue

            messages = record.get("messages")
            if not isinstance(messages, list):
                continue

            # Track task status history: content → list of statuses
            task_history: dict[str, list[str]] = {}
            final_todos: list[dict[str, str]] = []

            for message in messages:
                if not isinstance(message, Mapping):
                    continue

                tool_calls = message.get("tool_calls")
                if not isinstance(tool_calls, list):
                    continue

                for tc in tool_calls:
                    if not isinstance(tc, Mapping):
                        continue
                    if tc.get("tool_name") != "TodoWrite":
                        continue

                    total_todowrite_calls += 1
                    todos = tc.get("todos")
                    if not isinstance(todos, list):
                        continue

                    # Check single-active compliance
                    in_progress_count = sum(
                        1
                        for t in todos
                        if isinstance(t, Mapping) and t.get("status") == "in_progress"
                    )
                    if in_progress_count > 1:
                        multi_active_violations += 1

                    # Track transitions
                    for todo in todos:
                        if not isinstance(todo, Mapping):
                            continue
                        content = todo.get("content", "")
                        status = todo.get("status", "")
                        if not content or not status:
                            continue

                        if content not in task_history:
                            task_history[content] = []
                            total_tasks_seen += 1

                        prev_statuses = task_history[content]
                        if not prev_statuses or prev_statuses[-1] != status:
                            task_history[content].append(status)

                    # Update final state
                    final_todos = [
                        t for t in todos if isinstance(t, Mapping)
                    ]

            # Analyze final state for abandoned tasks
            for todo in final_todos:
                status = todo.get("status", "")
                if status in ("pending", "in_progress"):
                    abandoned_tasks += 1
                if status == "completed":
                    completed_tasks += 1

            # Analyze transitions
            for content, statuses in task_history.items():
                if len(statuses) > 1:
                    for i in range(1, len(statuses)):
                        total_transitions += 1
                        prev = statuses[i - 1]
                        curr = statuses[i]
                        # Valid transitions: pending→in_progress, in_progress→completed
                        if not self._is_valid_transition(prev, curr):
                            improper_transitions += 1

        # Handle no TodoWrite calls
        if total_todowrite_calls == 0:
            return {
                "completion_rate": 1.0,
                "transition_score": 1.0,
                "single_active_compliance": 1.0,
                "abandoned_tasks": 0,
                "overall_discipline_score": 1.0,
            }

        # Calculate metrics
        completion_rate = (
            completed_tasks / total_tasks_seen if total_tasks_seen > 0 else 1.0
        )

        transition_score = (
            1.0 - (improper_transitions / total_transitions)
            if total_transitions > 0
            else 1.0
        )

        single_active_compliance = (
            1.0 - (multi_active_violations / total_todowrite_calls)
            if total_todowrite_calls > 0
            else 1.0
        )

        # Weighted composite
        overall_discipline_score = (
            0.4 * completion_rate
            + 0.3 * transition_score
            + 0.3 * single_active_compliance
        )

        return {
            "completion_rate": round(completion_rate, 4),
            "transition_score": round(transition_score, 4),
            "single_active_compliance": round(single_active_compliance, 4),
            "abandoned_tasks": abandoned_tasks,
            "overall_discipline_score": round(overall_discipline_score, 4),
        }

    def _is_valid_transition(self, prev: str, curr: str) -> bool:
        """Check if a status transition is valid."""
        valid = {
            ("pending", "in_progress"),
            ("in_progress", "completed"),
            ("pending", "completed"),  # Acceptable shortcut for trivial tasks
        }
        return (prev, curr) in valid
