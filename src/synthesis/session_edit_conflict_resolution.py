"""Session Edit conflict resolution and old_string retry strategy analyzer.

Measures how well sessions handle Edit tool failures (old_string not found)
and whether retry strategies are effective.

Metrics:
- edit_success_rate: % of Edit calls that succeed
- recovery_rate: % of failures followed by successful retry
- read_before_retry_rate: % of retries preceded by a Read of the same file
- anti_patterns: List of detected anti-patterns
- resolution_score: Weighted composite (0-1)
"""

from __future__ import annotations

from typing import Any, Mapping


class SessionEditConflictResolutionAnalyzer:
    """Analyzes Edit tool failure handling and retry strategies within sessions."""

    def analyze(self, records: object) -> dict[str, Any]:
        """Analyze Edit conflict resolution across session records.

        Args:
            records: List of session dictionaries with:
                - session_id: Session identifier
                - messages: List of message dictionaries with tool_calls
                  where Edit calls have: tool_name, file_path, old_string,
                  success (bool), error (optional string).

        Returns:
            Dict with edit_success_rate, recovery_rate, read_before_retry_rate,
            anti_patterns, and resolution_score.

        Raises:
            ValueError: If records is not a list.
        """
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError("records must be a list of session dictionaries")

        total_edits = 0
        successful_edits = 0
        failed_edits = 0
        recovered_failures = 0
        read_before_retry_count = 0
        retry_after_failure_count = 0
        anti_patterns: list[str] = []

        for record in records:
            if not isinstance(record, Mapping):
                continue

            messages = record.get("messages")
            if not isinstance(messages, list):
                continue

            # Track state across messages
            failed_edit_files: dict[str, int] = {}  # file → consecutive failure count
            files_read_after_failure: set[str] = set()
            last_failed_old_strings: dict[str, str] = {}  # file → last failed old_string

            for message in messages:
                if not isinstance(message, Mapping):
                    continue

                tool_calls = message.get("tool_calls")
                if not isinstance(tool_calls, list):
                    continue

                for tc in tool_calls:
                    if not isinstance(tc, Mapping):
                        continue

                    tool_name = tc.get("tool_name", "")
                    file_path = tc.get("file_path", "")

                    # Track reads after failure
                    if tool_name == "Read" and file_path in failed_edit_files:
                        files_read_after_failure.add(file_path)

                    if tool_name != "Edit":
                        continue

                    total_edits += 1
                    success = tc.get("success", True)
                    old_string = tc.get("old_string", "")

                    if success:
                        successful_edits += 1
                        # Check if this was a recovery from failure
                        if file_path in failed_edit_files:
                            recovered_failures += 1
                            retry_after_failure_count += 1
                            if file_path in files_read_after_failure:
                                read_before_retry_count += 1
                            # Reset tracking
                            del failed_edit_files[file_path]
                            files_read_after_failure.discard(file_path)
                            if file_path in last_failed_old_strings:
                                del last_failed_old_strings[file_path]
                    else:
                        failed_edits += 1

                        # Check anti-pattern: identical retry without read
                        if file_path in last_failed_old_strings:
                            if (
                                last_failed_old_strings[file_path] == old_string
                                and file_path not in files_read_after_failure
                            ):
                                if "blind_retry" not in anti_patterns:
                                    anti_patterns.append("blind_retry")

                        # Track failure count
                        failed_edit_files[file_path] = (
                            failed_edit_files.get(file_path, 0) + 1
                        )
                        last_failed_old_strings[file_path] = old_string

                        # Check excessive retries
                        if failed_edit_files[file_path] > 3:
                            if "excessive_retries" not in anti_patterns:
                                anti_patterns.append("excessive_retries")

            # Check for give-ups (files with failures that were never recovered)
            for file_path, count in failed_edit_files.items():
                if count > 0:
                    if "give_up" not in anti_patterns:
                        anti_patterns.append("give_up")

        # Handle no edits
        if total_edits == 0:
            return {
                "edit_success_rate": 1.0,
                "recovery_rate": 1.0,
                "read_before_retry_rate": 1.0,
                "anti_patterns": [],
                "resolution_score": 1.0,
            }

        # Calculate metrics
        edit_success_rate = successful_edits / total_edits

        recovery_rate = (
            recovered_failures / failed_edits if failed_edits > 0 else 1.0
        )

        read_before_retry_rate = (
            read_before_retry_count / retry_after_failure_count
            if retry_after_failure_count > 0
            else 1.0
        )

        # Anti-pattern penalty
        anti_pattern_penalty = min(len(anti_patterns) * 0.15, 0.5)

        # Weighted composite
        resolution_score = (
            0.4 * edit_success_rate
            + 0.3 * recovery_rate
            + 0.3 * read_before_retry_rate
            - anti_pattern_penalty
        )
        resolution_score = max(0.0, min(1.0, resolution_score))

        return {
            "edit_success_rate": round(edit_success_rate, 4),
            "recovery_rate": round(recovery_rate, 4),
            "read_before_retry_rate": round(read_before_retry_rate, 4),
            "anti_patterns": anti_patterns,
            "resolution_score": round(resolution_score, 4),
        }
