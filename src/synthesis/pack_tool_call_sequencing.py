"""Pack tool call sequencing and dependency violation analyzer.

Detects tool call sequencing violations — cases where tools are called
in the wrong order or with missing prerequisites.

Rules enforced:
- Edit requires prior Read of same file
- Write requires prior Read of existing file
- git commit requires prior git add
- Parallel calls must be independent

Metrics:
- violation_count: Total sequencing violations detected
- violations: List of {tool, file, rule_violated}
- parallelization_opportunities: Sequential calls that could be parallel
- sequencing_score: Overall compliance score (0-1)
"""

from __future__ import annotations

from typing import Any, Mapping


class PackToolCallSequencingAnalyzer:
    """Analyzes tool call sequencing and dependency violations across pack sessions."""

    def analyze(self, records: object) -> dict[str, Any]:
        """Analyze tool call sequencing across pack records.

        Args:
            records: List of pack dictionaries with sessions/messages/tool_calls.

        Returns:
            Dict with violation_count, violations, parallelization_opportunities,
            and sequencing_score.

        Raises:
            ValueError: If records is not a list.
        """
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError("records must be a list of pack dictionaries")

        violations: list[dict[str, str]] = []
        parallelization_opportunities = 0
        total_tool_calls = 0

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

                # Track files that have been read in this session
                files_read: set[str] = set()
                git_added = False
                prev_single_call: dict[str, Any] | None = None

                for message in messages:
                    if not isinstance(message, Mapping):
                        continue

                    tool_calls = message.get("tool_calls")
                    if not isinstance(tool_calls, list):
                        prev_single_call = None
                        continue

                    for tc in tool_calls:
                        if not isinstance(tc, Mapping):
                            continue

                        total_tool_calls += 1
                        tool_name = tc.get("tool_name", "")
                        file_path = tc.get("file_path", "")
                        command = tc.get("command", "")

                        # Track reads
                        if tool_name == "Read" and file_path:
                            files_read.add(file_path)

                        # Check Edit-without-Read
                        if tool_name == "Edit" and file_path:
                            if file_path not in files_read:
                                violations.append({
                                    "tool": "Edit",
                                    "file": file_path,
                                    "rule_violated": "Edit requires prior Read of same file",
                                })

                        # Check Write-without-Read
                        if tool_name == "Write" and file_path:
                            if file_path not in files_read:
                                violations.append({
                                    "tool": "Write",
                                    "file": file_path,
                                    "rule_violated": "Write requires prior Read of existing file",
                                })

                        # Track git add
                        if tool_name == "Bash" and isinstance(command, str):
                            if "git add" in command:
                                git_added = True
                            # Check git commit without prior git add
                            if "git commit" in command and not git_added:
                                violations.append({
                                    "tool": "Bash",
                                    "file": "",
                                    "rule_violated": "git commit requires prior git add",
                                })

                    # Parallelization opportunity detection
                    if len(tool_calls) == 1:
                        current_call = tool_calls[0] if isinstance(tool_calls[0], Mapping) else None
                        if current_call and prev_single_call:
                            if self._are_independent(prev_single_call, current_call):
                                parallelization_opportunities += 1
                        prev_single_call = current_call
                    else:
                        prev_single_call = None

        # Calculate score
        if total_tool_calls == 0:
            sequencing_score = 1.0
        else:
            violation_ratio = len(violations) / total_tool_calls
            sequencing_score = max(0.0, 1.0 - violation_ratio * 2)

        return {
            "violation_count": len(violations),
            "violations": violations,
            "parallelization_opportunities": parallelization_opportunities,
            "sequencing_score": round(sequencing_score, 4),
        }

    def _are_independent(self, call1: Mapping, call2: Mapping) -> bool:
        """Check if two tool calls are independent and could be parallelized."""
        tool1 = call1.get("tool_name", "")
        tool2 = call2.get("tool_name", "")
        file1 = call1.get("file_path", "")
        file2 = call2.get("file_path", "")

        # Same file → likely dependent
        if file1 and file2 and file1 == file2:
            return False

        # Read + Read on different files → independent
        if tool1 == "Read" and tool2 == "Read" and file1 != file2:
            return True

        # Bash commands on different things → likely independent
        if tool1 == "Bash" and tool2 == "Bash":
            cmd1 = call1.get("command", "")
            cmd2 = call2.get("command", "")
            # If either modifies state, not independent
            modifiers = ["git add", "git commit", "rm ", "mv ", "cp ", "mkdir"]
            if any(m in str(cmd1) for m in modifiers) or any(m in str(cmd2) for m in modifiers):
                return False
            return True

        # Glob + Grep → independent
        if {tool1, tool2} <= {"Glob", "Grep", "Read"}:
            if file1 != file2:
                return True

        return False
