"""Session context awareness analyzer.

Measures how effectively the agent uses conversation context vs.
redundant exploration.
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_context_awareness(records: object) -> dict[str, Any]:
    """Analyze context awareness patterns in agent sessions."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool call dictionaries")

    total_tool_calls = 0
    read_call_count = 0
    redundant_read_count = 0
    context_references = 0

    read_files: dict[str, int] = {}

    for record in records:
        if not isinstance(record, Mapping):
            continue

        tool_name = str(record.get("tool_name", "")).strip().lower()
        if not tool_name:
            continue

        total_tool_calls += 1

        if tool_name == "read":
            read_call_count += 1
            file_path = str(record.get("file_path", "")).strip()

            if file_path:
                read_files[file_path] = read_files.get(file_path, 0) + 1

                if read_files[file_path] > 1:
                    redundant_read_count += 1

        # Check for context references
        if record.get("references_prior_context"):
            context_references += 1

    context_reuse_ratio = (context_references / total_tool_calls * 100.0) if total_tool_calls > 0 else 0.0
    wasted_token_percentage = (redundant_read_count / read_call_count * 100.0) if read_call_count > 0 else 0.0

    return {
        "total_tool_calls": total_tool_calls,
        "read_call_count": read_call_count,
        "redundant_read_count": redundant_read_count,
        "context_references": context_references,
        "context_reuse_ratio": round(context_reuse_ratio, 2),
        "wasted_token_percentage": round(wasted_token_percentage, 2),
    }
