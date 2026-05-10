"""Pack model cost attribution analyzer.

Examines tool calls across sessions in a pack to estimate and attribute
costs by pipeline stage and model tier.

Metrics:
- total_tool_calls: Total tool invocations across all sessions
- stage_counts: Tool calls broken down by pipeline stage
- model_counts: Tool calls broken down by model tier
- stage_cost_proxy: Weighted cost proxy per stage
- total_cost_proxy: Sum of all stage cost proxies
- inefficiencies: Detected cost inefficiency patterns
- cost_efficiency_score: 0.0-1.0 efficiency rating
"""

from __future__ import annotations

from typing import Any, Mapping

# Cost weight multipliers by model tier
MODEL_WEIGHTS: dict[str, float] = {
    "opus": 3.0,
    "sonnet": 1.0,
    "haiku": 0.25,
    "default": 1.0,
}

# Tools classified by pipeline stage
EXPLORATION_TOOLS = {"Read", "Glob", "Grep"}
IMPLEMENTATION_TOOLS = {"Edit", "Write", "NotebookEdit"}
COMMUNICATION_TOOLS = {"AskUserQuestion"}

# Bash command patterns indicating verification
VERIFICATION_PATTERNS = ("test", "build", "lint", "check", "pytest", "jest", "npm run")


def _empty_result() -> dict[str, Any]:
    """Return the default empty result structure."""
    return {
        "total_tool_calls": 0,
        "stage_counts": {
            "exploration": 0,
            "implementation": 0,
            "verification": 0,
            "communication": 0,
        },
        "model_counts": {"opus": 0, "sonnet": 0, "haiku": 0, "default": 0},
        "stage_cost_proxy": {
            "exploration": 0.0,
            "implementation": 0.0,
            "verification": 0.0,
            "communication": 0.0,
        },
        "total_cost_proxy": 0.0,
        "inefficiencies": [],
        "cost_efficiency_score": 1.0,
    }


def _classify_stage(tc: Mapping) -> str:
    """Classify a tool call into a pipeline stage."""
    tool_name = tc.get("tool_name", "")

    if tool_name == "Task":
        subagent_type = tc.get("subagent_type", "")
        if subagent_type == "Explore":
            return "exploration"
        if subagent_type in ("Verify", "verify"):
            return "verification"
        # Default Task calls fall to exploration
        return "exploration"

    if tool_name in EXPLORATION_TOOLS:
        return "exploration"

    if tool_name in IMPLEMENTATION_TOOLS:
        return "implementation"

    if tool_name == "Bash":
        command = str(tc.get("command", "")).lower()
        if any(pat in command for pat in VERIFICATION_PATTERNS):
            return "verification"
        # Non-verification bash defaults to exploration
        return "exploration"

    if tool_name in COMMUNICATION_TOOLS:
        return "communication"

    # assistant_response or unknown tools
    if tool_name == "assistant_response":
        return "communication"

    # Default to exploration for unclassified tools
    return "exploration"


def _detect_model(tc: Mapping) -> str:
    """Detect the model tier for a tool call."""
    model = tc.get("model", "")
    if model in ("opus", "sonnet", "haiku"):
        return model
    return "default"


def analyze_pack_model_cost_attribution(records: object) -> dict[str, Any]:
    """Analyze tool calls across pack sessions for cost attribution.

    Args:
        records: List of pack dictionaries with sessions/messages/tool_calls.

    Returns:
        Dict with cost attribution metrics.

    Raises:
        ValueError: If records is not a list.
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")
    if not records:
        return _empty_result()

    stage_counts: dict[str, int] = {
        "exploration": 0,
        "implementation": 0,
        "verification": 0,
        "communication": 0,
    }
    model_counts: dict[str, int] = {"opus": 0, "sonnet": 0, "haiku": 0, "default": 0}
    stage_cost_proxy: dict[str, float] = {
        "exploration": 0.0,
        "implementation": 0.0,
        "verification": 0.0,
        "communication": 0.0,
    }
    inefficiencies: list[str] = []
    total_tool_calls = 0

    # Track file reads across sessions for redundancy detection
    file_reads_by_session: dict[int, set[str]] = {}
    session_index = 0

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

            current_session_reads: set[str] = set()

            for message in messages:
                if not isinstance(message, Mapping):
                    continue

                tool_calls = message.get("tool_calls")
                if not isinstance(tool_calls, list):
                    continue

                for tc in tool_calls:
                    if not isinstance(tc, Mapping):
                        continue

                    total_tool_calls += 1
                    stage = _classify_stage(tc)
                    model = _detect_model(tc)

                    stage_counts[stage] += 1
                    model_counts[model] += 1

                    weight = MODEL_WEIGHTS[model]
                    stage_cost_proxy[stage] += weight

                    # Detect opus used for exploration
                    if model == "opus" and stage == "exploration":
                        tool_name = tc.get("tool_name", "unknown")
                        desc = str(tc.get("description", tc.get("prompt", "")))[:50]
                        inefficiencies.append(
                            f"Opus used for exploration ({tool_name}): {desc}"
                        )

                    # Track file reads for cross-session redundancy
                    tool_name = tc.get("tool_name", "")
                    if tool_name == "Read":
                        file_path = tc.get("file_path", "")
                        if file_path:
                            current_session_reads.add(file_path)

            file_reads_by_session[session_index] = current_session_reads
            session_index += 1

    # Detect redundant reads across sessions
    all_files_seen: set[str] = set()
    for sid in sorted(file_reads_by_session.keys()):
        session_reads = file_reads_by_session[sid]
        duplicates = session_reads & all_files_seen
        for fp in sorted(duplicates):
            inefficiencies.append(
                f"Redundant read across sessions: {fp}"
            )
        all_files_seen |= session_reads

    # Cap inefficiencies at 10
    inefficiencies = inefficiencies[:10]

    total_cost_proxy = sum(stage_cost_proxy.values())

    # Score: 1.0 - (inefficiency_count / total_tool_calls), clamped [0, 1]
    if total_tool_calls == 0:
        cost_efficiency_score = 1.0
    else:
        raw = 1.0 - (len(inefficiencies) / max(1, total_tool_calls))
        cost_efficiency_score = round(max(0.0, min(1.0, raw)), 4)

    return {
        "total_tool_calls": total_tool_calls,
        "stage_counts": stage_counts,
        "model_counts": model_counts,
        "stage_cost_proxy": stage_cost_proxy,
        "total_cost_proxy": total_cost_proxy,
        "inefficiencies": inefficiencies,
        "cost_efficiency_score": cost_efficiency_score,
    }
