"""Session parallel tool call missed opportunity analyzer.

Analyzes Claude Code session transcripts to detect missed opportunities
for parallel tool execution — cases where independent tool calls were
made sequentially instead of being batched into a single turn.

Detection categories:
- sequential_independent_pairs: Consecutive turns with independent tool calls
- repeated_single_tool_sequences: 3+ turns of same tool type on different targets
- independent_task_launches: Consecutive Task calls with unrelated prompts

Metrics:
- total_turns: Number of turns analyzed
- sequential_independent_pairs: Count of consecutive independent pairs
- repeated_single_tool_sequences: Count of 3+ same-type sequences
- independent_task_launches: Consecutive independent Task calls
- total_missed_opportunities: Sum of above
- parallelizable_turn_ratio: Proportion of turns that could be parallelized (0-1)
- parallel_efficiency_score: Higher = better parallelization (0-1)
"""

from __future__ import annotations

from typing import Any, Mapping


def _empty_result() -> dict[str, Any]:
    """Return the default empty result dict."""
    return {
        "total_turns": 0,
        "sequential_independent_pairs": 0,
        "repeated_single_tool_sequences": 0,
        "independent_task_launches": 0,
        "total_missed_opportunities": 0,
        "parallelizable_turn_ratio": 0.0,
        "parallel_efficiency_score": 1.0,
    }


def _extract_file_paths(params: dict) -> set[str]:
    """Extract file path references from tool params."""
    paths: set[str] = set()
    for key in ("file_path", "path", "notebook_path"):
        val = params.get(key)
        if isinstance(val, str) and val:
            paths.add(val)
    return paths


def _result_contains_paths(result: str, paths: set[str]) -> bool:
    """Check if a tool result string mentions any of the given paths."""
    if not result:
        return False
    return any(p in result for p in paths if p)


def _are_turns_independent(prev: Mapping, curr: Mapping) -> bool:
    """Determine if two consecutive turns are independent (no data dependency).

    Returns True if the current turn does NOT depend on the previous turn's result.
    """
    prev_tool = prev.get("tool_name", "")
    curr_tool = curr.get("tool_name", "")
    prev_params = prev.get("tool_params") or {}
    curr_params = curr.get("tool_params") or {}
    prev_result = str(prev.get("tool_result", ""))

    if not isinstance(prev_params, dict):
        prev_params = {}
    if not isinstance(curr_params, dict):
        curr_params = {}

    # Edit/Write after Read of same file is dependent
    prev_files = _extract_file_paths(prev_params)
    curr_files = _extract_file_paths(curr_params)

    if prev_tool == "Read" and curr_tool in ("Edit", "Write"):
        if prev_files & curr_files:
            return False

    # Bash with && is sequential-by-design — but that's within a single turn,
    # so it won't appear as two separate turns. If two Bash turns reference
    # the same state-modifying commands, treat as dependent.
    if prev_tool == "Bash" and curr_tool == "Bash":
        prev_cmd = str(prev_params.get("command", ""))
        curr_cmd = str(curr_params.get("command", ""))
        state_modifiers = ["git add", "git commit", "git push", "rm ", "mv ", "cp ", "mkdir"]
        if any(m in prev_cmd for m in state_modifiers) or any(m in curr_cmd for m in state_modifiers):
            return False

    # Check if current params reference values from previous result
    if curr_files and _result_contains_paths(prev_result, curr_files):
        # Current turn references a path that appeared in previous result
        return False

    # Same file in both turns → likely dependent
    if prev_files and curr_files and (prev_files & curr_files):
        return False

    # Different files or different tools with no overlap → independent
    return True


def _are_task_launches_independent(prev: Mapping, curr: Mapping) -> bool:
    """Check if two Task tool calls are independent based on prompt content."""
    prev_params = prev.get("tool_params") or {}
    curr_params = curr.get("tool_params") or {}
    if not isinstance(prev_params, dict):
        prev_params = {}
    if not isinstance(curr_params, dict):
        curr_params = {}

    prev_result = str(prev.get("tool_result", ""))

    # Check file paths specifically
    curr_files = _extract_file_paths(curr_params)
    if _result_contains_paths(prev_result, curr_files):
        return False

    # Check description overlap as a proxy for relatedness
    prev_desc = str(prev_params.get("description", "")).lower()
    curr_desc = str(curr_params.get("description", "")).lower()

    # If descriptions share significant words, they may be related
    prev_words = {w for w in prev_desc.split() if len(w) >= 4}
    curr_desc_words = {w for w in curr_desc.split() if len(w) >= 4}
    if prev_words and curr_desc_words:
        overlap = prev_words & curr_desc_words
        if len(overlap) > len(curr_desc_words) * 0.5:
            return False

    return True


def analyze_session_parallel_missed_opportunity(records: object) -> dict[str, Any]:
    """Analyze a session transcript for missed parallel tool execution opportunities.

    Args:
        records: List of turn dictionaries, each containing tool_name,
                 tool_params, tool_result, etc.

    Returns:
        Dict with total_turns, sequential_independent_pairs,
        repeated_single_tool_sequences, independent_task_launches,
        total_missed_opportunities, parallelizable_turn_ratio,
        and parallel_efficiency_score.

    Raises:
        ValueError: If records is not a list.
    """
    if records is None:
        return _empty_result()
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    # Filter to valid turn dicts
    turns: list[Mapping] = [r for r in records if isinstance(r, Mapping)]

    if not turns:
        return _empty_result()

    total_turns = len(turns)
    sequential_independent_pairs = 0
    independent_task_launches = 0

    # Track which pairs are already counted to avoid double-counting
    paired_indices: set[int] = set()

    # 1. Detect sequential independent pairs
    for i in range(len(turns) - 1):
        prev_turn = turns[i]
        curr_turn = turns[i + 1]

        if _are_turns_independent(prev_turn, curr_turn):
            sequential_independent_pairs += 1
            paired_indices.add(i)
            paired_indices.add(i + 1)

    # 2. Detect repeated single-tool sequences (3+ same tool type)
    repeated_single_tool_sequences = 0
    i = 0
    while i < len(turns):
        tool_name = turns[i].get("tool_name", "")
        if not tool_name:
            i += 1
            continue

        # Count consecutive turns with same tool name
        j = i + 1
        while j < len(turns) and turns[j].get("tool_name", "") == tool_name:
            j += 1

        seq_len = j - i
        if seq_len >= 3:
            # Verify they're on different targets (not same file)
            params_list = [turns[k].get("tool_params") or {} for k in range(i, j)]
            file_sets = [_extract_file_paths(p if isinstance(p, dict) else {}) for p in params_list]
            all_files = set()
            all_independent = True
            for fs in file_sets:
                if fs & all_files:
                    all_independent = False
                    break
                all_files |= fs

            if all_independent:
                repeated_single_tool_sequences += 1

        i = j

    # 3. Detect independent Task launches
    i = 0
    while i < len(turns) - 1:
        if turns[i].get("tool_name") != "Task":
            i += 1
            continue

        j = i + 1
        while j < len(turns) and turns[j].get("tool_name") == "Task":
            j += 1

        if j - i >= 2:
            # Check pairs within this Task sequence
            for k in range(i, j - 1):
                if _are_task_launches_independent(turns[k], turns[k + 1]):
                    independent_task_launches += 1

        i = j

    total_missed = (
        sequential_independent_pairs
        + repeated_single_tool_sequences
        + independent_task_launches
    )

    # Score calculations
    max_pairs = max(1, total_turns - 1)
    parallel_efficiency_score = 1.0 - (total_missed / max_pairs)
    parallel_efficiency_score = round(max(0.0, min(1.0, parallel_efficiency_score)), 3)

    parallelizable_turn_ratio = total_missed * 2 / max(1, total_turns)
    parallelizable_turn_ratio = round(max(0.0, min(1.0, parallelizable_turn_ratio)), 3)

    return {
        "total_turns": total_turns,
        "sequential_independent_pairs": sequential_independent_pairs,
        "repeated_single_tool_sequences": repeated_single_tool_sequences,
        "independent_task_launches": independent_task_launches,
        "total_missed_opportunities": total_missed,
        "parallelizable_turn_ratio": parallelizable_turn_ratio,
        "parallel_efficiency_score": parallel_efficiency_score,
    }
