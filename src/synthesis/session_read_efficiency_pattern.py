"""Session read efficiency pattern analyzer.

Analyzes Claude Code session transcripts to measure read efficiency patterns —
how effectively the agent uses targeted reads (with offset/limit) vs full file
reads, and how it handles post-edit verification reads.

Metrics:
- total_reads: Total Read tool calls
- targeted_reads: Reads with offset/limit params
- full_reads: Reads without offset/limit
- re_reads: Full reads of previously-read files
- targeted_read_ratio: Proportion of targeted reads (0-1)
- post_edit_reads: Reads within 2 turns of Edit/Write to same file
- targeted_post_edit_reads: Post-edit reads with offset/limit
- full_post_edit_reads: Post-edit reads without offset/limit
- avg_lines_per_read: Average lines per read call
- cache_before_read_count: Cache queries preceding reads within 2 turns
- read_efficiency_score: Overall efficiency score (0-1)
"""

from __future__ import annotations

from typing import Any


def _empty_result() -> dict[str, Any]:
    """Return the default empty result dict."""
    return {
        "total_reads": 0,
        "targeted_reads": 0,
        "full_reads": 0,
        "re_reads": 0,
        "targeted_read_ratio": 0.0,
        "post_edit_reads": 0,
        "targeted_post_edit_reads": 0,
        "full_post_edit_reads": 0,
        "avg_lines_per_read": 0.0,
        "cache_before_read_count": 0,
        "read_efficiency_score": 1.0,
    }


_DEFAULT_FULL_READ_LINES = 200


def _is_targeted(params: dict) -> bool:
    """Check if a Read call uses offset/limit params."""
    return "limit" in params or ("offset" in params and "limit" in params)


def _get_file_path(params: dict) -> str | None:
    """Extract file_path from tool params."""
    fp = params.get("file_path")
    return fp if isinstance(fp, str) and fp else None


def _get_lines(params: dict) -> int:
    """Estimate lines read from params."""
    limit = params.get("limit")
    if limit is not None:
        try:
            return int(limit)
        except (TypeError, ValueError):
            pass
    return _DEFAULT_FULL_READ_LINES


def _is_cache_query(turn: dict) -> bool:
    """Check if a turn is a Bash cache query."""
    if turn.get("tool_name") != "Bash":
        return False
    params = turn.get("tool_params")
    if not isinstance(params, dict):
        return False
    cmd = str(params.get("command", ""))
    return "/cache quer" in cmd


def analyze_session_read_efficiency_pattern(records: object) -> dict[str, Any]:
    """Analyze a session transcript for read efficiency patterns.

    Args:
        records: List of turn dictionaries, each containing tool_name,
                 tool_params, tool_result, etc.

    Returns:
        Dict with read efficiency metrics and score.

    Raises:
        ValueError: If records is not a list.
    """
    if records is None:
        return _empty_result()
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    turns: list[dict] = [r for r in records if isinstance(r, dict)]

    if not turns:
        return _empty_result()

    total_reads = 0
    targeted_reads = 0
    full_reads = 0
    re_reads = 0
    post_edit_reads = 0
    targeted_post_edit_reads = 0
    full_post_edit_reads = 0
    cache_before_read_count = 0
    total_lines = 0

    # Track files that have been read (for re-read detection)
    files_read: set[str] = set()

    for i, turn in enumerate(turns):
        if turn.get("tool_name") != "Read":
            continue

        params = turn.get("tool_params")
        if not isinstance(params, dict):
            params = {}

        total_reads += 1
        targeted = _is_targeted(params)
        file_path = _get_file_path(params)
        total_lines += _get_lines(params)

        if targeted:
            targeted_reads += 1
        else:
            full_reads += 1
            # Check for re-read
            if file_path and file_path in files_read:
                re_reads += 1

        if file_path:
            files_read.add(file_path)

        # Check if this is a post-edit read (Edit/Write to same file within
        # preceding 2 turns)
        is_post_edit = False
        if file_path:
            start = max(0, i - 2)
            for j in range(start, i):
                prior = turns[j]
                if prior.get("tool_name") in ("Edit", "Write"):
                    prior_params = prior.get("tool_params")
                    if not isinstance(prior_params, dict):
                        prior_params = {}
                    prior_fp = _get_file_path(prior_params)
                    if prior_fp == file_path:
                        is_post_edit = True
                        break

        if is_post_edit:
            post_edit_reads += 1
            if targeted:
                targeted_post_edit_reads += 1
            else:
                full_post_edit_reads += 1

        # Check for cache-before-read pattern (cache query within preceding
        # 2 turns)
        start = max(0, i - 2)
        for j in range(start, i):
            if _is_cache_query(turns[j]):
                cache_before_read_count += 1
                break

    if total_reads == 0:
        return _empty_result()

    targeted_read_ratio = round(targeted_reads / max(1, total_reads), 3)
    avg_lines_per_read = round(total_lines / max(1, total_reads), 1)

    # Score calculation
    targeted_ratio_component = 0.4 * targeted_read_ratio
    post_edit_component = 0.3 * (
        targeted_post_edit_reads / max(1, post_edit_reads)
    )
    reread_component = 0.3 * (1 - re_reads / max(1, total_reads))

    score = targeted_ratio_component + post_edit_component + reread_component
    score = round(max(0.0, min(1.0, score)), 3)

    return {
        "total_reads": total_reads,
        "targeted_reads": targeted_reads,
        "full_reads": full_reads,
        "re_reads": re_reads,
        "targeted_read_ratio": targeted_read_ratio,
        "post_edit_reads": post_edit_reads,
        "targeted_post_edit_reads": targeted_post_edit_reads,
        "full_post_edit_reads": full_post_edit_reads,
        "avg_lines_per_read": avg_lines_per_read,
        "cache_before_read_count": cache_before_read_count,
        "read_efficiency_score": score,
    }
