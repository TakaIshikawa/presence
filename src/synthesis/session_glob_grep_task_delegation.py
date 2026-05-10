"""Session Glob/Grep vs Task agent delegation appropriateness analyzer.

Analyzes when sessions use direct Glob/Grep vs delegating to Task/Explore agent,
measuring delegation appropriateness, search complexity classification, and
iteration patterns.

Delegation dimensions:
1. Search complexity classification:
   - Simple: Known path/pattern, specific file search
   - Moderate: Multi-file pattern matching, targeted grep
   - Complex: Open-ended exploration, multi-round sequences

2. Delegation appropriateness:
   - Broad searches delegated to Task (correct)
   - Simple targeted searches using Glob/Grep (correct)
   - Open-ended exploration using Task/Explore (correct)
   - Simple searches incorrectly delegated (wasteful)

3. Search iteration patterns:
   - Single-round successful searches
   - Multi-round refinement (2-3 rounds)
   - Extended sequences (3+ rounds, should delegate)

4. Search efficiency:
   - Average results per Glob
   - Grep output_mode selection (files_with_matches vs content)
   - Search success rate

Quality indicators:
- High delegation appropriateness (>80%): Correct tool selection
- Low wasteful delegation (<10%): Not delegating simple searches
- Low extended sequences (<15%): Delegating after 3rd round
- Appropriate output_mode usage (>70%): files_with_matches for discovery
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_glob_grep_task_delegation(records: object) -> dict[str, Any]:
    """Analyze Glob/Grep vs Task delegation appropriateness.

    Args:
        records: List of session dictionaries with keys:
            - session_id: Session identifier
            - total_glob_calls: Glob tool invocations
            - total_grep_calls: Grep tool invocations
            - total_task_search_delegations: Task/Explore delegations
            - simple_searches: Searches with known path/specific pattern
            - moderate_searches: Multi-file pattern matching
            - complex_searches: Open-ended exploratory searches
            - simple_delegated_to_task: Simple searches delegated (wasteful)
            - complex_using_direct_search: Complex searches using Glob/Grep
            - search_sequences_2_3_rounds: Moderate iteration
            - search_sequences_gt_3_rounds: Extended sequences (should delegate)
            - grep_files_with_matches: Grep using files_with_matches mode
            - grep_content_mode: Grep using content mode
            - avg_glob_results: Average results per Glob call
            - successful_searches: Searches finding results
            - failed_searches: Searches with no results

    Returns:
        Dict with:
            - total_sessions: Number of sessions analyzed
            - sessions_with_searches: Sessions performing searches
            - total_searches: Total search operations
            - delegation_appropriateness_score: % correct tool selection
            - wasteful_delegation_rate: % simple searches delegated to Task
            - missing_delegation_rate: % complex searches not delegated
            - extended_sequence_rate: % searches with 3+ rounds
            - grep_output_mode_appropriateness: % using files_with_matches
            - avg_glob_results: Average results per Glob
            - search_success_rate: % searches finding results
            - avg_search_iterations: Average rounds per search
            - tool_selection_score: Overall delegation score 0-1
            - high_appropriateness_sessions: Count with score >0.8
            - low_appropriateness_sessions: Count with score <0.5

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    if not records:
        return _empty_result()

    total_sessions = 0
    sessions_with_searches = 0
    total_glob = 0
    total_grep = 0
    total_task_delegations = 0
    simple_searches = 0
    moderate_searches = 0
    complex_searches = 0
    simple_delegated = 0
    complex_direct = 0
    sequences_2_3 = 0
    sequences_gt_3 = 0
    grep_files_mode = 0
    grep_content_mode = 0
    successful_searches = 0
    failed_searches = 0

    glob_results: list[int | float] = []
    search_iterations: list[int | float] = []
    session_scores: list[int | float] = []

    high_appropriateness_sessions = 0
    low_appropriateness_sessions = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        glob_calls = _int(record.get("total_glob_calls", 0))
        grep_calls = _int(record.get("total_grep_calls", 0))
        task_delegations = _int(record.get("total_task_search_delegations", 0))
        simple = _int(record.get("simple_searches", 0))
        moderate = _int(record.get("moderate_searches", 0))
        complex_s = _int(record.get("complex_searches", 0))
        simple_del = _int(record.get("simple_delegated_to_task", 0))
        complex_dir = _int(record.get("complex_using_direct_search", 0))
        seq_2_3 = _int(record.get("search_sequences_2_3_rounds", 0))
        seq_gt_3 = _int(record.get("search_sequences_gt_3_rounds", 0))
        grep_files = _int(record.get("grep_files_with_matches", 0))
        grep_content = _int(record.get("grep_content_mode", 0))
        avg_glob_res = _float(record.get("avg_glob_results", 0.0))
        successful = _int(record.get("successful_searches", 0))
        failed = _int(record.get("failed_searches", 0))

        if glob_calls > 0 or grep_calls > 0 or task_delegations > 0:
            sessions_with_searches += 1

        total_glob += glob_calls
        total_grep += grep_calls
        total_task_delegations += task_delegations
        simple_searches += simple
        moderate_searches += moderate
        complex_searches += complex_s
        simple_delegated += simple_del
        complex_direct += complex_dir
        sequences_2_3 += seq_2_3
        sequences_gt_3 += seq_gt_3
        grep_files_mode += grep_files
        grep_content_mode += grep_content
        successful_searches += successful
        failed_searches += failed

        if avg_glob_res > 0:
            glob_results.append(avg_glob_res)

        # Calculate average iterations per search
        total_search_ops = glob_calls + grep_calls + task_delegations
        if total_search_ops > 0:
            avg_iter = (simple + moderate*2 + complex_s*3) / total_search_ops if simple + moderate + complex_s > 0 else 1.0
            search_iterations.append(avg_iter)

        # Calculate session score
        session_score = _calculate_session_score(
            simple_searches=simple,
            complex_searches=complex_s,
            simple_delegated=simple_del,
            complex_direct=complex_dir,
            sequences_gt_3=seq_gt_3,
            grep_files_mode=grep_files,
            grep_content_mode=grep_content,
        )
        session_scores.append(session_score)

        if session_score > 0.8:
            high_appropriateness_sessions += 1
        elif session_score < 0.5:
            low_appropriateness_sessions += 1

    # Calculate rates
    total_searches = simple_searches + moderate_searches + complex_searches
    total_direct_searches = total_glob + total_grep

    # Delegation appropriateness: (correctly delegated complex + correctly direct simple) / total
    correctly_delegated_complex = max(0, complex_searches - complex_direct)
    correctly_direct_simple = max(0, simple_searches - simple_delegated)
    appropriate_selections = correctly_delegated_complex + correctly_direct_simple
    delegation_appropriateness = _percentage(appropriate_selections, total_searches)

    wasteful_delegation_rate = _percentage(simple_delegated, simple_searches)
    missing_delegation_rate = _percentage(complex_direct, complex_searches)

    total_sequences = sequences_2_3 + sequences_gt_3
    extended_sequence_rate = _percentage(sequences_gt_3, total_sequences)

    total_grep = grep_files_mode + grep_content_mode
    grep_output_mode_appropriateness = _percentage(grep_files_mode, total_grep)

    avg_glob_results_val = _average(glob_results)

    total_search_attempts = successful_searches + failed_searches
    search_success_rate = _percentage(successful_searches, total_search_attempts)

    avg_search_iterations_val = _average(search_iterations)

    # Calculate overall score
    tool_selection_score = _calculate_pack_score(
        delegation_appropriateness=delegation_appropriateness,
        wasteful_delegation_rate=wasteful_delegation_rate,
        missing_delegation_rate=missing_delegation_rate,
        extended_sequence_rate=extended_sequence_rate,
        grep_output_mode_appropriateness=grep_output_mode_appropriateness,
    )

    return {
        "total_sessions": total_sessions,
        "sessions_with_searches": sessions_with_searches,
        "total_searches": total_direct_searches + total_task_delegations,
        "delegation_appropriateness_score": delegation_appropriateness,
        "wasteful_delegation_rate": wasteful_delegation_rate,
        "missing_delegation_rate": missing_delegation_rate,
        "extended_sequence_rate": extended_sequence_rate,
        "grep_output_mode_appropriateness": grep_output_mode_appropriateness,
        "avg_glob_results": avg_glob_results_val,
        "search_success_rate": search_success_rate,
        "avg_search_iterations": avg_search_iterations_val,
        "tool_selection_score": tool_selection_score,
        "high_appropriateness_sessions": high_appropriateness_sessions,
        "low_appropriateness_sessions": low_appropriateness_sessions,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sessions": 0,
        "sessions_with_searches": 0,
        "total_searches": 0,
        "delegation_appropriateness_score": 0.0,
        "wasteful_delegation_rate": 0.0,
        "missing_delegation_rate": 0.0,
        "extended_sequence_rate": 0.0,
        "grep_output_mode_appropriateness": 0.0,
        "avg_glob_results": 0.0,
        "search_success_rate": 0.0,
        "avg_search_iterations": 0.0,
        "tool_selection_score": 0.0,
        "high_appropriateness_sessions": 0,
        "low_appropriateness_sessions": 0,
    }


def _int(value: object) -> int:
    """Convert value to int, returning 0 for invalid values."""
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    """Convert value to float, returning 0.0 for invalid values."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _calculate_session_score(
    simple_searches: int,
    complex_searches: int,
    simple_delegated: int,
    complex_direct: int,
    sequences_gt_3: int,
    grep_files_mode: int,
    grep_content_mode: int,
) -> float:
    """Calculate session-level delegation appropriateness score (0-1).

    Scoring components:
    - Low wasteful delegation (0-0.35): <10% simple delegated to Task
    - Low missing delegation (0-0.30): <10% complex using direct search
    - Low extended sequences (0-0.20): <15% sequences with 3+ rounds
    - Appropriate Grep mode (0-0.15): >70% using files_with_matches

    Returns:
        Session score from 0.0 to 1.0
    """
    score = 0.0

    # Low wasteful delegation component (0-0.35)
    if simple_searches > 0:
        wasteful_rate = _percentage(simple_delegated, simple_searches)
        if wasteful_rate <= 10:
            score += 0.35
        elif wasteful_rate <= 20:
            score += 0.25
        elif wasteful_rate <= 30:
            score += 0.15
    else:
        score += 0.35  # No simple searches = no wasteful delegation

    # Low missing delegation component (0-0.30)
    if complex_searches > 0:
        missing_rate = _percentage(complex_direct, complex_searches)
        if missing_rate <= 10:
            score += 0.30
        elif missing_rate <= 20:
            score += 0.20
        elif missing_rate <= 35:
            score += 0.10
    else:
        score += 0.30  # No complex searches = no missing delegation

    # Low extended sequences component (0-0.20)
    if sequences_gt_3 > 0:
        # Extended sequences are anti-pattern, reduce score
        if sequences_gt_3 <= 2:
            score += 0.15
        elif sequences_gt_3 <= 5:
            score += 0.10
        elif sequences_gt_3 <= 10:
            score += 0.05
    else:
        score += 0.20  # No extended sequences

    # Appropriate Grep mode component (0-0.15)
    total_grep = grep_files_mode + grep_content_mode
    if total_grep > 0:
        files_mode_rate = _percentage(grep_files_mode, total_grep)
        if files_mode_rate >= 70:
            score += 0.15
        elif files_mode_rate >= 50:
            score += 0.10
        elif files_mode_rate >= 30:
            score += 0.05
    else:
        score += 0.15  # No grep calls

    return round(score, 3)


def _calculate_pack_score(
    delegation_appropriateness: float,
    wasteful_delegation_rate: float,
    missing_delegation_rate: float,
    extended_sequence_rate: float,
    grep_output_mode_appropriateness: float,
) -> float:
    """Calculate overall pack delegation appropriateness score (0-1).

    Scoring components:
    - Delegation appropriateness (0-0.35): >80% correct tool selection
    - Low wasteful delegation (0-0.25): <10% simple delegated
    - Low missing delegation (0-0.20): <10% complex not delegated
    - Low extended sequences (0-0.10): <15% sequences with 3+ rounds
    - Grep mode appropriateness (0-0.10): >70% files_with_matches

    Returns:
        Pack score from 0.0 to 1.0
    """
    score = 0.0

    # Delegation appropriateness component (0-0.35)
    if delegation_appropriateness >= 80:
        score += 0.35
    elif delegation_appropriateness >= 70:
        score += 0.25
    elif delegation_appropriateness >= 60:
        score += 0.15

    # Low wasteful delegation component (0-0.25)
    if wasteful_delegation_rate <= 10:
        score += 0.25
    elif wasteful_delegation_rate <= 20:
        score += 0.18
    elif wasteful_delegation_rate <= 30:
        score += 0.10

    # Low missing delegation component (0-0.20)
    if missing_delegation_rate <= 10:
        score += 0.20
    elif missing_delegation_rate <= 20:
        score += 0.14
    elif missing_delegation_rate <= 35:
        score += 0.08

    # Low extended sequence component (0-0.10)
    if extended_sequence_rate <= 15:
        score += 0.10
    elif extended_sequence_rate <= 25:
        score += 0.07
    elif extended_sequence_rate <= 40:
        score += 0.04

    # Grep mode appropriateness (0-0.10)
    if grep_output_mode_appropriateness >= 70:
        score += 0.10
    elif grep_output_mode_appropriateness >= 50:
        score += 0.07
    elif grep_output_mode_appropriateness >= 30:
        score += 0.04

    return round(max(0.0, min(1.0, score)), 3)
