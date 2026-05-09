"""Session context awareness and state tracking analyzer.

Evaluates how well agents maintain context and track state across turns in a
session. Detects repeated questions, redundant file exploration, lost context,
and state consistency issues. Measures effective reuse of prior information vs
wasteful re-exploration.

Context awareness metrics:
- Repeated information requests: Same questions/requests within session
- File re-exploration: Reading same file multiple times without intervening edits
- Lost context instances: Repeating work already done
- Redundant searches: Duplicate grep/glob patterns
- State consistency: Working directory and variable tracking
- Reference usage: Citing prior results vs re-fetching

Anti-patterns detected:
- Forgetting prior decisions or user responses
- Re-reading unchanged files repeatedly
- Duplicate grep searches with same pattern
- Asking user same question multiple times
- Re-exploring code paths already examined

Quality indicators:
- High context awareness score (>0.8): Good memory and state tracking
- Low redundant read rate (<15%): Efficient file access
- High reference usage (>60%): Cites prior results effectively
- Low lost context instances (<5%): Maintains session continuity
- Good state consistency (>0.9): Tracks state accurately
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_context_awareness(records: object) -> dict[str, Any]:
    """Analyze context awareness and state tracking patterns in agent sessions.

    Evaluates how effectively the agent maintains context across turns,
    avoiding redundant exploration and maintaining state consistency.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - tool_name: Name of the tool used (Read, Grep, AskUserQuestion, etc.)
            - file_path: For Read tool, path to file
            - pattern: For Grep tool, search pattern
            - question: For AskUserQuestion tool, question text
            - working_directory: Current working directory
            - references_prior_context: Boolean if references prior turn results
            - repeats_prior_request: Boolean if duplicates earlier request
            - file_was_edited: Boolean if file was edited since last read
            - lost_context_indicator: Boolean if shows signs of forgetting prior work
            - state_inconsistency: Boolean if state tracking error detected
            - cites_prior_result: Boolean if explicitly cites prior tool result

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - total_tool_calls: Total tool calls made

            File exploration metrics:
            - read_call_count: Total Read tool calls
            - unique_files_read: Number of unique files read
            - redundant_read_count: Re-reads of unchanged files
            - redundant_read_rate: Percentage of reads that are redundant
            - files_read_multiple_times: Files read 2+ times
            - max_file_read_count: Maximum times any single file was read

            Search pattern metrics:
            - grep_call_count: Total Grep tool calls
            - unique_grep_patterns: Number of unique search patterns
            - duplicate_grep_count: Grep calls with duplicate patterns
            - duplicate_grep_rate: Percentage of grep calls that are duplicates

            User interaction metrics:
            - askuser_call_count: Total AskUserQuestion calls
            - repeated_questions_count: Questions asked multiple times
            - repeated_question_rate: Percentage of repeated questions

            Context usage metrics:
            - context_references: Tool calls referencing prior context
            - context_reference_rate: Percentage of calls using prior context
            - prior_result_citations: Tool calls citing specific prior results
            - citation_rate: Percentage of calls that cite prior results

            Lost context detection:
            - repeated_request_count: Requests duplicating prior requests
            - lost_context_instances: Signs of forgetting prior work
            - lost_context_rate: Percentage of turns showing lost context

            State consistency:
            - working_directory_changes: Number of directory changes
            - state_inconsistencies: Detected state tracking errors
            - state_consistency_score: 0-1 score for state tracking accuracy

            Anti-pattern detection:
            - total_anti_patterns: Sum of all anti-pattern occurrences
            - anti_pattern_rate: Percentage of actions that are anti-patterns

            Overall scores:
            - context_awareness_score: Overall awareness metric (0.0-1.0)
            - efficiency_score: Efficiency of information gathering (0.0-1.0)

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    # Basic counts
    total_turns = 0
    total_tool_calls = 0

    # File exploration tracking
    read_call_count = 0
    read_files: dict[str, list[int]] = {}  # file_path -> list of turn indices
    redundant_read_count = 0
    edited_files: set[str] = set()  # Files that have been edited

    # Search pattern tracking
    grep_call_count = 0
    grep_patterns: dict[str, list[int]] = {}  # pattern -> list of turn indices
    duplicate_grep_count = 0

    # User interaction tracking
    askuser_call_count = 0
    asked_questions: dict[str, list[int]] = {}  # question -> list of turn indices
    repeated_questions_count = 0

    # Context usage tracking
    context_references = 0
    prior_result_citations = 0
    repeated_request_count = 0
    lost_context_instances = 0

    # State consistency tracking
    working_directory_changes = 0
    state_inconsistencies = 0
    last_working_directory: str | None = None

    for record in records:
        total_turns += 1

        if not isinstance(record, Mapping):
            continue

        tool_name = _string(record.get("tool_name")).lower()
        if not tool_name:
            continue

        total_tool_calls += 1
        turn_index = record.get("turn_index", total_turns - 1)

        # Track context references
        if _bool(record.get("references_prior_context")):
            context_references += 1

        # Track prior result citations
        if _bool(record.get("cites_prior_result")):
            prior_result_citations += 1

        # Track repeated requests
        if _bool(record.get("repeats_prior_request")):
            repeated_request_count += 1

        # Track lost context indicators
        if _bool(record.get("lost_context_indicator")):
            lost_context_instances += 1

        # Track state inconsistencies
        if _bool(record.get("state_inconsistency")):
            state_inconsistencies += 1

        # Track working directory changes
        current_wd = record.get("working_directory")
        if current_wd and last_working_directory is not None:
            if current_wd != last_working_directory:
                working_directory_changes += 1
        if current_wd:
            last_working_directory = current_wd

        # Analyze Read tool usage
        if tool_name == "read":
            read_call_count += 1
            file_path = _string(record.get("file_path"))

            if file_path:
                if file_path not in read_files:
                    read_files[file_path] = []
                read_files[file_path].append(turn_index)

                # Check if this is a redundant read
                if len(read_files[file_path]) > 1:
                    # Only redundant if file hasn't been edited since last read
                    file_was_edited = _bool(record.get("file_was_edited"))
                    if file_was_edited:
                        edited_files.add(file_path)
                    elif file_path not in edited_files:
                        redundant_read_count += 1

        # Analyze Grep tool usage
        elif tool_name == "grep":
            grep_call_count += 1
            pattern = _string(record.get("pattern"))

            if pattern:
                if pattern not in grep_patterns:
                    grep_patterns[pattern] = []
                grep_patterns[pattern].append(turn_index)

                # Check if this is a duplicate grep
                if len(grep_patterns[pattern]) > 1:
                    duplicate_grep_count += 1

        # Analyze AskUserQuestion usage
        elif tool_name == "askuserquestion":
            askuser_call_count += 1
            question = _string(record.get("question"))

            if question:
                # Normalize question for comparison
                normalized_question = question.lower().strip()
                if normalized_question not in asked_questions:
                    asked_questions[normalized_question] = []
                asked_questions[normalized_question].append(turn_index)

                # Check if this is a repeated question
                if len(asked_questions[normalized_question]) > 1:
                    repeated_questions_count += 1

        # Track file edits
        if tool_name in ("edit", "write"):
            file_path = _string(record.get("file_path"))
            if file_path:
                edited_files.add(file_path)

    # Calculate derived metrics
    unique_files_read = len(read_files)
    files_read_multiple_times = sum(1 for reads in read_files.values() if len(reads) > 1)
    max_file_read_count = max((len(reads) for reads in read_files.values()), default=0)

    redundant_read_rate = _percentage(redundant_read_count, read_call_count)

    unique_grep_patterns = len(grep_patterns)
    duplicate_grep_rate = _percentage(duplicate_grep_count, grep_call_count)

    repeated_question_rate = _percentage(repeated_questions_count, askuser_call_count)

    context_reference_rate = _percentage(context_references, total_tool_calls)
    citation_rate = _percentage(prior_result_citations, total_tool_calls)

    lost_context_rate = _percentage(lost_context_instances, total_turns)

    # Calculate state consistency score
    state_consistency_score = _calculate_state_consistency_score(
        state_inconsistencies,
        total_tool_calls,
    )

    # Calculate anti-patterns
    total_anti_patterns = (
        redundant_read_count +
        duplicate_grep_count +
        repeated_questions_count +
        lost_context_instances +
        state_inconsistencies
    )
    anti_pattern_rate = _percentage(total_anti_patterns, total_tool_calls)

    # Calculate overall scores
    context_awareness_score = _calculate_context_awareness_score(
        context_reference_rate,
        citation_rate,
        lost_context_rate,
        state_consistency_score,
    )

    efficiency_score = _calculate_efficiency_score(
        redundant_read_rate,
        duplicate_grep_rate,
        repeated_question_rate,
        anti_pattern_rate,
    )

    return {
        # Basic metrics
        "total_turns": total_turns,
        "total_tool_calls": total_tool_calls,

        # File exploration metrics
        "read_call_count": read_call_count,
        "unique_files_read": unique_files_read,
        "redundant_read_count": redundant_read_count,
        "redundant_read_rate": redundant_read_rate,
        "files_read_multiple_times": files_read_multiple_times,
        "max_file_read_count": max_file_read_count,

        # Search pattern metrics
        "grep_call_count": grep_call_count,
        "unique_grep_patterns": unique_grep_patterns,
        "duplicate_grep_count": duplicate_grep_count,
        "duplicate_grep_rate": duplicate_grep_rate,

        # User interaction metrics
        "askuser_call_count": askuser_call_count,
        "repeated_questions_count": repeated_questions_count,
        "repeated_question_rate": repeated_question_rate,

        # Context usage metrics
        "context_references": context_references,
        "context_reference_rate": context_reference_rate,
        "prior_result_citations": prior_result_citations,
        "citation_rate": citation_rate,

        # Lost context detection
        "repeated_request_count": repeated_request_count,
        "lost_context_instances": lost_context_instances,
        "lost_context_rate": lost_context_rate,

        # State consistency
        "working_directory_changes": working_directory_changes,
        "state_inconsistencies": state_inconsistencies,
        "state_consistency_score": state_consistency_score,

        # Anti-pattern detection
        "total_anti_patterns": total_anti_patterns,
        "anti_pattern_rate": anti_pattern_rate,

        # Overall scores
        "context_awareness_score": context_awareness_score,
        "efficiency_score": efficiency_score,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "total_tool_calls": 0,
        "read_call_count": 0,
        "unique_files_read": 0,
        "redundant_read_count": 0,
        "redundant_read_rate": 0.0,
        "files_read_multiple_times": 0,
        "max_file_read_count": 0,
        "grep_call_count": 0,
        "unique_grep_patterns": 0,
        "duplicate_grep_count": 0,
        "duplicate_grep_rate": 0.0,
        "askuser_call_count": 0,
        "repeated_questions_count": 0,
        "repeated_question_rate": 0.0,
        "context_references": 0,
        "context_reference_rate": 0.0,
        "prior_result_citations": 0,
        "citation_rate": 0.0,
        "repeated_request_count": 0,
        "lost_context_instances": 0,
        "lost_context_rate": 0.0,
        "working_directory_changes": 0,
        "state_inconsistencies": 0,
        "state_consistency_score": 1.0,
        "total_anti_patterns": 0,
        "anti_pattern_rate": 0.0,
        "context_awareness_score": 1.0,
        "efficiency_score": 1.0,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _bool(value: object) -> bool:
    """Convert value to boolean."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() in ("true", "yes", "1")
    return bool(value)


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, returning 0.0 if denominator is 0."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _calculate_state_consistency_score(
    state_inconsistencies: int,
    total_tool_calls: int,
) -> float:
    """Calculate state consistency score (0-1).

    High score indicates good state tracking with few inconsistencies.
    Target: <2% inconsistency rate.
    """
    if total_tool_calls == 0:
        return 1.0

    inconsistency_rate = (state_inconsistencies / total_tool_calls) * 100.0

    if inconsistency_rate <= 2.0:
        return 1.0
    elif inconsistency_rate >= 20.0:
        return 0.0
    else:
        # Linear decay from 1.0 at 2% to 0.0 at 20%
        return round(1.0 - ((inconsistency_rate - 2.0) / 18.0), 3)


def _calculate_context_awareness_score(
    context_reference_rate: float,
    citation_rate: float,
    lost_context_rate: float,
    state_consistency_score: float,
) -> float:
    """Calculate overall context awareness score (0-1).

    High score indicates good context maintenance:
    - High context reference rate (target: >50%)
    - High citation rate (target: >40%)
    - Low lost context rate (target: <5%)
    - High state consistency (target: >0.9)
    """
    # Context reference component (0-0.30)
    # Target: >50% of tool calls reference prior context
    context_ref_normalized = min(context_reference_rate / 50.0, 1.0)
    context_ref_component = context_ref_normalized * 0.30

    # Citation component (0-0.25)
    # Target: >40% of tool calls cite prior results
    citation_normalized = min(citation_rate / 40.0, 1.0)
    citation_component = citation_normalized * 0.25

    # Lost context avoidance component (0-0.25)
    # Target: <5% lost context rate
    if lost_context_rate <= 5.0:
        lost_context_component = 0.25
    else:
        penalty = min(lost_context_rate - 5.0, 45.0) / 45.0
        lost_context_component = 0.25 * (1.0 - penalty)

    # State consistency component (0-0.20)
    state_component = state_consistency_score * 0.20

    score = (
        context_ref_component +
        citation_component +
        lost_context_component +
        state_component
    )
    return round(max(0.0, min(1.0, score)), 3)


def _calculate_efficiency_score(
    redundant_read_rate: float,
    duplicate_grep_rate: float,
    repeated_question_rate: float,
    anti_pattern_rate: float,
) -> float:
    """Calculate information gathering efficiency score (0-1).

    High score indicates efficient exploration without redundancy:
    - Low redundant read rate (target: <15%)
    - Low duplicate grep rate (target: <10%)
    - Low repeated question rate (target: <5%)
    - Low overall anti-pattern rate (target: <10%)
    """
    # Redundant read avoidance (0-0.30)
    if redundant_read_rate <= 15.0:
        read_component = 0.30
    else:
        penalty = min(redundant_read_rate - 15.0, 85.0) / 85.0
        read_component = 0.30 * (1.0 - penalty)

    # Duplicate grep avoidance (0-0.25)
    if duplicate_grep_rate <= 10.0:
        grep_component = 0.25
    else:
        penalty = min(duplicate_grep_rate - 10.0, 90.0) / 90.0
        grep_component = 0.25 * (1.0 - penalty)

    # Repeated question avoidance (0-0.20)
    if repeated_question_rate <= 5.0:
        question_component = 0.20
    else:
        penalty = min(repeated_question_rate - 5.0, 95.0) / 95.0
        question_component = 0.20 * (1.0 - penalty)

    # Overall anti-pattern avoidance (0-0.25)
    if anti_pattern_rate <= 10.0:
        anti_pattern_component = 0.25
    else:
        penalty = min(anti_pattern_rate - 10.0, 90.0) / 90.0
        anti_pattern_component = 0.25 * (1.0 - penalty)

    score = (
        read_component +
        grep_component +
        question_component +
        anti_pattern_component
    )
    return round(max(0.0, min(1.0, score)), 3)
