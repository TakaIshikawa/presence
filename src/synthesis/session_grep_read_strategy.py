"""Session Grep vs Read strategy analyzer.

Analyzes how agents balance Grep for discovery vs Read for deep context. Measures
Grep-then-targeted-Read sequences, direct full-file reads, pattern optimization,
and correlation with token consumption.

Strategy metrics:
- Grep-guided read ratio: % of Reads preceded by Grep for discovery
- Direct full-file read ratio: % of Reads without prior Grep
- Grep pattern quality: Specificity and effectiveness of patterns
- Search efficiency score: Overall search-then-read discipline
- Token correlation: Relationship between strategy and token usage

Strategy patterns:
- High Grep-guided ratio (>60%): Good search-then-read discipline
- Low direct read ratio (<20%): Minimal exploratory overhead
- High pattern specificity: Precise, targeted search patterns
- Strong token correlation: Efficient search reduces token consumption
- Optimal sequence: Grep → targeted Read with offset/limit
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_grep_read_strategy(records: object) -> dict[str, Any]:
    """Analyze Grep vs Read strategy balance in agent sessions.

    Evaluates search discipline through Grep-then-Read sequences and
    pattern quality, correlating with token efficiency.

    Args:
        records: List of tool sequence dictionaries with keys:
            - sequence_index: Sequential number
            - tool_calls: List of tool call dicts with:
                - tool_name: "Grep", "Read", "Glob", etc.
                - parameters: Tool parameters
                - timestamp: When tool was called
                - tokens_used: Optional tokens consumed
            - is_grep_guided: Boolean if Read was preceded by Grep
            - pattern_specificity: Score 0-1 for Grep pattern quality
            - total_tokens: Optional total tokens for sequence

    Returns:
        Dict with:
            - total_sequences: Total tool sequences analyzed
            - total_grep_calls: Total Grep invocations
            - total_read_calls: Total Read invocations
            - grep_guided_read_ratio: % of Reads preceded by Grep
            - direct_read_ratio: % of Reads without Grep
            - avg_pattern_specificity: Average Grep pattern quality (0-1)
            - optimal_sequence_ratio: % of Grep→targeted-Read sequences
            - inefficient_pattern_ratio: % of overly broad/narrow patterns
            - search_efficiency_score: Overall score 0-100
            - token_correlation: Correlation between strategy and tokens
            - avg_tokens_grep_guided: Avg tokens for Grep-guided sequences
            - avg_tokens_direct: Avg tokens for direct read sequences

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of tool sequence dictionaries")

    if not records:
        return _empty_result()

    total_sequences = 0
    total_grep_calls = 0
    total_read_calls = 0
    grep_guided_reads = 0
    direct_reads = 0
    pattern_specificities: list[float] = []
    optimal_sequences = 0
    inefficient_patterns = 0

    grep_guided_tokens: list[int] = []
    direct_read_tokens: list[int] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sequences += 1

        tool_calls = record.get("tool_calls")
        if not isinstance(tool_calls, list):
            continue

        # Count tool types
        sequence_grep_count = 0
        sequence_read_count = 0

        for call in tool_calls:
            if not isinstance(call, Mapping):
                continue

            tool_name = _string(call.get("tool_name"))
            if tool_name == "Grep":
                sequence_grep_count += 1
                total_grep_calls += 1
            elif tool_name == "Read":
                sequence_read_count += 1
                total_read_calls += 1

        # Check if this is a Grep-guided read
        is_grep_guided = record.get("is_grep_guided")
        if is_grep_guided is True:
            grep_guided_reads += sequence_read_count
        elif sequence_read_count > 0:
            direct_reads += sequence_read_count

        # Track pattern specificity
        pattern_spec = _float(record.get("pattern_specificity"))
        if pattern_spec is not None:
            pattern_specificities.append(pattern_spec)

            # Mark inefficient patterns (too broad <0.3 or too narrow >0.9)
            if pattern_spec < 0.3 or pattern_spec > 0.9:
                inefficient_patterns += 1

        # Check for optimal sequence (Grep → targeted Read)
        if is_grep_guided and sequence_read_count > 0:
            # Check if reads use offset/limit
            for call in tool_calls:
                if not isinstance(call, Mapping):
                    continue

                tool_name = _string(call.get("tool_name"))
                if tool_name == "Read":
                    params = call.get("parameters")
                    if isinstance(params, Mapping):
                        has_offset_limit = (
                            params.get("offset") is not None or
                            params.get("limit") is not None
                        )
                        if has_offset_limit:
                            optimal_sequences += 1

        # Track tokens by strategy
        tokens = _int(record.get("total_tokens"))
        if tokens is not None and tokens > 0:
            if is_grep_guided is True:
                grep_guided_tokens.append(tokens)
            elif sequence_read_count > 0:
                direct_read_tokens.append(tokens)

    # Calculate aggregate metrics
    total_reads_analyzed = grep_guided_reads + direct_reads
    grep_guided_ratio = _percentage(grep_guided_reads, total_reads_analyzed)
    direct_read_ratio = _percentage(direct_reads, total_reads_analyzed)

    avg_pattern_specificity = _average(pattern_specificities)

    optimal_sequence_ratio = _percentage(optimal_sequences, total_sequences)
    inefficient_pattern_ratio = _percentage(
        inefficient_patterns, len(pattern_specificities) if pattern_specificities else 0
    )

    # Calculate search efficiency score (0-100)
    efficiency_score = _calculate_efficiency_score(
        grep_guided_ratio,
        direct_read_ratio,
        avg_pattern_specificity,
        optimal_sequence_ratio,
    )

    # Calculate token correlation
    token_correlation = _calculate_token_correlation(
        grep_guided_tokens, direct_read_tokens
    )

    avg_tokens_grep_guided = _average([float(t) for t in grep_guided_tokens])
    avg_tokens_direct = _average([float(t) for t in direct_read_tokens])

    return {
        "total_sequences": total_sequences,
        "total_grep_calls": total_grep_calls,
        "total_read_calls": total_read_calls,
        "grep_guided_read_ratio": grep_guided_ratio,
        "direct_read_ratio": direct_read_ratio,
        "avg_pattern_specificity": avg_pattern_specificity,
        "optimal_sequence_ratio": optimal_sequence_ratio,
        "inefficient_pattern_ratio": inefficient_pattern_ratio,
        "search_efficiency_score": efficiency_score,
        "token_correlation": token_correlation,
        "avg_tokens_grep_guided": avg_tokens_grep_guided,
        "avg_tokens_direct": avg_tokens_direct,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_sequences": 0,
        "total_grep_calls": 0,
        "total_read_calls": 0,
        "grep_guided_read_ratio": 0.0,
        "direct_read_ratio": 0.0,
        "avg_pattern_specificity": 0.0,
        "optimal_sequence_ratio": 0.0,
        "inefficient_pattern_ratio": 0.0,
        "search_efficiency_score": 0.0,
        "token_correlation": 0.0,
        "avg_tokens_grep_guided": 0.0,
        "avg_tokens_direct": 0.0,
    }


def _calculate_efficiency_score(
    grep_guided_ratio: float,
    direct_read_ratio: float,
    pattern_specificity: float,
    optimal_sequence_ratio: float,
) -> float:
    """Calculate search efficiency score (0-100).

    Args:
        grep_guided_ratio: % of Grep-guided reads
        direct_read_ratio: % of direct reads
        pattern_specificity: Average pattern quality (0-1)
        optimal_sequence_ratio: % of optimal sequences

    Returns:
        Efficiency score 0-100
    """
    # 40 points: High Grep-guided ratio (target >60%)
    grep_component = (grep_guided_ratio / 100.0) * 40.0

    # 30 points: Good pattern specificity (target 0.4-0.7 range)
    # Penalize too broad (<0.3) or too narrow (>0.9)
    if 0.4 <= pattern_specificity <= 0.7:
        pattern_component = 30.0
    elif 0.3 <= pattern_specificity < 0.4 or 0.7 < pattern_specificity <= 0.9:
        pattern_component = 20.0
    else:
        pattern_component = 10.0

    # 30 points: High optimal sequence ratio
    optimal_component = (optimal_sequence_ratio / 100.0) * 30.0

    score = grep_component + pattern_component + optimal_component
    return round(max(0.0, min(100.0, score)), 2)


def _calculate_token_correlation(
    grep_guided_tokens: list[int],
    direct_tokens: list[int],
) -> float:
    """Calculate correlation between strategy and token usage.

    Negative correlation means Grep-guided uses fewer tokens.

    Args:
        grep_guided_tokens: Tokens for Grep-guided sequences
        direct_tokens: Tokens for direct read sequences

    Returns:
        Correlation coefficient -1.0 to 1.0
    """
    if not grep_guided_tokens or not direct_tokens:
        return 0.0

    avg_grep = sum(grep_guided_tokens) / len(grep_guided_tokens)
    avg_direct = sum(direct_tokens) / len(direct_tokens)

    if avg_grep < avg_direct:
        # Grep-guided is more efficient
        diff_ratio = (avg_direct - avg_grep) / avg_direct
        return round(-diff_ratio, 2)
    elif avg_grep > avg_direct:
        # Direct is more efficient
        diff_ratio = (avg_grep - avg_direct) / avg_grep
        return round(diff_ratio, 2)
    else:
        return 0.0


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace.

    Args:
        value: Value to convert

    Returns:
        String value
    """
    return value.strip() if isinstance(value, str) else ""


def _int(value: object) -> int | None:
    """Convert value to int.

    Args:
        value: Value to convert

    Returns:
        Int value, or None if invalid
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
    return None


def _float(value: object) -> float | None:
    """Convert value to float.

    Args:
        value: Value to convert

    Returns:
        Float value, or None if invalid
    """
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator.

    Args:
        numerator: Numerator value
        denominator: Denominator value

    Returns:
        Percentage value (0.0-100.0)
    """
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[float]) -> float:
    """Calculate average of numeric values.

    Args:
        values: List of numeric values

    Returns:
        Average value
    """
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
