"""Session context window usage analyzer.

Tracks context window token consumption patterns across sessions. Measures
cumulative input tokens over session lifetime, average tokens per turn,
context window pressure score (tokens / 200k budget), and turns exceeding
high-token thresholds. Calculates efficiency ratio as output_tokens / input_tokens.

Context window usage metrics:
- Cumulative input tokens: Total tokens consumed over session
- Average tokens per turn: Mean token consumption per turn
- Context window pressure: Token usage / 200k budget ratio
- High-token turns: Turns exceeding 50k token threshold
- Efficiency ratio: Output tokens / input tokens
- Token distribution: Variance and peaks in token usage

Quality indicators:
- Low context pressure (<50%): Healthy token budget usage
- High efficiency ratio (>0.3): Good output per input token
- Few high-token turns (<10%): Consistent token usage
- Stable token distribution: Predictable consumption patterns
- Early warning detection: Approaching budget limits
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_session_context_window_usage(records: object) -> dict[str, Any]:
    """Analyze context window token consumption patterns across sessions.

    Evaluates token usage efficiency and identifies context window pressure
    and high-token turns.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: Turn number in session
            - input_tokens: Number of input tokens consumed
            - output_tokens: Number of output tokens generated
            - cumulative_input_tokens: Running total of input tokens
            - exceeds_threshold: Boolean indicating turn >50k tokens

    Returns:
        Dict with:
            - total_turns: Total number of turns analyzed
            - total_input_tokens: Sum of all input tokens
            - total_output_tokens: Sum of all output tokens
            - avg_tokens_per_turn: Average input tokens per turn
            - max_tokens_per_turn: Maximum tokens in a single turn
            - context_window_pressure_score: Tokens / 200k budget (%)
            - turns_exceeding_50k: Count of turns with >50k tokens
            - high_token_turn_rate: Percentage of turns >50k tokens
            - efficiency_ratio: Output tokens / input tokens
            - final_cumulative_tokens: Final cumulative token count
            - budget_remaining: Tokens remaining in 200k budget
            - is_approaching_limit: Boolean if >80% budget used

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")

    if not records:
        return _empty_result()

    total_turns = 0
    total_input_tokens = 0
    total_output_tokens = 0
    tokens_per_turn: list[int | float] = []
    high_token_turns = 0
    final_cumulative = 0

    # Context window budget (200k tokens)
    CONTEXT_BUDGET = 200000
    HIGH_TOKEN_THRESHOLD = 50000

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_turns += 1

        input_tokens = _extract_number(record.get("input_tokens"))
        output_tokens = _extract_number(record.get("output_tokens"))
        cumulative = _extract_number(record.get("cumulative_input_tokens"))
        exceeds_threshold = record.get("exceeds_threshold")

        # Track input tokens
        if input_tokens is not None:
            total_input_tokens += int(input_tokens)
            tokens_per_turn.append(input_tokens)

            # Check high-token threshold
            if input_tokens > HIGH_TOKEN_THRESHOLD:
                high_token_turns += 1

        # Track output tokens
        if output_tokens is not None:
            total_output_tokens += int(output_tokens)

        # Track cumulative tokens
        if cumulative is not None:
            final_cumulative = int(cumulative)

        # Track threshold exceedance
        if exceeds_threshold is True:
            if input_tokens is None or input_tokens <= HIGH_TOKEN_THRESHOLD:
                # Explicit threshold flag overrides calculation
                high_token_turns += 1

    # Calculate aggregate metrics
    avg_tokens = _average(tokens_per_turn)
    max_tokens = max(tokens_per_turn) if tokens_per_turn else 0

    # Use final cumulative if available, otherwise use total
    cumulative_tokens = final_cumulative if final_cumulative > 0 else total_input_tokens

    # Calculate context window pressure
    pressure_score = _percentage(cumulative_tokens, CONTEXT_BUDGET)

    # Calculate high-token turn rate
    high_token_rate = _percentage(high_token_turns, total_turns)

    # Calculate efficiency ratio (output / input)
    efficiency = 0.0
    if total_input_tokens > 0:
        efficiency = round(total_output_tokens / total_input_tokens, 3)

    # Calculate budget remaining
    budget_remaining = max(0, CONTEXT_BUDGET - cumulative_tokens)

    # Check if approaching limit (>80% budget used)
    is_approaching = pressure_score > 80.0

    return {
        "total_turns": total_turns,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "avg_tokens_per_turn": avg_tokens,
        "max_tokens_per_turn": max_tokens,
        "context_window_pressure_score": pressure_score,
        "turns_exceeding_50k": high_token_turns,
        "high_token_turn_rate": high_token_rate,
        "efficiency_ratio": efficiency,
        "final_cumulative_tokens": cumulative_tokens,
        "budget_remaining": budget_remaining,
        "is_approaching_limit": is_approaching,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_turns": 0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
        "avg_tokens_per_turn": 0.0,
        "max_tokens_per_turn": 0,
        "context_window_pressure_score": 0.0,
        "turns_exceeding_50k": 0,
        "high_token_turn_rate": 0.0,
        "efficiency_ratio": 0.0,
        "final_cumulative_tokens": 0,
        "budget_remaining": 200000,
        "is_approaching_limit": False,
    }


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value (int or float) if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
