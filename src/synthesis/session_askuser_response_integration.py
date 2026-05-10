"""Session AskUserQuestion response integration analyzer.

Measures how effectively an agent integrates user answers from
AskUserQuestion into subsequent actions.

Dimensions: response integration rate, ignored responses,
clarification chains, action density after response.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def _int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def analyze_session_askuser_response_integration(records: object) -> dict[str, Any]:
    """Analyze AskUserQuestion response integration across sessions."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of session dictionaries")

    session_scores: list[float] = []

    total_sessions = 0
    agg_askuser_calls = 0
    agg_responses_acted_on = 0
    agg_ignored_responses = 0
    agg_clarification_chains = 0
    all_actions_after_response: list[int] = []

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_sessions += 1

        askuser_calls = _int(record.get("total_askuser_calls"))
        responses_acted_on = _int(record.get("responses_acted_on"))
        ignored_responses = _int(record.get("ignored_responses"))
        clarification_chains = _int(record.get("clarification_chains"))

        agg_askuser_calls += askuser_calls
        agg_responses_acted_on += responses_acted_on
        agg_ignored_responses += ignored_responses
        agg_clarification_chains += clarification_chains

        actions_values = record.get("actions_after_response_values")
        if isinstance(actions_values, list):
            all_actions_after_response.extend(actions_values)

        # Session score components
        if askuser_calls == 0:
            # No AskUserQuestion calls means no bad pattern
            session_scores.append(1.0)
            continue

        # Response integration rate (0-0.40): higher is better, baseline 0.80
        integration_ratio = responses_acted_on / askuser_calls if askuser_calls > 0 else 0.0
        integration_score = min(integration_ratio / 0.80, 1.0) * 0.40

        # Low ignored response rate (0-0.30): lower ignored is better
        ignored_ratio = ignored_responses / askuser_calls if askuser_calls > 0 else 0.0
        ignored_score = (1.0 - min(ignored_ratio / 0.50, 1.0)) * 0.30

        # Appropriate clarification chains (0-0.15): some follow-up is good, excessive is bad
        chain_ratio = clarification_chains / askuser_calls if askuser_calls > 0 else 0.0
        # Optimal is around 0.1-0.3; penalize both 0 and >0.5
        if chain_ratio <= 0.30:
            chain_score = min(chain_ratio / 0.30, 1.0) * 0.15
        else:
            # Penalize excessive chaining
            chain_score = max(0.0, (1.0 - (chain_ratio - 0.30) / 0.40)) * 0.15

        # Action density after response (0-0.15): at least 2 actions after each answer
        if isinstance(actions_values, list) and actions_values:
            avg_actions = sum(actions_values) / len(actions_values)
            density_score = min(avg_actions / 2.0, 1.0) * 0.15
        else:
            # No data — neutral
            density_score = 0.075

        session_score = round(
            integration_score + ignored_score + chain_score + density_score, 4
        )
        session_scores.append(session_score)

    # Aggregate metrics
    response_integration_rate = _percentage(agg_responses_acted_on, agg_askuser_calls)
    ignored_response_rate = _percentage(agg_ignored_responses, agg_askuser_calls)
    clarification_chain_rate = _percentage(agg_clarification_chains, agg_askuser_calls)
    avg_actions_after_response = _average(all_actions_after_response)

    high_quality_sessions = sum(1 for s in session_scores if s > 0.7)
    low_quality_sessions = sum(1 for s in session_scores if s < 0.4)

    askuser_response_integration_score = (
        round(_average(session_scores), 4) if session_scores else 0.0
    )

    return {
        "total_sessions": total_sessions,
        "total_askuser_calls": agg_askuser_calls,
        "responses_acted_on": agg_responses_acted_on,
        "response_integration_rate": response_integration_rate,
        "avg_actions_after_response": avg_actions_after_response,
        "ignored_responses": agg_ignored_responses,
        "ignored_response_rate": ignored_response_rate,
        "clarification_chains": agg_clarification_chains,
        "clarification_chain_rate": clarification_chain_rate,
        "high_quality_sessions": high_quality_sessions,
        "low_quality_sessions": low_quality_sessions,
        "askuser_response_integration_score": askuser_response_integration_score,
    }
