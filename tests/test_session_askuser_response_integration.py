"""Tests for session AskUserQuestion response integration analyzer."""

from __future__ import annotations

import pytest

from synthesis.session_askuser_response_integration import (
    analyze_session_askuser_response_integration,
)


class TestAnalyzeSessionAskuserResponseIntegration:
    """Tests for analyze_session_askuser_response_integration."""

    def test_empty_records_returns_zero_metrics(self) -> None:
        result = analyze_session_askuser_response_integration([])
        assert result["total_sessions"] == 0
        assert result["total_askuser_calls"] == 0
        assert result["responses_acted_on"] == 0
        assert result["response_integration_rate"] == 0.0
        assert result["avg_actions_after_response"] == 0.0
        assert result["ignored_responses"] == 0
        assert result["ignored_response_rate"] == 0.0
        assert result["clarification_chains"] == 0
        assert result["clarification_chain_rate"] == 0.0
        assert result["askuser_response_integration_score"] == 0.0
        assert result["high_quality_sessions"] == 0
        assert result["low_quality_sessions"] == 0

    def test_none_records_returns_zero_metrics(self) -> None:
        result = analyze_session_askuser_response_integration(None)
        assert result["total_sessions"] == 0
        assert result["total_askuser_calls"] == 0
        assert result["askuser_response_integration_score"] == 0.0

    def test_invalid_input_raises_value_error(self) -> None:
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_askuser_response_integration("not a list")
        with pytest.raises(ValueError, match="records must be a list of session dictionaries"):
            analyze_session_askuser_response_integration(42)

    def test_single_session_high_quality(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_askuser_calls": 5,
                "responses_acted_on": 5,
                "ignored_responses": 0,
                "clarification_chains": 1,
                "actions_after_response_values": [3, 4, 5, 3, 2],
            }
        ]
        result = analyze_session_askuser_response_integration(records)
        assert result["total_sessions"] == 1
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 0
        assert result["askuser_response_integration_score"] > 0.7

    def test_single_session_low_quality(self) -> None:
        records = [
            {
                "session_id": "s1",
                "total_askuser_calls": 10,
                "responses_acted_on": 1,
                "ignored_responses": 9,
                "clarification_chains": 8,
                "actions_after_response_values": [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
            }
        ]
        result = analyze_session_askuser_response_integration(records)
        assert result["total_sessions"] == 1
        assert result["low_quality_sessions"] == 1
        assert result["high_quality_sessions"] == 0
        assert result["askuser_response_integration_score"] < 0.4

    def test_multiple_sessions_mixed(self) -> None:
        records = [
            {
                "session_id": "high",
                "total_askuser_calls": 5,
                "responses_acted_on": 5,
                "ignored_responses": 0,
                "clarification_chains": 1,
                "actions_after_response_values": [3, 4, 5, 3, 2],
            },
            {
                "session_id": "low",
                "total_askuser_calls": 10,
                "responses_acted_on": 1,
                "ignored_responses": 9,
                "clarification_chains": 8,
                "actions_after_response_values": [0, 0, 1, 0, 0, 0, 0, 0, 0, 0],
            },
        ]
        result = analyze_session_askuser_response_integration(records)
        assert result["total_sessions"] == 2
        assert result["high_quality_sessions"] == 1
        assert result["low_quality_sessions"] == 1

    def test_zero_askuser_calls_session_gets_full_score(self) -> None:
        records = [
            {
                "session_id": "no_asks",
                "total_askuser_calls": 0,
                "responses_acted_on": 0,
                "ignored_responses": 0,
                "clarification_chains": 0,
                "actions_after_response_values": [],
            }
        ]
        result = analyze_session_askuser_response_integration(records)
        assert result["total_sessions"] == 1
        assert result["total_askuser_calls"] == 0
        assert result["askuser_response_integration_score"] == 1.0
        assert result["high_quality_sessions"] == 1

    def test_skips_non_mapping_records(self) -> None:
        records = [
            "not a dict",
            42,
            None,
            {
                "session_id": "valid",
                "total_askuser_calls": 3,
                "responses_acted_on": 3,
                "ignored_responses": 0,
                "clarification_chains": 0,
                "actions_after_response_values": [2, 3, 4],
            },
        ]
        result = analyze_session_askuser_response_integration(records)
        assert result["total_sessions"] == 1

    def test_result_keys_complete(self) -> None:
        result = analyze_session_askuser_response_integration([])
        expected_keys = {
            "total_sessions",
            "total_askuser_calls",
            "responses_acted_on",
            "response_integration_rate",
            "avg_actions_after_response",
            "ignored_responses",
            "ignored_response_rate",
            "clarification_chains",
            "clarification_chain_rate",
            "high_quality_sessions",
            "low_quality_sessions",
            "askuser_response_integration_score",
        }
        assert set(result.keys()) == expected_keys
