"""Tests for session verification timing analyzer."""

from datetime import datetime, timedelta

import pytest

from synthesis.session_verification_timing import (
    AgentTurn,
    SessionVerificationTiming,
    VerificationTurn,
    analyze_session_verification_timing,
)


def test_empty_input_returns_stable_zero_state():
    result = analyze_session_verification_timing([], [])

    assert result.metrics.total_verifications == 0
    assert result.metrics.acknowledged_verifications == 0
    assert result.metrics.abandoned_verifications == 0
    assert result.metrics.median_latency_seconds == 0.0
    assert result.metrics.abandonment_rate == 0.0
    assert result.metrics.acknowledgement_rate == 0.0
    assert "No verification" in result.insights[0]


def test_verification_with_immediate_acknowledgment():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verification = VerificationTurn(
        turn_index=5,
        timestamp=base_time,
        command="pytest tests/",
        exit_code=0,
    )
    agent = AgentTurn(
        turn_index=6,
        timestamp=base_time + timedelta(seconds=10),
        content="All tests passed successfully.",
    )

    result = analyze_session_verification_timing([verification], [agent])

    assert result.metrics.total_verifications == 1
    assert result.metrics.acknowledged_verifications == 1
    assert result.metrics.abandoned_verifications == 0
    assert result.metrics.median_latency_seconds == 10.0
    assert result.metrics.excessive_delay_count == 0
    assert result.metrics.acknowledgement_rate == 100.0


def test_verification_with_delayed_acknowledgment():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verification = VerificationTurn(
        turn_index=5,
        timestamp=base_time,
        command="pytest tests/",
        exit_code=1,
    )
    agent = AgentTurn(
        turn_index=6,
        timestamp=base_time + timedelta(seconds=200),
        content="Tests failed with 3 errors.",
    )

    result = analyze_session_verification_timing([verification], [agent])

    assert result.metrics.total_verifications == 1
    assert result.metrics.acknowledged_verifications == 1
    assert result.metrics.median_latency_seconds == 200.0
    assert result.metrics.excessive_delay_count == 1
    assert len(result.examples) == 1
    assert result.examples[0].latency_seconds == 200.0


def test_verification_without_acknowledgment_is_abandoned():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verification = VerificationTurn(
        turn_index=5,
        timestamp=base_time,
        command="mypy src/",
        exit_code=0,
    )
    agent = AgentTurn(
        turn_index=6,
        timestamp=base_time + timedelta(seconds=5),
        content="Let me now implement the next feature.",
    )

    result = analyze_session_verification_timing([verification], [agent])

    assert result.metrics.total_verifications == 1
    assert result.metrics.acknowledged_verifications == 0
    assert result.metrics.abandoned_verifications == 1
    assert result.metrics.abandonment_rate == 100.0
    assert len(result.examples) == 1
    assert result.examples[0].abandoned is True


def test_multiple_verifications_mixed_acknowledgment():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verifications = [
        VerificationTurn(1, base_time, "pytest tests/", 0),
        VerificationTurn(3, base_time + timedelta(minutes=1), "mypy src/", 0),
        VerificationTurn(5, base_time + timedelta(minutes=2), "npm test", 1),
    ]
    agents = [
        AgentTurn(2, base_time + timedelta(seconds=15), "All tests passed."),
        AgentTurn(4, base_time + timedelta(minutes=1, seconds=20), "Type checking complete."),
        AgentTurn(6, base_time + timedelta(minutes=2, seconds=5), "Moving to next task."),
    ]

    result = analyze_session_verification_timing(verifications, agents)

    assert result.metrics.total_verifications == 3
    assert result.metrics.acknowledged_verifications == 2
    assert result.metrics.abandoned_verifications == 1
    assert result.metrics.acknowledgement_rate == pytest.approx(66.67, rel=0.01)


def test_median_latency_with_odd_number_of_samples():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verifications = [
        VerificationTurn(1, base_time, "pytest a", 0),
        VerificationTurn(3, base_time + timedelta(minutes=1), "pytest b", 0),
        VerificationTurn(5, base_time + timedelta(minutes=2), "pytest c", 0),
    ]
    agents = [
        AgentTurn(2, base_time + timedelta(seconds=10), "Test a passed."),
        AgentTurn(4, base_time + timedelta(minutes=1, seconds=30), "Test b passed."),
        AgentTurn(6, base_time + timedelta(minutes=2, seconds=20), "Test c passed."),
    ]

    result = analyze_session_verification_timing(verifications, agents)

    assert result.metrics.median_latency_seconds == 20.0


def test_median_latency_with_even_number_of_samples():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verifications = [
        VerificationTurn(1, base_time, "pytest a", 0),
        VerificationTurn(3, base_time + timedelta(minutes=1), "pytest b", 0),
    ]
    agents = [
        AgentTurn(2, base_time + timedelta(seconds=10), "Test a passed."),
        AgentTurn(4, base_time + timedelta(minutes=1, seconds=30), "Test b passed."),
    ]

    result = analyze_session_verification_timing(verifications, agents)

    assert result.metrics.median_latency_seconds == 20.0


def test_examples_capped_at_five():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verifications = [
        VerificationTurn(i * 2, base_time + timedelta(minutes=i), f"test {i}", 0)
        for i in range(10)
    ]
    agents = []  # No acknowledgments

    result = analyze_session_verification_timing(verifications, agents)

    assert len(result.examples) <= 5


def test_excessive_delay_threshold_flagging():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verifications = [
        VerificationTurn(1, base_time, "pytest fast", 0),
        VerificationTurn(3, base_time + timedelta(minutes=5), "pytest slow", 0),
    ]
    agents = [
        AgentTurn(2, base_time + timedelta(seconds=30), "Fast test passed."),
        AgentTurn(4, base_time + timedelta(minutes=5, seconds=200), "Slow test passed."),
    ]

    result = analyze_session_verification_timing(verifications, agents)

    assert result.metrics.excessive_delay_count == 1


def test_insights_include_abandonment_when_present():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verification = VerificationTurn(1, base_time, "pytest", 0)

    result = analyze_session_verification_timing([verification], [])

    assert any("ignored" in insight.lower() for insight in result.insights)


def test_insights_include_excessive_delay_when_present():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verification = VerificationTurn(1, base_time, "pytest", 0)
    agent = AgentTurn(2, base_time + timedelta(seconds=250), "Tests passed.")

    result = analyze_session_verification_timing([verification], [agent])

    assert any("excessive delay" in insight.lower() for insight in result.insights)


@pytest.mark.parametrize(
    ("verifications", "message"),
    [
        ("not_a_list", "list or tuple"),
        ([{"turn": 1}], "VerificationTurn"),
        ([VerificationTurn(-1, datetime.now(), "test", 0)], "non-negative"),
        ([VerificationTurn(1, datetime.now(), "", 0)], "not be empty"),
        (
            [
                VerificationTurn(2, datetime.now(), "a", 0),
                VerificationTurn(1, datetime.now(), "b", 0),
            ],
            "strictly increasing",
        ),
    ],
)
def test_invalid_verification_turns_raise_errors(verifications, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_verification_timing(verifications, [])


@pytest.mark.parametrize(
    ("agents", "message"),
    [
        ("not_a_list", "list or tuple"),
        ([{"turn": 1}], "AgentTurn"),
        ([AgentTurn(-1, datetime.now(), "content")], "non-negative"),
        ([AgentTurn(1, datetime.now(), "")], "not be empty"),
        (
            [
                AgentTurn(2, datetime.now(), "a"),
                AgentTurn(1, datetime.now(), "b"),
            ],
            "strictly increasing",
        ),
    ],
)
def test_invalid_agent_turns_raise_errors(agents, message):
    with pytest.raises(ValueError, match=message):
        analyze_session_verification_timing([], agents)


def test_acknowledgment_requires_verification_keywords():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verification = VerificationTurn(1, base_time, "pytest", 0)
    agent_no_keyword = AgentTurn(2, base_time + timedelta(seconds=10), "Moving forward with implementation.")
    agent_with_keyword = AgentTurn(3, base_time + timedelta(seconds=20), "Tests are now passing.")

    # Without keywords - should be abandoned
    result1 = analyze_session_verification_timing([verification], [agent_no_keyword])
    assert result1.metrics.abandoned_verifications == 1

    # With keywords - should be acknowledged
    result2 = analyze_session_verification_timing([verification], [agent_with_keyword])
    assert result2.metrics.acknowledged_verifications == 1


def test_verification_turn_ordering_by_timestamp():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    verifications = [
        VerificationTurn(1, base_time + timedelta(seconds=10), "pytest", 0),
        VerificationTurn(2, base_time, "mypy", 0),  # Earlier timestamp but later turn
    ]

    with pytest.raises(ValueError, match="ordered by timestamp"):
        analyze_session_verification_timing(verifications, [])


def test_agent_turn_ordering_by_timestamp():
    base_time = datetime(2024, 1, 1, 12, 0, 0)
    agents = [
        AgentTurn(1, base_time + timedelta(seconds=10), "later"),
        AgentTurn(2, base_time, "earlier"),  # Earlier timestamp but later turn
    ]

    with pytest.raises(ValueError, match="ordered by timestamp"):
        analyze_session_verification_timing([], agents)
