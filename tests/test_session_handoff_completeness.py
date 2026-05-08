"""Tests for session handoff completeness analyzer."""

import pytest

from engagement.session_handoff_completeness import (
    SessionHandoff,
    analyze_session_handoff_completeness,
)


def test_complete_handoff_scores_full_completeness():
    result = analyze_session_handoff_completeness(
        SessionHandoff(
            objective="Add analyzer",
            changed_files=("src/analyzer.py",),
            verification_status="pytest passed",
            blockers=("none",),
            next_steps=("commit",),
            risk_notes=("low risk",),
        )
    )

    assert result.metrics.completeness_score == 1.0
    assert result.gap_labels == ()
    assert result.quality == "complete"
    assert result.verification_state == "passed"


def test_missing_verification_has_specific_gap_and_insight():
    result = analyze_session_handoff_completeness(
        SessionHandoff(
            objective="Add analyzer",
            changed_files=("src/analyzer.py",),
            blockers=("none",),
            next_steps=("commit",),
            risk_notes=("low risk",),
        )
    )

    assert result.metrics.completeness_score == 0.833
    assert "missing_verification" in result.gap_labels
    assert any("verification" in insight for insight in result.insights)


def test_missing_next_steps_is_reported():
    result = analyze_session_handoff_completeness(
        SessionHandoff(
            objective="Fix tests",
            changed_files=("tests/test_x.py",),
            verification_status="not run",
            blockers=("none",),
            risk_notes=("needs follow-up",),
        )
    )

    assert "missing_next_steps" in result.gap_labels
    assert result.verification_state == "not_run"
    assert "missing_verification" not in result.gap_labels
    assert any("not successful" in insight for insight in result.insights)


def test_failed_verification_counts_as_present_evidence_with_state():
    result = analyze_session_handoff_completeness(
        SessionHandoff(
            objective="Fix tests",
            changed_files=("tests/test_x.py",),
            verification_status="failed",
            blockers=("none",),
            next_steps=("rerun after fix",),
            risk_notes=("test failure remains",),
        )
    )

    assert result.verification_state == "failed"
    assert "missing_verification" not in result.gap_labels
    assert result.quality == "complete"
    assert any("not successful" in insight for insight in result.insights)


def test_blocked_verification_counts_as_present_evidence_with_state():
    result = analyze_session_handoff_completeness(
        SessionHandoff(
            objective="Fix tests",
            changed_files=("tests/test_x.py",),
            verification_status="blocked",
            blockers=("dependency unavailable",),
            next_steps=("retry verification",),
            risk_notes=("verification blocked",),
        )
    )

    assert result.verification_state == "blocked"
    assert "missing_verification" not in result.gap_labels
    assert result.metrics.completeness_score == 1.0
    assert any("not successful" in insight for insight in result.insights)


def test_blank_verification_status_is_missing_state():
    result = analyze_session_handoff_completeness(
        SessionHandoff(
            objective="Fix tests",
            changed_files=("tests/test_x.py",),
            verification_status=" ",
            blockers=("none",),
            next_steps=("run tests",),
            risk_notes=("verification missing",),
        )
    )

    assert result.verification_state == "missing"
    assert "missing_verification" in result.gap_labels


def test_blocker_only_handoff_is_incomplete_but_counts_blockers_present():
    result = analyze_session_handoff_completeness(SessionHandoff(blockers=("blocked",)))

    assert result.metrics.present_sections == 1
    assert result.quality == "incomplete"
    assert "missing_objective" in result.gap_labels


def test_none_input_is_empty_handoff():
    result = analyze_session_handoff_completeness(None)

    assert result.metrics.completeness_score == 0.0
    assert len(result.gap_labels) == 6


@pytest.mark.parametrize(
    "handoff",
    [
        {},
        SessionHandoff(objective=object()),
        SessionHandoff(changed_files="src/file.py"),
        SessionHandoff(next_steps=(1,)),
    ],
)
def test_malformed_handoff_fields_raise_value_error(handoff):
    with pytest.raises(ValueError):
        analyze_session_handoff_completeness(handoff)
