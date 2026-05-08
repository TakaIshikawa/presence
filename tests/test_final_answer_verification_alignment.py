"""Tests for final answer verification alignment analysis."""

import pytest

from synthesis.final_answer_verification_alignment import analyze_final_answer_verification_alignment


def test_aligned_passed_claim():
    report = analyze_final_answer_verification_alignment(
        [{"final_answer": "Tests passed: uv run pytest tests/test_a.py", "verification_status": "passed"}]
    )

    assert report["aligned_count"] == 1
    assert report["contradiction_count"] == 0


def test_aligned_not_run_claim():
    report = analyze_final_answer_verification_alignment(
        [{"final_answer": "Tests were not run.", "verification_status": "missing"}]
    )

    assert report["aligned_count"] == 1


def test_passed_claim_without_passed_status_is_contradiction():
    report = analyze_final_answer_verification_alignment(
        [{"session_id": "s1", "final_message": "pytest passed", "verification_status": "failed"}]
    )

    assert report["contradiction_count"] == 1
    assert report["examples"][0]["session_id"] == "s1"


def test_not_run_claim_with_passed_status_is_contradiction():
    report = analyze_final_answer_verification_alignment(
        [{"final_answer": "I did not run tests.", "status": "passed"}]
    )

    assert report["contradiction_count"] == 1


def test_unknown_text_is_counted():
    report = analyze_final_answer_verification_alignment([{"final_answer": "Implemented the change."}])

    assert report["unknown_count"] == 1


def test_command_mentions_are_included_in_examples():
    report = analyze_final_answer_verification_alignment(
        [{"final_answer": "uv run pytest tests/test_a.py passed", "verification_status": "missing"}]
    )

    assert report["examples"][0]["mentioned_command"] == "uv run pytest tests/test_a.py passed"


def test_invalid_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_final_answer_verification_alignment({"final_answer": "passed"})
