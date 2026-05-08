"""Tests for session summary evidence analysis."""

import pytest

from synthesis.session_summary_evidence import SessionEvidence, analyze_session_summary_evidence


def test_empty_summary_has_no_evidence():
    report = analyze_session_summary_evidence("", SessionEvidence(edited_files=("src/app.py",)))

    assert report.evidence_quality == "none"
    assert "files" in report.missing_evidence_categories


def test_summary_with_file_and_test_evidence_is_strong():
    report = analyze_session_summary_evidence(
        "Updated src/app.py and ran uv run pytest tests/test_app.py. 3 passed.",
        SessionEvidence(
            edited_files=("src/app.py",),
            commands=("uv run pytest tests/test_app.py",),
            test_outcomes=("3 passed",),
        ),
    )

    assert report.mentioned_files == ("src/app.py",)
    assert report.mentioned_commands == ("uv run pytest tests/test_app.py",)
    assert report.mentioned_passing_tests == 1
    assert report.evidence_quality == "strong"


def test_summary_mentions_untouched_files_separately():
    report = analyze_session_summary_evidence(
        "Updated src/app.py and src/other.py.",
        SessionEvidence(edited_files=("src/app.py",)),
    )

    assert report.untouched_file_mentions == ("src/other.py",)


def test_summary_missing_verification_evidence_is_partial():
    report = analyze_session_summary_evidence(
        "Updated src/app.py.",
        SessionEvidence(edited_files=("src/app.py",), commands=("pytest tests/test_app.py",)),
    )

    assert report.evidence_quality == "partial"
    assert "verification" in report.missing_evidence_categories


def test_command_mention_normalization_detects_test_command_family():
    report = analyze_session_summary_evidence(
        "Ran pytest after editing src/app.py.",
        SessionEvidence(edited_files=("src/app.py",), commands=("pytest tests/test_app.py",)),
    )

    assert report.mentioned_commands == ("pytest tests/test_app.py",)


def test_invalid_inputs_raise_value_error():
    with pytest.raises(ValueError, match="summary"):
        analyze_session_summary_evidence(None, SessionEvidence())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="SessionEvidence"):
        analyze_session_summary_evidence("done", object())  # type: ignore[arg-type]
