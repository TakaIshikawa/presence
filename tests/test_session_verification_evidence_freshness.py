"""Tests for session verification evidence freshness analysis."""

import pytest

from synthesis.session_verification_evidence_freshness import (
    analyze_session_verification_evidence_freshness,
)


def test_empty_input_returns_zeroed_metrics():
    report = analyze_session_verification_evidence_freshness([])

    assert report["total_artifacts"] == 0
    assert report["fresh_verifications"] == 0
    assert report["stale_verifications"] == 0
    assert report["missing_verifications"] == 0
    assert report["freshness_rate"] == 0.0
    assert report["examples"] == []


def test_verification_after_last_edit_counts_as_fresh():
    report = analyze_session_verification_evidence_freshness(
        [
            {
                "artifact_id": "file1.py",
                "last_edit_turn": 5,
                "verification_turn": 10,
                "command": "pytest file1.py",
                "status": "pass",
            },
            {
                "artifact_id": "file2.py",
                "last_edit_turn": 3,
                "verification_turn": 3,
                "command": "pytest file2.py",
                "status": "pass",
            },
        ]
    )

    assert report["fresh_verifications"] == 2
    assert report["stale_verifications"] == 0
    assert report["freshness_rate"] == 100.0
    assert report["examples"] == []


def test_verification_before_last_edit_counts_as_stale():
    report = analyze_session_verification_evidence_freshness(
        [
            {
                "artifact_id": "file1.py",
                "last_edit_turn": 10,
                "verification_turn": 5,
                "command": "pytest file1.py",
                "status": "pass",
            }
        ]
    )

    assert report["fresh_verifications"] == 0
    assert report["stale_verifications"] == 1
    assert report["freshness_rate"] == 0.0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["artifact_id"] == "file1.py"
    assert report["examples"][0]["last_edit_turn"] == 10
    assert report["examples"][0]["verification_turn"] == 5


def test_missing_command_counts_as_missing():
    report = analyze_session_verification_evidence_freshness(
        [
            {
                "artifact_id": "file1.py",
                "last_edit_turn": 5,
                "verification_turn": 10,
                "command": "",
                "status": "pass",
            }
        ]
    )

    assert report["missing_verifications"] == 1
    assert report["fresh_verifications"] == 0
    assert report["stale_verifications"] == 0


def test_missing_verification_turn_counts_as_missing():
    report = analyze_session_verification_evidence_freshness(
        [
            {
                "artifact_id": "file1.py",
                "last_edit_turn": 5,
                "command": "pytest file1.py",
                "status": "pass",
            }
        ]
    )

    assert report["missing_verifications"] == 1
    assert report["fresh_verifications"] == 0
    assert report["stale_verifications"] == 0


def test_freshness_rate_rounded_to_three_decimals():
    report = analyze_session_verification_evidence_freshness(
        [
            {
                "artifact_id": "file1.py",
                "last_edit_turn": 5,
                "verification_turn": 10,
                "command": "pytest file1.py",
                "status": "pass",
            },
            {
                "artifact_id": "file2.py",
                "last_edit_turn": 5,
                "verification_turn": 3,
                "command": "pytest file2.py",
                "status": "pass",
            },
            {
                "artifact_id": "file3.py",
                "last_edit_turn": 5,
                "verification_turn": 3,
                "command": "pytest file3.py",
                "status": "pass",
            },
        ]
    )

    assert report["fresh_verifications"] == 1
    assert report["stale_verifications"] == 2
    assert report["freshness_rate"] == 33.333


def test_invalid_artifact_id_raises_error():
    with pytest.raises(ValueError, match="artifact_id must be a non-empty string"):
        analyze_session_verification_evidence_freshness(
            [
                {
                    "artifact_id": "",
                    "last_edit_turn": 5,
                    "verification_turn": 10,
                    "command": "pytest",
                }
            ]
        )


def test_missing_artifact_id_raises_error():
    with pytest.raises(ValueError, match="artifact_id must be a non-empty string"):
        analyze_session_verification_evidence_freshness(
            [
                {
                    "last_edit_turn": 5,
                    "verification_turn": 10,
                    "command": "pytest",
                }
            ]
        )


def test_negative_last_edit_turn_raises_error():
    with pytest.raises(ValueError, match="last_edit_turn must be non-negative"):
        analyze_session_verification_evidence_freshness(
            [
                {
                    "artifact_id": "file1.py",
                    "last_edit_turn": -1,
                    "verification_turn": 10,
                    "command": "pytest",
                }
            ]
        )


def test_negative_verification_turn_raises_error():
    with pytest.raises(ValueError, match="verification_turn must be non-negative"):
        analyze_session_verification_evidence_freshness(
            [
                {
                    "artifact_id": "file1.py",
                    "last_edit_turn": 5,
                    "verification_turn": -1,
                    "command": "pytest",
                }
            ]
        )


def test_boolean_last_edit_turn_raises_error():
    with pytest.raises(ValueError, match="last_edit_turn must be an integer"):
        analyze_session_verification_evidence_freshness(
            [
                {
                    "artifact_id": "file1.py",
                    "last_edit_turn": True,
                    "verification_turn": 10,
                    "command": "pytest",
                }
            ]
        )


def test_boolean_verification_turn_raises_error():
    with pytest.raises(ValueError, match="verification_turn must be an integer"):
        analyze_session_verification_evidence_freshness(
            [
                {
                    "artifact_id": "file1.py",
                    "last_edit_turn": 5,
                    "verification_turn": False,
                    "command": "pytest",
                }
            ]
        )


def test_invalid_status_raises_error():
    with pytest.raises(ValueError, match="status must be one of: pass, fail, skip, or empty"):
        analyze_session_verification_evidence_freshness(
            [
                {
                    "artifact_id": "file1.py",
                    "last_edit_turn": 5,
                    "verification_turn": 10,
                    "command": "pytest",
                    "status": "invalid",
                }
            ]
        )


def test_duplicate_artifact_ids_raise_error():
    with pytest.raises(ValueError, match="duplicate artifact_id found in records"):
        analyze_session_verification_evidence_freshness(
            [
                {
                    "artifact_id": "file1.py",
                    "last_edit_turn": 5,
                    "verification_turn": 10,
                    "command": "pytest",
                },
                {
                    "artifact_id": "file1.py",
                    "last_edit_turn": 3,
                    "verification_turn": 8,
                    "command": "pytest",
                },
            ]
        )


def test_non_list_input_raises_error():
    with pytest.raises(ValueError, match="records must be a list of verification record dictionaries"):
        analyze_session_verification_evidence_freshness({"artifact_id": "file1.py"})


def test_non_mapping_record_raises_error():
    with pytest.raises(ValueError, match="records must be a list of verification record dictionaries"):
        analyze_session_verification_evidence_freshness(["not a dict"])


def test_examples_capped_at_five():
    records = [
        {
            "artifact_id": f"file{i}.py",
            "last_edit_turn": 10,
            "verification_turn": 5,
            "command": "pytest",
            "status": "pass",
        }
        for i in range(7)
    ]

    report = analyze_session_verification_evidence_freshness(records)

    assert report["stale_verifications"] == 7
    assert len(report["examples"]) == 5


def test_valid_status_values_accepted():
    report = analyze_session_verification_evidence_freshness(
        [
            {
                "artifact_id": "file1.py",
                "last_edit_turn": 5,
                "verification_turn": 10,
                "command": "pytest file1.py",
                "status": "pass",
            },
            {
                "artifact_id": "file2.py",
                "last_edit_turn": 5,
                "verification_turn": 10,
                "command": "pytest file2.py",
                "status": "fail",
            },
            {
                "artifact_id": "file3.py",
                "last_edit_turn": 5,
                "verification_turn": 10,
                "command": "pytest file3.py",
                "status": "skip",
            },
            {
                "artifact_id": "file4.py",
                "last_edit_turn": 5,
                "verification_turn": 10,
                "command": "pytest file4.py",
                "status": "",
            },
        ]
    )

    assert report["fresh_verifications"] == 4
    assert report["total_artifacts"] == 4
