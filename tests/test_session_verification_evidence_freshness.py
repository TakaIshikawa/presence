"""Tests for session verification evidence freshness analysis."""

import pytest

from synthesis.session_verification_evidence_freshness import analyze_session_verification_evidence_freshness


def test_empty_input_returns_zeroed_metrics():
    report = analyze_session_verification_evidence_freshness([])

    assert report["total_artifacts"] == 0
    assert report["fresh_verifications"] == 0
    assert report["stale_verifications"] == 0
    assert report["missing_verifications"] == 0
    assert report["freshness_rate"] == 0.0
    assert report["examples"] == []


def test_verification_after_last_edit_counts_as_fresh():
    report = analyze_session_verification_evidence_freshness([
        {
            "artifact_id": "src/foo.py",
            "last_edit_turn": 10,
            "verification_turn": 15,
            "command": "pytest tests/test_foo.py",
            "status": "passed",
        }
    ])

    assert report["fresh_verifications"] == 1
    assert report["stale_verifications"] == 0
    assert report["examples"] == []


def test_verification_at_same_turn_as_last_edit_counts_as_fresh():
    report = analyze_session_verification_evidence_freshness([
        {
            "artifact_id": "src/foo.py",
            "last_edit_turn": 10,
            "verification_turn": 10,
            "command": "pytest tests/test_foo.py",
            "status": "passed",
        }
    ])

    assert report["fresh_verifications"] == 1
    assert report["stale_verifications"] == 0


def test_verification_before_last_edit_counts_as_stale():
    report = analyze_session_verification_evidence_freshness([
        {
            "artifact_id": "src/foo.py",
            "last_edit_turn": 15,
            "verification_turn": 10,
            "command": "pytest tests/test_foo.py",
            "status": "passed",
        }
    ])

    assert report["stale_verifications"] == 1
    assert report["fresh_verifications"] == 0
    assert len(report["examples"]) == 1
    assert report["examples"][0]["artifact_id"] == "src/foo.py"
    assert report["examples"][0]["last_edit_turn"] == 15
    assert report["examples"][0]["verification_turn"] == 10


def test_missing_command_counts_as_missing_verification():
    report = analyze_session_verification_evidence_freshness([
        {
            "artifact_id": "src/foo.py",
            "last_edit_turn": 10,
            "verification_turn": 15,
            "status": "passed",
        }
    ])

    assert report["missing_verifications"] == 1
    assert report["fresh_verifications"] == 0
    assert report["stale_verifications"] == 0


def test_missing_verification_turn_counts_as_missing_verification():
    report = analyze_session_verification_evidence_freshness([
        {
            "artifact_id": "src/foo.py",
            "last_edit_turn": 10,
            "command": "pytest tests/test_foo.py",
            "status": "passed",
        }
    ])

    assert report["missing_verifications"] == 1


def test_freshness_rate_rounded_to_three_decimals():
    report = analyze_session_verification_evidence_freshness([
        {
            "artifact_id": "src/foo.py",
            "last_edit_turn": 10,
            "verification_turn": 15,
            "command": "pytest",
            "status": "passed",
        },
        {
            "artifact_id": "src/bar.py",
            "last_edit_turn": 10,
            "verification_turn": 5,
            "command": "pytest",
            "status": "passed",
        },
        {
            "artifact_id": "src/baz.py",
            "last_edit_turn": 10,
            "verification_turn": 15,
            "command": "pytest",
            "status": "passed",
        },
    ])

    assert report["fresh_verifications"] == 2
    assert report["stale_verifications"] == 1
    assert report["total_artifacts"] == 3
    assert report["freshness_rate"] == 66.667


def test_invalid_artifact_id_raises_value_error():
    with pytest.raises(ValueError, match="invalid artifact_id"):
        analyze_session_verification_evidence_freshness([
            {
                "artifact_id": "",
                "last_edit_turn": 10,
                "verification_turn": 15,
            }
        ])


def test_missing_artifact_id_raises_value_error():
    with pytest.raises(ValueError, match="invalid artifact_id"):
        analyze_session_verification_evidence_freshness([
            {
                "last_edit_turn": 10,
                "verification_turn": 15,
            }
        ])


def test_whitespace_only_artifact_id_raises_value_error():
    with pytest.raises(ValueError, match="invalid artifact_id"):
        analyze_session_verification_evidence_freshness([
            {
                "artifact_id": "   ",
                "last_edit_turn": 10,
                "verification_turn": 15,
            }
        ])


def test_negative_last_edit_turn_raises_value_error():
    with pytest.raises(ValueError, match="negative last_edit_turn"):
        analyze_session_verification_evidence_freshness([
            {
                "artifact_id": "src/foo.py",
                "last_edit_turn": -5,
                "verification_turn": 15,
            }
        ])


def test_negative_verification_turn_raises_value_error():
    with pytest.raises(ValueError, match="negative verification_turn"):
        analyze_session_verification_evidence_freshness([
            {
                "artifact_id": "src/foo.py",
                "last_edit_turn": 10,
                "verification_turn": -5,
                "command": "pytest",
            }
        ])


def test_boolean_last_edit_turn_raises_value_error():
    with pytest.raises(ValueError, match="invalid last_edit_turn"):
        analyze_session_verification_evidence_freshness([
            {
                "artifact_id": "src/foo.py",
                "last_edit_turn": True,
                "verification_turn": 15,
            }
        ])


def test_boolean_verification_turn_raises_value_error():
    with pytest.raises(ValueError, match="invalid verification_turn"):
        analyze_session_verification_evidence_freshness([
            {
                "artifact_id": "src/foo.py",
                "last_edit_turn": 10,
                "verification_turn": False,
                "command": "pytest",
            }
        ])


def test_invalid_status_raises_value_error():
    with pytest.raises(ValueError, match="invalid status 'invalid-status'"):
        analyze_session_verification_evidence_freshness([
            {
                "artifact_id": "src/foo.py",
                "last_edit_turn": 10,
                "verification_turn": 15,
                "command": "pytest",
                "status": "invalid-status",
            }
        ])


def test_duplicate_artifact_id_raises_value_error():
    with pytest.raises(ValueError, match="duplicate artifact_id 'src/foo.py'"):
        analyze_session_verification_evidence_freshness([
            {
                "artifact_id": "src/foo.py",
                "last_edit_turn": 10,
                "verification_turn": 15,
                "command": "pytest",
            },
            {
                "artifact_id": "src/foo.py",
                "last_edit_turn": 20,
                "verification_turn": 25,
                "command": "pytest",
            },
        ])


def test_non_mapping_record_raises_value_error():
    with pytest.raises(ValueError, match="record at index 0 is not a dictionary"):
        analyze_session_verification_evidence_freshness(["not a dict"])


def test_non_list_input_raises_value_error():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_verification_evidence_freshness({"artifact_id": "foo"})


def test_examples_capped_at_five():
    records = [
        {
            "artifact_id": f"src/file{i}.py",
            "last_edit_turn": 20,
            "verification_turn": 10,
            "command": "pytest",
            "status": "passed",
        }
        for i in range(7)
    ]

    report = analyze_session_verification_evidence_freshness(records)

    assert report["stale_verifications"] == 7
    assert len(report["examples"]) == 5


def test_status_normalization():
    report = analyze_session_verification_evidence_freshness([
        {
            "artifact_id": "src/foo.py",
            "last_edit_turn": 10,
            "verification_turn": 5,
            "command": "pytest",
            "status": "PASSED",
        }
    ])

    assert report["examples"][0]["status"] == "passed"


def test_empty_status_is_valid():
    report = analyze_session_verification_evidence_freshness([
        {
            "artifact_id": "src/foo.py",
            "last_edit_turn": 10,
            "verification_turn": 5,
            "command": "pytest",
            "status": "",
        }
    ])

    assert report["stale_verifications"] == 1
    assert report["examples"][0]["status"] == ""


def test_none_status_is_valid():
    report = analyze_session_verification_evidence_freshness([
        {
            "artifact_id": "src/foo.py",
            "last_edit_turn": 10,
            "verification_turn": 5,
            "command": "pytest",
            "status": None,
        }
    ])

    assert report["stale_verifications"] == 1
    assert report["examples"][0]["status"] == ""


def test_valid_status_values():
    for status in ["passed", "failed", "skipped"]:
        report = analyze_session_verification_evidence_freshness([
            {
                "artifact_id": f"src/{status}.py",
                "last_edit_turn": 10,
                "verification_turn": 5,
                "command": "pytest",
                "status": status,
            }
        ])
        assert report["stale_verifications"] == 1
