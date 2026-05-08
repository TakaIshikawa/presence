"""Tests for session artifact reference hygiene analysis."""

import pytest

from synthesis.session_artifact_reference_hygiene import analyze_session_artifact_reference_hygiene


def test_file_path_references_are_concrete():
    report = analyze_session_artifact_reference_hygiene(
        [{"session_id": "s1", "artifacts": ["reports/out.json"], "final_answer": "See reports/out.json."}]
    )

    assert report["concrete_reference_count"] == 1
    assert report["concrete_reference_rate"] == 100.0


def test_url_references_are_concrete():
    report = analyze_session_artifact_reference_hygiene(
        [{"artifacts": [{"url": "https://example.com/report"}], "final_answer": "https://example.com/report"}]
    )

    assert report["concrete_reference_count"] == 1


def test_command_references_are_concrete():
    report = analyze_session_artifact_reference_hygiene(
        [{"artifacts": ["coverage.xml"], "commands": ["uv run pytest"], "final_answer": "Run uv run pytest for details."}]
    )

    assert report["concrete_reference_count"] == 1


def test_vague_only_summaries_are_counted():
    report = analyze_session_artifact_reference_hygiene(
        [{"session_id": "s1", "artifacts": ["reports/out.json"], "final_answer": "The report is ready."}]
    )

    assert report["vague_reference_count"] == 1
    assert report["missing_reference_count"] == 1
    assert report["examples"][0]["session_id"] == "s1"


def test_artifact_records_without_final_mentions_are_missing():
    report = analyze_session_artifact_reference_hygiene(
        [{"session_id": "s1", "artifacts": ["reports/out.json"], "final_answer": "Done."}]
    )

    assert report["missing_reference_count"] == 1


def test_invalid_input_raises():
    with pytest.raises(ValueError, match="records must be a list"):
        analyze_session_artifact_reference_hygiene({"artifacts": []})
