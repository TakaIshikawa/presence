"""Tests for planned topic source_material reference validation."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.planned_topic_source_validator import (
    ISSUE_AMBIGUOUS_PLAIN_TEXT_REFERENCE,
    ISSUE_EMPTY_SOURCE_MATERIAL,
    ISSUE_INVALID_JSON,
    ISSUE_UNRESOLVED_REFERENCE,
    build_planned_topic_source_validator_report,
    format_planned_topic_source_validator_json,
    format_planned_topic_source_validator_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "validate_planned_topic_sources.py"
spec = importlib.util.spec_from_file_location("validate_planned_topic_sources_script", SCRIPT_PATH)
validate_planned_topic_sources_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(validate_planned_topic_sources_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _campaign(db, name: str, *, status: str = "active") -> int:
    return db.create_campaign(
        name=name,
        goal=f"{name} work",
        start_date="2026-05-01",
        end_date="2026-05-31",
        status=status,
    )


def _topic(db, topic: str, campaign_id: int, source_material: str | None, **kwargs) -> int:
    return db.insert_planned_topic(
        topic=topic,
        angle=f"{topic} angle",
        target_date="2026-05-05",
        source_material=source_material,
        campaign_id=campaign_id,
        **kwargs,
    )


def _commit(db, sha: str) -> int:
    return db.insert_commit(
        repo_name="presence",
        commit_sha=sha,
        commit_message=f"commit {sha}",
        timestamp="2026-05-03T10:00:00+00:00",
        author="alice",
    )


def _message(db, session_id: str, message_uuid: str) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path="/repo",
        timestamp="2026-05-03T10:00:00+00:00",
        prompt_text="source session",
    )


def _activity(db, number: int | str = 42) -> int:
    return db.upsert_github_activity(
        repo_name="presence",
        activity_type="pull_request",
        number=number,
        title=f"PR {number}",
        state="open",
        author="alice",
        url=f"https://example.com/presence/pull/{number}",
        updated_at="2026-05-03T10:00:00+00:00",
    )


def test_valid_json_references_are_not_flagged(db):
    campaign_id = _campaign(db, "Launch")
    activity_id = _activity(db)
    _commit(db, "abcdef1234567890")
    _message(db, "session-abc123", "11111111-2222-3333-4444-555555555555")
    _topic(
        db,
        "valid",
        campaign_id,
        json.dumps(
            {
                "commits": ["abcdef1234567890"],
                "messages": ["11111111-2222-3333-4444-555555555555"],
                "sessions": ["session-abc123"],
                "activity_ids": [activity_id, "presence#42:pull_request"],
            }
        ),
    )

    report = build_planned_topic_source_validator_report(db, now=NOW)

    assert report.ok is True
    assert report.audited_count == 1
    assert report.issue_count == 0
    assert report.items == ()


def test_missing_references_are_reported_with_aggregate_counts(db):
    campaign_id = _campaign(db, "Launch")
    _topic(
        db,
        "missing",
        campaign_id,
        json.dumps(
            {
                "commits": ["deadbee"],
                "messages": ["aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"],
                "sessions": ["missing-session"],
                "activity_ids": [999],
            }
        ),
    )

    report = build_planned_topic_source_validator_report(db, now=NOW)
    item = report.items[0]

    assert report.ok is False
    assert report.issue_count == 4
    assert report.by_issue_type == {ISSUE_UNRESOLVED_REFERENCE: 4}
    assert item.topic == "missing"
    assert [(issue.source_type, issue.reference) for issue in item.issues] == [
        ("commit", "deadbee"),
        ("message", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        ("session", "missing-session"),
        ("activity", "999"),
    ]


def test_invalid_json_and_empty_source_material_are_reported(db):
    campaign_id = _campaign(db, "Launch")
    invalid_id = _topic(db, "invalid", campaign_id, '{"commits": ["abc1234"')
    empty_id = _topic(db, "empty", campaign_id, "  ")

    report = build_planned_topic_source_validator_report(db, now=NOW)

    issues_by_topic = {
        item.planned_topic_id: [issue.issue_type for issue in item.issues]
        for item in report.items
    }
    assert issues_by_topic[invalid_id] == [ISSUE_INVALID_JSON]
    assert issues_by_topic[empty_id] == [ISSUE_EMPTY_SOURCE_MATERIAL]
    assert report.by_issue_type == {
        ISSUE_EMPTY_SOURCE_MATERIAL: 1,
        ISSUE_INVALID_JSON: 1,
    }


def test_plain_text_extraction_and_ambiguous_tokens(db):
    campaign_id = _campaign(db, "Launch")
    activity_id = _activity(db)
    _commit(db, "abcdef1234567890")
    _message(db, "session-abc123", "11111111-2222-3333-4444-555555555555")
    good_id = _topic(
        db,
        "plain valid",
        campaign_id,
        (
            f"commit abcdef1234567890, "
            "message 11111111-2222-3333-4444-555555555555, "
            f"session session-abc123, activity {activity_id}"
        ),
    )
    ambiguous_id = _topic(db, "ambiguous", campaign_id, "Use abcdef1234567890 as background.")

    report = build_planned_topic_source_validator_report(db, now=NOW)

    assert [item.planned_topic_id for item in report.items] == [ambiguous_id]
    assert good_id not in [item.planned_topic_id for item in report.items]
    assert report.items[0].issues[0].issue_type == ISSUE_AMBIGUOUS_PLAIN_TEXT_REFERENCE


def test_campaign_and_status_filters_limit_audited_topics(db):
    launch_id = _campaign(db, "Launch")
    other_id = _campaign(db, "Other")
    completed_id = _campaign(db, "Done", status="completed")
    launch_topic = _topic(db, "launch", launch_id, None)
    _topic(db, "other", other_id, None)
    _topic(db, "generated", launch_id, None, status="generated")
    _topic(db, "completed campaign", completed_id, None)

    report = build_planned_topic_source_validator_report(
        db,
        campaign_id=launch_id,
        status="planned",
        limit=10,
        now=NOW,
    )
    all_status = build_planned_topic_source_validator_report(
        db,
        campaign_id=launch_id,
        status="all",
        limit=10,
        now=NOW,
    )

    assert report.audited_count == 1
    assert [item.planned_topic_id for item in report.items] == [launch_topic]
    assert all_status.audited_count == 2


def test_json_and_text_output_are_deterministic(db):
    campaign_id = _campaign(db, "Launch")
    topic_id = _topic(db, "empty", campaign_id, None)

    report = build_planned_topic_source_validator_report(db, now=NOW)
    payload = json.loads(format_planned_topic_source_validator_json(report))
    text = format_planned_topic_source_validator_text(report)

    assert payload["artifact_type"] == "planned_topic_source_validator"
    assert payload["generated_at"] == "2026-05-03T12:00:00+00:00"
    assert payload["items"][0]["planned_topic_id"] == topic_id
    assert payload["by_issue_type"] == {ISSUE_EMPTY_SOURCE_MATERIAL: 1}
    assert "Planned Topic Source Validator" in text
    assert f"topic_id={topic_id}" in text
    assert "empty_source_material=1" in text


def test_cli_supports_campaign_status_format_and_limit(db, monkeypatch, capsys):
    campaign_id = _campaign(db, "CLI Campaign")
    topic_id = _topic(db, "cli", campaign_id, None)
    monkeypatch.setattr(
        validate_planned_topic_sources_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        validate_planned_topic_sources_script,
        "build_planned_topic_source_validator_report",
        lambda db, **kwargs: build_planned_topic_source_validator_report(db, now=NOW, **kwargs),
    )

    assert validate_planned_topic_sources_script.main(["--campaign-id", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err

    exit_code = validate_planned_topic_sources_script.main(
        [
            "--campaign-id",
            str(campaign_id),
            "--status",
            "planned",
            "--limit",
            "5",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"]["campaign_id"] == campaign_id
    assert payload["filters"]["status"] == "planned"
    assert payload["filters"]["limit"] == 5
    assert payload["items"][0]["planned_topic_id"] == topic_id
