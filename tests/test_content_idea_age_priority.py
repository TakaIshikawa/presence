"""Tests for stale content idea priority reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.content_idea_age_priority import (
    build_content_idea_age_priority_report,
    build_content_idea_age_priority_report_from_fixture,
    format_content_idea_age_priority_json,
    format_content_idea_age_priority_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_idea_age_priority.py"
spec = importlib.util.spec_from_file_location("content_idea_age_priority_script", SCRIPT_PATH)
content_idea_age_priority_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_idea_age_priority_script)

NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _set_timestamps(
    db,
    idea_id: int,
    *,
    created_at: str,
    updated_at: str | None = None,
) -> None:
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ?, updated_at = ? WHERE id = ?",
        (created_at, updated_at or created_at, idea_id),
    )
    db.conn.commit()


def _set_metadata(db, idea_id: int, metadata: dict) -> None:
    db.conn.execute(
        "UPDATE content_ideas SET source_metadata = ? WHERE id = ?",
        (json.dumps(metadata, sort_keys=True), idea_id),
    )
    db.conn.commit()


def test_ranks_stale_unpublished_ideas_by_age_priority_campaign_and_touch(db):
    campaign_id = db.create_campaign(
        "Launch",
        start_date="2026-04-01T00:00:00+00:00",
        end_date="2026-06-01T00:00:00+00:00",
        status="active",
    )
    high_id = db.add_content_idea(
        "Turn launch learnings into a practical post",
        topic="launch",
        priority="high",
        source_metadata={"campaign_id": campaign_id},
    )
    older_normal_id = db.add_content_idea("Older but less urgent idea", priority="normal")
    low_id = db.add_content_idea("Very old low priority idea", priority="low")
    _set_timestamps(
        db,
        high_id,
        created_at="2026-03-15T12:00:00+00:00",
        updated_at="2026-04-25T12:00:00+00:00",
    )
    _set_timestamps(db, older_normal_id, created_at="2026-02-01T12:00:00+00:00")
    _set_timestamps(db, low_id, created_at="2026-01-01T12:00:00+00:00")

    report = build_content_idea_age_priority_report(db, stale_days=30, now=NOW)

    assert [item.idea_id for item in report.ideas] == [high_id, older_normal_id, low_id]
    assert report.ideas[0].campaign_relevance == f"active_campaign:{campaign_id}"
    assert report.ideas[0].score_components["priority"] == 100
    assert report.ideas[0].score_components["campaign"] == 30
    assert report.ideas[0].last_touched_days == 7


def test_fresh_ideas_are_not_reported_until_threshold(db):
    stale_id = db.add_content_idea("Old enough", priority="normal")
    fresh_id = db.add_content_idea("Too fresh", priority="high")
    _set_timestamps(db, stale_id, created_at="2026-03-01T12:00:00+00:00")
    _set_timestamps(db, fresh_id, created_at="2026-04-20T12:00:00+00:00")

    report = build_content_idea_age_priority_report(db, stale_days=30, now=NOW)

    assert [item.idea_id for item in report.ideas] == [stale_id]
    assert fresh_id not in {item.idea_id for item in report.ideas}


def test_blocked_ideas_are_reported_separately(db):
    blocked_id = db.add_content_idea(
        "Publish once the release lands",
        priority="high",
        source_metadata={"blocked_by": {"ref": "release-42", "status": "open"}},
    )
    clear_id = db.add_content_idea("Clear stale idea", priority="normal")
    _set_timestamps(db, blocked_id, created_at="2026-03-01T12:00:00+00:00")
    _set_timestamps(db, clear_id, created_at="2026-03-01T12:00:00+00:00")

    report = build_content_idea_age_priority_report(db, stale_days=30, now=NOW)

    assert [item.idea_id for item in report.ideas] == [clear_id]
    assert [item.idea_id for item in report.blocked_ideas] == [blocked_id]
    assert report.blocked_ideas[0].dependency_status == "blocked"
    assert report.blocked_ideas[0].dependency_summary == "release-42"


def test_already_published_ideas_are_excluded(db):
    idea_id = db.add_content_idea("Already published", priority="high")
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Published content",
        eval_score=9.0,
        eval_feedback="ok",
    )
    _set_metadata(db, idea_id, {"generated_content_id": content_id})
    _set_timestamps(db, idea_id, created_at="2026-02-01T12:00:00+00:00")
    db.mark_published(content_id, "https://example.com/post")

    report = build_content_idea_age_priority_report(db, stale_days=30, now=NOW)

    assert report.ideas == ()
    assert report.blocked_ideas == ()
    assert report.total_candidates == 0


def test_fixture_json_and_formatting_are_stable(tmp_path):
    fixture = tmp_path / "ideas.json"
    fixture.write_text(
        json.dumps(
            {
                "active_campaign_ids": [7],
                "ideas": [
                    {
                        "id": 10,
                        "note": "Fixture stale campaign idea",
                        "priority": "high",
                        "status": "open",
                        "created_at": "2026-03-01T12:00:00+00:00",
                        "updated_at": "2026-04-01T12:00:00+00:00",
                        "source_metadata": {"campaign_id": 7},
                    },
                    {
                        "id": 11,
                        "note": "Fixture blocked idea",
                        "priority": "high",
                        "status": "open",
                        "created_at": "2026-03-01T12:00:00+00:00",
                        "source_metadata": {"waiting_for": "issue #44"},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    report = build_content_idea_age_priority_report_from_fixture(
        fixture,
        stale_days=30,
        now=NOW,
    )
    payload = json.loads(format_content_idea_age_priority_json(report))
    text = format_content_idea_age_priority_text(report)

    assert [item["idea_id"] for item in payload["ideas"]] == [10]
    assert [item["idea_id"] for item in payload["blocked_ideas"]] == [11]
    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["generated_at"] == "2026-05-02T12:00:00+00:00"
    assert "Content Idea Age Priority" in text
    assert "idea_id=10" in text
    assert "Blocked ideas" in text


def test_cli_supports_db_and_fixture_json_output(db, monkeypatch, capsys, tmp_path):
    idea_id = db.add_content_idea("CLI stale idea", priority="normal")
    _set_timestamps(db, idea_id, created_at="2026-03-01T12:00:00+00:00")
    monkeypatch.setattr(
        content_idea_age_priority_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = content_idea_age_priority_script.main(
        ["--stale-days", "30", "--limit", "5", "--format", "json"]
    )
    db_payload = json.loads(capsys.readouterr().out)

    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps(
            [
                {
                    "id": 99,
                    "note": "Fixture CLI idea",
                    "status": "open",
                    "created_at": "2026-03-01T12:00:00+00:00",
                }
            ]
        ),
        encoding="utf-8",
    )
    fixture_exit_code = content_idea_age_priority_script.main(
        ["--fixture", str(fixture), "--stale-days", "30", "--format", "json"]
    )
    fixture_payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert [item["idea_id"] for item in db_payload["ideas"]] == [idea_id]
    assert fixture_exit_code == 0
    assert [item["idea_id"] for item in fixture_payload["ideas"]] == [99]
