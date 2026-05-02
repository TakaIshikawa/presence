"""Tests for active campaign date gap planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from synthesis.campaign_date_gaps import (
    format_campaign_date_gaps_json,
    format_campaign_date_gaps_text,
    plan_campaign_date_gaps,
)


NOW = datetime(2026, 5, 1, 9, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "campaign_date_gaps.py"
spec = importlib.util.spec_from_file_location("campaign_date_gaps_script", SCRIPT_PATH)
campaign_date_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(campaign_date_gaps_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str = "Campaign content") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="usable",
    )


def _link_topic(db, topic_id: int, content_id: int) -> None:
    db.conn.execute(
        "UPDATE planned_topics SET content_id = ?, status = 'generated' WHERE id = ?",
        (content_id, topic_id),
    )
    db.conn.commit()


def test_reports_gaps_for_requested_active_campaign_only(db):
    selected_id = db.create_campaign(
        name="Launch Arc",
        goal="Maintain launch education",
        start_date="2026-05-01",
        end_date="2026-05-07",
        status="active",
    )
    other_id = db.create_campaign(
        name="Other Arc",
        start_date="2026-05-01",
        end_date="2026-05-07",
        status="active",
    )
    paused_id = db.create_campaign(
        name="Paused Arc",
        start_date="2026-05-01",
        end_date="2026-05-07",
        status="paused",
    )
    db.insert_planned_topic("launch", target_date="2026-05-01", campaign_id=selected_id)
    db.insert_planned_topic("proof", target_date="2026-05-04", campaign_id=selected_id)
    db.insert_planned_topic("other", target_date="2026-05-02", campaign_id=other_id)
    db.insert_planned_topic("paused", target_date="2026-05-02", campaign_id=paused_id)

    report = plan_campaign_date_gaps(
        db,
        campaign_id=selected_id,
        days_ahead=7,
        min_gap_days=2,
        now=NOW,
    )

    assert report.campaign_count == 1
    assert [(gap.start_date, gap.end_date, gap.days) for gap in report.gaps] == [
        ("2026-05-02", "2026-05-03", 2),
        ("2026-05-05", "2026-05-07", 3),
    ]
    assert {gap.campaign_id for gap in report.gaps} == {selected_id}
    assert "Launch Arc" in format_campaign_date_gaps_text(report)
    assert "Other Arc" not in format_campaign_date_gaps_text(report)


def test_queued_content_linked_to_planned_topic_counts_as_coverage(db):
    campaign_id = db.create_campaign(
        name="Queue Arc",
        goal="Keep publishing queue healthy",
        start_date="2026-05-01",
        end_date="2026-05-05",
        status="active",
    )
    topic_id = db.insert_planned_topic("queued proof", campaign_id=campaign_id)
    content_id = _content(db, "Queued campaign post")
    _link_topic(db, topic_id, content_id)
    db.queue_for_publishing(
        content_id,
        scheduled_at="2026-05-03T10:00:00+00:00",
        platform="x",
    )

    report = plan_campaign_date_gaps(
        db,
        days_ahead=5,
        min_gap_days=2,
        now=NOW,
    )

    assert [(gap.start_date, gap.end_date) for gap in report.gaps] == [
        ("2026-05-01", "2026-05-02"),
        ("2026-05-04", "2026-05-05"),
    ]
    assert db.get_planned_topics(status="planned") == []


def test_published_generated_content_linked_to_planned_topic_counts_as_coverage(db):
    campaign_id = db.create_campaign(
        name="Published Arc",
        goal="Show shipped examples",
        start_date="2026-05-01",
        end_date="2026-05-05",
        status="active",
    )
    topic_id = db.insert_planned_topic("published proof", campaign_id=campaign_id)
    content_id = _content(db, "Published campaign post")
    _link_topic(db, topic_id, content_id)
    db.upsert_publication_success(
        content_id,
        platform="x",
        platform_post_id="post-1",
        published_at="2026-05-04T12:00:00+00:00",
    )

    report = plan_campaign_date_gaps(
        db,
        days_ahead=5,
        min_gap_days=2,
        now=NOW,
    )

    assert [(gap.start_date, gap.end_date, gap.days) for gap in report.gaps] == [
        ("2026-05-01", "2026-05-03", 3)
    ]
    assert report.gaps[0].suggestion.target_date == "2026-05-01"
    assert "Show shipped examples" in report.gaps[0].suggestion.angle


def test_json_and_text_output_are_deterministic_and_read_only(db):
    campaign_id = db.create_campaign(
        name="Output Arc",
        goal="Explain deterministic operators",
        start_date="2026-05-01",
        end_date="2026-05-04",
        status="active",
    )
    db.insert_planned_topic("opening", target_date="2026-05-01", campaign_id=campaign_id)

    report = plan_campaign_date_gaps(
        db,
        days_ahead=4,
        min_gap_days=2,
        now=NOW,
    )
    payload = json.loads(format_campaign_date_gaps_json(report))
    text = format_campaign_date_gaps_text(report)

    assert list(payload) == sorted(payload)
    assert payload["summary"] == {
        "campaigns": 1,
        "gaps": 1,
        "suggestions": 1,
        "uncovered_days": 3,
    }
    assert payload["gaps"][0]["suggestion"]["topic"] == "Follow-up: opening"
    assert text.startswith("Campaign Date Gaps")
    assert "Output Arc (ID" in text
    assert db.conn.execute("SELECT COUNT(*) FROM planned_topics").fetchone()[0] == 1


def test_cli_supports_requested_flags_and_json_output(db, monkeypatch, capsys):
    campaign_id = db.create_campaign(
        name="CLI Arc",
        start_date="2026-05-01",
        end_date="2026-05-03",
        status="active",
    )
    monkeypatch.setattr(campaign_date_gaps_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        campaign_date_gaps_script,
        "plan_campaign_date_gaps",
        lambda db, **kwargs: plan_campaign_date_gaps(db, now=NOW, **kwargs),
    )

    exit_code = campaign_date_gaps_script.main(
        [
            "--campaign-id",
            str(campaign_id),
            "--days-ahead",
            "3",
            "--min-gap-days",
            "2",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["campaign_id"] == campaign_id
    assert payload["gaps"][0]["start_date"] == "2026-05-01"
    assert payload["gaps"][0]["end_date"] == "2026-05-03"
