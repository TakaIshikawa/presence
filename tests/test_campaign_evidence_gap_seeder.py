"""Tests for seeding content ideas from campaign evidence gaps."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_campaign_evidence_gaps import main  # noqa: E402
from synthesis.campaign_evidence_gap_seeder import (  # noqa: E402
    SOURCE_NAME,
    format_campaign_evidence_gap_seed_json,
    format_campaign_evidence_gap_seed_text,
    seed_campaign_evidence_gaps,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _campaign(db, name: str) -> int:
    return db.create_campaign(
        name=name,
        goal="Seed evidence-backed follow-up ideas",
        start_date="2026-05-01",
        end_date="2026-05-31",
        status="active",
    )


def _knowledge(db, text: str) -> None:
    db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, content, insight, approved, published_at, ingested_at)
           VALUES (?, ?, ?, ?, 1, ?, ?)""",
        (
            "curated_article",
            f"knowledge-{abs(hash(text))}",
            text,
            text,
            "2026-04-30T12:00:00+00:00",
            "2026-04-30T12:00:00+00:00",
        ),
    )
    db.conn.commit()


def test_dry_run_reports_eligible_topics_without_inserting_content_ideas(db):
    campaign_id = _campaign(db, "Launch")
    topic_id = db.insert_planned_topic(
        topic="testing",
        angle="Add deterministic fixture coverage",
        target_date="2026-05-03",
        campaign_id=campaign_id,
    )

    report = seed_campaign_evidence_gaps(
        db,
        campaign_id=campaign_id,
        days_ahead=7,
        min_evidence=2,
        dry_run=True,
        now=NOW,
    )

    assert [(result.status, result.planned_topic_id, result.reason) for result in report.results] == [
        ("proposed", topic_id, "dry run")
    ]
    assert report.proposed_count == 1
    assert report.created_count == 0
    assert db.get_content_ideas(status="open") == []


def test_apply_inserts_one_content_idea_with_planned_topic_metadata(db):
    campaign_id = _campaign(db, "Launch")
    topic_id = db.insert_planned_topic(
        topic="architecture",
        angle="Explain why the campaign planning boundary moved",
        target_date="2026-05-04",
        campaign_id=campaign_id,
    )

    report = seed_campaign_evidence_gaps(
        db,
        campaign_id=campaign_id,
        min_evidence=3,
        now=NOW,
    )

    assert report.created_count == 1
    result = report.results[0]
    assert result.status == "created"
    assert result.idea_id is not None
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    idea = ideas[0]
    assert idea["source"] == SOURCE_NAME
    assert idea["topic"] == "architecture"
    assert idea["priority"] == "high"
    assert f"Planned campaign topic #{topic_id}" in idea["note"]
    metadata = json.loads(idea["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["planned_topic_id"] == topic_id
    assert metadata["campaign_id"] == campaign_id
    assert metadata["readiness"] == "missing"
    assert metadata["min_evidence"] == 3


def test_campaign_filtering_and_date_window_only_seed_matching_topics(db):
    campaign_id = _campaign(db, "Launch")
    other_campaign_id = _campaign(db, "Other")
    matching_id = db.insert_planned_topic(
        topic="testing",
        target_date="2026-05-05",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="outside-window",
        target_date="2026-05-20",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="other-campaign",
        target_date="2026-05-05",
        campaign_id=other_campaign_id,
    )

    report = seed_campaign_evidence_gaps(
        db,
        campaign_id=campaign_id,
        days_ahead=7,
        min_evidence=2,
        dry_run=True,
        now=NOW,
    )

    assert [result.planned_topic_id for result in report.results] == [matching_id]


def test_ready_topics_are_not_seeded_when_evidence_meets_threshold(db):
    campaign_id = _campaign(db, "Ready")
    ready_id = db.insert_planned_topic(
        topic="architecture",
        target_date="2026-05-03",
        source_material=json.dumps({"commits": ["sha-1"], "messages": ["msg-1"]}),
        campaign_id=campaign_id,
    )
    thin_id = db.insert_planned_topic(
        topic="testing",
        target_date="2026-05-04",
        source_material="sha-2",
        campaign_id=campaign_id,
    )
    _knowledge(db, "Architecture notes about module boundaries.")

    report = seed_campaign_evidence_gaps(
        db,
        campaign_id=campaign_id,
        min_evidence=3,
        dry_run=True,
        now=NOW,
    )

    assert [result.planned_topic_id for result in report.results] == [thin_id]
    assert ready_id not in [result.planned_topic_id for result in report.results]
    assert report.results[0].readiness == "thin"


def test_second_run_suppresses_duplicate_open_idea_for_same_planned_topic(db):
    campaign_id = _campaign(db, "Duplicates")
    topic_id = db.insert_planned_topic(
        topic="operations",
        target_date="2026-05-03",
        campaign_id=campaign_id,
    )

    first = seed_campaign_evidence_gaps(db, campaign_id=campaign_id, now=NOW)
    second = seed_campaign_evidence_gaps(db, campaign_id=campaign_id, now=NOW)

    assert first.results[0].status == "created"
    assert second.results[0].status == "skipped"
    assert second.results[0].reason == "open duplicate"
    assert second.results[0].planned_topic_id == topic_id
    assert second.results[0].idea_id == first.results[0].idea_id
    assert len(db.get_content_ideas(status="open")) == 1


def test_text_and_json_output_include_seed_results(db):
    campaign_id = _campaign(db, "Output")
    topic_id = db.insert_planned_topic(
        topic="testing",
        target_date="2026-05-03",
        campaign_id=campaign_id,
    )
    report = seed_campaign_evidence_gaps(
        db,
        campaign_id=campaign_id,
        dry_run=True,
        now=NOW,
    )

    payload = json.loads(format_campaign_evidence_gap_seed_json(report))
    text = format_campaign_evidence_gap_seed_text(report)

    assert payload["results"][0]["planned_topic_id"] == topic_id
    assert payload["results"][0]["status"] == "proposed"
    assert "Campaign Evidence Gap Seeder" in text
    assert f"planned topic #{topic_id}" in text
    assert "proposed" in text


def test_cli_wiring_json_output(db, capsys):
    campaign_id = _campaign(db, "CLI")
    topic_id = db.insert_planned_topic(
        topic="architecture",
        target_date="2026-05-03",
        campaign_id=campaign_id,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_campaign_evidence_gaps.script_context", fake_script_context):
        exit_code = main(
            [
                "--campaign-id",
                str(campaign_id),
                "--days-ahead",
                "7",
                "--min-evidence",
                "2",
                "--dry-run",
                "--format",
                "json",
            ]
        )

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dry_run"] is True
    assert payload["results"][0]["planned_topic_id"] == topic_id
    assert payload["results"][0]["status"] == "proposed"
    assert db.get_content_ideas(status="open") == []
