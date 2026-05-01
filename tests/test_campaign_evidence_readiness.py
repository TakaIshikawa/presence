"""Tests for campaign evidence readiness reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from campaign_evidence_readiness import main  # noqa: E402
from synthesis.campaign_evidence_readiness import (  # noqa: E402
    build_campaign_evidence_readiness_report,
    format_campaign_evidence_readiness_json,
    format_campaign_evidence_readiness_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _campaign(db, name: str) -> int:
    return db.create_campaign(
        name=name,
        goal="Plan evidence-backed campaign posts",
        start_date="2026-05-01",
        end_date="2026-05-31",
        status="active",
    )


def _knowledge(db, text: str, *, approved: int = 1) -> None:
    db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, content, insight, approved, published_at, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            f"knowledge-{abs(hash(text))}",
            text,
            text,
            approved,
            "2026-04-30T12:00:00+00:00",
            "2026-04-30T12:00:00+00:00",
        ),
    )
    db.conn.commit()


def _prior_content(db, topic: str, content: str) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.insert_content_topics(content_id, [(topic, "", 0.9)])
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        ("2026-04-29T12:00:00+00:00", content_id),
    )
    db.conn.commit()
    return content_id


def _github_activity(
    db,
    title: str,
    *,
    updated_at: str = "2026-04-30T12:00:00+00:00",
) -> int:
    return db.upsert_github_activity(
        repo_name="presence",
        activity_type="pull_request",
        number=title,
        title=title,
        body="",
        state="open",
        author="taka",
        url=f"https://example.com/{title}",
        updated_at=updated_at,
        labels=["planning"],
    )


def test_report_filters_campaign_and_upcoming_target_window_with_counts(db):
    campaign_id = _campaign(db, "Launch")
    other_campaign_id = _campaign(db, "Other")
    ready_id = db.insert_planned_topic(
        topic="architecture",
        angle="Show module boundary decisions for campaign planning",
        target_date="2026-05-03",
        source_material=json.dumps({"commits": ["sha-1"], "messages": ["msg-1"]}),
        campaign_id=campaign_id,
    )
    thin_id = db.insert_planned_topic(
        topic="testing",
        angle="Fixture coverage for deterministic tests",
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
        target_date="2026-05-03",
        campaign_id=other_campaign_id,
    )

    _knowledge(db, "Architecture notes for campaign planning boundaries.")
    _knowledge(db, "Testing fixtures can keep deterministic assertions stable.")
    _github_activity(db, "Architecture readiness report")
    _prior_content(db, "architecture", "Prior architecture post about planning.")

    report = build_campaign_evidence_readiness_report(
        db,
        campaign_id=campaign_id,
        days_ahead=7,
        min_evidence=3,
        now=NOW,
    )

    assert [topic.planned_topic_id for topic in report.topics] == [ready_id, thin_id]
    ready = report.topics[0]
    assert ready.readiness == "ready"
    assert ready.evidence_counts == {
        "source_material": 2,
        "knowledge": 1,
        "github_activity": 1,
        "prior_content": 1,
    }
    assert ready.total_evidence == 5
    assert report.topics[1].readiness == "thin"
    assert report.topics[1].evidence_counts["knowledge"] == 1
    assert report.ready_count == 1
    assert report.thin_count == 1
    assert report.missing_count == 0


def test_missing_topic_gets_blocking_recommendations(db):
    campaign_id = _campaign(db, "Missing Evidence")
    db.insert_planned_topic(
        topic="operations",
        angle="Publish without evidence",
        target_date="2026-05-02",
        campaign_id=campaign_id,
    )

    report = build_campaign_evidence_readiness_report(
        db,
        campaign_id=campaign_id,
        days_ahead=3,
        min_evidence=2,
        now=NOW,
    )

    topic = report.topics[0]
    assert topic.readiness == "missing"
    assert topic.total_evidence == 0
    assert "attach explicit source_material" in topic.recommendations[0]
    assert any("block generation" in item for item in topic.recommendations)


def test_min_evidence_changes_ready_label_deterministically(db):
    campaign_id = _campaign(db, "Threshold")
    db.insert_planned_topic(
        topic="architecture",
        target_date="2026-05-03",
        source_material="sha-1 sha-2",
        campaign_id=campaign_id,
    )

    thin = build_campaign_evidence_readiness_report(
        db,
        campaign_id=campaign_id,
        min_evidence=3,
        now=NOW,
    )
    ready = build_campaign_evidence_readiness_report(
        db,
        campaign_id=campaign_id,
        min_evidence=2,
        now=NOW,
    )

    assert thin.topics[0].readiness == "thin"
    assert ready.topics[0].readiness == "ready"


def test_old_github_activity_and_unapproved_knowledge_do_not_count(db):
    campaign_id = _campaign(db, "Recency")
    db.insert_planned_topic(
        topic="testing",
        target_date="2026-05-03",
        campaign_id=campaign_id,
    )
    _knowledge(db, "Testing evidence that is not approved.", approved=0)
    _github_activity(
        db,
        "Testing activity too old",
        updated_at=(NOW - timedelta(days=90)).isoformat(),
    )

    report = build_campaign_evidence_readiness_report(
        db,
        campaign_id=campaign_id,
        now=NOW,
    )

    assert report.topics[0].evidence_counts["knowledge"] == 0
    assert report.topics[0].evidence_counts["github_activity"] == 0
    assert report.topics[0].readiness == "missing"


def test_json_and_text_output(db):
    campaign_id = _campaign(db, "Readable")
    topic_id = db.insert_planned_topic(
        topic="architecture",
        target_date="2026-05-03",
        source_material="sha-1",
        campaign_id=campaign_id,
    )

    report = build_campaign_evidence_readiness_report(db, campaign_id=campaign_id, now=NOW)
    payload = json.loads(format_campaign_evidence_readiness_json(report))
    text = format_campaign_evidence_readiness_text(report)

    assert payload["topics"][0]["planned_topic_id"] == topic_id
    assert payload["topics"][0]["evidence_counts"]["source_material"] == 1
    assert "Campaign Evidence Readiness" in text
    assert f"planned topic #{topic_id}" in text
    assert "source_material=1" in text


def test_cli_wiring_json_output(db, capsys):
    campaign_id = _campaign(db, "CLI")
    topic_id = db.insert_planned_topic(
        topic="architecture",
        target_date="2026-05-03",
        source_material="sha-1 sha-2",
        campaign_id=campaign_id,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("campaign_evidence_readiness.script_context", fake_script_context):
        exit_code = main(
            [
                "--campaign-id",
                str(campaign_id),
                "--days-ahead",
                "7",
                "--min-evidence",
                "2",
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["topics"][0]["planned_topic_id"] == topic_id
    assert payload["topics"][0]["readiness"] == "ready"


def test_unknown_campaign_returns_cli_error(db, capsys):
    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("campaign_evidence_readiness.script_context", fake_script_context):
        exit_code = main(["--campaign-id", "999", "--format", "json"])

    assert exit_code == 1
    assert "Campaign 999 does not exist" in capsys.readouterr().err
