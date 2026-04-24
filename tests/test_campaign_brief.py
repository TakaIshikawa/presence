"""Tests for campaign brief generation."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from campaign_brief import format_json_brief, format_markdown_brief, main
from synthesis.campaign_brief import CampaignBriefBuilder


def seed_campaign_brief_data(db) -> int:
    now = datetime.now(timezone.utc)
    campaign_id = db.create_campaign(
        name="April Launch",
        goal="Explain practical launch lessons",
        start_date=(now - timedelta(days=5)).date().isoformat(),
        end_date=(now + timedelta(days=10)).date().isoformat(),
        status="active",
    )
    db.insert_planned_topic(
        topic="architecture",
        angle="Show the module boundary decision behind campaign planning",
        target_date=(now + timedelta(days=1)).date().isoformat(),
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="testing",
        angle="Fixture coverage for planning artifacts",
        target_date=(now + timedelta(days=2)).date().isoformat(),
        campaign_id=campaign_id,
    )

    db.insert_commit(
        "presence",
        "sha-architecture-1",
        "refactor: clarify architecture boundary for campaign brief generation",
        (now - timedelta(days=1)).isoformat(),
        "taka",
    )
    db.insert_claude_message(
        "sess-architecture",
        "msg-architecture-1",
        "/repo",
        (now - timedelta(hours=6)).isoformat(),
        "Investigate architecture tradeoffs for the campaign planning brief",
    )
    db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved,
            published_at, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "knowledge-architecture-1",
            "https://example.com/architecture",
            "Example Author",
            "Architecture boundaries work best when each planning artifact is read-only.",
            "Use read-only planning artifacts to keep synthesis from mutating workflow state.",
            1,
            (now - timedelta(days=2)).isoformat(),
            (now - timedelta(days=2)).isoformat(),
        ),
    )

    related_content = db.insert_generated_content(
        content_type="x_post",
        source_commits=["oldsha"],
        source_messages=["oldmsg"],
        content="A previous architecture post about campaign planning boundaries.",
        eval_score=8.0,
        eval_feedback="Good",
        content_format="micro_story",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        ((now - timedelta(days=3)).isoformat(), related_content),
    )
    db.insert_content_topics(related_content, [("architecture", "planning", 0.95)])

    repeated_content = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Another architecture post using the same micro story pattern.",
        eval_score=7.5,
        eval_feedback="Good",
        content_format="micro_story",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        ((now - timedelta(days=4)).isoformat(), repeated_content),
    )
    db.insert_content_topics(repeated_content, [("architecture", "patterns", 0.9)])
    db.conn.commit()
    return campaign_id


def test_campaign_brief_includes_evidence_knowledge_previous_posts_and_risks(db):
    campaign_id = seed_campaign_brief_data(db)

    brief = CampaignBriefBuilder(db).build(campaign_id=campaign_id, limit=1)

    assert brief.campaign["id"] == campaign_id
    assert len(brief.topics) == 1
    topic = brief.topics[0]
    assert topic.topic == "architecture"
    assert any(item.source_type == "commit" for item in topic.evidence)
    assert any(item.source_type == "session" for item in topic.evidence)
    assert topic.knowledge_snippets
    assert "read-only planning artifacts" in topic.knowledge_snippets[0].excerpt
    assert topic.previous_related_posts
    assert any("recent similar content" in risk for risk in topic.risks)
    assert any("overused pattern: micro_story" in risk for risk in topic.risks)


def test_campaign_brief_does_not_mutate_planned_topics_or_content(db):
    campaign_id = seed_campaign_brief_data(db)
    before_topics = [
        dict(row)
        for row in db.conn.execute(
            "SELECT id, status, content_id FROM planned_topics ORDER BY id"
        ).fetchall()
    ]
    before_content_count = db.conn.execute(
        "SELECT COUNT(*) FROM generated_content"
    ).fetchone()[0]

    CampaignBriefBuilder(db).build(campaign_id=campaign_id, limit=2)

    after_topics = [
        dict(row)
        for row in db.conn.execute(
            "SELECT id, status, content_id FROM planned_topics ORDER BY id"
        ).fetchall()
    ]
    after_content_count = db.conn.execute(
        "SELECT COUNT(*) FROM generated_content"
    ).fetchone()[0]
    assert after_topics == before_topics
    assert after_content_count == before_content_count


def test_campaign_brief_json_and_markdown_formats(db):
    campaign_id = seed_campaign_brief_data(db)
    brief = CampaignBriefBuilder(db).build(campaign_id=campaign_id, limit=1)

    markdown = format_markdown_brief(brief)
    payload = json.loads(format_json_brief(brief))

    assert "# Campaign Brief: April Launch" in markdown
    assert "## 1. architecture" in markdown
    assert "### Supporting Evidence" in markdown
    assert payload["campaign"]["id"] == campaign_id
    assert payload["topics"][0]["topic"] == "architecture"
    assert payload["topics"][0]["previous_related_posts"]


def test_campaign_brief_cli_json_and_output_file(db, capsys, tmp_path):
    campaign_id = seed_campaign_brief_data(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("campaign_brief.script_context", fake_script_context):
        main(["--campaign-id", str(campaign_id), "--limit", "1", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["topics"][0]["topic"] == "architecture"

    output_path = tmp_path / "brief.md"
    with patch("campaign_brief.script_context", fake_script_context):
        main(["--campaign-id", str(campaign_id), "--limit", "1", "--output", str(output_path)])

    assert output_path.read_text(encoding="utf-8").startswith("# Campaign Brief: April Launch")
