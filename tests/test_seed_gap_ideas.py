"""Tests for seeding content ideas from gap findings."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from seed_gap_ideas import format_results_table, main, seed_gap_ideas


def _add_generated(db, topic: str, published_at: str) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"Generated content about {topic}",
        eval_score=8.0,
        eval_feedback="good",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ? WHERE id = ?",
        (published_at, content_id),
    )
    db.conn.commit()
    db.insert_content_topics(content_id, [(topic, "", 1.0)])
    return content_id


def seed_gap_idea_data(db) -> int:
    campaign_id = db.create_campaign(
        name="Gap Campaign",
        goal="Cover missed work",
        start_date="2026-04-01",
        end_date="2026-04-30",
        status="active",
    )
    db.insert_planned_topic(
        topic="architecture",
        angle="module boundary write-up",
        target_date="2026-04-22",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="testing",
        angle="fixture cleanup",
        target_date="2026-04-20",
        campaign_id=campaign_id,
    )
    _add_generated(db, "testing", "2026-04-20T09:00:00+00:00")

    db.insert_commit(
        "presence",
        "sha-perf-1",
        "perf: optimize cache latency in source scoring",
        "2026-04-21T12:00:00+00:00",
        "taka",
    )
    db.insert_commit(
        "presence",
        "sha-perf-2",
        "fix: reduce slow throughput path in generation",
        "2026-04-22T12:00:00+00:00",
        "taka",
    )
    return campaign_id


def test_seed_gap_ideas_creates_planned_and_source_ideas(db):
    campaign_id = seed_gap_idea_data(db)

    results = seed_gap_ideas(
        db,
        days=7,
        campaign_id=campaign_id,
        priority="high",
        target_date=datetime(2026, 4, 23, tzinfo=timezone.utc),
    )

    assert [(result.status, result.kind, result.topic) for result in results] == [
        ("created", "planned", "architecture"),
        ("created", "source", "performance"),
    ]

    ideas = db.get_content_ideas(status="open", priority="high")
    assert len(ideas) == 2
    planned = next(idea for idea in ideas if idea["topic"] == "architecture")
    planned_metadata = json.loads(planned["source_metadata"])
    assert planned["source"] == "content_gap_detector"
    assert planned_metadata["gap_type"] == "planned_topic"
    assert planned_metadata["planned_topic_id"]
    assert planned_metadata["campaign_id"] == campaign_id

    source = next(idea for idea in ideas if idea["topic"] == "performance")
    source_metadata = json.loads(source["source_metadata"])
    assert source_metadata["gap_type"] == "source_rich"
    assert source_metadata["gap_fingerprint"]
    assert source_metadata["source_count"] == 2


def test_seed_gap_ideas_skips_duplicate_open_gap_ideas(db):
    campaign_id = seed_gap_idea_data(db)
    now = datetime(2026, 4, 23, tzinfo=timezone.utc)
    first = seed_gap_ideas(db, days=7, campaign_id=campaign_id, target_date=now)

    second = seed_gap_ideas(db, days=7, campaign_id=campaign_id, target_date=now)

    assert [result.status for result in first] == ["created", "created"]
    assert [result.status for result in second] == ["skipped", "skipped"]
    assert [result.idea_id for result in second] == [result.idea_id for result in first]
    assert all(result.reason == "open duplicate" for result in second)
    assert len(db.get_content_ideas(status="open")) == 2


def test_seed_gap_ideas_dry_run_and_limit_do_not_write(db):
    campaign_id = seed_gap_idea_data(db)

    results = seed_gap_ideas(
        db,
        days=7,
        campaign_id=campaign_id,
        dry_run=True,
        limit=1,
        target_date=datetime(2026, 4, 23, tzinfo=timezone.utc),
    )

    assert len(results) == 1
    assert results[0].status == "skipped"
    assert results[0].reason == "dry run"
    assert db.get_content_ideas(status="open") == []


def test_format_results_table_is_concise(db):
    campaign_id = seed_gap_idea_data(db)
    results = seed_gap_ideas(
        db,
        days=7,
        campaign_id=campaign_id,
        limit=1,
        target_date=datetime(2026, 4, 23, tzinfo=timezone.utc),
    )

    output = format_results_table(results)

    assert "Status" in output
    assert "created" in output
    assert "architecture" in output


def test_main_prints_seed_table(db, capsys):
    campaign_id = seed_gap_idea_data(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("seed_gap_ideas.script_context", fake_script_context), patch(
        "seed_gap_ideas.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value = datetime(2026, 4, 23, tzinfo=timezone.utc)
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        main(["--days", "7", "--campaign-id", str(campaign_id), "--priority", "low"])

    output = capsys.readouterr().out
    assert "created" in output
    assert "architecture" in output
    assert "performance" in output
