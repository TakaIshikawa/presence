"""Tests for stale topic resurfacing."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from resurface_topics import (
    format_json_payload,
    main,
    seed_content_ideas,
)
from synthesis.stale_topic_resurfacer import StaleTopicResurfacer


NOW = datetime(2026, 4, 25, tzinfo=timezone.utc)


def _add_topic_content(
    db,
    *,
    topic: str,
    published_at: str,
    x_score: float | None = None,
    bluesky_score: float | None = None,
    content: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content or f"Generated content about {topic}",
        eval_score=8.0,
        eval_feedback="good",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, published_at = ?, created_at = ?
           WHERE id = ?""",
        (published_at, published_at, content_id),
    )
    db.conn.commit()
    db.insert_content_topics(content_id, [(topic, "", 1.0)])
    if x_score is not None:
        db.insert_engagement(content_id, f"tw-{content_id}", 1, 0, 0, 0, x_score)
    if bluesky_score is not None:
        db.insert_bluesky_engagement(
            content_id,
            f"at://did:example/app.bsky.feed.post/{content_id}",
            1,
            0,
            0,
            0,
            bluesky_score,
        )
    return content_id


def test_stale_topics_rank_strong_old_topics_with_reasons(db):
    testing_id = _add_topic_content(
        db,
        topic="testing",
        published_at="2026-02-10T10:00:00+00:00",
        x_score=10.0,
        bluesky_score=3.0,
    )
    _add_topic_content(
        db,
        topic="testing",
        published_at="2026-01-20T10:00:00+00:00",
        x_score=7.0,
    )
    _add_topic_content(
        db,
        topic="architecture",
        published_at="2026-02-15T10:00:00+00:00",
        x_score=6.0,
    )
    _add_topic_content(
        db,
        topic="workflow",
        published_at="2026-02-20T10:00:00+00:00",
        x_score=1.0,
    )

    report = StaleTopicResurfacer(db).detect(
        min_age_days=30,
        lookback_days=120,
        limit=5,
        target_date=NOW,
    )

    assert [topic.topic for topic in report.topics] == ["testing", "architecture"]
    assert report.topics[0].sample_count == 2
    assert report.topics[0].avg_engagement == 10.0
    assert testing_id in report.topics[0].source_content_ids
    assert "Average historical engagement" in report.topics[0].reasons[0]
    assert report.generated_after == "2026-03-26T00:00:00+00:00"


def test_recent_generated_content_excludes_topic_from_resurfacing(db):
    _add_topic_content(
        db,
        topic="testing",
        published_at="2026-02-10T10:00:00+00:00",
        x_score=12.0,
    )
    _add_topic_content(
        db,
        topic="testing",
        published_at="2026-04-10T10:00:00+00:00",
        x_score=2.0,
    )
    _add_topic_content(
        db,
        topic="architecture",
        published_at="2026-02-15T10:00:00+00:00",
        x_score=7.0,
    )

    report = StaleTopicResurfacer(db).detect(
        min_age_days=30,
        lookback_days=120,
        target_date=NOW,
    )

    assert [topic.topic for topic in report.topics] == ["architecture"]


def test_seed_content_ideas_creates_metadata_and_skips_duplicates(db):
    first_id = _add_topic_content(
        db,
        topic="testing",
        published_at="2026-02-10T10:00:00+00:00",
        x_score=12.0,
    )
    report = StaleTopicResurfacer(db).detect(
        min_age_days=30,
        lookback_days=120,
        target_date=NOW,
    )

    first = seed_content_ideas(db, report)
    second = seed_content_ideas(db, report)

    assert [(result.status, result.topic) for result in first] == [("created", "testing")]
    assert [(result.status, result.topic) for result in second] == [("skipped", "testing")]
    assert second[0].idea_id == first[0].idea_id

    idea = db.get_content_idea(first[0].idea_id)
    metadata = json.loads(idea["source_metadata"])
    assert idea["source"] == "stale_topic_resurfacer"
    assert idea["priority"] == "high"
    assert metadata["stale_topic"] == "testing"
    assert metadata["source_id"] == "stale-topic:testing"
    assert metadata["source_content_ids"] == [first_id]


def test_json_payload_includes_topics_and_seed_results(db):
    _add_topic_content(
        db,
        topic="testing",
        published_at="2026-02-10T10:00:00+00:00",
        x_score=12.0,
    )
    report = StaleTopicResurfacer(db).detect(
        min_age_days=30,
        lookback_days=120,
        target_date=NOW,
    )
    seed_results = seed_content_ideas(db, report)

    payload = json.loads(format_json_payload(report, seed_results))

    assert payload["topics"][0]["topic"] == "testing"
    assert payload["seed_results"][0]["status"] == "created"
    assert payload["seed_results"][0]["idea_id"] is not None


def test_main_can_emit_json_and_seed_ideas(db, capsys):
    _add_topic_content(
        db,
        topic="testing",
        published_at="2026-02-10T10:00:00+00:00",
        x_score=12.0,
    )

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("resurface_topics.script_context", fake_script_context), patch(
        "resurface_topics.datetime"
    ) as mock_datetime:
        mock_datetime.now.return_value = NOW
        mock_datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)
        main(["--min-age-days", "30", "--lookback-days", "120", "--seed-ideas", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["topics"][0]["topic"] == "testing"
    assert payload["seed_results"][0]["status"] == "created"
    assert len(db.get_content_ideas(status="open")) == 1
