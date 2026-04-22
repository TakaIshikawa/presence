"""Tests for engagement-based posting window recommendations."""

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.posting_windows import PostingWindowRecommender, recommend_posting_windows
from posting_windows import format_json_report, format_text_report, main
from storage.db import Database


@pytest.fixture
def db():
    db_instance = Database(":memory:")
    db_instance.connect()
    schema_path = Path(__file__).parent.parent / "schema.sql"
    db_instance.init_schema(str(schema_path))
    yield db_instance
    db_instance.close()


def _insert_content(db: Database) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha"],
        source_messages=["uuid"],
        content="Test content",
        eval_score=7.0,
        eval_feedback="Good",
    )


def _set_x_post(db: Database, published_at: datetime, score: float) -> None:
    content_id = _insert_content(db)
    tweet_id = f"tweet-{content_id}"
    db.mark_published(content_id, f"https://x.com/test/{content_id}", tweet_id=tweet_id)
    db.conn.execute(
        """UPDATE generated_content SET published_at = ? WHERE id = ?""",
        (published_at.isoformat(), content_id),
    )
    db.conn.execute(
        """UPDATE content_publications SET published_at = ?
           WHERE content_id = ? AND platform = 'x'""",
        (published_at.isoformat(), content_id),
    )
    db.conn.commit()
    db.insert_engagement(content_id, tweet_id, 1, 0, 0, 0, score)


def _set_bluesky_post(db: Database, published_at: datetime, score: float) -> None:
    content_id = _insert_content(db)
    uri = f"at://test/post/{content_id}"
    db.mark_published_bluesky(content_id, uri)
    db.conn.execute(
        """UPDATE content_publications SET published_at = ?
           WHERE content_id = ? AND platform = 'bluesky'""",
        (published_at.isoformat(), content_id),
    )
    db.conn.commit()
    db.insert_bluesky_engagement(content_id, uri, 1, 0, 0, 0, score)


def test_recommend_ranks_combined_platform_windows_and_labels_confidence(db):
    now = datetime.now(timezone.utc)
    monday = now - timedelta(days=(now.weekday() - 0) % 7)
    monday = monday.replace(hour=10, minute=0, second=0, microsecond=0)
    wednesday = now - timedelta(days=(now.weekday() - 2) % 7)
    wednesday = wednesday.replace(hour=15, minute=0, second=0, microsecond=0)

    for index in range(4):
        _set_x_post(db, monday - timedelta(weeks=index), 20.0 + index)
        _set_bluesky_post(db, monday - timedelta(weeks=index), 18.0 + index)
    _set_x_post(db, wednesday, 50.0)

    windows = PostingWindowRecommender(db).recommend(days=90, platform="all", limit=2)

    assert len(windows) == 2
    assert windows[0].day_of_week == 0
    assert windows[0].hour_utc == 10
    assert windows[0].sample_size == 8
    assert windows[0].confidence_label == "medium"
    assert windows[0].normalized_engagement > windows[1].normalized_engagement
    assert windows[1].confidence_label == "low"


def test_recommend_supports_platform_filter(db):
    now = datetime.now(timezone.utc)
    monday = now - timedelta(days=(now.weekday() - 0) % 7)
    monday = monday.replace(hour=9, minute=0, second=0, microsecond=0)
    friday = now - timedelta(days=(now.weekday() - 4) % 7)
    friday = friday.replace(hour=21, minute=0, second=0, microsecond=0)

    _set_x_post(db, monday, 30.0)
    _set_bluesky_post(db, friday, 40.0)

    windows = recommend_posting_windows(db, days=90, platform="bluesky", limit=5)

    assert len(windows) == 1
    assert windows[0].platform == "bluesky"
    assert windows[0].day_of_week == 4
    assert windows[0].hour_utc == 21


def test_formatters_include_expected_fields():
    recommender = MagicMock()
    window = MagicMock(
        platform="all",
        day_of_week=1,
        day_name="Tuesday",
        hour_utc=14,
        sample_size=3,
        avg_engagement=12.0,
        normalized_engagement=10.5,
        confidence=0.5,
        confidence_label="medium",
    )
    recommender.recommend.return_value = [window]

    text = format_text_report([window], days=30, platform="all", limit=10)
    assert "Tuesday 14:00 UTC" in text
    assert "medium confidence" in text

    data = json.loads(format_json_report([window]))
    assert data[0]["normalized_engagement"] == 10.5
    assert data[0]["confidence_label"] == "medium"


def test_main_supports_flags_and_json_output(capsys):
    recommender = MagicMock()
    recommender.recommend.return_value = []

    @contextmanager
    def fake_script_context():
        yield None, MagicMock()

    with patch.object(
        sys,
        "argv",
        ["posting_windows.py", "--days", "14", "--platform", "x", "--limit", "3", "--json"],
    ):
        with patch("posting_windows.script_context", fake_script_context):
            with patch("posting_windows.PostingWindowRecommender", return_value=recommender):
                main()

    recommender.recommend.assert_called_once_with(days=14, platform="x", limit=3)
    assert json.loads(capsys.readouterr().out) == []
