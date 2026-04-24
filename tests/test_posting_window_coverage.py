"""Tests for platform posting window coverage planning."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.posting_window_coverage import (
    PostingWindowCoveragePlanner,
    coverage_slots_to_dicts,
)
from posting_window_coverage import format_json_report, format_text_report, main
from storage.db import Database


@pytest.fixture
def db():
    db_instance = Database(":memory:")
    db_instance.connect()
    schema_path = Path(__file__).parent.parent / "schema.sql"
    db_instance.init_schema(str(schema_path))
    yield db_instance
    db_instance.close()


def _insert_content(db: Database, content: str = "Coverage test") -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha"],
        source_messages=["uuid"],
        content=content,
        eval_score=7.0,
        eval_feedback="Good",
    )


def _queue_item(
    db: Database,
    scheduled_at: datetime,
    *,
    platform: str = "x",
    status: str = "queued",
) -> None:
    content_id = _insert_content(db, f"{platform} queue item")
    db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, scheduled_at.isoformat(), platform, status),
    )
    db.conn.commit()


def _published_item(db: Database, published_at: datetime, *, platform: str = "x") -> None:
    content_id = _insert_content(db, f"{platform} published item")
    if platform == "x":
        db.mark_published(content_id, f"https://x.com/test/{content_id}", tweet_id=str(content_id))
    else:
        db.mark_published_bluesky(content_id, f"at://test/post/{content_id}")
    db.conn.execute(
        """UPDATE content_publications
           SET published_at = ?
           WHERE content_id = ? AND platform = ?""",
        (published_at.isoformat(), content_id, platform),
    )
    db.conn.commit()


def _window(day_of_week: int, hour_utc: int, platform: str):
    return MagicMock(
        day_of_week=day_of_week,
        hour_utc=hour_utc,
        normalized_engagement=12.5,
        confidence=0.6,
        confidence_label="medium",
        sample_size=4,
        platform=platform,
    )


def test_planner_returns_platform_filtered_open_slots(db):
    now = datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc)
    recommender = MagicMock()
    recommender.recommend.return_value = [_window(0, 10, "x")]

    slots = PostingWindowCoveragePlanner(db, recommender=recommender).recommend_slots(
        now=now,
        days_ahead=8,
        platform="x",
        limit_per_platform=2,
    )

    assert [slot.platform for slot in slots] == ["x", "x"]
    assert [slot.scheduled_at for slot in slots] == [
        datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc),
        datetime(2026, 4, 27, 10, 0, tzinfo=timezone.utc),
    ]
    recommender.recommend.assert_called_once_with(days=90, platform="x", limit=12)


def test_empty_queue_uses_fallback_windows_for_all_platforms(db):
    now = datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc)
    recommender = MagicMock()
    recommender.recommend.return_value = []

    slots = PostingWindowCoveragePlanner(db, recommender=recommender).recommend_slots(
        now=now,
        days_ahead=1,
        platform="all",
        limit_per_platform=1,
    )

    assert [(slot.platform, slot.scheduled_at.hour, slot.source) for slot in slots] == [
        ("bluesky", 9, "fallback"),
        ("x", 9, "fallback"),
    ]


def test_queued_items_suppress_occupied_platform_windows(db):
    now = datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc)
    occupied = datetime(2026, 4, 20, 10, 30, tzinfo=timezone.utc)
    _queue_item(db, occupied, platform="x")
    recommender = MagicMock()
    recommender.recommend.return_value = [_window(0, 10, "x"), _window(0, 12, "x")]

    slots = PostingWindowCoveragePlanner(db, recommender=recommender).recommend_slots(
        now=now,
        days_ahead=1,
        platform="x",
        limit_per_platform=2,
    )

    assert [slot.scheduled_at for slot in slots] == [
        datetime(2026, 4, 20, 12, 0, tzinfo=timezone.utc)
    ]


def test_all_platform_queue_suppresses_both_platforms(db):
    now = datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc)
    _queue_item(db, datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc), platform="all")
    recommender = MagicMock()
    recommender.recommend.return_value = [_window(0, 10, "x"), _window(0, 12, "x")]

    slots = PostingWindowCoveragePlanner(db, recommender=recommender).recommend_slots(
        now=now,
        days_ahead=1,
        platform="all",
        limit_per_platform=1,
    )

    assert [(slot.platform, slot.hour_utc) for slot in slots] == [("bluesky", 12), ("x", 12)]


def test_include_published_controls_publication_suppression(db):
    now = datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc)
    _published_item(db, datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc), platform="x")
    recommender = MagicMock()
    recommender.recommend.return_value = [_window(0, 10, "x"), _window(0, 12, "x")]
    planner = PostingWindowCoveragePlanner(db, recommender=recommender)

    without_published = planner.recommend_slots(
        now=now,
        days_ahead=1,
        platform="x",
        include_published=False,
        limit_per_platform=1,
    )
    with_published = planner.recommend_slots(
        now=now,
        days_ahead=1,
        platform="x",
        include_published=True,
        limit_per_platform=1,
    )

    assert without_published[0].hour_utc == 10
    assert with_published[0].hour_utc == 12


def test_formatters_emit_stable_table_and_json():
    slot = PostingWindowCoveragePlanner(
        MagicMock(),
        recommender=MagicMock(),
    )._open_slots_for_platform(
        platform="x",
        windows=[(0, 10, 11.0, 0.5, "medium", 3, "learned")],
        start=datetime(2026, 4, 20, 8, 0, tzinfo=timezone.utc),
        end=datetime(2026, 4, 20, 23, 0, tzinfo=timezone.utc),
        occupied=set(),
        embargo_windows=None,
        limit=1,
    )[0]

    text = format_text_report([slot], days_ahead=7, platform="x")
    assert "PLATFORM" in text
    assert "SCHEDULED_UTC" in text
    assert "x         2026-04-20T10:00:00+00:00" in text

    data = json.loads(format_json_report([slot]))
    assert data == coverage_slots_to_dicts([slot])


def test_main_supports_flags_and_json_output(capsys):
    planner = MagicMock()
    planner.recommend_slots.return_value = []

    @contextmanager
    def fake_script_context():
        config = MagicMock()
        config.publishing.embargo_windows = []
        yield config, MagicMock()

    with patch.object(
        sys,
        "argv",
        [
            "posting_window_coverage.py",
            "--platform",
            "bluesky",
            "--days-ahead",
            "3",
            "--include-published",
            "--json",
        ],
    ):
        with patch("posting_window_coverage.script_context", fake_script_context):
            with patch(
                "posting_window_coverage.PostingWindowCoveragePlanner",
                return_value=planner,
            ):
                main()

    planner.recommend_slots.assert_called_once_with(
        days_ahead=3,
        platform="bluesky",
        include_published=True,
        embargo_windows=[],
    )
    assert json.loads(capsys.readouterr().out) == []
