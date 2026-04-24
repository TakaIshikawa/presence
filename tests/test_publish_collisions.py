"""Tests for queued publish collision scanning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import sys
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.publish_collision import (
    collisions_to_json,
    format_text_collisions,
    scan_publish_collisions,
)
import publish_collisions


NOW = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _insert_content(
    db,
    content: str,
    *,
    content_type: str = "x_post",
    eval_score: float = 8.0,
    content_format: str | None = "tip",
) -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, content_format)
           VALUES (?, ?, ?, 0, ?)""",
        (content, content_type, eval_score, content_format),
    ).lastrowid


def _queue(
    db,
    content_id: int,
    scheduled_at: datetime,
    *,
    platform: str = "x",
    status: str = "queued",
) -> int:
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, scheduled_at.isoformat(), platform, status),
    ).lastrowid
    db.conn.commit()
    return queue_id


def test_scanner_groups_queued_items_by_platform_within_window(db):
    first = _insert_content(db, "First queued X post")
    second = _insert_content(db, "Second queued X post")
    distant = _insert_content(db, "Later X post")
    bluesky = _insert_content(db, "Nearby Bluesky post")
    _queue(db, first, NOW + timedelta(hours=1), platform="x")
    _queue(db, second, NOW + timedelta(hours=1, minutes=10), platform="x")
    _queue(db, distant, NOW + timedelta(hours=2), platform="x")
    _queue(db, bluesky, NOW + timedelta(hours=1, minutes=5), platform="bluesky")

    collisions = scan_publish_collisions(
        db,
        window_minutes=15,
        days_ahead=1,
        platform="all",
        now=NOW,
    )

    assert len(collisions) == 1
    assert collisions[0].platform == "x"
    assert [item.content_id for item in collisions[0].items] == [first, second]


def test_all_platform_queue_item_overlaps_platform_specific_items(db):
    all_content = _insert_content(db, "Cross-post to both platforms")
    x_content = _insert_content(db, "X-specific post")
    bluesky_content = _insert_content(db, "Bluesky-specific post")
    _queue(db, all_content, NOW + timedelta(hours=1), platform="all")
    _queue(db, x_content, NOW + timedelta(hours=1, minutes=4), platform="x")
    _queue(db, bluesky_content, NOW + timedelta(hours=1, minutes=8), platform="bluesky")

    collisions = scan_publish_collisions(
        db,
        window_minutes=10,
        days_ahead=1,
        platform="all",
        now=NOW,
    )

    assert [collision.platform for collision in collisions] == ["x", "bluesky"]
    assert [item.platform for item in collisions[0].items] == ["all", "x"]
    assert [item.platform for item in collisions[1].items] == ["all", "bluesky"]


def test_json_serialization_includes_generated_content_metadata(db):
    first = _insert_content(
        db,
        "A detailed post about queue planning and publishing cadence.",
        content_type="x_thread",
        eval_score=9.25,
        content_format="observation",
    )
    second = _insert_content(db, "Another queue planning post.")
    _queue(db, first, NOW + timedelta(minutes=30), platform="x")
    _queue(db, second, NOW + timedelta(minutes=35), platform="x")

    collisions = scan_publish_collisions(db, window_minutes=10, now=NOW)
    payload = json.loads(collisions_to_json(collisions))

    assert payload[0]["platform"] == "x"
    assert payload[0]["items"][0]["generated_content"] == {
        "content_type": "x_thread",
        "content_preview": "A detailed post about queue planning and publishing cadence.",
        "eval_score": 9.25,
        "content_format": "observation",
    }


def test_text_formatting_shows_collision_summary_and_item_snippets(db):
    first = _insert_content(db, "First post snippet")
    second = _insert_content(db, "Second post snippet")
    first_queue = _queue(db, first, NOW + timedelta(minutes=30), platform="x")
    second_queue = _queue(db, second, NOW + timedelta(minutes=34), platform="x")

    text = format_text_collisions(scan_publish_collisions(db, window_minutes=5, now=NOW))

    assert "Publish queue collisions: 1" in text
    assert "x: 2 queued items within 5 minutes" in text
    assert f"queue {first_queue} content {first}" in text
    assert f"queue {second_queue} content {second}" in text
    assert "First post snippet" in text


def test_text_formatting_no_collisions_message():
    assert format_text_collisions([]) == "No publish queue collisions found."


@contextmanager
def _fake_script_context(db):
    yield object(), db


def test_cli_returns_success_without_fail_flag_when_collisions_exist(db, capsys):
    first = _insert_content(db, "First CLI post")
    second = _insert_content(db, "Second CLI post")
    _queue(db, first, datetime.now(timezone.utc) + timedelta(minutes=30), platform="x")
    _queue(db, second, datetime.now(timezone.utc) + timedelta(minutes=32), platform="x")

    with patch("publish_collisions.script_context", return_value=_fake_script_context(db)):
        exit_code = publish_collisions.main(["--window-minutes", "5", "--format", "json"])

    assert exit_code == 0
    assert json.loads(capsys.readouterr().out)[0]["platform"] == "x"


def test_cli_fail_on_collision_returns_nonzero(db):
    first = _insert_content(db, "First fail post")
    second = _insert_content(db, "Second fail post")
    _queue(db, first, datetime.now(timezone.utc) + timedelta(minutes=30), platform="x")
    _queue(db, second, datetime.now(timezone.utc) + timedelta(minutes=31), platform="x")

    with patch("publish_collisions.script_context", return_value=_fake_script_context(db)):
        exit_code = publish_collisions.main(["--window-minutes", "5", "--fail-on-collision"])

    assert exit_code == 1


def test_cli_fail_on_collision_returns_success_when_clear(db):
    content_id = _insert_content(db, "Only queued post")
    _queue(db, content_id, datetime.now(timezone.utc) + timedelta(minutes=30), platform="x")

    with patch("publish_collisions.script_context", return_value=_fake_script_context(db)):
        exit_code = publish_collisions.main(["--fail-on-collision"])

    assert exit_code == 0
