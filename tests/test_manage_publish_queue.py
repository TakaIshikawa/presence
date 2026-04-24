"""Tests for manage_publish_queue.py."""

from __future__ import annotations

import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from manage_publish_queue import format_queue_rows, main, parse_args


def _insert_content(db, content: str = "Queued post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, 'x_post', 7.0, 0)""",
        (content,),
    ).lastrowid


def _queue_item(
    db,
    *,
    scheduled_at: str = "2026-04-23T12:00:00+00:00",
    platform: str = "x",
    status: str = "queued",
    error: str | None = None,
    error_category: str | None = None,
) -> int:
    content_id = _insert_content(db, f"{status} {platform} post")
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, error, error_category)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (content_id, scheduled_at, platform, status, error, error_category),
    ).lastrowid
    db.conn.commit()
    return queue_id


@contextmanager
def _script_context(db):
    yield None, db


def test_get_publish_queue_items_filters_and_limits(db):
    first_id = _queue_item(db, platform="x", status="queued")
    _queue_item(db, platform="bluesky", status="queued")
    _queue_item(db, platform="x", status="failed")

    rows = db.get_publish_queue_items(status="queued", platform="x", limit=1)

    assert len(rows) == 1
    assert rows[0]["id"] == first_id
    assert rows[0]["status"] == "queued"
    assert rows[0]["platform"] == "x"


def test_get_publish_queue_items_includes_held_reason(db):
    queue_id = _queue_item(db, status="queued", error="old", error_category="unknown")

    held = db.hold_publish_queue_item(queue_id, reason="waiting for approval")
    rows = db.get_publish_queue_items(status="held")

    assert held["status"] == "held"
    assert held["hold_reason"] == "waiting for approval"
    assert held["error"] is None
    assert rows[0]["id"] == queue_id
    assert rows[0]["hold_reason"] == "waiting for approval"


def test_get_publish_queue_items_rejects_invalid_inputs(db):
    with pytest.raises(ValueError, match="invalid publish queue status"):
        db.get_publish_queue_items(status="paused")

    with pytest.raises(ValueError, match="invalid publish queue platform"):
        db.get_publish_queue_items(platform="mastodon")

    with pytest.raises(ValueError, match="limit must be positive"):
        db.get_publish_queue_items(limit=0)


def test_reschedule_publish_queue_item_updates_scheduled_at(db):
    queue_id = _queue_item(db, status="queued")

    row = db.reschedule_publish_queue_item(
        queue_id,
        "2026-04-24T09:30:00+00:00",
    )

    assert row["scheduled_at"] == "2026-04-24T09:30:00+00:00"
    assert row["status"] == "queued"


def test_reschedule_publish_queue_item_rejects_published(db):
    queue_id = _queue_item(db, status="published")

    with pytest.raises(ValueError, match="published queue items cannot be rescheduled"):
        db.reschedule_publish_queue_item(queue_id, "2026-04-24T09:30:00+00:00")


def test_cancel_publish_queue_item_clears_transient_errors(db):
    queue_id = _queue_item(
        db,
        status="failed",
        error="X: rate limit",
        error_category="rate_limit",
    )

    row = db.cancel_publish_queue_item(queue_id)

    assert row["status"] == "cancelled"
    assert row["error"] is None
    assert row["error_category"] is None


def test_restore_publish_queue_item_moves_failed_to_queued_with_new_time(db):
    queue_id = _queue_item(
        db,
        status="failed",
        error="Bluesky: network",
        error_category="network",
    )

    row = db.restore_publish_queue_item(
        queue_id,
        scheduled_at="2026-04-25T18:00:00+00:00",
    )

    assert row["status"] == "queued"
    assert row["scheduled_at"] == "2026-04-25T18:00:00+00:00"
    assert row["error"] is None
    assert row["error_category"] is None


def test_restore_publish_queue_item_requires_cancelled_or_failed(db):
    queue_id = _queue_item(db, status="queued")

    with pytest.raises(
        ValueError,
        match="only cancelled or failed queue items can be restored",
    ):
        db.restore_publish_queue_item(queue_id)


def test_release_publish_queue_item_preserves_scheduled_time(db):
    queue_id = _queue_item(db, status="queued")
    original = db.get_publish_queue_item(queue_id)["scheduled_at"]
    db.hold_publish_queue_item(queue_id, reason="manual review")

    row = db.release_publish_queue_item(queue_id)

    assert row["status"] == "queued"
    assert row["scheduled_at"] == original
    assert row["hold_reason"] is None


def test_hold_publish_queue_item_rejects_published(db):
    queue_id = _queue_item(db, status="published")

    with pytest.raises(ValueError, match="published queue items cannot be held"):
        db.hold_publish_queue_item(queue_id, reason="too late")


def test_release_publish_queue_item_requires_held(db):
    queue_id = _queue_item(db, status="queued")

    with pytest.raises(ValueError, match="only held queue items can be released"):
        db.release_publish_queue_item(queue_id)


def test_parse_args_validates_iso_timestamp():
    args = parse_args(["reschedule", "1", "2026-04-24T09:30:00+00:00"])
    assert args.scheduled_at == "2026-04-24T09:30:00+00:00"

    with pytest.raises(SystemExit):
        parse_args(["reschedule", "1", "not-a-date"])


def test_format_queue_rows_empty():
    assert format_queue_rows([]) == "No publish queue items found."


def test_main_lists_filtered_rows(db, capsys):
    _queue_item(db, platform="x", status="queued")
    _queue_item(db, platform="bluesky", status="failed", error="timeout")

    with patch("manage_publish_queue.script_context", return_value=_script_context(db)):
        result = main(["list", "--status", "failed", "--platform", "bluesky"])

    output = capsys.readouterr().out
    assert result == 0
    assert "failed" in output
    assert "bluesky" in output
    assert "timeout" in output


def test_main_cancel_reports_changed_row(db, capsys):
    queue_id = _queue_item(db, status="queued", error="stale", error_category="unknown")

    with patch("manage_publish_queue.script_context", return_value=_script_context(db)):
        result = main(["cancel", str(queue_id)])

    output = capsys.readouterr().out
    row = db.get_publish_queue_item(queue_id)
    assert result == 0
    assert "Cancelled publish queue item" in output
    assert row["status"] == "cancelled"
    assert row["error"] is None


def test_main_schedule_accepts_manual_timestamp(db, capsys):
    content_id = _insert_content(db, "Manual schedule")

    with patch("manage_publish_queue.script_context", return_value=_script_context(db)):
        result = main([
            "schedule",
            str(content_id),
            "--platform",
            "x",
            "--scheduled-at",
            "2026-04-24T09:30:00+00:00",
        ])

    output = capsys.readouterr().out
    rows = db.get_publish_queue_items(platform="x")
    assert result == 0
    assert "Scheduled publish queue item" in output
    assert rows[0]["content_id"] == content_id
    assert rows[0]["scheduled_at"] == "2026-04-24T09:30:00+00:00"


def test_main_schedule_uses_next_recommended_slot(db, capsys):
    content_id = _insert_content(db, "Recommended schedule")

    class FakeScheduler:
        def __init__(self, db, config, *, recommendation_days, recommendation_limit):
            self.db = db
            assert recommendation_days == 30
            assert recommendation_limit == 2

        def schedule_content(self, content_id, platform):
            queue_id = self.db.queue_for_publishing(
                content_id,
                "2026-04-27T10:00:00+00:00",
                platform=platform,
            )
            return SimpleNamespace(queue_id=queue_id)

    with patch("manage_publish_queue.script_context", return_value=_script_context(db)):
        with patch("manage_publish_queue.QueueScheduler", FakeScheduler):
            result = main([
                "schedule",
                str(content_id),
                "--platform",
                "bluesky",
                "--next-recommended",
                "--recommendation-days",
                "30",
                "--recommendation-limit",
                "2",
            ])

    output = capsys.readouterr().out
    rows = db.get_publish_queue_items(platform="bluesky")
    assert result == 0
    assert "Scheduled publish queue item" in output
    assert rows[0]["content_id"] == content_id
    assert rows[0]["scheduled_at"] == "2026-04-27T10:00:00+00:00"


def test_main_hold_accepts_multiple_ids_and_reason(db, capsys):
    first_id = _queue_item(db, status="queued")
    second_id = _queue_item(db, status="failed", error="timeout")

    with patch("manage_publish_queue.script_context", return_value=_script_context(db)):
        result = main([
            "hold",
            str(first_id),
            str(second_id),
            "--reason",
            "campaign paused",
        ])

    output = capsys.readouterr().out
    assert result == 0
    assert output.count("Held publish queue item") == 2
    for queue_id in (first_id, second_id):
        row = db.get_publish_queue_item(queue_id)
        assert row["status"] == "held"
        assert row["hold_reason"] == "campaign paused"
        assert row["error"] is None


def test_main_release_accepts_multiple_ids(db, capsys):
    first_id = _queue_item(db, status="queued")
    second_id = _queue_item(db, status="queued")
    db.hold_publish_queue_item(first_id, reason="pause")
    db.hold_publish_queue_item(second_id, reason="pause")

    with patch("manage_publish_queue.script_context", return_value=_script_context(db)):
        result = main(["release", str(first_id), str(second_id)])

    output = capsys.readouterr().out
    assert result == 0
    assert output.count("Released publish queue item") == 2
    assert db.get_publish_queue_item(first_id)["status"] == "queued"
    assert db.get_publish_queue_item(second_id)["status"] == "queued"


def test_main_returns_error_for_invalid_restore(db, capsys):
    queue_id = _queue_item(db, status="queued")

    with patch("manage_publish_queue.script_context", return_value=_script_context(db)):
        result = main(["restore", str(queue_id)])

    captured = capsys.readouterr()
    assert result == 1
    assert "only cancelled or failed queue items can be restored" in captured.err
