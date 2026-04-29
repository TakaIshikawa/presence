"""Tests for held publish queue review workflows."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.queue_holds import report_held_items
from review_queue_holds import main


def _insert_content(db, content: str) -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, 'x_post', 7.0, 0)""",
        (content,),
    ).lastrowid


def _queue_item(
    db,
    *,
    content: str = "Queued post content",
    scheduled_at: str = "2026-04-23T12:00:00+00:00",
    platform: str = "x",
    status: str = "held",
    hold_reason: str | None = "manual review",
    created_at: str = "2026-04-20 10:00:00",
) -> int:
    content_id = _insert_content(db, content)
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, hold_reason, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (content_id, scheduled_at, platform, status, hold_reason, created_at),
    ).lastrowid
    db.conn.commit()
    return queue_id


@contextmanager
def _script_context(db):
    yield None, db


def test_report_returns_held_items_with_stable_keys_and_preview(db):
    queue_id = _queue_item(
        db,
        content="First line\nsecond line with enough text for preview.",
        hold_reason="campaign pause",
        created_at="2026-04-20 10:00:00",
    )
    _queue_item(db, status="queued", hold_reason=None)

    rows = report_held_items(
        db,
        now=datetime(2026, 4, 21, 12, 30, tzinfo=timezone.utc),
    )

    assert len(rows) == 1
    assert list(rows[0].keys()) == [
        "queue_id",
        "platform",
        "scheduled_at",
        "hold_reason",
        "content_preview",
        "age",
    ]
    assert rows[0]["queue_id"] == queue_id
    assert rows[0]["content_preview"] == "First line second line with enough text for preview."
    assert rows[0]["age"] == "1d 2h"


def test_release_only_changes_matching_held_items_and_preserves_scheduled_at(db):
    matching = _queue_item(
        db,
        scheduled_at="2026-04-22T09:00:00+00:00",
        hold_reason="campaign pause",
    )
    too_late = _queue_item(
        db,
        scheduled_at="2026-04-25T09:00:00+00:00",
        hold_reason="campaign pause",
    )
    other_reason = _queue_item(
        db,
        scheduled_at="2026-04-21T09:00:00+00:00",
        hold_reason="legal review",
    )

    rows = db.release_held_publish_queue_items(
        before="2026-04-23T00:00:00+00:00",
        reason_match="campaign",
    )

    assert [row["id"] for row in rows] == [matching]
    assert db.get_publish_queue_item(matching)["status"] == "queued"
    assert db.get_publish_queue_item(matching)["scheduled_at"] == "2026-04-22T09:00:00+00:00"
    assert db.get_publish_queue_item(too_late)["status"] == "held"
    assert db.get_publish_queue_item(other_reason)["status"] == "held"


def test_cancel_marks_matching_held_items_cancelled_with_status_message(db):
    queue_id = _queue_item(db, hold_reason="stale campaign")

    rows = db.cancel_held_publish_queue_items(
        reason_match="stale",
        status_message="Cancelled from hold review: stale campaign",
    )

    row = db.get_publish_queue_item(queue_id)
    assert [changed["id"] for changed in rows] == [queue_id]
    assert row["status"] == "cancelled"
    assert row["error"] == "Cancelled from hold review: stale campaign"
    assert row["hold_reason"] is None


def test_release_dry_run_reports_matches_without_database_writes(db):
    queue_id = _queue_item(db, hold_reason="manual pause")

    rows = db.release_held_publish_queue_items(reason_match="manual", dry_run=True)

    assert [row["id"] for row in rows] == [queue_id]
    assert db.get_publish_queue_item(queue_id)["status"] == "held"
    assert db.get_publish_queue_item(queue_id)["hold_reason"] == "manual pause"


def test_cancel_dry_run_reports_matches_without_database_writes(db):
    queue_id = _queue_item(db, hold_reason="manual pause")

    rows = db.cancel_held_publish_queue_items(
        reason_match="manual",
        status_message="Cancelled from hold review",
        dry_run=True,
    )

    assert [row["id"] for row in rows] == [queue_id]
    row = db.get_publish_queue_item(queue_id)
    assert row["status"] == "held"
    assert row["error"] is None
    assert row["hold_reason"] == "manual pause"


def test_cli_report_json_uses_stable_payload(db, capsys):
    queue_id = _queue_item(db, hold_reason="campaign pause")

    with patch("review_queue_holds.script_context", return_value=_script_context(db)):
        result = main(["report", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["command"] == "report"
    assert payload["dry_run"] is False
    assert payload["count"] == 1
    assert payload["items"][0]["queue_id"] == queue_id


def test_cli_release_dry_run_does_not_write(db, capsys):
    queue_id = _queue_item(db, hold_reason="campaign pause")

    with patch("review_queue_holds.script_context", return_value=_script_context(db)):
        result = main(["release", "--reason-match", "campaign", "--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["command"] == "release"
    assert payload["dry_run"] is True
    assert payload["items"][0]["queue_id"] == queue_id
    assert db.get_publish_queue_item(queue_id)["status"] == "held"
