"""Tests for reply_sla.py."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from reply_sla import build_report, main


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_pending_reply_sla_sorts_by_priority_then_age(db):
    old_low = _insert_reply(db, "old-low", priority="low")
    fresh_high = _insert_reply(db, "fresh-high", priority="high")
    old_normal = _insert_reply(db, "old-normal", priority="normal")
    _set_detected_at(db, old_low, "2026-04-20 12:00:00")
    _set_detected_at(db, fresh_high, "2026-04-23 10:00:00")
    _set_detected_at(db, old_normal, "2026-04-21 12:00:00")

    rows = db.get_pending_reply_sla(now=NOW)

    assert [row["inbound_tweet_id"] for row in rows] == [
        "fresh-high",
        "old-normal",
        "old-low",
    ]
    assert rows[0]["age_hours"] == 2
    assert rows[1]["age_hours"] == 48
    assert rows[2]["age_hours"] == 72


def test_pending_reply_sla_filters_by_platform_and_max_age(db):
    old_x = _insert_reply(db, "old-x", platform="x")
    fresh_x = _insert_reply(db, "fresh-x", platform="x")
    fresh_bluesky = _insert_reply(db, "fresh-bsky", platform="bluesky")
    _set_detected_at(db, old_x, "2026-04-20 12:00:00")
    _set_detected_at(db, fresh_x, "2026-04-23 09:00:00")
    _set_detected_at(db, fresh_bluesky, "2026-04-23 09:00:00")

    rows = db.get_pending_reply_sla(max_age_hours=24, platform="x", now=NOW)

    assert [row["inbound_tweet_id"] for row in rows] == ["fresh-x"]


def test_build_report_json_shape_includes_breakdowns(db):
    ctx = json.dumps({"tier_name": "Key Network", "dunbar_tier": 2})
    reply_id = _insert_reply(
        db,
        "json-row",
        platform="bluesky",
        priority="high",
        relationship_context=ctx,
        quality_score=8.5,
    )
    _set_detected_at(db, reply_id, "2026-04-23 06:00:00")
    rows = db.get_pending_reply_sla(now=NOW)

    report = build_report(rows, max_age_hours=None, platform=None)
    encoded = json.dumps(report)
    decoded = json.loads(encoded)

    assert decoded["total_pending"] == 1
    assert decoded["by_priority"] == {"high": 1}
    assert decoded["by_platform"] == {"bluesky": 1}
    assert decoded["by_author"] == {"alice": 1}
    assert decoded["by_relationship_tier"] == {"Key Network (tier 2)": 1}
    assert decoded["replies"][0]["quality_score"] == 8.5
    assert decoded["replies"][0]["age_hours"] == 6.0


def test_main_json_output(capsys):
    class FakeDb:
        def get_pending_reply_sla(self, max_age_hours=None, platform=None):
            assert max_age_hours == 24
            assert platform == "x"
            return [
                {
                    "id": 1,
                    "age_hours": 3,
                    "priority": "normal",
                    "platform": "x",
                    "inbound_author_handle": "alice",
                    "relationship_context": None,
                    "quality_score": 7.0,
                    "intent": "question",
                    "detected_at": "2026-04-23 09:00:00",
                    "inbound_tweet_id": "tw-1",
                }
            ]

    with patch("reply_sla.script_context", _mock_script_context(FakeDb())):
        assert main(["--json", "--platform", "x", "--max-age-hours", "24"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {"max_age_hours": 24, "platform": "x"}
    assert payload["replies"][0]["author"] == "alice"


def test_mark_stale_dismisses_only_old_low_priority_and_records_reason(db):
    old_low = _insert_reply(db, "old-low", priority="low", quality_flags='["generic"]')
    old_normal = _insert_reply(db, "old-normal", priority="normal")
    fresh_low = _insert_reply(db, "fresh-low", priority="low")
    posted_low = _insert_reply(db, "posted-low", priority="low", status="posted")
    _set_detected_at(db, old_low, "2026-04-20 12:00:00")
    _set_detected_at(db, old_normal, "2026-04-20 12:00:00")
    _set_detected_at(db, fresh_low, "2026-04-23 09:00:00")
    _set_detected_at(db, posted_low, "2026-04-20 12:00:00")

    dismissed = db.dismiss_stale_low_priority_replies(
        48,
        reason="test_sla",
        now=NOW,
    )

    assert dismissed == 1
    rows = {
        row["inbound_tweet_id"]: dict(row)
        for row in db.conn.execute(
            "SELECT inbound_tweet_id, status, reviewed_at, quality_flags FROM reply_queue"
        )
    }
    assert rows["old-low"]["status"] == "dismissed"
    assert rows["old-low"]["reviewed_at"] == "2026-04-23T12:00:00+00:00"
    assert json.loads(rows["old-low"]["quality_flags"]) == [
        "generic",
        "dismissed:test_sla",
    ]
    assert rows["old-normal"]["status"] == "pending"
    assert rows["fresh-low"]["status"] == "pending"
    assert rows["posted-low"]["status"] == "posted"
