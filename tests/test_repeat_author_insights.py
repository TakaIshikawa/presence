"""Tests for repeat author engagement insights."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.repeat_author_insights import (  # noqa: E402
    build_repeat_author_insights_report,
    format_repeat_author_insights_text,
    normalize_handle,
)
from repeat_reply_authors import main  # noqa: E402


NOW = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="author-a",
        inbound_text="Nice post",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text="Thanks for reading.",
        status="pending",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _insert_action(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        action_type="reply",
        target_tweet_id=tweet_id,
        target_tweet_text="Interesting thread",
        target_author_handle="alice",
        target_author_id="author-a",
        discovery_source="curated_timeline",
        relevance_score=0.8,
        draft_text="Useful point.",
    )
    defaults.update(kwargs)
    return db.insert_proactive_action(**defaults)


def _set_reply_times(
    db,
    reply_id: int,
    *,
    detected_at: str,
    reviewed_at: str | None = None,
    posted_at: str | None = None,
) -> None:
    db.conn.execute(
        """UPDATE reply_queue
           SET detected_at = ?, reviewed_at = ?, posted_at = ?
           WHERE id = ?""",
        (detected_at, reviewed_at, posted_at, reply_id),
    )
    db.conn.commit()


def _set_action_times(
    db,
    action_id: int,
    *,
    created_at: str,
    reviewed_at: str | None = None,
    posted_at: str | None = None,
    status: str | None = None,
) -> None:
    db.conn.execute(
        """UPDATE proactive_actions
           SET created_at = ?, reviewed_at = ?, posted_at = ?,
               status = COALESCE(?, status)
           WHERE id = ?""",
        (created_at, reviewed_at, posted_at, status, action_id),
    )
    db.conn.commit()


def test_handle_normalization_strips_at_and_casefolds():
    assert normalize_handle("  @Alice  ") == "alice"
    assert normalize_handle("@ALICE.BSKY.SOCIAL") == "alice.bsky.social"
    assert normalize_handle("   ") is None


def test_mixed_reply_and_proactive_rows_are_aggregated(db):
    reply_id = _insert_reply(db, "reply-1", inbound_author_handle="@Alice", status="posted")
    action_id = _insert_action(
        db,
        "action-1",
        target_author_handle="alice",
        platform_metadata=json.dumps({"platform": "bluesky"}),
    )
    _set_reply_times(db, reply_id, detected_at="2026-04-28 09:00:00")
    _set_action_times(
        db,
        action_id,
        created_at="2026-04-29 10:00:00",
        status="approved",
    )

    report = build_repeat_author_insights_report(db, days=7, min_count=2, now=NOW)

    assert report["totals"] == {
        "authors": 1,
        "interactions": 2,
        "reply_queue": 1,
        "proactive_actions": 1,
    }
    author = report["authors"][0]
    assert author["normalized_handle"] == "alice"
    assert author["raw_handles"] == ["@Alice", "alice"]
    assert author["source_counts"] == {"proactive_actions": 1, "reply_queue": 1}
    assert author["status_counts"] == {"approved": 1, "posted": 1}
    assert author["platform_counts"] == {"bluesky": 1, "x": 1}
    assert author["latest_seen_at"] == "2026-04-29T10:00:00+00:00"
    assert author["classification"] == "active"


def test_stale_classification_uses_latest_seen_threshold(db):
    first = _insert_reply(db, "old-reply", inbound_author_handle="bob")
    second = _insert_action(db, "old-action", target_author_handle="@BOB")
    _set_reply_times(db, first, detected_at="2026-04-01 09:00:00")
    _set_action_times(db, second, created_at="2026-04-03 10:00:00")

    report = build_repeat_author_insights_report(
        db,
        days=60,
        min_count=2,
        stale_days=14,
        now=NOW,
    )

    assert report["authors"][0]["normalized_handle"] == "bob"
    assert report["authors"][0]["classification"] == "stale"
    assert report["classification_counts"] == {"stale": 1}


def test_min_count_one_can_show_emerging_author(db):
    reply_id = _insert_reply(db, "solo", inbound_author_handle="carol")
    _set_reply_times(db, reply_id, detected_at="2026-04-29 09:00:00")

    report = build_repeat_author_insights_report(db, days=7, min_count=2, now=NOW)

    assert report["authors"][0]["normalized_handle"] == "carol"
    assert report["authors"][0]["classification"] == "emerging"


def test_empty_output_is_stable_and_text_mentions_empty_state(db):
    report = build_repeat_author_insights_report(db, days=7, min_count=2, now=NOW)
    text = format_repeat_author_insights_text(report)

    assert report["totals"] == {
        "authors": 0,
        "interactions": 0,
        "reply_queue": 0,
        "proactive_actions": 0,
    }
    assert report["authors"] == []
    assert report["classification_counts"] == {}
    assert "No repeat authors matched." in text


def test_cli_json_output_supports_threshold_flags(db, capsys):
    reply_id = _insert_reply(db, "cli-reply", inbound_author_handle="@Dana")
    action_id = _insert_action(db, "cli-action", target_author_handle="dana")
    _set_reply_times(db, reply_id, detected_at="2026-04-29 09:00:00")
    _set_action_times(db, action_id, created_at="2026-04-29 10:00:00")

    fixed_report = build_repeat_author_insights_report(
        db,
        days=14,
        min_count=2,
        stale_days=5,
        now=NOW,
    )

    with patch("repeat_reply_authors.script_context", _mock_script_context(db)), patch(
        "repeat_reply_authors.build_repeat_author_insights_report",
        return_value=fixed_report,
    ):
        result = main(
            [
                "--days",
                "14",
                "--min-count",
                "2",
                "--stale-days",
                "5",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["lookback_days"] == 14
    assert payload["thresholds"] == {"min_count": 2, "stale_days": 5}
    assert payload["authors"][0]["normalized_handle"] == "dana"
