"""Tests for reply author reputation scoring."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_author_reputation import (  # noqa: E402
    build_reply_author_reputation_report,
    format_reply_author_reputation_json,
    format_reply_author_reputation_text,
    normalize_author_value,
)
from reply_author_reputation import main  # noqa: E402


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
        priority="normal",
        platform="x",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_times(
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


def _author(report: dict, normalized_author: str) -> dict:
    return next(
        author
        for author in report["authors"]
        if author["normalized_author"] == normalized_author
    )


def test_normalizes_author_values_consistently():
    assert normalize_author_value("  @Alice  ") == "alice"
    assert normalize_author_value("@ALICE.BSKY.SOCIAL") == "alice.bsky.social"
    assert normalize_author_value("   ") is None


def test_groups_handle_case_with_platform_and_counts_outcomes(db):
    first = _insert_reply(
        db,
        "alice-1",
        inbound_author_handle="@Alice",
        status="posted",
        priority="high",
    )
    second = _insert_reply(
        db,
        "alice-2",
        inbound_author_handle="alice",
        status="approved",
        priority="normal",
    )
    _insert_reply(
        db,
        "alice-bsky",
        inbound_author_handle="alice",
        platform="bluesky",
        status="posted",
    )
    _set_times(
        db,
        first,
        detected_at="2026-04-28 09:00:00",
        posted_at="2026-04-28T11:00:00+00:00",
    )
    _set_times(
        db,
        second,
        detected_at="2026-04-29 09:00:00",
        reviewed_at="2026-04-29T10:00:00+00:00",
    )
    db.record_reply_review_event(
        second,
        "approved",
        old_status="pending",
        new_status="approved",
        created_at="2026-04-29T10:00:00+00:00",
    )

    report = build_reply_author_reputation_report(
        db,
        days=7,
        min_interactions=2,
        limit=None,
        now=NOW,
    )

    assert report["totals"] == {"authors": 1, "interactions": 2}
    author = report["authors"][0]
    assert author["platform"] == "x"
    assert author["normalized_author"] == "alice"
    assert author["raw_handles"] == ["@Alice", "alice"]
    assert author["counts"] == {
        "total": 2,
        "accepted": 1,
        "posted": 1,
        "dismissed": 0,
        "expired": 0,
        "pending": 0,
    }
    assert author["rates"]["acceptance_rate"] == 1.0
    assert author["recent_priority_distribution"] == {"high": 1, "normal": 1}
    assert author["last_seen_at"] == "2026-04-29T10:00:00+00:00"
    assert author["tier"] == "trusted"


def test_tier_boundaries_include_noisy_and_blocked_candidate(db):
    for index in range(2):
        reply_id = _insert_reply(
            db,
            f"noisy-{index}",
            inbound_author_handle="noisy",
            status="dismissed",
        )
        _set_times(db, reply_id, detected_at=f"2026-04-2{index} 09:00:00")
        db.record_reply_review_event(
            reply_id,
            "rejected",
            old_status="pending",
            new_status="dismissed",
            created_at=f"2026-04-2{index}T10:00:00+00:00",
        )

    for index in range(4):
        reply_id = _insert_reply(
            db,
            f"blocked-{index}",
            inbound_author_handle="blocked",
            status="dismissed",
        )
        _set_times(db, reply_id, detected_at=f"2026-04-2{index} 09:00:00")
        db.record_reply_review_event(
            reply_id,
            "expired" if index < 3 else "rejected",
            old_status="pending",
            new_status="dismissed",
            created_at=f"2026-04-2{index}T10:00:00+00:00",
        )

    report = build_reply_author_reputation_report(
        db,
        days=14,
        min_interactions=2,
        limit=None,
        now=NOW,
    )

    assert _author(report, "noisy")["tier"] == "noisy"
    blocked = _author(report, "blocked")
    assert blocked["tier"] == "blocked_candidate"
    assert blocked["counts"]["expired"] == 3


def test_pending_and_expired_states_are_counted_from_status_and_events(db):
    pending = _insert_reply(db, "pending", inbound_author_handle="zoe", status="pending")
    expired = _insert_reply(db, "expired", inbound_author_handle="zoe", status="dismissed")
    _set_times(db, pending, detected_at="2026-04-29 09:00:00")
    _set_times(db, expired, detected_at="2026-04-28 09:00:00")
    db.record_reply_review_event(
        expired,
        "expired",
        old_status="pending",
        new_status="dismissed",
        created_at="2026-04-28T10:00:00+00:00",
    )

    report = build_reply_author_reputation_report(
        db,
        days=7,
        min_interactions=2,
        now=NOW,
    )

    author = report["authors"][0]
    assert author["normalized_author"] == "zoe"
    assert author["counts"]["pending"] == 1
    assert author["counts"]["expired"] == 1
    assert author["counts"]["dismissed"] == 1
    assert author["tier"] == "noisy"


def test_platform_filter_and_min_interactions_are_applied(db):
    for index in range(2):
        reply_id = _insert_reply(
            db,
            f"x-{index}",
            inbound_author_handle="sam",
            platform="x",
            status="posted",
        )
        _set_times(db, reply_id, detected_at=f"2026-04-2{index} 09:00:00")
    bluesky = _insert_reply(
        db,
        "bsky-1",
        inbound_author_handle="sam",
        platform="bluesky",
        status="posted",
    )
    _set_times(db, bluesky, detected_at="2026-04-29 09:00:00")

    report = build_reply_author_reputation_report(
        db,
        days=14,
        min_interactions=2,
        platform="bluesky",
        now=NOW,
    )

    assert report["authors"] == []
    assert report["totals"] == {"authors": 0, "interactions": 0}


def test_stable_ordering_prefers_tier_score_count_recent_then_author(db):
    for handle in ("ann", "bob"):
        for index in range(2):
            reply_id = _insert_reply(
                db,
                f"{handle}-{index}",
                inbound_author_handle=handle,
                status="posted",
            )
            _set_times(db, reply_id, detected_at=f"2026-04-2{index} 09:00:00")

    report = build_reply_author_reputation_report(
        db,
        days=14,
        min_interactions=2,
        limit=None,
        now=NOW,
    )

    assert [author["normalized_author"] for author in report["authors"]] == ["ann", "bob"]


def test_text_and_json_reports_include_reputation_fields(db):
    for index in range(2):
        reply_id = _insert_reply(
            db,
            f"report-{index}",
            inbound_author_handle="reporter",
            status="posted",
        )
        _set_times(db, reply_id, detected_at=f"2026-04-2{index} 09:00:00")

    report = build_reply_author_reputation_report(db, days=14, now=NOW)
    text = format_reply_author_reputation_text(report)
    decoded = json.loads(format_reply_author_reputation_json(report))

    assert "trusted score=" in text
    assert "last_seen_at=" in text
    assert decoded["authors"][0]["tier"] == "trusted"
    assert decoded["authors"][0]["counts"]["posted"] == 2
    assert "last_seen_at" in decoded["authors"][0]


def test_cli_json_output_supports_requested_flags(db, capsys):
    for index in range(2):
        reply_id = _insert_reply(
            db,
            f"cli-{index}",
            inbound_author_handle="cli",
            platform="x",
            status="posted",
        )
        _set_times(db, reply_id, detected_at=f"2026-04-2{index} 09:00:00")
    fixed_report = build_reply_author_reputation_report(
        db,
        days=14,
        min_interactions=2,
        platform="x",
        limit=5,
        now=NOW,
    )

    with patch("reply_author_reputation.script_context", _mock_script_context(db)), patch(
        "reply_author_reputation.build_reply_author_reputation_report",
        return_value=fixed_report,
    ):
        result = main(
            [
                "--days",
                "14",
                "--min-interactions",
                "2",
                "--platform",
                "x",
                "--limit",
                "5",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"] == {
        "days": 14,
        "min_interactions": 2,
        "platform": "x",
        "limit": 5,
    }
    assert payload["authors"][0]["normalized_author"] == "cli"
