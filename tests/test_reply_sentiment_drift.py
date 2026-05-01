"""Tests for reply sentiment drift reporting."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from engagement.reply_sentiment_drift import (
    build_reply_sentiment_drift_report,
    format_reply_sentiment_drift_json,
    format_reply_sentiment_drift_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_sentiment_drift.py"
spec = importlib.util.spec_from_file_location("reply_sentiment_drift_script", SCRIPT_PATH)
reply_sentiment_drift_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_sentiment_drift_script)


def _add_reply(
    db,
    *,
    handle: str,
    detected_at: datetime,
    score: float | None,
    flags: list[str] | None = None,
    status: str = "pending",
    platform: str = "x",
) -> int:
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=f"{platform}-{handle}-{detected_at.isoformat()}-{score}",
        platform=platform,
        inbound_author_handle=handle,
        inbound_author_id=f"id-{handle}",
        inbound_text=f"inbound from {handle}",
        our_tweet_id=f"our-{handle}",
        our_content_id=None,
        our_post_text="our post",
        draft_text=f"draft to {handle}",
        quality_score=score,
        quality_flags=json.dumps(flags) if flags else None,
        status=status,
    )
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ?, reviewed_at = ? WHERE id = ?",
        (
            detected_at.isoformat(),
            detected_at.isoformat() if status in {"approved", "posted", "dismissed"} else None,
            reply_id,
        ),
    )
    db.conn.commit()
    return int(reply_id)


def test_report_buckets_quality_drift_flags_and_repeated_targets(db):
    _add_reply(db, handle="alice", detected_at=NOW - timedelta(days=5), score=8.5)
    _add_reply(db, handle="bob", detected_at=NOW - timedelta(days=5), score=8.0)
    _add_reply(db, handle="alice", detected_at=NOW - timedelta(days=4), score=7.5)
    _add_reply(db, handle="bob", detected_at=NOW - timedelta(days=4), score=8.0)

    low_one = _add_reply(
        db,
        handle="alice",
        detected_at=NOW - timedelta(days=1),
        score=4.0,
        flags=["generic"],
        status="dismissed",
    )
    low_two = _add_reply(
        db,
        handle="alice",
        detected_at=NOW - timedelta(days=1),
        score=5.0,
        flags=["sycophantic"],
    )
    _add_reply(
        db,
        handle="carol",
        detected_at=NOW - timedelta(days=1),
        score=7.0,
        flags=["unsafe"],
    )
    db.record_reply_review_event(
        low_one,
        event_type="rejected",
        actor="operator",
        old_status="pending",
        new_status="dismissed",
        created_at=(NOW - timedelta(hours=20)).isoformat(),
    )

    report = build_reply_sentiment_drift_report(
        db,
        days=7,
        bucket="day",
        min_bucket_sample=2,
        now=NOW,
    )
    text = format_reply_sentiment_drift_text(report)

    assert report.totals["draft_count"] == 7
    assert report.totals["reviewed_count"] == 1
    assert [bucket.draft_count for bucket in report.buckets] == [2, 2, 3]
    assert any("average quality worsened" in warning for warning in report.warnings)
    assert any("low-quality draft rate rose" in warning for warning in report.warnings)
    assert any("generic flag rate rose" in warning for warning in report.warnings)
    assert any("sycophantic flag rate rose" in warning for warning in report.warnings)
    assert any("unsafe flag rate rose" in warning for warning in report.warnings)
    assert report.repeated_low_quality_targets[0].target_handle == "alice"
    assert report.repeated_low_quality_targets[0].reply_ids == (low_one, low_two)
    assert "Repeated low-quality targets:" in text


def test_week_bucket_and_platform_filter(db):
    _add_reply(
        db,
        handle="xuser",
        detected_at=NOW - timedelta(days=2),
        score=4.0,
        flags=["generic"],
        platform="x",
    )
    _add_reply(
        db,
        handle="bsky",
        detected_at=NOW - timedelta(days=2),
        score=8.0,
        platform="bluesky",
    )

    report = build_reply_sentiment_drift_report(
        db,
        days=14,
        bucket="week",
        platform="bluesky",
        now=NOW,
    )

    assert report.platform == "bluesky"
    assert report.totals["draft_count"] == 1
    assert report.totals["platforms"] == ["bluesky"]
    assert len(report.buckets) == 1
    assert report.buckets[0].average_quality_score == 8.0


def test_recent_review_event_includes_old_draft(db):
    reply_id = _add_reply(
        db,
        handle="late",
        detected_at=NOW - timedelta(days=45),
        score=5.0,
        flags=["generic"],
    )
    db.record_reply_review_event(
        reply_id,
        event_type="edited",
        actor="operator",
        old_status="pending",
        new_status="pending",
        created_at=(NOW - timedelta(days=1)).isoformat(),
    )

    report = build_reply_sentiment_drift_report(db, days=7, bucket="day", now=NOW)

    assert report.totals["draft_count"] == 1
    assert report.totals["reviewed_count"] == 1
    assert report.buckets[0].reply_ids == (reply_id,)


def test_empty_and_partial_reply_tables_return_stable_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            quality_score REAL,
            detected_at TEXT
        );
        INSERT INTO reply_queue (id, quality_score, detected_at)
        VALUES (1, NULL, NULL);
        """
    )

    report = build_reply_sentiment_drift_report(conn, days=3, now=NOW)
    payload = json.loads(format_reply_sentiment_drift_json(report))

    assert report.totals["draft_count"] == 1
    assert report.buckets[0].average_quality_score is None
    assert payload["missing_columns"]["reply_queue"] == [
        "platform",
        "inbound_author_handle",
        "quality_flags",
        "status",
        "reviewed_at",
    ]

    empty = sqlite3.connect(":memory:")
    empty.row_factory = sqlite3.Row
    empty_report = build_reply_sentiment_drift_report(empty, days=3, now=NOW)
    assert empty_report.totals["draft_count"] == 0
    assert empty_report.missing_tables == ("reply_queue",)
    assert "No reply quality rows found" in format_reply_sentiment_drift_text(empty_report)


def test_cli_outputs_json_and_reports_validation_errors(db, capsys):
    _add_reply(db, handle="cli", detected_at=NOW - timedelta(days=1), score=7.0)

    @contextmanager
    def fake_context():
        yield None, db

    with patch.object(reply_sentiment_drift_script, "script_context", fake_context):
        result = reply_sentiment_drift_script.main(
            ["--days", "30", "--bucket", "day", "--platform", "x", "--json"]
        )

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["platform"] == "x"
    assert payload["totals"]["draft_count"] == 1

    result = reply_sentiment_drift_script.main(["--days", "0"])
    captured = capsys.readouterr()
    assert result == 1
    assert "days must be at least 1" in captured.err
