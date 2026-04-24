"""Tests for publication failure digest reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.publication_failures import (  # noqa: E402
    build_publication_failure_digest,
    format_publication_failure_digest,
)
from publication_failures import main  # noqa: E402


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _insert_content(db, text: str) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        ((BASE_TIME - timedelta(hours=2)).isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def _seed_failures(db) -> dict[str, int]:
    auth_content = _insert_content(db, "X auth failure")
    media_content = _insert_content(db, "Bluesky media failure")
    unknown_content = _insert_content(db, "Unknown queue category")
    queued_content = _insert_content(db, "Queued future item")

    auth_queue = db.queue_for_publishing(
        auth_content,
        (BASE_TIME - timedelta(hours=3)).isoformat(),
        platform="x",
    )
    db.mark_queue_failed(auth_queue, "401 expired token", error_category="auth")
    db.conn.execute(
        """UPDATE publish_queue
           SET created_at = ?, scheduled_at = ?
           WHERE id = ?""",
        (
            (BASE_TIME - timedelta(hours=3)).isoformat(),
            (BASE_TIME - timedelta(hours=3)).isoformat(),
            auth_queue,
        ),
    )

    db.upsert_publication_failure(
        media_content,
        "bluesky",
        "image upload failed",
        error_category="media",
    )
    db.conn.execute(
        """UPDATE content_publications
           SET attempt_count = 2,
               last_error_at = ?,
               updated_at = ?,
               next_retry_at = ?
           WHERE content_id = ? AND platform = 'bluesky'""",
        (
            (BASE_TIME - timedelta(minutes=20)).isoformat(),
            (BASE_TIME - timedelta(minutes=20)).isoformat(),
            (BASE_TIME + timedelta(minutes=10)).isoformat(),
            media_content,
        ),
    )

    unknown_queue = db.queue_for_publishing(
        unknown_content,
        (BASE_TIME - timedelta(hours=26)).isoformat(),
        platform="all",
    )
    db.conn.execute(
        """UPDATE publish_queue
           SET status = 'failed', error = ?, error_category = ?, created_at = ?
           WHERE id = ?""",
        (
            "strange platform response",
            "not-a-category",
            (BASE_TIME - timedelta(hours=26)).isoformat(),
            unknown_queue,
        ),
    )

    queued_queue = db.queue_for_publishing(
        queued_content,
        (BASE_TIME + timedelta(hours=1)).isoformat(),
        platform="x",
    )

    db.conn.commit()
    return {
        "auth_content": auth_content,
        "media_content": media_content,
        "unknown_content": unknown_content,
        "queued_content": queued_content,
        "auth_queue": auth_queue,
        "unknown_queue": unknown_queue,
        "queued_queue": queued_queue,
    }


def test_digest_groups_failures_with_recommendations(db):
    ids = _seed_failures(db)

    summary = build_publication_failure_digest(db, days=7, now=BASE_TIME)

    assert summary["generated_at"] == BASE_TIME.isoformat()
    assert summary["totals"]["failures"] == 4
    assert summary["totals"]["by_platform"] == {"bluesky": 2, "x": 2}
    assert summary["totals"]["by_error_category"] == {
        "auth": 1,
        "media": 1,
        "unknown": 2,
    }

    auth = next(
        bucket
        for bucket in summary["buckets"]
        if bucket["platform"] == "x" and bucket["error_category"] == "auth"
    )
    assert auth["count"] == 1
    assert auth["retry_age_bucket"] == "1h_to_6h"
    assert auth["recommendation"].startswith("Refresh platform credentials")
    assert auth["representative_failures"][0]["content_id"] == ids["auth_content"]
    assert auth["representative_failures"][0]["queue_id"] == ids["auth_queue"]

    media = next(
        bucket
        for bucket in summary["buckets"]
        if bucket["platform"] == "bluesky" and bucket["error_category"] == "media"
    )
    assert media["attempt_count"] == 2
    assert media["next_retry_at"] == (BASE_TIME + timedelta(minutes=10)).isoformat()
    assert media["representative_failures"][0]["publication_id"] is not None


def test_text_output_lists_buckets_and_actions(db):
    _seed_failures(db)

    output = format_publication_failure_digest(
        build_publication_failure_digest(db, days=7, now=BASE_TIME)
    )

    assert "Publication failure digest" in output
    assert "x / auth: 1" in output
    assert "bluesky / media: 1" in output
    assert "next_action: Refresh platform credentials" in output
    assert "next_action: Fix media attachment" in output
    assert "queue=" in output
    assert "publication=" in output


def test_platform_filter_applies_to_queue_and_publication_rows(db):
    _seed_failures(db)

    summary = build_publication_failure_digest(
        db,
        days=7,
        platform="bluesky",
        now=BASE_TIME,
    )

    assert summary["totals"]["failures"] == 2
    assert {bucket["platform"] for bucket in summary["buckets"]} == {"bluesky"}
    assert {bucket["error_category"] for bucket in summary["buckets"]} == {
        "media",
        "unknown",
    }


def test_include_queued_adds_queued_rows(db):
    ids = _seed_failures(db)

    without_queued = build_publication_failure_digest(db, days=7, now=BASE_TIME)
    with_queued = build_publication_failure_digest(
        db,
        days=7,
        now=BASE_TIME,
        include_queued=True,
    )

    assert without_queued["totals"]["failures"] == 4
    assert with_queued["totals"]["failures"] == 9
    queued_examples = [
        failure
        for bucket in with_queued["buckets"]
        for failure in bucket["representative_failures"]
        if failure["content_id"] == ids["queued_content"]
    ]
    assert queued_examples


def test_main_supports_json_flags(db, capsys):
    _seed_failures(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    fixed_summary = build_publication_failure_digest(
        db,
        days=7,
        platform="x",
        include_queued=True,
        now=BASE_TIME,
    )

    with patch("publication_failures.script_context", fake_script_context), patch(
        "publication_failures.build_publication_failure_digest",
        return_value=fixed_summary,
    ):
        main(["--days", "7", "--platform", "x", "--include-queued", "--format", "json"])

    output = json.loads(capsys.readouterr().out)
    assert output["platform"] == "x"
    assert output["include_queued"] is True
    assert output["totals"]["failures"] == 6
