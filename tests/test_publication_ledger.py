"""Tests for publication_ledger.py."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from publication_ledger import format_json_ledger, format_table_ledger, main


BASE_TIME = datetime(2026, 4, 22, 12, 0, tzinfo=timezone.utc)


def _insert_content(db, text: str, created_at: datetime | None = None) -> int:
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
        ((created_at or BASE_TIME).isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def seed_publication_ledger(db) -> dict[str, int]:
    """Seed mixed queued, successful, failed, and partial publications."""
    published_all = _insert_content(db, "Cross-post published successfully")
    queued_x = _insert_content(db, "Queued only for X")
    failed_bsky = _insert_content(db, "Bluesky publish failed")
    partial = _insert_content(db, "X published while Bluesky is still queued")

    db.queue_for_publishing(
        published_all,
        (BASE_TIME - timedelta(hours=3)).isoformat(),
        platform="all",
    )
    db.upsert_publication_success(
        published_all,
        "x",
        platform_post_id="tw-ok",
        platform_url="https://x.test/tw-ok",
        published_at=(BASE_TIME - timedelta(hours=2)).isoformat(),
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, tweet_id = ?, published_url = ?, published_at = ?
           WHERE id = ?""",
        (
            "tw-ok",
            "https://x.test/tw-ok",
            (BASE_TIME - timedelta(hours=2)).isoformat(),
            published_all,
        ),
    )
    db.upsert_publication_success(
        published_all,
        "bluesky",
        platform_post_id="at://did:plc:ok/app.bsky.feed.post/ok",
        platform_url="https://bsky.app/profile/test/post/ok",
        published_at=(BASE_TIME - timedelta(hours=2)).isoformat(),
    )
    db.conn.execute(
        "UPDATE generated_content SET bluesky_uri = ? WHERE id = ?",
        ("at://did:plc:ok/app.bsky.feed.post/ok", published_all),
    )

    db.queue_for_publishing(
        queued_x,
        (BASE_TIME + timedelta(hours=2)).isoformat(),
        platform="x",
    )

    db.queue_for_publishing(
        failed_bsky,
        (BASE_TIME - timedelta(hours=1)).isoformat(),
        platform="bluesky",
    )
    db.upsert_publication_failure(
        failed_bsky,
        "bluesky",
        "auth failed",
    )
    db.conn.execute(
        "UPDATE publish_queue SET status = 'failed', error = ? WHERE content_id = ?",
        ("Bluesky: auth failed", failed_bsky),
    )

    db.queue_for_publishing(
        partial,
        (BASE_TIME - timedelta(minutes=30)).isoformat(),
        platform="all",
    )
    db.upsert_publication_success(
        partial,
        "x",
        platform_post_id="tw-partial",
        platform_url="https://x.test/tw-partial",
        published_at=(BASE_TIME - timedelta(minutes=20)).isoformat(),
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1, tweet_id = ?, published_url = ?, published_at = ?
           WHERE id = ?""",
        (
            "tw-partial",
            "https://x.test/tw-partial",
            (BASE_TIME - timedelta(minutes=20)).isoformat(),
            partial,
        ),
    )
    db.conn.commit()

    return {
        "published_all": published_all,
        "queued_x": queued_x,
        "failed_bsky": failed_bsky,
        "partial": partial,
    }


def test_publication_ledger_table_output(db):
    seed_publication_ledger(db)

    rows = db.get_publication_ledger(days=7, now=BASE_TIME)
    output = format_table_ledger(rows)

    assert "CID" in output
    assert "PLATFORM" in output
    assert "TWEET_ID" in output
    assert "BLUESKY_URI" in output
    assert "tw-ok" in output
    assert "tw-partial" in output
    assert "auth failed" in output
    assert "Queued only for X" in output
    assert "bluesky" in output
    assert "queued" in output
    assert "failed" in output
    assert "published" in output


def test_publication_ledger_json_output(db):
    ids = seed_publication_ledger(db)

    rows = db.get_publication_ledger(days=7, platform="bluesky", now=BASE_TIME)
    data = json.loads(format_json_ledger(rows))

    by_content = {row["content_id"]: row for row in data}
    failed = by_content[ids["failed_bsky"]]
    assert failed["platform"] == "bluesky"
    assert failed["status"] == "failed"
    assert failed["error"] == "auth failed"
    assert failed["publish_queue"]["status"] == "failed"
    assert failed["content_publication"]["attempt_count"] == 1

    published = by_content[ids["published_all"]]
    assert published["bluesky_uri"] == "at://did:plc:ok/app.bsky.feed.post/ok"
    assert published["content_publication"]["status"] == "published"


def test_publication_ledger_filters_status_and_platform(db):
    ids = seed_publication_ledger(db)

    rows = db.get_publication_ledger(
        days=7,
        status="queued",
        platform="bluesky",
        now=BASE_TIME,
    )

    assert [row["content_id"] for row in rows] == [ids["partial"]]
    assert rows[0]["platform"] == "bluesky"
    assert rows[0]["status"] == "queued"


def test_main_supports_flags(db, capsys):
    seed_publication_ledger(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("publication_ledger.script_context", fake_script_context):
        main(["--days", "7", "--platform", "x", "--status", "published", "--json"])

    output = json.loads(capsys.readouterr().out)
    assert {row["tweet_id"] for row in output} == {"tw-ok", "tw-partial"}
    assert all(row["platform"] == "x" for row in output)
    assert all(row["status"] == "published" for row in output)
