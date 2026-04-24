"""Tests for publication URL backfill utility."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from backfill_publication_urls import main
from output.publication_url_backfill import (
    PublicationAccountHandles,
    backfill_publication_urls,
    bluesky_publication_url,
)


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


def test_dry_run_reports_exact_updates_without_writing(db):
    content_id = _insert_content(db, "Missing X URL")
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id="12345",
        published_at=(BASE_TIME - timedelta(hours=1)).isoformat(),
    )

    report = backfill_publication_urls(
        db,
        PublicationAccountHandles(x_username="taka"),
        days=7,
        platform="x",
        dry_run=True,
    )

    assert report["dry_run"] is True
    assert report["updates"] == [
        {
            "publication_id": 1,
            "content_id": content_id,
            "platform": "x",
            "platform_post_id": "12345",
            "current_platform_url": None,
            "platform_url": "https://x.com/taka/status/12345",
            "applied": False,
        }
    ]
    assert db.get_publication_state(content_id, "x")["platform_url"] is None


def test_backfills_x_url_from_generated_tweet_id(db):
    content_id = _insert_content(db, "Generated content has tweet ID")
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id=None,
        published_at=BASE_TIME.isoformat(),
    )
    db.conn.execute(
        "UPDATE generated_content SET tweet_id = ? WHERE id = ?",
        ("tw-from-generated", content_id),
    )
    db.conn.commit()

    report = backfill_publication_urls(
        db,
        PublicationAccountHandles(x_username="@taka"),
        days=7,
        platform="x",
    )

    assert report["update_count"] == 1
    assert report["updates"][0]["platform_url"] == (
        "https://x.com/taka/status/tw-from-generated"
    )
    assert db.get_publication_state(content_id, "x")["platform_url"] == (
        "https://x.com/taka/status/tw-from-generated"
    )


def test_backfills_bluesky_url_from_at_uri_and_handle(db):
    content_id = _insert_content(db, "Missing Bluesky URL")
    uri = "at://did:plc:abc/app.bsky.feed.post/3ltest"
    db.upsert_publication_success(
        content_id,
        "bluesky",
        platform_post_id=uri,
        published_at=BASE_TIME.isoformat(),
    )

    report = backfill_publication_urls(
        db,
        PublicationAccountHandles(bluesky_handle="taka.bsky.social"),
        days=7,
        platform="bluesky",
    )

    assert report["unresolved"] == []
    assert report["updates"][0]["platform_url"] == (
        "https://bsky.app/profile/taka.bsky.social/post/3ltest"
    )


def test_backfills_bluesky_url_from_generated_bluesky_uri(db):
    content_id = _insert_content(db, "Generated content has Bluesky URI")
    uri = "at://did:plc:abc/app.bsky.feed.post/3lfromgenerated"
    db.upsert_publication_success(
        content_id,
        "bluesky",
        platform_post_id=None,
        published_at=BASE_TIME.isoformat(),
    )
    db.conn.execute(
        "UPDATE generated_content SET bluesky_uri = ? WHERE id = ?",
        (uri, content_id),
    )
    db.conn.commit()

    report = backfill_publication_urls(
        db,
        PublicationAccountHandles(bluesky_handle="taka.bsky.social"),
        days=7,
        platform="bluesky",
    )

    assert report["updates"][0]["platform_post_id"] == uri
    assert report["updates"][0]["platform_url"] == (
        "https://bsky.app/profile/taka.bsky.social/post/3lfromgenerated"
    )


def test_bluesky_uri_with_handle_repo_is_resolvable_without_configured_handle():
    assert bluesky_publication_url(
        "at://taka.bsky.social/app.bsky.feed.post/3ltest",
        configured_handle=None,
    ) == "https://bsky.app/profile/taka.bsky.social/post/3ltest"


def test_reports_unresolved_bluesky_uri_without_handle(db):
    content_id = _insert_content(db, "Unresolvable Bluesky URL")
    db.upsert_publication_success(
        content_id,
        "bluesky",
        platform_post_id="at://did:plc:abc/app.bsky.feed.post/3ltest",
        published_at=BASE_TIME.isoformat(),
    )

    report = backfill_publication_urls(
        db,
        PublicationAccountHandles(),
        days=7,
        platform="bluesky",
    )

    assert report["updates"] == []
    assert report["unresolved"] == [
        {
            "publication_id": 1,
            "content_id": content_id,
            "platform": "bluesky",
            "platform_post_id": "at://did:plc:abc/app.bsky.feed.post/3ltest",
            "current_platform_url": None,
            "reason": "unresolvable_bluesky_uri",
        }
    ]


def test_existing_platform_url_is_not_overwritten(db):
    content_id = _insert_content(db, "Existing URL")
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id="new-id",
        platform_url="https://x.com/taka/status/original",
        published_at=BASE_TIME.isoformat(),
    )

    report = backfill_publication_urls(
        db,
        PublicationAccountHandles(x_username="taka"),
        days=7,
        platform="x",
    )

    assert report["candidate_count"] == 0
    assert db.get_publication_state(content_id, "x")["platform_url"] == (
        "https://x.com/taka/status/original"
    )


def test_main_supports_flags(db, capsys):
    content_id = _insert_content(db, "Script backfill")
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id="script-id",
        published_at=BASE_TIME.isoformat(),
    )
    config = SimpleNamespace(
        x=SimpleNamespace(username="script_user"),
        github=SimpleNamespace(username="fallback_user"),
        bluesky=SimpleNamespace(handle="script.bsky.social"),
    )

    @contextmanager
    def fake_script_context():
        yield config, db

    with patch("backfill_publication_urls.script_context", fake_script_context):
        main(["--days", "7", "--platform", "x", "--dry-run", "--json"])

    output = json.loads(capsys.readouterr().out)
    assert output["dry_run"] is True
    assert output["updates"][0]["platform_url"] == (
        "https://x.com/script_user/status/script-id"
    )
