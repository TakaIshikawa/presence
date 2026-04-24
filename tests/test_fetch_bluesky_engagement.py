"""Tests for scripts/fetch_bluesky_engagement.py."""

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetch_bluesky_engagement import fetch_bluesky_engagement, main


def _make_config(enabled=True):
    return SimpleNamespace(
        bluesky=SimpleNamespace(
            enabled=enabled,
            handle="test.bsky.social",
            app_password="app-password",
        )
    )


def _mock_script_context(config, db):
    @contextmanager
    def _ctx():
        yield config, db

    return _ctx()


def _seed_bluesky_publication(db, *, platform_post_id="at://did:plc:test/app.bsky.feed.post/abc123"):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Bluesky post",
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.upsert_publication_success(
        content_id,
        "bluesky",
        platform_post_id=platform_post_id,
        platform_url="https://bsky.app/profile/test.bsky.social/post/abc123",
    )
    return content_id


class _Client:
    def __init__(self, metrics_by_ref=None, errors_by_ref=None):
        self.metrics_by_ref = metrics_by_ref or {}
        self.errors_by_ref = errors_by_ref or {}
        self.calls = []

    def get_post_engagement(self, post_ref):
        self.calls.append(post_ref)
        if post_ref in self.errors_by_ref:
            raise self.errors_by_ref[post_ref]
        return self.metrics_by_ref.get(post_ref)


def test_dry_run_lists_payloads_without_insert(db):
    uri = "at://did:plc:test/app.bsky.feed.post/abc123"
    content_id = _seed_bluesky_publication(db, platform_post_id=uri)
    client = _Client(
        metrics_by_ref={
            uri: {
                "like_count": 4,
                "repost_count": 2,
                "reply_count": 1,
                "quote_count": 1,
            }
        }
    )

    results = fetch_bluesky_engagement(
        _make_config(),
        db,
        dry_run=True,
        client_factory=lambda **_kwargs: client,
    )

    assert results == [
        {
            "content_id": content_id,
            "platform_post_id": uri,
            "status": "dry_run",
            "like_count": 4,
            "repost_count": 2,
            "reply_count": 1,
            "quote_count": 1,
            "engagement_score": 19.0,
        }
    ]
    assert db.get_bluesky_engagement(content_id) == []


def test_non_dry_run_inserts_bluesky_engagement_snapshot(db):
    uri = "at://did:plc:test/app.bsky.feed.post/abc123"
    content_id = _seed_bluesky_publication(db, platform_post_id=uri)
    client = _Client(
        metrics_by_ref={
            uri: {
                "like_count": 3,
                "repost_count": 1,
                "reply_count": 2,
                "quote_count": 1,
            }
        }
    )

    results = fetch_bluesky_engagement(
        _make_config(),
        db,
        client_factory=lambda **_kwargs: client,
    )

    assert results[0]["status"] == "inserted"
    engagement = db.get_bluesky_engagement(content_id)
    assert len(engagement) == 1
    assert engagement[0]["bluesky_uri"] == uri
    assert engagement[0]["like_count"] == 3
    assert engagement[0]["repost_count"] == 1
    assert engagement[0]["reply_count"] == 2
    assert engagement[0]["quote_count"] == 1
    assert engagement[0]["engagement_score"] == 19.0
    assert engagement[0]["fetched_at"]


def test_client_errors_are_reported_per_content_item_and_batch_continues(db):
    first_uri = "at://did:plc:test/app.bsky.feed.post/one"
    second_uri = "at://did:plc:test/app.bsky.feed.post/two"
    first_id = _seed_bluesky_publication(db, platform_post_id=first_uri)
    second_id = _seed_bluesky_publication(db, platform_post_id=second_uri)
    client = _Client(
        metrics_by_ref={
            second_uri: {
                "like_count": 1,
                "repost_count": 0,
                "reply_count": 0,
                "quote_count": 0,
            }
        },
        errors_by_ref={first_uri: TimeoutError("network timeout")},
    )

    results = fetch_bluesky_engagement(
        _make_config(),
        db,
        client_factory=lambda **_kwargs: client,
    )

    by_id = {item["content_id"]: item for item in results}
    assert by_id[first_id]["status"] == "error"
    assert by_id[first_id]["platform_post_id"] == first_uri
    assert by_id[first_id]["error_category"] == "network"
    assert "network timeout" in by_id[first_id]["error"]
    assert by_id[second_id]["status"] == "inserted"
    assert len(db.get_bluesky_engagement(first_id)) == 0
    assert len(db.get_bluesky_engagement(second_id)) == 1


def test_limit_restricts_processed_rows(db):
    first_uri = "at://did:plc:test/app.bsky.feed.post/one"
    second_uri = "at://did:plc:test/app.bsky.feed.post/two"
    _seed_bluesky_publication(db, platform_post_id=first_uri)
    _seed_bluesky_publication(db, platform_post_id=second_uri)
    client = _Client(
        metrics_by_ref={
            first_uri: {"like_count": 1, "repost_count": 0, "reply_count": 0, "quote_count": 0},
            second_uri: {"like_count": 2, "repost_count": 0, "reply_count": 0, "quote_count": 0},
        }
    )

    results = fetch_bluesky_engagement(
        _make_config(),
        db,
        limit=1,
        dry_run=True,
        client_factory=lambda **_kwargs: client,
    )

    assert len(results) == 1
    assert len(client.calls) == 1


def test_json_main_emits_stable_result_objects(db, capsys):
    uri = "at://did:plc:test/app.bsky.feed.post/abc123"
    content_id = _seed_bluesky_publication(db, platform_post_id=uri)

    with patch("fetch_bluesky_engagement.script_context") as mock_context, patch(
        "fetch_bluesky_engagement.BlueskyClient"
    ) as mock_client_cls:
        mock_context.return_value = _mock_script_context(_make_config(), db)
        mock_client_cls.return_value.get_post_engagement.return_value = {
            "like_count": 2,
            "repost_count": 1,
            "reply_count": 0,
            "quote_count": 0,
        }

        main(["--dry-run", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert payload == [
        {
            "content_id": content_id,
            "platform_post_id": uri,
            "status": "dry_run",
            "like_count": 2,
            "repost_count": 1,
            "reply_count": 0,
            "quote_count": 0,
            "engagement_score": 5.0,
        }
    ]


def test_missing_credentials_reports_auth_errors_without_client(db):
    uri = "at://did:plc:test/app.bsky.feed.post/abc123"
    content_id = _seed_bluesky_publication(db, platform_post_id=uri)
    config = _make_config()
    config.bluesky.app_password = ""
    factory = MagicMock()

    results = fetch_bluesky_engagement(config, db, client_factory=factory)

    assert results[0]["content_id"] == content_id
    assert results[0]["platform_post_id"] == uri
    assert results[0]["status"] == "error"
    assert results[0]["error_category"] == "auth"
    factory.assert_not_called()
