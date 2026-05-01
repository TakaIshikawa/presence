import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from ingestion.mastodon_mentions import (
    fetch_mastodon_mention_notifications,
    ingest_mastodon_mentions,
    normalize_author_handle,
    poll_mastodon_mentions,
)


class _Response:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class _Session:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return _Response(self.payload)


def _notification(
    *,
    notification_id="10",
    status_id="100",
    in_reply_to_id="900",
    acct="alice@example.social",
    content="<p>How does this work?</p>",
):
    return {
        "id": notification_id,
        "type": "mention",
        "created_at": "2026-05-01T10:00:00Z",
        "account": {"id": "acct-1", "acct": acct, "url": "https://example.social/@alice"},
        "status": {
            "id": status_id,
            "uri": f"https://example.social/users/alice/statuses/{status_id}",
            "url": f"https://example.social/@alice/{status_id}",
            "created_at": "2026-05-01T10:00:00Z",
            "content": content,
            "visibility": "public",
            "in_reply_to_id": in_reply_to_id,
            "in_reply_to_account_id": "me",
            "account": {"id": "acct-1", "acct": acct, "url": "https://example.social/@alice"},
        },
    }


def _mastodon_content(db, status_id="900"):
    content_id = db.insert_generated_content(
        "x_post",
        [],
        [],
        "Original Mastodon post",
        8.0,
        "good",
    )
    db.upsert_publication_success(
        content_id,
        "mastodon",
        platform_post_id=status_id,
        platform_url=f"https://mastodon.social/@me/{status_id}",
    )
    return content_id


def test_fetch_mastodon_notifications_uses_since_cursor_and_mentions_filter():
    session = _Session([_notification()])

    result = fetch_mastodon_mention_notifications(
        base_url="https://mastodon.social/",
        access_token="token",
        cursor="9",
        limit=5,
        session=session,
        timeout=7,
    )

    assert result[0]["id"] == "10"
    url, kwargs = session.calls[0]
    assert url == "https://mastodon.social/api/v1/notifications"
    assert kwargs["headers"]["Authorization"] == "Bearer token"
    assert kwargs["params"] == {"types[]": "mention", "limit": 5, "since_id": "9"}
    assert kwargs["timeout"] == 7


def test_normalizes_author_handle_for_local_and_remote_accounts():
    assert normalize_author_handle({"acct": "Alice"}, base_url="https://mastodon.social") == "@alice@mastodon.social"
    assert normalize_author_handle({"acct": "Bob@remote.example"}) == "@bob@remote.example"


def test_ingests_mastodon_mentions_into_reply_queue(db):
    content_id = _mastodon_content(db)

    report = ingest_mastodon_mentions(
        db=db,
        notifications=[_notification()],
        base_url="https://mastodon.social",
    )

    assert report["counts"] == {"fetched": 1, "inserted": 1, "skipped": 0}
    row = db.conn.execute("SELECT * FROM reply_queue").fetchone()
    assert row["platform"] == "mastodon"
    assert row["inbound_tweet_id"] == "100"
    assert row["inbound_author_handle"] == "@alice@example.social"
    assert row["inbound_text"] == "How does this work?"
    assert row["inbound_url"] == "https://example.social/@alice/100"
    assert row["our_tweet_id"] == "900"
    assert row["our_platform_id"] == "900"
    assert row["our_content_id"] == content_id
    assert row["our_post_text"] == "Original Mastodon post"
    assert row["draft_text"] == ""
    metadata = json.loads(row["platform_metadata"])
    assert metadata["notification_id"] == "10"
    assert metadata["status_id"] == "100"


def test_skips_already_processed_status_ids(db):
    _mastodon_content(db)
    ingest_mastodon_mentions(db=db, notifications=[_notification()])

    report = ingest_mastodon_mentions(
        db=db,
        notifications=[_notification(notification_id="11", status_id="100")],
    )

    assert report["counts"] == {"fetched": 1, "inserted": 0, "skipped": 1}
    assert report["skipped"][0]["reason"] == "already_processed"
    count = db.conn.execute("SELECT COUNT(*) FROM reply_queue").fetchone()[0]
    assert count == 1


def test_skips_already_processed_notification_ids(db):
    _mastodon_content(db)
    ingest_mastodon_mentions(db=db, notifications=[_notification()])

    report = ingest_mastodon_mentions(
        db=db,
        notifications=[_notification(notification_id="10", status_id="101")],
    )

    assert report["counts"]["inserted"] == 0
    assert report["skipped"][0]["reason"] == "already_processed"
    count = db.conn.execute("SELECT COUNT(*) FROM reply_queue").fetchone()[0]
    assert count == 1


def test_dry_run_returns_summary_without_writing(db):
    _mastodon_content(db)

    report = ingest_mastodon_mentions(
        db=db,
        notifications=[_notification()],
        dry_run=True,
    )

    assert report["dry_run"] is True
    assert report["counts"]["inserted"] == 1
    assert "reply_queue_id" not in report["inserted"][0]
    count = db.conn.execute("SELECT COUNT(*) FROM reply_queue").fetchone()[0]
    assert count == 0


def test_poller_advances_mastodon_cursor_after_successful_non_dry_run(db):
    _mastodon_content(db)
    session = _Session([_notification(notification_id="10"), _notification(notification_id="12", status_id="101")])

    report = poll_mastodon_mentions(
        db=db,
        base_url="https://mastodon.social",
        access_token="token",
        limit=10,
        session=session,
    )

    assert report["next_cursor"] == "12"
    assert db.get_platform_reply_cursor("mastodon") == "12"


def test_poller_does_not_advance_cursor_on_dry_run(db):
    _mastodon_content(db)
    db.set_platform_reply_cursor("mastodon", "8")
    session = _Session([_notification(notification_id="10")])

    report = poll_mastodon_mentions(
        db=db,
        base_url="https://mastodon.social",
        access_token="token",
        dry_run=True,
        session=session,
    )

    assert report["next_cursor"] == "10"
    assert db.get_platform_reply_cursor("mastodon") == "8"
    count = db.conn.execute("SELECT COUNT(*) FROM reply_queue").fetchone()[0]
    assert count == 0


def test_cli_prints_dry_run_summary_without_direct_db_writes(capsys):
    import poll_mastodon_mentions as cli

    config = SimpleNamespace(
        mastodon=SimpleNamespace(
            enabled=True,
            base_url="https://mastodon.social",
            access_token="token",
        ),
        timeouts=SimpleNamespace(http_seconds=3),
    )
    db = MagicMock()
    report = {
        "platform": "mastodon",
        "dry_run": True,
        "cursor": None,
        "next_cursor": "10",
        "counts": {"fetched": 1, "inserted": 1, "skipped": 0},
        "inserted": [],
        "skipped": [],
    }

    with patch.object(cli, "script_context") as script_context, patch.object(
        cli, "poll_mastodon_mentions", return_value=report
    ) as poller, patch.object(cli, "update_monitoring") as update_monitoring:
        script_context.return_value.__enter__.return_value = (config, db)
        script_context.return_value.__exit__.return_value = None

        assert cli.main(["--dry-run", "--limit", "5"]) == 0

    poller.assert_called_once_with(
        db=db,
        base_url="https://mastodon.social",
        access_token="token",
        limit=5,
        dry_run=True,
        timeout=3.0,
    )
    update_monitoring.assert_not_called()
    assert "Would insert 1 Mastodon mention." in capsys.readouterr().out
