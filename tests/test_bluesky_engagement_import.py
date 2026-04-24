"""Tests for Bluesky engagement CSV import logic."""

from src.evaluation.bluesky_engagement_import import (
    import_bluesky_engagement_rows,
    normalize_metrics,
)


def _content(db, sample_content, text="Bluesky post"):
    return db.insert_generated_content(
        **{**sample_content, "content": text, "content_type": "x_post"}
    )


def test_normalize_metrics_accepts_common_csv_headers():
    metrics = normalize_metrics(
        {
            "Likes": "1,200",
            "Reposts": "30",
            "Replies": "4",
            "Quotes": "2",
        }
    )

    assert metrics.like_count == 1200
    assert metrics.repost_count == 30
    assert metrics.reply_count == 4
    assert metrics.quote_count == 2
    assert metrics.engagement_score > 1200


def test_dry_run_reports_matched_skipped_and_invalid_without_writing(db, sample_content):
    content_id = _content(db, sample_content)
    db.mark_published_bluesky(content_id, "at://did:plc:test/app.bsky.feed.post/abc")

    result = import_bluesky_engagement_rows(
        db,
        [
            {"content_id": str(content_id), "likes": "10", "reposts": "2"},
            {"content_id": "9999", "likes": "1"},
            {"content_id": "bad", "likes": "1"},
        ],
        dry_run=True,
    )

    assert result["counts"]["matched"] == 1
    assert result["counts"]["skipped"] == 1
    assert result["counts"]["invalid"] == 1
    assert result["rows"][0]["status"] == "matched"
    assert db.get_bluesky_engagement(content_id) == []


def test_import_matches_by_bluesky_uri_and_is_idempotent(db, sample_content):
    content_id = _content(db, sample_content)
    uri = "at://did:plc:test/app.bsky.feed.post/abc"
    db.mark_published_bluesky(content_id, uri)

    row = {
        "bluesky_uri": uri,
        "like_count": "10",
        "repost_count": "2",
        "reply_count": "1",
        "quote_count": "0",
    }

    first = import_bluesky_engagement_rows(db, [row])
    second = import_bluesky_engagement_rows(
        db,
        [{**row, "like_count": "12"}],
    )

    snapshots = db.get_bluesky_engagement(content_id)
    assert first["counts"]["inserted"] == 1
    assert second["counts"]["updated"] == 1
    assert len(snapshots) == 1
    assert snapshots[0]["like_count"] == 12


def test_import_matches_by_published_url_from_publication_state(db, sample_content):
    content_id = _content(db, sample_content)
    uri = "at://did:plc:test/app.bsky.feed.post/urlmatch"
    url = "https://bsky.app/profile/test.bsky.social/post/urlmatch"
    db.mark_published_bluesky(content_id, uri, url=url)

    result = import_bluesky_engagement_rows(
        db,
        [{"published_url": url, "likes": "5", "reposts": "1"}],
    )

    snapshots = db.get_bluesky_engagement(content_id)
    assert result["counts"]["matched"] == 1
    assert snapshots[0]["bluesky_uri"] == uri
    assert snapshots[0]["like_count"] == 5


def test_import_rejects_negative_metrics(db, sample_content):
    content_id = _content(db, sample_content)
    db.mark_published_bluesky(content_id, "at://did:plc:test/app.bsky.feed.post/abc")

    result = import_bluesky_engagement_rows(
        db,
        [{"content_id": str(content_id), "likes": "-1"}],
    )

    assert result["counts"]["invalid"] == 1
    assert "like_count" in result["rows"][0]["error"]
