"""Tests for Mastodon engagement CSV import logic."""

import json

from src.evaluation.mastodon_engagement_import import (
    import_mastodon_engagement_rows,
    normalize_mastodon_engagement_row,
)


FETCHED_AT = "2026-05-01T12:00:00+00:00"


def _content(db, sample_content, text="Mastodon post"):
    return db.insert_generated_content(
        **{**sample_content, "content": text, "content_type": "x_post"}
    )


def _mastodon_publication(db, content_id, *, url=None, post_id=None):
    url = url or f"https://mastodon.social/@taka/{post_id or '12345'}"
    post_id = post_id or "12345"
    db.upsert_publication_success(
        content_id,
        "mastodon",
        platform_post_id=post_id,
        platform_url=url,
        published_at="2026-04-30T10:00:00+00:00",
    )
    return url, post_id


def _snapshots(db):
    return [
        dict(row)
        for row in db.conn.execute(
            "SELECT * FROM mastodon_engagement ORDER BY fetched_at, id"
        ).fetchall()
    ]


def test_normalize_row_accepts_common_header_variants_and_scores():
    row = normalize_mastodon_engagement_row(
        {
            "Status URL": "https://mastodon.social/@taka/12345?utm_source=x",
            "Favourites": "1,200",
            "Boosts": "30",
            "Replies": "4",
        },
        source_row=2,
    )

    assert row.source_row == 2
    assert row.mastodon_url == "https://mastodon.social/@taka/12345"
    assert row.post_id == "12345"
    assert row.favourite_count == 1200
    assert row.boost_count == 30
    assert row.reply_count == 4
    assert row.engagement_score == 1200 + (30 * 3) + (4 * 4)


def test_dry_run_reports_matched_unmatched_inserted_and_duplicates_without_writing(
    db,
    sample_content,
):
    content_id = _content(db, sample_content)
    url, _post_id = _mastodon_publication(db, content_id, post_id="12345")

    result = import_mastodon_engagement_rows(
        db,
        [
            {"URL": url, "Favourites": "10", "Boosts": "2", "Replies": "1"},
            {"URL": url, "Favourites": "11", "Boosts": "2", "Replies": "1"},
            {"Post ID": "99999", "Favourites": "1"},
        ],
        fetched_at=FETCHED_AT,
        dry_run=True,
    )

    assert result["counts"]["matched"] == 2
    assert result["counts"]["unmatched"] == 1
    assert result["counts"]["inserted"] == 1
    assert result["counts"]["duplicates"] == 1
    assert result["rows"][0]["status"] == "matched"
    assert result["rows"][1]["status"] == "duplicate"
    assert _snapshots(db) == []


def test_import_matches_by_post_id_and_persists_raw_metrics(db, sample_content):
    content_id = _content(db, sample_content)
    url, post_id = _mastodon_publication(db, content_id, post_id="12345")

    result = import_mastodon_engagement_rows(
        db,
        [{"Status ID": post_id, "Favorites": "5", "Reblogs": "2", "Comments": "3"}],
        fetched_at=FETCHED_AT,
    )

    snapshots = _snapshots(db)
    assert result["counts"]["inserted"] == 1
    assert snapshots[0]["content_id"] == content_id
    assert snapshots[0]["mastodon_url"] == url
    assert snapshots[0]["post_id"] == post_id
    assert snapshots[0]["favourite_count"] == 5
    assert snapshots[0]["boost_count"] == 2
    assert snapshots[0]["reply_count"] == 3
    assert snapshots[0]["engagement_score"] == 5 + (2 * 3) + (3 * 4)
    assert json.loads(snapshots[0]["raw_metrics"])["Status ID"] == post_id


def test_import_matches_by_content_id_and_skips_existing_duplicate(db, sample_content):
    content_id = _content(db, sample_content)
    url, _post_id = _mastodon_publication(db, content_id, post_id="12345")

    first = import_mastodon_engagement_rows(
        db,
        [{"content_id": str(content_id), "URL": url, "Favourites": "1"}],
        fetched_at=FETCHED_AT,
    )
    second = import_mastodon_engagement_rows(
        db,
        [{"content_id": str(content_id), "URL": url, "Favourites": "9"}],
        fetched_at=FETCHED_AT,
    )

    snapshots = _snapshots(db)
    assert first["counts"]["inserted"] == 1
    assert second["counts"]["duplicates"] == 1
    assert len(snapshots) == 1
    assert snapshots[0]["favourite_count"] == 1


def test_import_rejects_negative_metrics(db, sample_content):
    content_id = _content(db, sample_content)
    url, _post_id = _mastodon_publication(db, content_id, post_id="12345")

    result = import_mastodon_engagement_rows(
        db,
        [{"URL": url, "Favourites": "-1"}],
        fetched_at=FETCHED_AT,
    )

    assert result["counts"]["invalid"] == 1
    assert "non-negative" in result["rows"][0]["error"]
