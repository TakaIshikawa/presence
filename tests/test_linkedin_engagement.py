"""Tests for LinkedIn engagement CSV imports."""

import pytest

from evaluation.linkedin_engagement import (
    compute_linkedin_engagement_score,
    import_linkedin_engagement_csv,
    normalize_url,
    parse_linkedin_engagement_csv,
)


def _content(db, url=None, publication_url=None, post_id=None):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Post for LinkedIn",
        eval_score=8.0,
        eval_feedback="ok",
    )
    if url:
        db.conn.execute(
            "UPDATE generated_content SET published_url = ? WHERE id = ?",
            (url, content_id),
        )
        db.conn.commit()
    if publication_url or post_id:
        db.upsert_publication_success(
            content_id,
            "linkedin",
            platform_post_id=post_id,
            platform_url=publication_url,
            published_at="2026-04-20T10:00:00+00:00",
        )
    return content_id


def test_compute_linkedin_engagement_score_weights_counts():
    assert compute_linkedin_engagement_score(
        like_count=10,
        comment_count=2,
        share_count=3,
        impression_count=100,
    ) == 28.0


def test_parse_csv_accepts_common_headers(tmp_path):
    path = tmp_path / "linkedin.csv"
    path.write_text(
        "URL,impressions,likes,comments,shares\n"
        'https://www.linkedin.com/feed/update/urn:li:activity:123,"1,234",10,2,3\n',
        encoding="utf-8",
    )

    rows = parse_linkedin_engagement_csv(path)

    assert len(rows) == 1
    assert rows[0].post_id == "123"
    assert rows[0].impression_count == 1234
    assert rows[0].like_count == 10
    assert rows[0].comment_count == 2
    assert rows[0].share_count == 3


def test_import_matches_generated_content_published_url(db, tmp_path):
    content_id = _content(
        db,
        url="https://www.linkedin.com/feed/update/urn:li:activity:123/",
    )
    path = tmp_path / "linkedin.csv"
    path.write_text(
        "Post URL,Impressions,Likes,Comments,Shares\n"
        "https://www.linkedin.com/feed/update/urn:li:activity:123/?utm_source=csv,100,5,1,2\n",
        encoding="utf-8",
    )

    result = import_linkedin_engagement_csv(
        db,
        path,
        fetched_at="2026-04-24T12:00:00+00:00",
    )

    assert result.insert_count == 1
    assert result.unmatched_count == 0
    snapshots = db.get_linkedin_engagement(content_id)
    assert len(snapshots) == 1
    assert snapshots[0]["linkedin_url"].startswith("https://www.linkedin.com/feed/")
    assert snapshots[0]["post_id"] == "123"
    assert snapshots[0]["impression_count"] == 100
    assert snapshots[0]["like_count"] == 5
    assert snapshots[0]["comment_count"] == 1
    assert snapshots[0]["share_count"] == 2
    assert snapshots[0]["engagement_score"] == pytest.approx(16.0)
    assert snapshots[0]["fetched_at"] == "2026-04-24T12:00:00+00:00"


def test_import_matches_content_publication_platform_url(db, tmp_path):
    content_id = _content(
        db,
        publication_url="https://www.linkedin.com/feed/update/urn:li:activity:999",
    )
    path = tmp_path / "linkedin.csv"
    path.write_text(
        "linkedin_url,view_count,reaction count,comment_count,share_count\n"
        "https://www.linkedin.com/feed/update/urn:li:activity:999,50,4,0,1\n",
        encoding="utf-8",
    )

    result = import_linkedin_engagement_csv(db, path)

    assert result.inserted[0].content_id == content_id
    assert len(db.get_linkedin_engagement(content_id)) == 1


def test_import_matches_content_publication_post_id(db, tmp_path):
    content_id = _content(db, post_id="urn:li:activity:777")
    path = tmp_path / "linkedin.csv"
    path.write_text(
        "Activity ID,Views,Reactions,Comments,Shares\n"
        "777,25,2,1,0\n",
        encoding="utf-8",
    )

    result = import_linkedin_engagement_csv(db, path)

    assert result.inserted[0].content_id == content_id
    assert len(db.get_linkedin_engagement(content_id)) == 1


def test_import_reports_unmatched_without_crashing(db, tmp_path):
    path = tmp_path / "linkedin.csv"
    path.write_text(
        "URL,impressions,likes,comments,shares\n"
        "https://www.linkedin.com/feed/update/urn:li:activity:404,10,1,0,0\n",
        encoding="utf-8",
    )

    result = import_linkedin_engagement_csv(db, path)

    assert result.insert_count == 0
    assert result.unmatched_count == 1


def test_dry_run_reports_inserts_without_writing_snapshots(db, tmp_path):
    content_id = _content(
        db,
        publication_url="https://www.linkedin.com/feed/update/urn:li:activity:555",
    )
    path = tmp_path / "linkedin.csv"
    path.write_text(
        "URL,impressions,likes,comments,shares\n"
        "https://www.linkedin.com/feed/update/urn:li:activity:555,10,1,0,0\n",
        encoding="utf-8",
    )

    result = import_linkedin_engagement_csv(db, path, dry_run=True)

    assert result.dry_run is True
    assert result.insert_count == 1
    assert db.get_linkedin_engagement(content_id) == []


def test_normalize_url_removes_tracking_and_trailing_slash():
    assert normalize_url("HTTPS://WWW.LINKEDIN.COM/feed/update/1/?utm_source=x&a=b") == (
        "https://www.linkedin.com/feed/update/1?a=b"
    )
