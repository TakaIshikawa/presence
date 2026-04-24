"""Tests for engagement decay reporting."""

from __future__ import annotations

import json
from datetime import datetime, timezone

import pytest

from evaluation.engagement_decay import (
    DECLINE_RECOMMENDATION,
    FOLLOW_UP_RECOMMENDATION,
    MONITOR_RECOMMENDATION,
    REPURPOSE_RECOMMENDATION,
    EngagementDecayAnalyzer,
    format_engagement_decay_json,
    format_engagement_decay_table,
    normalize_platform,
)


NOW = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _content(db, text: str, *, published: bool = True) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=text,
        eval_score=8.0,
        eval_feedback="good",
    )
    if published:
        db.mark_published(content_id, f"https://x.com/test/status/{content_id}", str(content_id))
    return content_id


def _x_snapshot(db, content_id: int, score: float, fetched_at: str) -> None:
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, ?, 0, 0, 0, 0, ?, ?)""",
        (content_id, str(content_id), score, fetched_at),
    )
    db.conn.commit()


def _bluesky_snapshot(db, content_id: int, score: float, fetched_at: str) -> None:
    uri = f"at://did:plc:test/app.bsky.feed.post/{content_id}"
    db.mark_published_bluesky(content_id, uri)
    db.conn.execute(
        """INSERT INTO bluesky_engagement
           (content_id, bluesky_uri, like_count, repost_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, ?, 0, 0, 0, 0, ?, ?)""",
        (content_id, uri, score, fetched_at),
    )
    db.conn.commit()


def test_decay_report_includes_sorted_positive_rows(db):
    declining = _content(db, "Early response faded after the first day.")
    flat = _content(db, "Strong opening response then the graph stopped moving.")
    single_snapshot = _content(db, "Only one snapshot should not appear.")
    unpublished = _content(db, "Unpublished rows should be ignored.", published=False)

    _x_snapshot(db, declining, 20, "2026-04-22T12:00:00+00:00")
    _x_snapshot(db, declining, 14, "2026-04-23T12:00:00+00:00")
    _x_snapshot(db, flat, 12, "2026-04-22T00:00:00+00:00")
    _x_snapshot(db, flat, 13, "2026-04-24T00:00:00+00:00")
    _x_snapshot(db, single_snapshot, 7, "2026-04-23T00:00:00+00:00")
    _x_snapshot(db, unpublished, 30, "2026-04-22T00:00:00+00:00")
    _x_snapshot(db, unpublished, 1, "2026-04-23T00:00:00+00:00")

    report = EngagementDecayAnalyzer(db).analyze(days=7, platform="x", now=NOW)

    assert [row.content_id for row in report.rows] == [declining, flat]
    assert report.rows[0].first_score == 20
    assert report.rows[0].latest_score == 14
    assert report.rows[0].score_delta == -6
    assert report.rows[0].hours_observed == 24
    assert report.rows[0].decay_rate_per_day == -6
    assert report.rows[0].recommendation == DECLINE_RECOMMENDATION
    assert report.rows[1].recommendation == REPURPOSE_RECOMMENDATION


def test_platform_filtering_and_aliases(db):
    content_id = _content(db, "Cross-posted row has separate X and Bluesky momentum.")
    _x_snapshot(db, content_id, 8, "2026-04-22T12:00:00+00:00")
    _x_snapshot(db, content_id, 16, "2026-04-23T12:00:00+00:00")
    _bluesky_snapshot(db, content_id, 18, "2026-04-22T12:00:00+00:00")
    _bluesky_snapshot(db, content_id, 10, "2026-04-23T12:00:00+00:00")

    analyzer = EngagementDecayAnalyzer(db)

    assert normalize_platform("twitter") == "x"
    assert normalize_platform("bsky") == "bluesky"
    assert [row.platform for row in analyzer.analyze(days=7, platform="x", now=NOW).rows] == ["x"]
    assert [
        row.platform
        for row in analyzer.analyze(days=7, platform="bsky", now=NOW).rows
    ] == ["bluesky"]
    assert [row.platform for row in analyzer.analyze(days=7, platform="all", now=NOW).rows] == [
        "bluesky",
        "x",
    ]


def test_no_matching_data_formats_empty_state(db):
    report = EngagementDecayAnalyzer(db).analyze(days=7, platform="all", now=NOW)

    assert report.rows == []
    table = format_engagement_decay_table(report)
    payload = json.loads(format_engagement_decay_json(report))

    assert "No published posts had at least two engagement snapshots" in table
    assert payload["status"] == "empty"
    assert payload["rows"] == []


def test_recommendation_thresholds_and_json_serialization(db):
    declining = _content(db, "Decline")
    repurpose = _content(db, "Flat with strong initial traction")
    follow_up = _content(db, "Flat with moderate initial traction")
    growing = _content(db, "Still growing")

    for content_id, first, latest in [
        (declining, 9, 8),
        (repurpose, 10, 11),
        (follow_up, 5, 5.4),
        (growing, 4, 20),
    ]:
        _x_snapshot(db, content_id, first, "2026-04-22T12:00:00+00:00")
        _x_snapshot(db, content_id, latest, "2026-04-23T12:00:00+00:00")

    report = EngagementDecayAnalyzer(db).analyze(days=7, platform="x", now=NOW)
    recommendations = {row.content_id: row.recommendation for row in report.rows}

    assert recommendations[declining] == DECLINE_RECOMMENDATION
    assert recommendations[repurpose] == REPURPOSE_RECOMMENDATION
    assert recommendations[follow_up] == FOLLOW_UP_RECOMMENDATION
    assert recommendations[growing] == MONITOR_RECOMMENDATION

    data = json.loads(format_engagement_decay_json(report))
    assert data["generated_at"] == "2026-04-24T12:00:00+00:00"
    assert data["row_count"] == 4
    assert data["rows"][0]["recommendation"] == DECLINE_RECOMMENDATION


def test_invalid_platform_raises(db):
    with pytest.raises(ValueError, match="platform must be one of"):
        EngagementDecayAnalyzer(db).analyze(platform="linkedin", now=NOW)
