"""Tests for engagement anomaly detection."""

from datetime import datetime, timedelta, timezone

from evaluation.anomaly_detector import EngagementAnomalyDetector


def _add_x_post(db, score: float, content_format: str = "micro_story") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"X post with score {score}",
        eval_score=7.0,
        eval_feedback="Test",
        content_format=content_format,
    )
    db.mark_published(
        content_id,
        url=f"https://x.com/test/status/{content_id}",
        tweet_id=str(content_id),
    )
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        ((datetime.now(timezone.utc) - timedelta(days=1)).isoformat(), content_id),
    )
    db.insert_engagement(
        content_id=content_id,
        tweet_id=str(content_id),
        like_count=int(score),
        retweet_count=0,
        reply_count=0,
        quote_count=0,
        engagement_score=score,
    )
    return content_id


def _add_bluesky_post(db, score: float, content_format: str = "micro_story") -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"Bluesky post with score {score}",
        eval_score=7.0,
        eval_feedback="Test",
        content_format=content_format,
    )
    uri = f"at://did:plc:test/app.bsky.feed.post/{content_id}"
    db.mark_published_bluesky(content_id, uri=uri)
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        ((datetime.now(timezone.utc) - timedelta(days=1)).isoformat(), content_id),
    )
    db.insert_bluesky_engagement(
        content_id=content_id,
        bluesky_uri=uri,
        like_count=int(score),
        repost_count=0,
        reply_count=0,
        quote_count=0,
        engagement_score=score,
    )
    return content_id


def test_detects_high_anomaly(db):
    for score in [10.0, 10.0, 10.0, 10.0, 10.0]:
        _add_x_post(db, score)
    anomalous_id = _add_x_post(db, 40.0)

    anomalies = EngagementAnomalyDetector(db).detect_anomalies(days=30)

    assert len(anomalies) == 1
    assert anomalies[0].content_id == anomalous_id
    assert anomalies[0].direction == "high"
    assert anomalies[0].baseline_median == 10.0
    assert anomalies[0].score_delta == 30.0


def test_detects_low_anomaly(db):
    for score in [10.0, 10.0, 10.0, 10.0, 10.0]:
        _add_x_post(db, score)
    anomalous_id = _add_x_post(db, 0.0)

    anomalies = EngagementAnomalyDetector(db).detect_anomalies(days=30)

    assert len(anomalies) == 1
    assert anomalies[0].content_id == anomalous_id
    assert anomalies[0].direction == "low"
    assert anomalies[0].baseline_median == 10.0
    assert anomalies[0].score_delta == -10.0


def test_skips_when_baseline_samples_are_insufficient(db):
    for score in [10.0, 10.0, 10.0, 10.0]:
        _add_x_post(db, score)
    _add_x_post(db, 40.0)

    anomalies = EngagementAnomalyDetector(db).detect_anomalies(days=30)

    assert anomalies == []


def test_baselines_are_separated_per_platform(db):
    for score in [10.0, 10.0, 10.0, 10.0, 10.0]:
        _add_x_post(db, score)
    anomalous_x_id = _add_x_post(db, 40.0)

    for score in [100.0, 100.0, 100.0, 100.0, 100.0]:
        _add_bluesky_post(db, score)

    anomalies = EngagementAnomalyDetector(db).detect_anomalies(days=30)

    assert len(anomalies) == 1
    assert anomalies[0].content_id == anomalous_x_id
    assert anomalies[0].platform == "x"
    assert anomalies[0].baseline_median == 10.0


def test_platform_filter_limits_results(db):
    for score in [10.0, 10.0, 10.0, 10.0, 10.0]:
        _add_x_post(db, score)
    _add_x_post(db, 40.0)

    for score in [100.0, 100.0, 100.0, 100.0, 100.0]:
        _add_bluesky_post(db, score)
    anomalous_bluesky_id = _add_bluesky_post(db, 60.0)

    anomalies = EngagementAnomalyDetector(db).detect_anomalies(
        days=30,
        platform="bluesky",
    )

    assert len(anomalies) == 1
    assert anomalies[0].content_id == anomalous_bluesky_id
    assert anomalies[0].platform == "bluesky"
