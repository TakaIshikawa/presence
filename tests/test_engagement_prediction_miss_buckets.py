from __future__ import annotations

from datetime import datetime, timezone
import json

import pytest

from evaluation.engagement_prediction_miss_buckets import (
    build_engagement_prediction_miss_buckets_report,
    format_engagement_prediction_miss_buckets_json,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)


def _content(db, content_type="x_post"):
    db.conn.execute(
        "INSERT INTO generated_content (content_type, content) VALUES (?, ?)",
        (content_type, "content"),
    )
    db.conn.commit()
    return db.conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _prediction(db, *, content_id=None, error=None, actual=5.0, prompt_type="score", prompt_version="v1", prompt_hash="abc"):
    db.conn.execute(
        """INSERT INTO engagement_predictions
           (content_id, predicted_score, actual_engagement_score, prediction_error,
            prompt_type, prompt_version, prompt_hash, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (content_id, 4.0, actual, error, prompt_type, prompt_version, prompt_hash, NOW.isoformat()),
    )
    db.conn.commit()


def test_buckets_prediction_errors_and_joins_content_type(db):
    content_id = _content(db, "blog_post")
    _prediction(db, content_id=content_id, error=-3.0)
    _prediction(db, content_id=content_id, error=3.0)
    _prediction(db, content_id=content_id, error=0.5)

    payload = json.loads(format_engagement_prediction_miss_buckets_json(
        build_engagement_prediction_miss_buckets_report(db, absolute_error_threshold=2.0, now=NOW)
    ))

    assert payload["artifact_type"] == "engagement_prediction_miss_buckets"
    assert payload["bucket_counts"] == {
        "high_overprediction": 1,
        "high_underprediction": 1,
        "well_calibrated": 1,
    }
    group = payload["prompt_groups"][0]
    assert group["content_types"] == {"blog_post": 3}
    assert group["representative_content_ids"] == [content_id, content_id, content_id]


def test_missing_actual_or_error_is_missing_actual(db):
    _prediction(db, actual=None, error=1.0, prompt_hash="missing-actual")
    _prediction(db, actual=5.0, error=None, prompt_hash="missing-error")

    report = build_engagement_prediction_miss_buckets_report(db, now=NOW)

    assert report["bucket_counts"] == {"missing_actual": 2}
    assert {row["bucket"] for row in report["rows"]} == {"missing_actual"}


def test_threshold_is_configurable_and_validated(db):
    _prediction(db, error=1.5)

    default = build_engagement_prediction_miss_buckets_report(db, absolute_error_threshold=2.0, now=NOW)
    lower = build_engagement_prediction_miss_buckets_report(db, absolute_error_threshold=1.0, now=NOW)

    assert default["bucket_counts"] == {"well_calibrated": 1}
    assert lower["bucket_counts"] == {"high_underprediction": 1}
    with pytest.raises(ValueError, match="absolute_error_threshold"):
        build_engagement_prediction_miss_buckets_report(db, absolute_error_threshold=-1, now=NOW)
