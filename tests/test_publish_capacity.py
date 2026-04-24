"""Tests for publish queue capacity forecasting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from evaluation.publish_capacity import forecast_publish_capacity
from publish_capacity import format_json_report, format_text_report, main


def _config(*, limits=None, embargo_windows=None):
    return SimpleNamespace(
        publishing=SimpleNamespace(
            daily_platform_limits=limits if limits is not None else {"x": 3, "bluesky": 3},
            embargo_windows=embargo_windows or [],
        )
    )


@contextmanager
def _script_context(config, db):
    yield config, db


def _content(db, text="queued"):
    return db.insert_generated_content(
        "x_post",
        [],
        [],
        text,
        8.0,
        "ok",
    )


def _queue(db, platform, scheduled_at):
    content_id = _content(db, f"queued for {platform}")
    return db.queue_for_publishing(
        content_id,
        scheduled_at.isoformat(),
        platform=platform,
    )


def _historical_window(db, base, score):
    for weeks in range(1, 4):
        content_id = _content(db, f"history {base.isoformat()} {weeks}")
        db.mark_published(content_id, "https://x.example/post", tweet_id=str(content_id))
        published_at = base - timedelta(weeks=weeks)
        db.conn.execute(
            "UPDATE generated_content SET published_at = ? WHERE id = ?",
            (published_at.isoformat(), content_id),
        )
        db.upsert_publication_success(
            content_id,
            "x",
            platform_post_id=f"x-{content_id}",
            published_at=published_at.isoformat(),
        )
        db.insert_engagement(
            content_id=content_id,
            tweet_id=str(content_id),
            like_count=10,
            retweet_count=1,
            reply_count=1,
            quote_count=0,
            engagement_score=score,
        )


def test_forecast_counts_queue_slots_overflow_and_clearance(db):
    now = datetime(2026, 4, 20, 9, tzinfo=timezone.utc)  # Monday
    _historical_window(db, datetime(2026, 4, 20, 10, tzinfo=timezone.utc), 20.0)
    _historical_window(db, datetime(2026, 4, 21, 11, tzinfo=timezone.utc), 15.0)

    already_posted = _content(db, "already posted today")
    db.upsert_publication_success(
        already_posted,
        "x",
        platform_post_id="x-posted",
        published_at=now.replace(hour=1).isoformat(),
    )
    for _ in range(3):
        _queue(db, "x", now)

    forecast = forecast_publish_capacity(
        db,
        _config(limits={"x": 2, "bluesky": 2}),
        days=2,
        platform="x",
        now=now,
    )

    platform = forecast.platforms[0]
    assert platform.platform == "x"
    assert platform.queued_count == 3
    assert platform.projected_publish_slots == [
        "2026-04-20T10:00:00+00:00",
        "2026-04-21T11:00:00+00:00",
        "2026-04-21T11:00:00+00:00",
    ]
    assert platform.overflow_count == 0
    assert platform.estimated_clearance_time == "2026-04-21T11:00:00+00:00"


def test_forecast_expands_all_queue_items_and_filters_completed_platforms(db):
    now = datetime(2026, 4, 20, 9, tzinfo=timezone.utc)
    _historical_window(db, datetime(2026, 4, 20, 10, tzinfo=timezone.utc), 20.0)

    all_content_id = _content(db, "all platforms")
    db.queue_for_publishing(all_content_id, now.isoformat(), platform="all")
    completed_x_id = _content(db, "x already complete")
    db.conn.execute(
        "UPDATE generated_content SET published = 1 WHERE id = ?",
        (completed_x_id,),
    )
    db.queue_for_publishing(completed_x_id, now.isoformat(), platform="all")

    forecast = forecast_publish_capacity(
        db,
        _config(limits={"x": 3, "bluesky": 3}),
        days=1,
        platform="all",
        now=now,
    )

    by_platform = {item.platform: item for item in forecast.platforms}
    assert by_platform["x"].queued_count == 1
    assert by_platform["bluesky"].queued_count == 2


def test_forecast_respects_embargo_and_reports_overflow(db):
    now = datetime(2026, 4, 20, 9, tzinfo=timezone.utc)
    _historical_window(db, datetime(2026, 4, 20, 10, tzinfo=timezone.utc), 20.0)
    _queue(db, "x", now)

    forecast = forecast_publish_capacity(
        db,
        _config(
            limits={"x": 3},
            embargo_windows=[
                {
                    "timezone": "UTC",
                    "date": "2026-04-20",
                    "start": "09:00",
                    "end": "12:00",
                }
            ],
        ),
        days=1,
        platform="x",
        now=now,
    )

    platform = forecast.platforms[0]
    assert platform.projected_publish_slots == []
    assert platform.overflow_count == 1
    assert platform.estimated_clearance_time is None


def test_forecast_handles_empty_queue(db):
    now = datetime(2026, 4, 20, 9, tzinfo=timezone.utc)

    forecast = forecast_publish_capacity(
        db,
        _config(),
        days=1,
        platform="all",
        now=now,
    )

    assert [item.queued_count for item in forecast.platforms] == [0, 0]
    assert [item.projected_publish_slots for item in forecast.platforms] == [[], []]
    assert [item.overflow_count for item in forecast.platforms] == [0, 0]


def test_json_output_contains_same_platform_fields_as_human_report(db):
    now = datetime(2026, 4, 20, 9, tzinfo=timezone.utc)
    _historical_window(db, datetime(2026, 4, 20, 10, tzinfo=timezone.utc), 20.0)
    _queue(db, "x", now)
    forecast = forecast_publish_capacity(
        db,
        _config(limits={"x": 3}),
        days=1,
        platform="x",
        now=now,
    )

    human = format_text_report(forecast)
    parsed = json.loads(format_json_report(forecast))
    fields = parsed["platforms"][0].keys()

    for field in fields:
        assert field in human
    assert parsed["generated_at"] == "2026-04-20T09:00:00+00:00"


def test_cli_supports_deterministic_json_output(db, capsys):
    config = _config(limits={"x": 3})
    now = datetime(2026, 4, 20, 9, tzinfo=timezone.utc)
    _historical_window(db, datetime(2026, 4, 20, 10, tzinfo=timezone.utc), 20.0)
    _queue(db, "x", now)

    with patch("publish_capacity.script_context", return_value=_script_context(config, db)):
        result = main(
            [
                "--platform",
                "x",
                "--days",
                "1",
                "--now",
                "2026-04-20T09:00:00+00:00",
                "--json",
            ]
        )

    output = json.loads(capsys.readouterr().out)
    assert result == 0
    assert output["generated_at"] == "2026-04-20T09:00:00+00:00"
    assert output["platforms"][0]["queued_count"] == 1


def test_forecast_rejects_invalid_platform(db):
    with pytest.raises(ValueError, match="invalid platform"):
        forecast_publish_capacity(db, _config(), platform="mastodon")
