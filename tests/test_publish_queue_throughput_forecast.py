"""Tests for publish queue throughput forecasting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.publish_queue_throughput_forecast import (
    build_publish_queue_throughput_forecast,
    format_publish_queue_throughput_forecast_json,
    format_publish_queue_throughput_forecast_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "publish_queue_throughput_forecast.py"
)
spec = importlib.util.spec_from_file_location("publish_queue_throughput_forecast", SCRIPT_PATH)
publish_queue_throughput_forecast = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_queue_throughput_forecast)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, label: str = "Queued copy") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, 'x_post', 7.0, 0)""",
        (label,),
    ).lastrowid


def _queue(
    db,
    *,
    platform: str = "x",
    status: str = "queued",
    scheduled_at: datetime | None = None,
    published_at: datetime | None = None,
) -> tuple[int, int]:
    scheduled_at = scheduled_at or (NOW + timedelta(days=1))
    content_id = _content(db, f"{platform} {status}")
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, published_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            content_id,
            scheduled_at.isoformat(),
            platform,
            status,
            published_at.isoformat() if published_at else None,
        ),
    ).lastrowid
    db.conn.commit()
    return queue_id, content_id


def _publication(
    db,
    content_id: int,
    *,
    platform: str = "x",
    status: str = "published",
    published_at: datetime | None = None,
) -> None:
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at)
           VALUES (?, ?, ?, ?)""",
        (
            content_id,
            platform,
            status,
            (published_at or (NOW - timedelta(days=1))).isoformat(),
        ),
    )
    db.conn.commit()


def test_forecast_counts_backlog_throughput_clearance_and_at_risk(db):
    old_queue_id, old_content_id = _queue(
        db,
        platform="x",
        status="queued",
        scheduled_at=NOW - timedelta(days=2),
    )
    _queue(db, platform="x", status="held", scheduled_at=NOW + timedelta(days=1))
    _queue(db, platform="bluesky", status="queued", scheduled_at=NOW + timedelta(days=1))
    _publication(
        db,
        old_content_id,
        platform="x",
        published_at=NOW - timedelta(days=1),
    )
    other_content = _content(db, "recent x success")
    _publication(
        db,
        other_content,
        platform="x",
        published_at=NOW - timedelta(days=3),
    )

    forecast = build_publish_queue_throughput_forecast(
        db,
        lookback_days=4,
        horizon_days=7,
        now=NOW,
    )

    x_forecast = next(item for item in forecast.platforms if item.platform == "x")
    assert x_forecast.backlog_count == 2
    assert x_forecast.backlog_by_status == {"held": 1, "queued": 1}
    assert x_forecast.recent_success_count == 2
    assert x_forecast.recent_daily_throughput == 0.5
    assert x_forecast.estimated_clearance_days == 4.0
    assert x_forecast.at_risk_items[0].queue_id == old_queue_id
    assert forecast.totals["backlog_count"] == 3
    assert forecast.totals["at_risk_count"] == 1
    assert "Publish Queue Throughput Forecast" in format_publish_queue_throughput_forecast_text(
        forecast
    )


def test_platform_filter_limits_backlog_and_throughput_to_queue_value(db):
    _queue(db, platform="x", status="queued")
    bluesky_queue_id, bluesky_content_id = _queue(db, platform="bluesky", status="held")
    _publication(db, bluesky_content_id, platform="bluesky")
    x_content = _content(db, "x success")
    _publication(db, x_content, platform="x")

    forecast = build_publish_queue_throughput_forecast(
        db,
        platform="bluesky",
        lookback_days=7,
        horizon_days=3,
        now=NOW,
    )

    assert [item.platform for item in forecast.platforms] == ["bluesky"]
    assert forecast.platforms[0].backlog_count == 1
    assert forecast.platforms[0].recent_success_count == 1
    assert forecast.platforms[0].at_risk_items == ()
    assert forecast.platforms[0].backlog_by_status == {"held": 1}
    assert forecast.platforms[0].estimated_clearance_days == 7.0
    assert forecast.platforms[0].recommendation == "reduce_intake"
    assert forecast.totals["held_count"] == 1
    assert forecast.totals["queued_count"] == 0
    assert bluesky_queue_id


def test_zero_recent_throughput_has_unknown_clearance_without_division(db):
    _queue(
        db,
        platform="mastodon",
        status="queued",
        scheduled_at=NOW - timedelta(hours=2),
    )

    forecast = build_publish_queue_throughput_forecast(
        db,
        platform="mastodon",
        lookback_days=7,
        horizon_days=7,
        now=NOW,
    )

    platform = forecast.platforms[0]
    assert platform.recent_success_count == 0
    assert platform.recent_daily_throughput == 0.0
    assert platform.estimated_clearance_days is None
    assert platform.recommendation == "reduce_intake"
    assert platform.at_risk_count == 1


def test_rebalance_recommendation_when_other_platform_has_capacity(db):
    for index in range(5):
        _queue(
            db,
            platform="x",
            status="queued",
            scheduled_at=NOW + timedelta(hours=index + 1),
        )
    x_content = _content(db, "one recent x success")
    _publication(db, x_content, platform="x", published_at=NOW - timedelta(days=1))
    for index in range(4):
        content_id = _content(db, f"bluesky success {index}")
        _publication(
            db,
            content_id,
            platform="bluesky",
            published_at=NOW - timedelta(days=index + 1),
        )

    forecast = build_publish_queue_throughput_forecast(
        db,
        lookback_days=4,
        horizon_days=7,
        now=NOW,
    )

    recommendations = {item.platform: item.recommendation for item in forecast.platforms}
    assert recommendations["x"] == "rebalance_platform"
    assert recommendations["bluesky"] == "normal"


def test_absent_publication_table_uses_publish_queue_successes_and_reports_metadata():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE publish_queue (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            scheduled_at TEXT NOT NULL,
            platform TEXT,
            status TEXT,
            published_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, published_at)
           VALUES (1, 10, ?, 'x', 'queued', NULL)""",
        ((NOW + timedelta(days=1)).isoformat(),),
    )
    conn.execute(
        """INSERT INTO publish_queue
           (id, content_id, scheduled_at, platform, status, published_at)
           VALUES (2, 11, ?, 'x', 'published', ?)""",
        ((NOW - timedelta(days=1)).isoformat(), (NOW - timedelta(days=1)).isoformat()),
    )
    conn.commit()
    try:
        forecast = build_publish_queue_throughput_forecast(
            conn,
            lookback_days=7,
            horizon_days=7,
            now=NOW,
        )
    finally:
        conn.close()

    assert forecast.missing_tables == ("content_publications",)
    assert forecast.platforms[0].platform == "x"
    assert forecast.platforms[0].recent_success_count == 1


def test_json_text_and_cli_outputs_are_deterministic(db, capsys):
    _, content_id = _queue(db, platform="x", status="queued")
    _publication(db, content_id, platform="x", published_at=NOW - timedelta(days=1))

    forecast = build_publish_queue_throughput_forecast(
        db,
        lookback_days=7,
        horizon_days=7,
        now=NOW,
    )

    assert format_publish_queue_throughput_forecast_json(
        forecast
    ) == format_publish_queue_throughput_forecast_json(forecast)
    payload = json.loads(format_publish_queue_throughput_forecast_json(forecast))
    assert payload["filters"]["lookback_days"] == 7
    assert "recommendation=normal" in format_publish_queue_throughput_forecast_text(forecast)

    with patch.object(
        publish_queue_throughput_forecast,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        publish_queue_throughput_forecast,
        "build_publish_queue_throughput_forecast",
        wraps=lambda db, **kwargs: build_publish_queue_throughput_forecast(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert (
            publish_queue_throughput_forecast.main(
                [
                    "--lookback-days",
                    "7",
                    "--horizon-days",
                    "7",
                    "--platform",
                    "x",
                    "--format",
                    "json",
                ]
            )
            == 0
        )

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["platform"] == "x"
    assert cli_payload["platforms"][0]["recent_success_count"] == 1
