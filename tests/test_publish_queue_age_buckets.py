"""Tests for open publish queue age bucket monitoring."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from output.publish_queue_age_buckets import (
    age_bucket,
    build_publish_queue_age_bucket_report,
    format_publish_queue_age_bucket_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_queue_age_buckets.py"
spec = importlib.util.spec_from_file_location("publish_queue_age_buckets_script", SCRIPT_PATH)
publish_queue_age_buckets_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_queue_age_buckets_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str = "x_post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, created_at)
           VALUES (?, ?, 7.0, 0, ?)""",
        ("Queued copy", content_type, (NOW - timedelta(days=5)).isoformat()),
    ).lastrowid


def _queue(
    db,
    *,
    hours_old: float,
    platform: str = "x",
    status: str = "queued",
    content_type: str = "x_post",
    published_at: datetime | None = None,
) -> int:
    content_id = _content(db, content_type)
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, created_at, published_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            (NOW + timedelta(hours=1)).isoformat(),
            platform,
            status,
            (NOW - timedelta(hours=hours_old)).isoformat(),
            published_at.isoformat() if published_at else None,
        ),
    ).lastrowid
    db.conn.commit()
    return queue_id


def test_age_bucket_boundaries_are_deterministic():
    assert age_bucket(-1) == "future"
    assert age_bucket(0) == "0h-1h"
    assert age_bucket(1) == "1h-6h"
    assert age_bucket(24) == "1d-3d"
    assert age_bucket(72) == "3d+"


def test_report_groups_fresh_aging_and_stale_items_by_platform_and_status(db):
    fresh = _queue(db, hours_old=0.5, platform="x", status="queued")
    aging = _queue(db, hours_old=12, platform="bluesky", status="held", content_type="x_thread")
    stale = _queue(db, hours_old=80, platform="x", status="failed")
    _queue(db, hours_old=120, platform="x", status="published")
    _queue(db, hours_old=120, platform="bluesky", status="queued", published_at=NOW)

    report = build_publish_queue_age_bucket_report(
        db,
        stale_threshold_hours=72,
        now=NOW,
    )

    assert report["counts"]["rows_scanned"] == 5
    assert report["counts"]["items"] == 3
    assert report["counts"]["by_platform"] == {"bluesky": 1, "x": 2}
    assert report["counts"]["by_status"] == {"failed": 1, "held": 1, "queued": 1}
    assert report["counts"]["by_bucket"]["0h-1h"] == 1
    assert report["counts"]["by_bucket"]["6h-1d"] == 1
    assert report["counts"]["by_bucket"]["3d+"] == 1

    by_label = {bucket["label"]: bucket for bucket in report["buckets"]}
    assert by_label["0h-1h"]["by_platform"] == {"x": 1}
    assert by_label["6h-1d"]["by_status"] == {"held": 1}
    assert by_label["3d+"]["by_status"] == {"failed": 1}

    assert [item["queue_id"] for item in report["stale_items"]] == [stale]
    item_ids = {item["queue_id"] for item in report["stale_items"]}
    assert fresh not in item_ids
    assert aging not in item_ids


def test_json_formatter_and_cli_emit_monitoring_json(db, monkeypatch, capsys):
    queue_id = _queue(db, hours_old=30, platform="x", status="queued")
    monkeypatch.setattr(
        publish_queue_age_buckets_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publish_queue_age_buckets_script,
        "build_publish_queue_age_bucket_report",
        lambda db, **kwargs: build_publish_queue_age_bucket_report(db, now=NOW, **kwargs),
    )

    exit_code = publish_queue_age_buckets_script.main(["--stale-threshold-hours", "24"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["artifact_type"] == "publish_queue_age_buckets"
    assert payload["stale_items"][0]["queue_id"] == queue_id
    assert json.loads(format_publish_queue_age_bucket_json(payload))["counts"]["items"] == 1


def test_invalid_arguments_and_row_input():
    rows = [
        {
            "id": 1,
            "content_id": 10,
            "platform": "x",
            "status": "queued",
            "created_at": (NOW - timedelta(hours=2)).isoformat(),
        },
        {
            "id": 2,
            "content_id": 11,
            "platform": "x",
            "status": "cancelled",
            "created_at": (NOW - timedelta(hours=200)).isoformat(),
        },
    ]

    report = build_publish_queue_age_bucket_report(rows, now=NOW)

    assert report["counts"]["items"] == 1
    assert report["counts"]["by_bucket"]["1h-6h"] == 1
    with pytest.raises(ValueError, match="bucket_hours must not be empty"):
        build_publish_queue_age_bucket_report([], bucket_hours=(), now=NOW)
    with pytest.raises(ValueError, match="bucket_hours values must be positive"):
        build_publish_queue_age_bucket_report([], bucket_hours=(1, 0), now=NOW)
    with pytest.raises(ValueError, match="stale_threshold_hours must be non-negative"):
        build_publish_queue_age_bucket_report([], stale_threshold_hours=-1, now=NOW)
