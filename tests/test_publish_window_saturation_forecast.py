"""Tests for publish-window saturation forecasting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.publish_window_saturation_forecast import (
    build_publish_window_saturation_forecast_report,
    build_publish_window_saturation_forecast_report_from_db,
    format_publish_window_saturation_forecast_json,
    format_publish_window_saturation_forecast_text,
)


NOW = datetime(2026, 5, 19, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_window_saturation_forecast.py"
spec = importlib.util.spec_from_file_location("publish_window_saturation_forecast_script", SCRIPT_PATH)
publish_window_saturation_forecast_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_window_saturation_forecast_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _row(content_id: int, when: datetime, *, channel: str = "x", status: str = "queued"):
    return {
        "queue_id": f"q-{content_id}",
        "content_id": str(content_id),
        "scheduled_at": when.isoformat(),
        "channel": channel,
        "status": status,
    }


def _content(db) -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, created_at)
           VALUES ('Queued copy', 'x_post', 8.0, 0, ?)""",
        ((NOW - timedelta(days=1)).isoformat(),),
    ).lastrowid


def _queue(db, when: datetime, *, platform: str = "x", status: str = "queued") -> int:
    content_id = _content(db)
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, created_at)
           VALUES (?, ?, ?, ?, ?)""",
        (content_id, when.isoformat(), platform, status, NOW.isoformat()),
    ).lastrowid
    db.conn.commit()
    return queue_id


def test_detects_overloaded_windows_from_rows():
    target = NOW + timedelta(days=1, hours=2)
    rows = [_row(1, target), _row(2, target.replace(minute=15)), _row(3, target.replace(minute=45))]

    report = build_publish_window_saturation_forecast_report(
        rows,
        days=3,
        capacity=2,
        now=NOW,
    )

    assert report["overloaded_windows"] == [
        {
            "channel": "x",
            "weekday": target.weekday(),
            "day_name": "Wednesday",
            "hour": 14,
            "scheduled_count": 3,
            "capacity": 2,
            "excess_count": 1,
            "scheduled_content_ids": ["1", "2", "3"],
            "scheduled_queue_ids": ["q-1", "q-2", "q-3"],
            "scheduled_at": [row["scheduled_at"] for row in rows],
        }
    ]
    assert report["channel_summary"]["x"]["overloaded_window_count"] == 1


def test_flags_empty_preferred_windows():
    scheduled = NOW + timedelta(days=1)
    report = build_publish_window_saturation_forecast_report(
        [_row(1, scheduled, channel="x")],
        days=3,
        capacity=2,
        preferred_windows=[
            {"channel": "x", "weekday": scheduled.weekday(), "hour": scheduled.hour},
            {"channel": "bluesky", "day_name": "Thursday", "hour_utc": 9},
        ],
        now=NOW,
    )

    assert report["empty_preferred_windows"] == [
        {
            "channel": "bluesky",
            "weekday": 3,
            "day_name": "Thursday",
            "hour": 9,
            "scheduled_count": 0,
            "capacity": 2,
        }
    ]
    assert report["channel_summary"]["bluesky"]["empty_preferred_window_count"] == 1


def test_healthy_schedule_has_no_issues():
    report = build_publish_window_saturation_forecast_report(
        [
            _row(1, NOW + timedelta(days=1), channel="x"),
            _row(2, NOW + timedelta(days=2), channel="bluesky"),
            _row(3, NOW + timedelta(days=1), channel="x", status="published"),
        ],
        days=3,
        capacity=1,
        preferred_windows=[{"channel": "x", "weekday": 2, "hour": 12}],
        now=NOW,
    )
    text = format_publish_window_saturation_forecast_text(report)

    assert report["overloaded_windows"] == []
    assert report["empty_preferred_windows"] == []
    assert "No publish window saturation issues found." in text


def test_db_loader_reads_publish_queue_with_tolerant_names(db):
    target = NOW + timedelta(days=1, hours=3)
    ids = [_queue(db, target), _queue(db, target.replace(minute=30)), _queue(db, target.replace(minute=45))]
    _queue(db, NOW + timedelta(days=8))
    _queue(db, target, status="cancelled")

    report = build_publish_window_saturation_forecast_report_from_db(
        db,
        days=2,
        capacity=2,
        now=NOW,
    )

    assert report["overloaded_windows"][0]["scheduled_queue_ids"] == [str(item) for item in ids]
    assert report["totals"]["scheduled_count"] == 3


def test_cli_json_and_text_output_are_deterministic(db, file_db, capsys):
    target = NOW + timedelta(days=1, hours=1)
    configured_queue = _queue(db, target)
    file_queue = _queue(file_db, target, platform="bluesky")
    _queue(file_db, target.replace(minute=20), platform="bluesky")

    with patch.object(
        publish_window_saturation_forecast_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        publish_window_saturation_forecast_script,
        "build_publish_window_saturation_forecast_report_from_db",
        wraps=lambda db, **kwargs: build_publish_window_saturation_forecast_report_from_db(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert publish_window_saturation_forecast_script.main(["--days", "2", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert list(payload) == sorted(payload)
    assert payload["channel_summary"]["x"]["scheduled_count"] == 1
    assert payload["overloaded_windows"] == []
    assert str(configured_queue) not in json.dumps(payload["overloaded_windows"])

    assert (
        publish_window_saturation_forecast_script.main(
            ["--db", str(file_db.db_path), "--days", "2", "--capacity", "1", "--format", "text"]
        )
        == 0
    )
    text = capsys.readouterr().out
    assert "Publish Window Saturation Forecast" in text
    assert f"ids={file_queue}" in text
    assert publish_window_saturation_forecast_script.main(["--days", "0"]) == 2
