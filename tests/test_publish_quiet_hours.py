"""Tests for publish quiet-hours adjustment planning."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sys
from pathlib import Path

import yaml

from output.publish_quiet_hours import (
    build_publish_quiet_hours_plan,
    format_publish_quiet_hours_json,
    format_publish_quiet_hours_text,
    parse_quiet_hour_windows,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "plan_publish_quiet_hours.py"
spec = importlib.util.spec_from_file_location("plan_publish_quiet_hours_script", SCRIPT_PATH)
plan_publish_quiet_hours_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules[spec.name] = plan_publish_quiet_hours_script
spec.loader.exec_module(plan_publish_quiet_hours_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _config(windows):
    return {"publishing": {"quiet_hours": windows}}


def _insert_content(db, content: str, *, content_type: str = "x_post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, 8.0, 0)""",
        (content, content_type),
    ).lastrowid


def _queue(
    db,
    content_id: int,
    scheduled_at: datetime,
    *,
    platform: str = "x",
    status: str = "queued",
) -> int:
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, scheduled_at.isoformat(), platform, status),
    ).lastrowid
    db.conn.commit()
    return queue_id


def _publication(
    db,
    content_id: int,
    next_retry_at: datetime,
    *,
    platform: str = "bluesky",
    status: str = "failed",
    attempt_count: int = 2,
) -> int:
    publication_id = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, attempt_count, next_retry_at)
           VALUES (?, ?, ?, ?, ?)""",
        (content_id, platform, status, attempt_count, next_retry_at.isoformat()),
    ).lastrowid
    db.conn.commit()
    return publication_id


def test_queued_items_inside_quiet_hours_receive_next_allowed_time(db):
    blocked_content = _insert_content(db, "Blocked queued X post")
    clear_content = _insert_content(db, "Clear queued X post")
    blocked_id = _queue(db, blocked_content, NOW + timedelta(hours=1), platform="x")
    clear_id = _queue(db, clear_content, NOW + timedelta(hours=3), platform="x")

    report = build_publish_quiet_hours_plan(
        db,
        _config([{"timezone": "UTC", "platform": "x", "start": "12:30", "end": "14:00"}]),
        now=NOW,
        days=1,
    )

    by_id = {item.queue_item_id: item for item in report.items}

    assert by_id[blocked_id].action == "reschedule"
    assert by_id[blocked_id].recommended_at.isoformat() == "2026-05-01T14:00:00+00:00"
    assert by_id[clear_id].action == "unchanged"
    assert by_id[clear_id].recommended_at == by_id[clear_id].scheduled_at
    assert report.adjustment_queue_item_ids == [blocked_id]


def test_platform_specific_windows_and_all_platform_queue_items(db):
    x_content = _insert_content(db, "X blocked")
    bluesky_content = _insert_content(db, "Bluesky clear")
    all_content = _insert_content(db, "Cross-post blocked")
    x_id = _queue(db, x_content, NOW + timedelta(hours=1), platform="x")
    bluesky_id = _queue(db, bluesky_content, NOW + timedelta(hours=1), platform="bluesky")
    all_id = _queue(db, all_content, NOW + timedelta(hours=1), platform="all")

    report = build_publish_quiet_hours_plan(
        db,
        _config([{"timezone": "UTC", "platform": "x", "start": "12:00", "end": "14:00"}]),
        now=NOW,
        days=1,
    )

    by_id = {item.queue_item_id: item for item in report.items}

    assert by_id[x_id].action == "reschedule"
    assert by_id[bluesky_id].action == "unchanged"
    assert by_id[all_id].action == "reschedule"
    assert report.totals_by_platform == {
        "x": {"reschedule": 2, "unchanged": 0},
        "bluesky": {"reschedule": 1, "unchanged": 1},
    }


def test_timezone_inputs_are_evaluated_in_local_time(db):
    content_id = _insert_content(db, "Tokyo quiet-hour post")
    queue_id = _queue(
        db,
        content_id,
        datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc),
        platform="x",
    )

    report = build_publish_quiet_hours_plan(
        db,
        _config([{"timezone": "Asia/Tokyo", "platform": "x", "start": "22:00", "end": "23:00"}]),
        now=NOW,
        days=1,
    )

    item = next(item for item in report.items if item.queue_item_id == queue_id)

    assert item.action == "reschedule"
    assert item.recommended_at.isoformat() == "2026-05-01T14:00:00+00:00"


def test_overnight_windows_reschedule_before_and_after_midnight(db):
    late_content = _insert_content(db, "Late blocked")
    early_content = _insert_content(db, "Early blocked")
    clear_content = _insert_content(db, "Daytime clear")
    late_id = _queue(
        db,
        late_content,
        datetime(2026, 5, 1, 23, 30, tzinfo=timezone.utc),
        platform="x",
    )
    early_id = _queue(
        db,
        early_content,
        datetime(2026, 5, 2, 5, 30, tzinfo=timezone.utc),
        platform="x",
    )
    clear_id = _queue(
        db,
        clear_content,
        datetime(2026, 5, 2, 8, 0, tzinfo=timezone.utc),
        platform="x",
    )

    report = build_publish_quiet_hours_plan(
        db,
        _config([{"timezone": "UTC", "platform": "x", "start": "22:00", "end": "07:00"}]),
        now=NOW,
        days=1,
    )

    by_id = {item.queue_item_id: item for item in report.items}

    assert by_id[late_id].recommended_at.isoformat() == "2026-05-02T07:00:00+00:00"
    assert by_id[early_id].recommended_at.isoformat() == "2026-05-02T07:00:00+00:00"
    assert by_id[clear_id].action == "unchanged"


def test_retry_publications_keep_publication_identity_and_status(db):
    retry_content = _insert_content(db, "Retry blocked")
    publication_id = _publication(
        db,
        retry_content,
        NOW + timedelta(hours=1),
        platform="bluesky",
        status="failed",
    )

    report = build_publish_quiet_hours_plan(
        db,
        _config([{"timezone": "UTC", "platform": "bluesky", "start": "12:30", "end": "14:00"}]),
        now=NOW,
        days=1,
    )

    item = next(item for item in report.items if item.publication_id == publication_id)

    assert item.source == "content_publications"
    assert item.status == "failed"
    assert item.action == "reschedule"
    assert item.recommended_at.isoformat() == "2026-05-01T14:00:00+00:00"
    assert report.adjustment_publication_ids == [publication_id]


def test_window_parsing_validates_required_times():
    assert parse_quiet_hour_windows([{"start": "22:00", "end": "07:00"}])[0].is_overnight

    try:
        parse_quiet_hour_windows([{"start": "22:00"}])
    except ValueError as exc:
        assert "start and end" in str(exc)
    else:
        raise AssertionError("expected invalid quiet-hour window")


def test_json_and_text_output_are_stable(db):
    content_id = _insert_content(db, "Output blocked")
    queue_id = _queue(db, content_id, NOW + timedelta(hours=1), platform="x")

    report = build_publish_quiet_hours_plan(
        db,
        _config([{"timezone": "UTC", "start": "12:30", "end": "13:30"}]),
        now=NOW,
        days=1,
    )
    payload = json.loads(format_publish_quiet_hours_json(report))
    text = format_publish_quiet_hours_text(report)

    assert list(payload) == sorted(payload)
    assert payload["adjustment_queue_item_ids"] == [queue_id]
    assert payload["items"][0]["queue_item_id"] == queue_id
    assert "Publish Quiet-Hours Plan" in text
    assert "reschedule queue" in text


def test_cli_reads_config_db_and_accepts_quiet_hour_override(file_db, tmp_path, capsys):
    content_id = _insert_content(file_db, "CLI blocked")
    queue_id = _queue(file_db, content_id, NOW + timedelta(hours=1), platform="x")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "paths": {"database": str(file_db.db_path)},
                "publishing": {
                    "quiet_hours": [
                        {"timezone": "UTC", "platform": "bluesky", "start": "12:30", "end": "13:30"}
                    ]
                },
            }
        )
    )

    exit_code = plan_publish_quiet_hours_script.main(
        [
            "--config",
            str(config_path),
            "--quiet-hours",
            "x=12:30-13:30@UTC",
            "--now",
            NOW.isoformat(),
            "--days",
            "1",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["adjustment_queue_item_ids"] == [queue_id]
