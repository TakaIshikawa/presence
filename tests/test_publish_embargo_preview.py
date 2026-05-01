"""Tests for publish embargo preview expansion and queue joins."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sys
from pathlib import Path

import yaml

from output.publish_embargo_preview import (
    build_publish_embargo_preview,
    expand_embargo_windows,
    format_publish_embargo_preview_json,
    format_publish_embargo_preview_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "preview_publish_embargoes.py"
spec = importlib.util.spec_from_file_location("preview_publish_embargoes_script", SCRIPT_PATH)
preview_publish_embargoes_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
sys.modules[spec.name] = preview_publish_embargoes_script
spec.loader.exec_module(preview_publish_embargoes_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _config(windows):
    return {"publishing": {"embargo_windows": windows}}


def _insert_content(db, content: str, *, content_type: str = "x_post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, ?, 8.0, 0)""",
        (content, content_type),
    ).lastrowid


def _queue(db, content_id: int, scheduled_at: datetime, *, platform: str = "x", status: str = "queued") -> int:
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
) -> int:
    publication_id = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, next_retry_at)
           VALUES (?, ?, ?, ?)""",
        (content_id, platform, status, next_retry_at.isoformat()),
    ).lastrowid
    db.conn.commit()
    return publication_id


def test_expands_one_time_and_recurring_embargo_windows_deterministically():
    windows = expand_embargo_windows(
        _config(
            [
                {"timezone": "UTC", "date": "2026-05-02"},
                {"timezone": "Asia/Tokyo", "start": "22:00", "end": "07:00"},
                {
                    "timezone": "UTC",
                    "days": ["sunday"],
                    "start": "10:00",
                    "end": "12:00",
                    "platform": "x",
                },
            ]
        ),
        now=NOW,
        days=3,
    )

    as_dicts = [window.to_dict() for window in windows]

    assert as_dicts[0]["start_at"] == "2026-05-01T13:00:00+00:00"
    assert as_dicts[0]["end_at"] == "2026-05-01T22:00:00+00:00"
    assert any(
        window["start_at"] == "2026-05-02T00:00:00+00:00"
        and window["end_at"] == "2026-05-03T00:00:00+00:00"
        for window in as_dicts
    )
    assert any(
        window["start_at"] == "2026-05-03T10:00:00+00:00"
        and window["platforms"] == ["x"]
        for window in as_dicts
    )


def test_preview_lists_publish_queue_and_retry_publications_inside_windows(db):
    blocked_queue_content = _insert_content(db, "Blocked queued X post")
    clear_queue_content = _insert_content(db, "Clear queued X post")
    retry_content = _insert_content(db, "Blocked retryable Bluesky post")
    blocked_queue_id = _queue(db, blocked_queue_content, NOW + timedelta(hours=2), platform="x")
    _queue(db, clear_queue_content, NOW + timedelta(days=2), platform="x")
    publication_id = _publication(db, retry_content, NOW + timedelta(hours=3), platform="bluesky")

    report = build_publish_embargo_preview(
        db,
        _config([{"timezone": "UTC", "start": "13:00", "end": "17:00"}]),
        now=NOW,
        days=1,
    )

    assert report.affected_queue_item_ids == [blocked_queue_id]
    assert report.affected_publication_ids == [publication_id]
    assert report.totals_by_platform == {"x": 1, "bluesky": 1}
    assert [item.source for item in report.affected_items] == [
        "publish_queue",
        "content_publications",
    ]


def test_all_platform_queue_item_counts_for_each_effective_platform(db):
    content_id = _insert_content(db, "Cross-post blocked by embargo")
    queue_id = _queue(db, content_id, NOW + timedelta(hours=1, minutes=30), platform="all")

    report = build_publish_embargo_preview(
        db,
        _config([{"timezone": "UTC", "start": "13:00", "end": "15:00"}]),
        now=NOW,
        days=1,
    )

    assert report.affected_queue_item_ids == [queue_id]
    assert report.totals_by_platform == {"x": 1, "bluesky": 1}


def test_json_output_includes_totals_and_affected_ids(db):
    content_id = _insert_content(db, "JSON blocked item")
    queue_id = _queue(db, content_id, NOW + timedelta(hours=1), platform="x")

    report = build_publish_embargo_preview(
        db,
        _config([{"timezone": "UTC", "start": "12:30", "end": "13:30"}]),
        now=NOW,
        days=1,
    )
    payload = json.loads(format_publish_embargo_preview_json(report))

    assert list(payload) == sorted(payload)
    assert payload["affected_queue_item_ids"] == [queue_id]
    assert payload["totals_by_platform"] == {"bluesky": 0, "x": 1}
    assert payload["affected_items"][0]["queue_item_id"] == queue_id


def test_text_output_is_readable_without_embargoes_or_without_affected_items(db):
    no_windows = build_publish_embargo_preview(db, _config([]), now=NOW, days=1)
    content_id = _insert_content(db, "Outside embargo")
    _queue(db, content_id, NOW + timedelta(hours=4), platform="x")
    no_affected = build_publish_embargo_preview(
        db,
        _config([{"timezone": "UTC", "start": "13:00", "end": "14:00"}]),
        now=NOW,
        days=1,
    )

    assert "No embargo windows are configured for this horizon." in format_publish_embargo_preview_text(no_windows)
    assert "No queued publications are scheduled inside embargo windows." in format_publish_embargo_preview_text(no_affected)


def test_cli_reads_config_and_db_outputs_json(file_db, tmp_path, capsys):
    content_id = _insert_content(file_db, "CLI blocked queue item")
    queue_id = _queue(file_db, content_id, NOW + timedelta(hours=1), platform="x")
    config_path = tmp_path / "config.yaml"
    config_path.write_text(
        yaml.safe_dump(
            {
                "paths": {"database": str(file_db.db_path)},
                "publishing": {
                    "embargo_windows": [
                        {"timezone": "UTC", "start": "12:30", "end": "13:30"}
                    ]
                },
            }
        )
    )

    exit_code = preview_publish_embargoes_script.main(
        [
            "--config",
            str(config_path),
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
    assert payload["affected_queue_item_ids"] == [queue_id]
