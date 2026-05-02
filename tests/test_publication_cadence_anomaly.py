"""Tests for publication cadence anomaly reports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from output.publication_cadence_anomaly import (
    build_publication_cadence_anomaly_report,
    bucket_events_by_day,
    bucket_events_by_hour,
    format_publication_cadence_anomaly_json,
    format_publication_cadence_anomaly_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_cadence_anomaly.py"
spec = importlib.util.spec_from_file_location("publication_cadence_anomaly_script", SCRIPT_PATH)
publication_cadence_anomaly_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_cadence_anomaly_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db) -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published, created_at)
           VALUES ('Cadence copy', 'x_post', 7.0, 0, ?)""",
        ((NOW - timedelta(days=10)).isoformat(),),
    ).lastrowid


def _published(
    db,
    *,
    at: datetime,
    platform: str = "x",
    content_id: int | None = None,
) -> int:
    content_id = content_id or _content(db)
    row_id = db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at, updated_at)
           VALUES (?, ?, 'published', ?, ?)""",
        (content_id, platform, at.isoformat(), at.isoformat()),
    ).lastrowid
    db.conn.commit()
    return row_id


def _queue(
    db,
    *,
    at: datetime,
    platform: str = "x",
    status: str = "queued",
    content_id: int | None = None,
    published_at: datetime | None = None,
) -> int:
    content_id = content_id or _content(db)
    row_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, published_at, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            at.isoformat(),
            platform,
            status,
            published_at.isoformat() if published_at else None,
            (NOW - timedelta(days=1)).isoformat(),
        ),
    ).lastrowid
    db.conn.commit()
    return row_id


def test_burst_windows_are_flagged_over_configurable_threshold(db):
    for minute in (0, 20, 40, 70):
        _published(db, at=NOW - timedelta(hours=4) + timedelta(minutes=minute))
    _published(db, at=NOW - timedelta(days=2), platform="bluesky")

    report = build_publication_cadence_anomaly_report(
        db,
        window_hours=2,
        max_posts_per_window=3,
        max_gap_hours=72,
        repeated_hour_threshold=10,
        now=NOW,
    )

    assert len(report.bursts) == 1
    burst = report.bursts[0]
    assert burst.platform == "x"
    assert burst.post_count == 4
    assert [event.status for event in burst.events] == ["published"] * 4
    assert report.totals["burst_count"] == 1


def test_long_silence_windows_are_flagged_between_adjacent_events(db):
    _published(db, at=NOW - timedelta(days=6), platform="x")
    _queue(db, at=NOW + timedelta(hours=6), platform="x")
    _published(db, at=NOW - timedelta(days=1), platform="bluesky")

    report = build_publication_cadence_anomaly_report(
        db,
        max_gap_hours=48,
        max_posts_per_window=10,
        repeated_hour_threshold=10,
        now=NOW,
    )

    assert len(report.silences) == 1
    silence = report.silences[0]
    assert silence.platform == "x"
    assert silence.gap_hours == 150.0
    assert silence.previous_event.status == "published"
    assert silence.next_event.status == "queued"


def test_repeated_same_hour_scheduling_patterns_are_summarized(db):
    for days_ago in (6, 4, 2):
        _published(db, at=NOW - timedelta(days=days_ago, hours=3), platform="x")
    _queue(db, at=NOW + timedelta(days=1, hours=2), platform="x")

    report = build_publication_cadence_anomaly_report(
        db,
        repeated_hour_threshold=3,
        max_posts_per_window=10,
        max_gap_hours=240,
        now=NOW,
    )

    assert len(report.repeated_hours) == 1
    pattern = report.repeated_hours[0]
    assert pattern.platform == "x"
    assert pattern.hour == 9
    assert pattern.event_count == 3
    assert pattern.day_count == 3


def test_mixed_published_and_queued_rows_share_timeline_and_buckets(db):
    published_content = _content(db)
    _published(db, content_id=published_content, at=NOW - timedelta(hours=2), platform="x")
    _queue(
        db,
        content_id=published_content,
        at=NOW - timedelta(hours=3),
        platform="x",
        status="published",
        published_at=NOW - timedelta(hours=2),
    )
    _queue(db, at=NOW + timedelta(hours=1), platform="all", status="queued")
    _queue(db, at=NOW + timedelta(hours=2), platform="x", status="held")

    report = build_publication_cadence_anomaly_report(
        db,
        platform="all",
        max_posts_per_window=10,
        max_gap_hours=240,
        repeated_hour_threshold=10,
        now=NOW,
    )
    payload = json.loads(format_publication_cadence_anomaly_json(report))

    assert report.totals["event_count"] == 4
    assert report.totals["published_count"] == 1
    assert report.totals["queued_count"] == 3
    assert [(event.platform, event.status) for event in report.timeline] == [
        ("bluesky", "queued"),
        ("x", "published"),
        ("x", "queued"),
        ("x", "held"),
    ]
    assert bucket_events_by_hour(report.timeline)[(NOW + timedelta(hours=1)).strftime("%Y-%m-%dT%H:00:00+00:00")] == 2
    assert bucket_events_by_day(report.timeline)[NOW.date().isoformat()] == 4
    assert payload["artifact_type"] == "publication_cadence_anomaly"


def test_empty_timeline_formats_deterministically(db):
    report = build_publication_cadence_anomaly_report(db, now=NOW)
    text = format_publication_cadence_anomaly_text(report)
    payload = json.loads(format_publication_cadence_anomaly_json(report))

    assert report.timeline == ()
    assert report.bursts == ()
    assert report.silences == ()
    assert report.repeated_hours == ()
    assert "No published or queued scheduled events found." in text
    assert list(payload.keys()) == sorted(payload.keys())


def test_missing_schema_and_invalid_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_publication_cadence_anomaly_report(conn, now=NOW)

    assert report.missing_tables == ("content_publications", "publish_queue")
    assert report.timeline == ()

    with pytest.raises(ValueError, match="window_hours must be positive"):
        build_publication_cadence_anomaly_report(conn, window_hours=0, now=NOW)
    with pytest.raises(ValueError, match="unsupported platform"):
        build_publication_cadence_anomaly_report(conn, platform="mastodon", now=NOW)
    conn.close()


def test_cli_outputs_text_and_json_for_configured_and_file_db(db, file_db, monkeypatch, capsys):
    _queue(db, at=NOW + timedelta(hours=1), platform="x")
    _queue(file_db, at=NOW + timedelta(hours=1), platform="bluesky")

    monkeypatch.setattr(
        publication_cadence_anomaly_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        publication_cadence_anomaly_script,
        "build_publication_cadence_anomaly_report",
        lambda db, **kwargs: build_publication_cadence_anomaly_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert publication_cadence_anomaly_script.main(["--format", "text"]) == 0
    assert "Publication Cadence Anomaly Report" in capsys.readouterr().out

    assert publication_cadence_anomaly_script.main(["--json"]) == 0
    configured_payload = json.loads(capsys.readouterr().out)
    assert configured_payload["totals"]["event_count"] == 1

    assert (
        publication_cadence_anomaly_script.main(
            ["--db", str(file_db.db_path), "--platform", "bluesky", "--format", "json"]
        )
        == 0
    )
    file_payload = json.loads(capsys.readouterr().out)
    assert file_payload["timeline"][0]["platform"] == "bluesky"
