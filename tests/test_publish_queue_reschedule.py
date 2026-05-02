"""Tests for dry-run publish queue reschedule planning."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.publish_queue_reschedule import (
    build_publish_queue_reschedule_report,
    format_publish_queue_reschedule_json,
    format_publish_queue_reschedule_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "plan_publish_reschedule.py"
spec = importlib.util.spec_from_file_location("plan_publish_reschedule", SCRIPT_PATH)
plan_publish_reschedule = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(plan_publish_reschedule)

NOW = datetime(2026, 5, 2, 12, 30, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "x_post",
    content_format: str | None = "tip",
) -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, content_format, eval_score, published)
           VALUES (?, ?, ?, 7.0, 0)""",
        ("Queued copy", content_type, content_format),
    ).lastrowid


def _queue(
    db,
    *,
    scheduled_at: datetime,
    platform: str = "x",
    status: str = "queued",
    hold_reason: str | None = None,
    content_type: str = "x_post",
    content_format: str | None = "tip",
) -> int:
    content_id = _content(db, content_type=content_type, content_format=content_format)
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status, hold_reason)
           VALUES (?, ?, ?, ?, ?)""",
        (content_id, scheduled_at.isoformat(), platform, status, hold_reason),
    ).lastrowid
    db.conn.commit()
    return queue_id


def _publication(
    db,
    *,
    published_at: datetime,
    platform: str = "x",
    content_type: str = "x_post",
) -> int:
    content_id = _content(db, content_type=content_type)
    db.conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, published_at)
           VALUES (?, ?, 'published', ?)""",
        (content_id, platform, published_at.isoformat()),
    )
    db.conn.commit()
    return content_id


def test_reschedule_suggests_future_slot_without_updating_queue(db):
    stale = _queue(db, scheduled_at=NOW - timedelta(days=2), platform="x")
    original = dict(db.get_publish_queue_item(stale))

    report = build_publish_queue_reschedule_report(
        db,
        days_ahead=3,
        platform="x",
        status="queued",
        now=NOW,
    )

    assert report.totals["stale_count"] == 1
    assert len(report.suggestions) == 1
    suggestion = report.suggestions[0]
    assert suggestion.queue_id == stale
    assert suggestion.suggested_at == "2026-05-02T15:00:00+00:00"
    assert suggestion.reason_codes == ("overdue", "fallback_window", "platform_slot_open")
    assert dict(db.get_publish_queue_item(stale)) == original


def test_suggestions_avoid_occupied_queue_and_publication_slots(db):
    stale = _queue(db, scheduled_at=NOW - timedelta(days=1), platform="x")
    _queue(db, scheduled_at=datetime(2026, 5, 2, 15, tzinfo=timezone.utc), platform="x")
    _queue(db, scheduled_at=datetime(2026, 5, 3, 9, tzinfo=timezone.utc), platform="x")
    _publication(db, published_at=datetime(2026, 5, 3, 15, 10, tzinfo=timezone.utc), platform="x")

    report = build_publish_queue_reschedule_report(
        db,
        days_ahead=3,
        platform="x",
        now=NOW,
    )

    assert report.suggestions[0].queue_id == stale
    assert report.suggestions[0].suggested_at == "2026-05-04T09:00:00+00:00"


def test_held_items_preserve_hold_reason_and_status_filter(db):
    held = _queue(
        db,
        scheduled_at=NOW - timedelta(days=3),
        platform="bluesky",
        status="held",
        hold_reason="needs media refresh",
        content_format="observation",
    )
    _queue(db, scheduled_at=NOW - timedelta(days=2), platform="x", status="queued")

    report = build_publish_queue_reschedule_report(
        db,
        days_ahead=2,
        platform="bluesky",
        status="held",
        now=NOW,
    )

    assert [suggestion.queue_id for suggestion in report.suggestions] == [held]
    suggestion = report.suggestions[0]
    assert suggestion.status == "held"
    assert suggestion.hold_reason == "needs media refresh"
    assert suggestion.content_format == "observation"
    assert "held" in suggestion.reason_codes


def test_all_platform_item_reserves_both_targets(db):
    first = _queue(db, scheduled_at=NOW - timedelta(days=4), platform="all")
    second = _queue(db, scheduled_at=NOW - timedelta(days=3), platform="bluesky")

    report = build_publish_queue_reschedule_report(db, days_ahead=2, now=NOW)

    suggestions = {suggestion.queue_id: suggestion for suggestion in report.suggestions}
    assert suggestions[first].target_platforms == ("x", "bluesky")
    assert suggestions[first].suggested_at == "2026-05-02T15:00:00+00:00"
    assert suggestions[second].suggested_at == "2026-05-03T09:00:00+00:00"


def test_limit_and_formatters_are_deterministic(db):
    _queue(db, scheduled_at=NOW - timedelta(days=4), platform="x")
    _queue(db, scheduled_at=NOW - timedelta(days=3), platform="x")

    report = build_publish_queue_reschedule_report(db, days_ahead=2, limit=1, now=NOW)
    payload = json.loads(format_publish_queue_reschedule_json(report))
    text = format_publish_queue_reschedule_text(report)

    assert report.totals["stale_count"] == 2
    assert report.totals["suggestion_count"] == 1
    assert len(payload["suggestions"]) == 1
    assert list(payload.keys()) == sorted(payload.keys())
    assert "Dry run: no publish_queue rows were changed." in text


def test_cli_supports_filters_and_json_output(db, capsys):
    held = _queue(
        db,
        scheduled_at=NOW - timedelta(days=1),
        platform="x",
        status="held",
        hold_reason="operator review",
    )
    _queue(db, scheduled_at=NOW - timedelta(days=1), platform="bluesky", status="queued")

    with patch.object(
        plan_publish_reschedule,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        plan_publish_reschedule,
        "build_publish_queue_reschedule_report",
        wraps=lambda db, **kwargs: build_publish_queue_reschedule_report(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert (
            plan_publish_reschedule.main(
                [
                    "--days-ahead",
                    "2",
                    "--platform",
                    "x",
                    "--status",
                    "held",
                    "--limit",
                    "5",
                    "--format",
                    "json",
                ]
            )
            == 0
        )

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["platform"] == "x"
    assert payload["filters"]["status"] == "held"
    assert payload["suggestions"][0]["queue_id"] == held
    assert payload["suggestions"][0]["hold_reason"] == "operator review"
