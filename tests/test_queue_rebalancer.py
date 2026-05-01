"""Tests for publish queue rebalancing."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.queue_rebalancer import (  # noqa: E402
    apply_publish_queue_rebalance,
    format_queue_rebalance_report_text,
    parse_quiet_hours,
    plan_publish_queue_rebalance,
)
from rebalance_publish_queue import main  # noqa: E402


BASE_TIME = datetime(2026, 4, 24, 0, 0, tzinfo=timezone.utc)


def _config(limits: dict[str, int] | None = None):
    return SimpleNamespace(
        publishing=SimpleNamespace(
            daily_platform_limits=limits or {},
        )
    )


def _content(db, label: str) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=label,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _queue(
    db,
    *,
    scheduled_at: datetime,
    platform: str = "x",
    status: str = "queued",
) -> int:
    content_id = _content(db, f"{platform} {scheduled_at.isoformat()}")
    return db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, scheduled_at.isoformat(), platform, status),
    ).lastrowid


def test_planner_detects_cap_violation_and_preserves_platform_order(db):
    first = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=9), platform="x")
    second = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=10), platform="x")
    third = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=11), platform="x")
    db.conn.commit()

    report = plan_publish_queue_rebalance(
        db,
        _config({"x": 2}),
        days=1,
        now=BASE_TIME,
    )

    assert [(item.platform, item.excess_count) for item in report.violations] == [
        ("x", 1)
    ]
    assert report.violations[0].queue_ids == [first, second, third]
    assert [change.queue_id for change in report.changes] == [third]
    assert report.changes[0].scheduled_at == (BASE_TIME + timedelta(hours=11)).isoformat()
    assert report.changes[0].proposed_scheduled_at == (
        BASE_TIME + timedelta(days=1, hours=11)
    ).isoformat()
    assert "daily_cap" in report.changes[0].reasons[0]


def test_quiet_hours_are_avoided_when_finding_new_slot(db):
    _queue(db, scheduled_at=BASE_TIME + timedelta(hours=21), platform="x")
    moved = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=22), platform="x")
    db.conn.commit()

    report = plan_publish_queue_rebalance(
        db,
        _config({"x": 1}),
        days=1,
        quiet_hours=parse_quiet_hours("22:00-06:00"),
        now=BASE_TIME,
    )

    assert [change.queue_id for change in report.changes] == [moved]
    assert report.changes[0].proposed_scheduled_at == (
        BASE_TIME + timedelta(days=1, hours=6)
    ).isoformat()
    assert any("quiet_hours" in reason for reason in report.changes[0].reasons)


def test_held_failed_published_and_cancelled_items_are_excluded(db):
    eligible = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=9), platform="x")
    held = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=10), platform="x", status="held")
    failed = _queue(
        db,
        scheduled_at=BASE_TIME + timedelta(hours=11),
        platform="x",
        status="failed",
    )
    published = _queue(
        db,
        scheduled_at=BASE_TIME + timedelta(hours=12),
        platform="x",
        status="published",
    )
    cancelled = _queue(
        db,
        scheduled_at=BASE_TIME + timedelta(hours=13),
        platform="x",
        status="cancelled",
    )
    overflow = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=14), platform="x")
    db.conn.commit()

    report = plan_publish_queue_rebalance(
        db,
        _config({"x": 1}),
        days=1,
        now=BASE_TIME,
    )
    apply_publish_queue_rebalance(db, report)

    assert [change.queue_id for change in report.changes] == [overflow]
    assert db.get_publish_queue_item(eligible)["scheduled_at"] == (
        BASE_TIME + timedelta(hours=9)
    ).isoformat()
    assert db.get_publish_queue_item(held)["status"] == "held"
    assert db.get_publish_queue_item(failed)["scheduled_at"] == (
        BASE_TIME + timedelta(hours=11)
    ).isoformat()
    assert db.get_publish_queue_item(published)["scheduled_at"] == (
        BASE_TIME + timedelta(hours=12)
    ).isoformat()
    assert db.get_publish_queue_item(cancelled)["scheduled_at"] == (
        BASE_TIME + timedelta(hours=13)
    ).isoformat()
    assert report.applied_count == 1


def test_idempotent_no_op_after_apply(db):
    _queue(db, scheduled_at=BASE_TIME + timedelta(hours=9), platform="x")
    _queue(db, scheduled_at=BASE_TIME + timedelta(hours=10), platform="x")
    db.conn.commit()

    first_report = plan_publish_queue_rebalance(
        db,
        _config({"x": 1}),
        days=1,
        now=BASE_TIME,
    )
    apply_publish_queue_rebalance(db, first_report)
    second_report = plan_publish_queue_rebalance(
        db,
        _config({"x": 1}),
        days=1,
        now=BASE_TIME,
    )

    assert first_report.applied_count == 1
    assert second_report.violations == []
    assert second_report.changes == []
    assert "No schedule changes proposed." in format_queue_rebalance_report_text(
        second_report
    )


def test_all_platform_rows_consume_both_platform_caps(db):
    _queue(db, scheduled_at=BASE_TIME + timedelta(hours=9), platform="all")
    moved = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=10), platform="all")
    db.conn.commit()

    report = plan_publish_queue_rebalance(
        db,
        _config({"x": 1, "bluesky": 1}),
        days=1,
        now=BASE_TIME,
    )

    assert {(item.platform, item.excess_count) for item in report.violations} == {
        ("x", 1),
        ("bluesky", 1),
    }
    assert [change.queue_id for change in report.changes] == [moved]


def test_cli_defaults_to_dry_run_json(db, capsys):
    _queue(db, scheduled_at=BASE_TIME + timedelta(hours=9), platform="x")
    moved = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=10), platform="x")
    db.conn.commit()

    @contextmanager
    def fake_script_context():
        yield _config({"x": 1}), db

    with patch("rebalance_publish_queue.script_context", fake_script_context), patch(
        "output.queue_rebalancer.datetime"
    ) as mocked_datetime:
        mocked_datetime.now.return_value = BASE_TIME
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        result = main(["--days", "1", "--format", "json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["change_count"] == 1
    assert payload["changes"][0]["queue_id"] == moved
    assert payload["applied_count"] == 0
    assert db.get_publish_queue_item(moved)["scheduled_at"] == (
        BASE_TIME + timedelta(hours=10)
    ).isoformat()


def test_cli_apply_reports_update_counts(db, capsys):
    _queue(db, scheduled_at=BASE_TIME + timedelta(hours=9), platform="x")
    moved = _queue(db, scheduled_at=BASE_TIME + timedelta(hours=10), platform="x")
    db.conn.commit()

    @contextmanager
    def fake_script_context():
        yield _config({"x": 1}), db

    with patch("rebalance_publish_queue.script_context", fake_script_context), patch(
        "output.queue_rebalancer.datetime"
    ) as mocked_datetime:
        mocked_datetime.now.return_value = BASE_TIME
        mocked_datetime.fromisoformat.side_effect = datetime.fromisoformat
        result = main(["--days", "1", "--apply"])

    assert result == 0
    output = capsys.readouterr().out
    assert "Applied updates to 1 queued row(s)" in output
    assert db.get_publish_queue_item(moved)["scheduled_at"] == (
        BASE_TIME + timedelta(days=1, hours=10)
    ).isoformat()
