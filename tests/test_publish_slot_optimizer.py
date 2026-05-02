"""Tests for publish queue slot optimization plans."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.publish_slot_optimizer import (
    build_publish_slot_optimizer_report,
    format_publish_slot_optimizer_json,
    format_publish_slot_optimizer_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "optimize_publish_slots.py"
spec = importlib.util.spec_from_file_location("optimize_publish_slots_script", SCRIPT_PATH)
optimize_publish_slots_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(optimize_publish_slots_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, text: str = "Queued post", content_type: str = "x_post") -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha"],
        source_messages=["uuid"],
        content=text,
        eval_score=8.0,
        eval_feedback="Good",
    )


def _past_bucket(now: datetime, weekday: int, hour: int, weeks_ago: int = 1) -> datetime:
    days_back = (now.weekday() - weekday) % 7
    if days_back == 0 and now.hour <= hour:
        days_back = 7
    return (now - timedelta(days=days_back, weeks=weeks_ago - 1)).replace(
        hour=hour,
        minute=0,
        second=0,
        microsecond=0,
    )


def _set_x_history(db, published_at: datetime, score: float) -> int:
    content_id = _content(db, f"Published x {score}")
    tweet_id = f"tweet-{content_id}"
    db.mark_published(content_id, f"https://x.test/{content_id}", tweet_id=tweet_id)
    db.conn.execute(
        "UPDATE generated_content SET published_at = ? WHERE id = ?",
        (published_at.isoformat(), content_id),
    )
    db.conn.execute(
        """UPDATE content_publications SET published_at = ?
           WHERE content_id = ? AND platform = 'x'""",
        (published_at.isoformat(), content_id),
    )
    db.conn.commit()
    db.insert_engagement(content_id, tweet_id, 1, 0, 0, 0, score)
    return content_id


def _set_bluesky_history(db, published_at: datetime, score: float) -> int:
    content_id = _content(db, f"Published bluesky {score}")
    uri = f"at://test/post/{content_id}"
    db.mark_published_bluesky(content_id, uri)
    db.conn.execute(
        """UPDATE content_publications SET published_at = ?
           WHERE content_id = ? AND platform = 'bluesky'""",
        (published_at.isoformat(), content_id),
    )
    db.conn.commit()
    db.insert_bluesky_engagement(content_id, uri, 1, 0, 0, 0, score)
    return content_id


def _queue(db, scheduled_at: datetime, *, platform: str = "x", status: str = "queued") -> int:
    queue_id = db.queue_for_publishing(
        _content(db, f"{platform} {status}"),
        scheduled_at.isoformat(),
        platform=platform,
    )
    if status != "queued":
        db.conn.execute("UPDATE publish_queue SET status = ? WHERE id = ?", (status, queue_id))
        db.conn.commit()
    return queue_id


def test_planner_moves_excess_item_from_crowded_platform_hour_to_open_high_window(db):
    crowded_at = (NOW + timedelta(days=1)).replace(hour=9, minute=15)
    better_at = crowded_at.replace(hour=10, minute=0)
    first = _queue(db, crowded_at, platform="x")
    second = _queue(db, crowded_at.replace(minute=35), platform="x")
    _set_x_history(db, _past_bucket(NOW, better_at.weekday(), better_at.hour), 60.0)

    report = build_publish_slot_optimizer_report(
        db,
        days=3,
        platform="x",
        limit=5,
        now=NOW,
    )

    assert [(slot.platform, slot.item_count, slot.queue_ids) for slot in report.crowded_slots] == [
        ("x", 2, (first, second))
    ]
    assert report.open_windows[0].platform == "x"
    assert report.open_windows[0].start_time == better_at.isoformat()
    assert [move.queue_id for move in report.moves] == [second]
    assert report.moves[0].current_scheduled_at == crowded_at.replace(minute=35).isoformat()
    assert report.moves[0].proposed_scheduled_at == better_at.isoformat()
    assert "hour has 2 queued items" in report.moves[0].reason


def test_all_platform_items_conflict_with_platform_specific_slots(db):
    crowded_at = (NOW + timedelta(days=1)).replace(hour=9, minute=0)
    better_at = crowded_at.replace(hour=11)
    all_item = _queue(db, crowded_at, platform="all")
    x_item = _queue(db, crowded_at.replace(minute=10), platform="x")
    _set_x_history(db, _past_bucket(NOW, better_at.weekday(), better_at.hour), 50.0)
    _set_bluesky_history(db, _past_bucket(NOW, better_at.weekday(), 12), 45.0)

    report = build_publish_slot_optimizer_report(db, days=3, platform="all", now=NOW)

    x_crowded = [slot for slot in report.crowded_slots if slot.platform == "x"]
    assert x_crowded and x_crowded[0].queue_ids == (all_item, x_item)
    assert [slot.platform for slot in report.crowded_slots] == ["x"]
    assert report.moves[0].queue_id == x_item
    assert report.moves[0].target_platforms == ("x",)
    assert report.moves[0].proposed_scheduled_at == better_at.isoformat()


def test_held_and_failed_rows_are_excluded_unless_statuses_are_allowed(db):
    crowded_at = (NOW + timedelta(days=1)).replace(hour=9, minute=0)
    _queue(db, crowded_at, platform="x", status="queued")
    held = _queue(db, crowded_at.replace(minute=5), platform="x", status="held")
    failed = _queue(db, crowded_at.replace(minute=10), platform="x", status="failed")
    _set_x_history(db, _past_bucket(NOW, crowded_at.weekday(), 10), 30.0)

    default_report = build_publish_slot_optimizer_report(db, days=3, platform="x", now=NOW)
    allowed_report = build_publish_slot_optimizer_report(
        db,
        days=3,
        platform="x",
        allowed_statuses=("queued", "held", "failed"),
        now=NOW,
    )

    assert default_report.crowded_slots == ()
    assert allowed_report.crowded_slots[0].queue_ids == (1, held, failed)
    assert allowed_report.allowed_statuses == ("queued", "held", "failed")


def test_json_and_text_formatters_include_required_move_fields(db):
    crowded_at = (NOW + timedelta(days=1)).replace(hour=9, minute=0)
    _queue(db, crowded_at, platform="x")
    second = _queue(db, crowded_at.replace(minute=20), platform="x")
    _set_x_history(db, _past_bucket(NOW, crowded_at.weekday(), 10), 30.0)

    report = build_publish_slot_optimizer_report(db, days=3, platform="x", now=NOW)
    payload = json.loads(format_publish_slot_optimizer_json(report))
    text = format_publish_slot_optimizer_text(report)

    assert payload["artifact_type"] == "publish_slot_optimizer"
    assert payload["moves"][0]["queue_id"] == second
    assert payload["moves"][0]["current_scheduled_at"] == crowded_at.replace(
        minute=20
    ).isoformat()
    assert payload["moves"][0]["proposed_scheduled_at"] == crowded_at.replace(
        hour=10
    ).isoformat()
    assert payload["moves"][0]["reason"]
    assert f"queue {second}:" in text
    assert " -> " in text


def test_cli_supports_requested_flags(db, monkeypatch, capsys):
    crowded_at = (NOW + timedelta(days=1)).replace(hour=9, minute=0)
    _queue(db, crowded_at, platform="x")
    second = _queue(db, crowded_at.replace(minute=20), platform="x")
    _set_x_history(db, _past_bucket(NOW, crowded_at.weekday(), 10), 30.0)
    monkeypatch.setattr(
        optimize_publish_slots_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        optimize_publish_slots_script,
        "build_publish_slot_optimizer_report",
        lambda db, **kwargs: build_publish_slot_optimizer_report(db, now=NOW, **kwargs),
    )

    exit_code = optimize_publish_slots_script.main(
        ["--days", "3", "--platform", "x", "--limit", "1", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["platform"] == "x"
    assert payload["move_count"] == 1
    assert payload["moves"][0]["queue_id"] == second
