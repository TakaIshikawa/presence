"""Tests for publish queue scheduling collision audit."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from output.publish_queue_audit import audit_publish_queue


def _insert_content(db, content: str = "Queued post") -> int:
    return db.conn.execute(
        """INSERT INTO generated_content
           (content, content_type, eval_score, published)
           VALUES (?, 'x_post', 7.0, 0)""",
        (content,),
    ).lastrowid


def _queue_item(
    db,
    *,
    scheduled_at: datetime,
    platform: str = "x",
    status: str = "queued",
) -> int:
    content_id = _insert_content(db, f"{platform} {scheduled_at.isoformat()}")
    queue_id = db.conn.execute(
        """INSERT INTO publish_queue
           (content_id, scheduled_at, platform, status)
           VALUES (?, ?, ?, ?)""",
        (content_id, scheduled_at.isoformat(), platform, status),
    ).lastrowid
    db.conn.commit()
    return queue_id


def test_audit_groups_queued_collisions_by_platform(db):
    base = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    first = _queue_item(db, scheduled_at=base, platform="x")
    second = _queue_item(db, scheduled_at=base + timedelta(minutes=20), platform="x")
    _queue_item(db, scheduled_at=base + timedelta(minutes=20), platform="bluesky")

    result = audit_publish_queue(db, window_minutes=30, apply_holds=False)

    assert result.collision_count == 1
    assert result.collision_groups[0].platform == "x"
    assert result.collision_groups[0].queue_ids == [first, second]
    assert result.affected_queue_ids == [second]
    assert "publish_queue_collision: x" in result.hold_reasons[second]
    assert db.get_publish_queue_item(second)["status"] == "queued"


def test_audit_expands_all_platform_rows_against_x_and_bluesky(db):
    base = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    all_id = _queue_item(db, scheduled_at=base, platform="all")
    x_id = _queue_item(db, scheduled_at=base + timedelta(minutes=10), platform="x")
    bluesky_id = _queue_item(
        db,
        scheduled_at=base + timedelta(minutes=12),
        platform="bluesky",
    )

    result = audit_publish_queue(db, window_minutes=30)

    assert [group.platform for group in result.collision_groups] == ["x", "bluesky"]
    assert result.collision_groups[0].queue_ids == [all_id, x_id]
    assert result.collision_groups[1].queue_ids == [all_id, bluesky_id]
    assert sorted(result.hold_reasons) == [x_id, bluesky_id]


def test_apply_holds_updates_only_deferred_queued_rows(db):
    base = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    first = _queue_item(db, scheduled_at=base, platform="x")
    second = _queue_item(db, scheduled_at=base + timedelta(minutes=15), platform="x")
    third = _queue_item(db, scheduled_at=base + timedelta(hours=2), platform="x")

    result = audit_publish_queue(db, window_minutes=30, apply_holds=True)

    assert result.affected_queue_ids == [second]
    assert db.get_publish_queue_item(first)["status"] == "queued"
    assert db.get_publish_queue_item(second)["status"] == "held"
    assert db.get_publish_queue_item(second)["hold_reason"].startswith(
        "publish_queue_collision: x"
    )
    assert db.get_publish_queue_item(third)["status"] == "queued"


def test_suggested_slot_respects_daily_limit_and_embargo(db):
    base = datetime(2026, 4, 24, 22, 45, tzinfo=timezone.utc)
    first = _queue_item(db, scheduled_at=base, platform="x")
    second = _queue_item(db, scheduled_at=base + timedelta(minutes=5), platform="x")

    result = audit_publish_queue(
        db,
        window_minutes=30,
        daily_platform_limits={"x": 1},
        embargo_windows=[
            {
                "start": "00:00",
                "end": "09:00",
                "timezone": "UTC",
            }
        ],
    )

    assert result.collision_groups[0].queue_ids == [first, second]
    assert "suggest defer until 2026-04-25T09:00:00+00:00" in result.hold_reasons[
        second
    ]


def test_non_queued_items_are_ignored(db):
    base = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)
    _queue_item(db, scheduled_at=base, platform="x", status="held")
    _queue_item(db, scheduled_at=base + timedelta(minutes=10), platform="x")

    result = audit_publish_queue(db, window_minutes=30)

    assert result.collision_count == 0
