from __future__ import annotations

from datetime import date

import pytest

from output.calendar_export import (
    escape_text,
    export_calendar,
    first_url,
    planned_topic_events,
)


def _insert_content(db, content: str = "Queued post") -> int:
    cursor = db.conn.execute(
        """INSERT INTO generated_content
           (content_type, content, eval_score, published)
           VALUES ('x_post', ?, 7.0, 0)""",
        (content,),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_export_planned_topics_as_ics(db):
    campaign_id = db.insert_content_campaign(
        name="Launch",
        goal="Teach launch lessons",
        status="active",
    )
    first_id = db.insert_planned_topic(
        topic="architecture",
        angle="API boundaries, queues; and ownership",
        target_date="2026-04-25",
        source_material="Use https://example.com/source?a=1,b=2",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="testing",
        angle="Outside the range",
        target_date="2026-05-30",
    )
    db.insert_planned_topic(
        topic="other",
        angle="Already generated",
        target_date="2026-04-26",
        status="generated",
    )

    ics = export_calendar(db, start=date(2026, 4, 24), days=7)

    assert ics.startswith("BEGIN:VCALENDAR\r\n")
    assert ics.count("BEGIN:VEVENT") == 1
    assert f"UID:planned-topic-{first_id}@presence.local" in ics
    assert "DTSTART;VALUE=DATE:20260425" in ics
    assert "SUMMARY:Planned: architecture" in ics
    unfolded = ics.replace("\r\n ", "")
    assert "DESCRIPTION:Type: Planned topic\\nTopic: architecture\\nAngle: API boundaries\\," in unfolded
    assert "URL:https://example.com/source?a=1,b=2" in ics
    assert "testing" not in ics
    assert ics.endswith("END:VCALENDAR\r\n")


def test_queue_items_are_optional_and_include_utc_times(db):
    content_id = _insert_content(db, "Queued copy\nwith punctuation, semicolons; and slashes")
    queue_id = db.queue_for_publishing(
        content_id,
        "2026-04-25T12:30:00+09:00",
        platform="x",
    )
    db.conn.execute(
        """UPDATE content_publications
           SET status = 'failed',
               next_retry_at = '2026-04-26T08:00:00+00:00',
               error = 'rate limited',
               error_category = 'rate_limit'
           WHERE content_id = ? AND platform = 'x'""",
        (content_id,),
    )
    db.conn.commit()

    planned_only = export_calendar(db, start=date(2026, 4, 24), days=7)
    with_queue = export_calendar(
        db,
        start=date(2026, 4, 24),
        days=7,
        include_queue=True,
    )

    assert "publish-queue" not in planned_only
    assert "content-publication" not in planned_only
    assert with_queue.count("BEGIN:VEVENT") == 2
    assert f"UID:publish-queue-{queue_id}@presence.local" in with_queue
    assert "DTSTART:20260425T033000Z" in with_queue
    assert "SUMMARY:Publish (x): x_post" in with_queue
    assert "UID:content-publication-" in with_queue
    assert "DTSTART:20260426T080000Z" in with_queue
    assert "SUMMARY:Retry publish (x): x_post" in with_queue


def test_planned_topic_window_excludes_past_and_unscheduled(db):
    db.insert_planned_topic(
        topic="past",
        target_date="2026-04-23",
    )
    db.insert_planned_topic(
        topic="unscheduled",
    )
    expected_id = db.insert_planned_topic(
        topic="today",
        target_date="2026-04-24",
    )

    events = planned_topic_events(db, start=date(2026, 4, 24), days=1)

    assert [event.uid for event in events] == [
        f"planned-topic-{expected_id}@presence.local"
    ]


@pytest.mark.parametrize(
    ("raw", "escaped"),
    [
        ("comma, semicolon; slash\\", "comma\\, semicolon\\; slash\\\\"),
        ("line one\nline two", "line one\\nline two"),
    ],
)
def test_escape_text(raw, escaped):
    assert escape_text(raw) == escaped


def test_first_url_trims_trailing_punctuation():
    assert first_url("Read https://example.com/article, then summarize") == (
        "https://example.com/article"
    )
