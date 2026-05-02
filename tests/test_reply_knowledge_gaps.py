"""Tests for reply knowledge gap reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.reply_knowledge_gaps import (  # noqa: E402
    build_reply_knowledge_gap_report,
    format_reply_knowledge_gap_json,
    format_reply_knowledge_gap_text,
)
from reply_knowledge_gaps import main  # noqa: E402


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _mock_script_context(db):
    @contextmanager
    def _ctx():
        yield (SimpleNamespace(), db)

    return _ctx


def _insert_reply(db, tweet_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="How do you handle API docs feedback for SDK users?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Good developer experience needs sharp docs.",
        draft_text="Thanks, that makes sense.",
        intent="question",
        quality_score=7.0,
        quality_flags=None,
        status="pending",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def _link(db, reply_id: int, knowledge_id: int = 1) -> None:
    db.conn.execute(
        """INSERT INTO knowledge (id, source_type, source_id, content)
           VALUES (?, 'own_post', ?, 'Prior knowledge')
           ON CONFLICT(id) DO NOTHING""",
        (knowledge_id, f"k-{knowledge_id}"),
    )
    db.conn.execute(
        """INSERT INTO reply_knowledge_links
           (reply_queue_id, knowledge_id, relevance_score)
           VALUES (?, ?, 0.9)""",
        (reply_id, knowledge_id),
    )
    db.conn.commit()


def test_report_surfaces_unsupported_low_quality_generic_and_repeated_gaps(db):
    unsupported = _insert_reply(
        db,
        "unsupported",
        inbound_author_handle="devrel",
        quality_score=8.0,
    )
    low_quality = _insert_reply(
        db,
        "low-quality",
        inbound_author_handle="devrel",
        inbound_text="API docs feedback for SDK onboarding is still confusing",
        quality_score=4.5,
        quality_flags=json.dumps(["generic"]),
    )
    linked_repeat = _insert_reply(
        db,
        "linked-repeat",
        inbound_author_handle="devrel",
        inbound_text="What API docs examples help SDK users most?",
        quality_score=8.5,
    )
    ignored = _insert_reply(
        db,
        "ignored",
        inbound_author_handle="ops",
        inbound_text="Release reliability looked good",
        intent="praise",
        quality_score=8.0,
    )
    _set_detected_at(db, unsupported, "2026-04-23 01:00:00")
    _set_detected_at(db, low_quality, "2026-04-23 02:00:00")
    _set_detected_at(db, linked_repeat, "2026-04-23 03:00:00")
    _set_detected_at(db, ignored, "2026-04-23 04:00:00")
    _link(db, low_quality, 1)
    _link(db, linked_repeat, 2)
    _link(db, ignored, 3)

    report = build_reply_knowledge_gap_report(
        db,
        days=7,
        status="pending",
        min_quality=6,
        now=NOW,
    )

    assert report["totals"] == {
        "replies_scanned": 4,
        "gap_replies": 3,
        "unsupported_replies": 1,
        "low_quality_replies": 1,
        "generic_feedback_replies": 1,
        "repeated_target_topic_replies": 3,
    }
    assert [group["target_handle"] for group in report["groups"]] == ["@devrel"]
    group = report["groups"][0]
    assert group["topic"] == "question"
    assert group["reply_ids"] == [unsupported, low_quality, linked_repeat]
    assert group["reason_counts"] == {
        "generic_feedback": 1,
        "low_quality": 1,
        "repeated_target_topic": 3,
        "unsupported": 1,
    }
    assert "Collect first-party knowledge" in group["suggested_ingestion_prompts"][0]
    assert "docs" in group["themes"]

    items = {item["id"]: item for item in report["items"]}
    assert items[unsupported]["knowledge_link_count"] == 0
    assert "unsupported" in items[unsupported]["gap_reasons"]
    assert "generic_feedback" in items[low_quality]["gap_reasons"]
    assert items[linked_repeat]["gap_reasons"] == ["repeated_target_topic"]
    assert ignored not in items


def test_text_and_json_output_are_deterministic(db):
    first = _insert_reply(
        db,
        "tw-1",
        inbound_author_handle="bob",
        inbound_text="API docs need better SDK examples",
        quality_score=5.0,
    )
    _set_detected_at(db, first, "2026-04-23 00:00:00")
    report = build_reply_knowledge_gap_report(
        db,
        days=7,
        status="pending",
        min_quality=6,
        now=NOW,
    )

    assert json.loads(format_reply_knowledge_gap_json(report))["filters"] == {
        "days": 7,
        "lookback_end": "2026-04-23T12:00:00+00:00",
        "lookback_start": "2026-04-16T12:00:00+00:00",
        "min_quality": 6.0,
        "status": ["pending"],
    }
    assert format_reply_knowledge_gap_text(report) == "\n".join(
        [
            "Reply Knowledge Gap Report",
            "Generated: 2026-04-23T12:00:00+00:00",
            (
                "Lookback: 7 days "
                "(2026-04-16T12:00:00+00:00 to 2026-04-23T12:00:00+00:00)"
            ),
            "Status: pending",
            "Min quality: 6",
            "Rows: scanned=1 gaps=1 unsupported=1 low_quality=1 generic=0 repeated=0",
            "",
            "Gaps:",
            "  Handle             Topic                Rows  NoK  LowQ  Gen  Rpt   AvgQ  Reply ids",
            "  ------------------------------------------------------------------------------------",
            f"  @bob               question                1    1     1    0    0   5.00  {first}",
            "",
            "Suggested ingestion prompts:",
            (
                "- @bob / question: Collect first-party knowledge, prior posts, "
                "and relationship notes for @bob on question; focus on docs, better, "
                "examples, good. Address gaps: low quality, unsupported."
            ),
        ]
    )


def test_status_filter_and_review_feedback_can_create_generic_gap(db):
    reviewed = _insert_reply(
        db,
        "reviewed",
        inbound_author_handle="casey",
        quality_score=8.0,
        status="approved",
    )
    pending = _insert_reply(db, "pending", inbound_author_handle="casey", status="pending")
    _set_detected_at(db, reviewed, "2026-04-23 00:00:00")
    _set_detected_at(db, pending, "2026-04-23 00:00:00")
    _link(db, reviewed, 1)
    _link(db, pending, 2)
    db.record_reply_review_event(
        reviewed,
        "edited",
        old_status="pending",
        new_status="approved",
        notes="Too generic; needs concrete relationship context.",
        created_at="2026-04-23T02:00:00+00:00",
    )

    report = build_reply_knowledge_gap_report(
        db,
        days=7,
        status="approved",
        min_quality=6,
        now=NOW,
    )

    assert report["totals"]["replies_scanned"] == 1
    assert report["totals"]["generic_feedback_replies"] == 1
    assert report["items"][0]["id"] == reviewed
    assert report["items"][0]["feedback"] == "Too generic; needs concrete relationship context."


def test_partial_schema_and_empty_schema_are_handled():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            detected_at TEXT,
            inbound_author_handle TEXT,
            inbound_text TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO reply_queue
           (id, status, detected_at, inbound_author_handle, inbound_text)
           VALUES (7, 'pending', '2026-04-23 00:00:00', 'alice', 'Prompt context question')"""
    )

    report = build_reply_knowledge_gap_report(conn, days=7, status="pending", now=NOW)

    assert report["totals"]["unsupported_replies"] == 1
    assert report["items"][0]["target_handle"] == "@alice"
    assert report["items"][0]["topic"] == "ai-workflow"

    empty = build_reply_knowledge_gap_report(
        sqlite3.connect(":memory:"),
        days=7,
        status="pending",
        now=NOW,
    )
    assert empty["items"] == []
    assert empty["totals"]["replies_scanned"] == 0


def test_cli_json_output(capsys):
    class FakeDb:
        def __init__(self):
            self.conn = sqlite3.connect(":memory:")
            self.conn.execute(
                """CREATE TABLE reply_queue (
                    id INTEGER PRIMARY KEY,
                    status TEXT,
                    detected_at TEXT,
                    inbound_author_handle TEXT,
                    inbound_text TEXT,
                    quality_score REAL,
                    quality_flags TEXT
                )"""
            )
            self.conn.execute(
                """INSERT INTO reply_queue
                   (id, status, detected_at, inbound_author_handle, inbound_text, quality_score)
                   VALUES (1, 'pending', '2026-05-02 00:00:00', 'alice', 'API docs question', 4.0)"""
            )

    with patch("reply_knowledge_gaps.script_context", _mock_script_context(FakeDb())):
        assert main(["--format", "json", "--days", "7", "--status", "pending"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["days"] == 7
    assert payload["filters"]["status"] == ["pending"]
    assert payload["totals"]["gap_replies"] == 1


@pytest.mark.parametrize(
    "argv",
    [
        ["--days", "0"],
        ["--min-quality", "-1"],
        ["--min-quality", "11"],
    ],
)
def test_invalid_cli_arguments_exit_2(argv):
    with pytest.raises(SystemExit) as excinfo:
        main(argv)

    assert excinfo.value.code == 2
