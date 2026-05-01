"""Tests for durable content feedback trend reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from content_feedback_trends import main  # noqa: E402
from evaluation.content_feedback_trends import (  # noqa: E402
    build_content_feedback_trends_report,
    format_content_feedback_trends_text,
)


BASE_TIME = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)


def _insert_content(db, content_type: str, text: str) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=7.5,
        eval_feedback="ok",
    )


def _add_feedback(
    db,
    content_id: int,
    feedback_type: str,
    notes: str,
    *,
    days_ago: int = 0,
    replacement_text: str | None = None,
) -> int:
    feedback_id = db.add_content_feedback(
        content_id,
        feedback_type,
        notes,
        replacement_text,
    )
    created_at = (BASE_TIME - timedelta(days=days_ago)).isoformat()
    db.conn.execute(
        "UPDATE content_feedback SET created_at = ? WHERE id = ?",
        (created_at, feedback_id),
    )
    db.conn.commit()
    return feedback_id


def test_empty_database_returns_stable_empty_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_content_feedback_trends_report(conn, now=BASE_TIME)
    text = format_content_feedback_trends_text(report)

    assert report["totals"] == {
        "total": 0,
        "by_feedback_type": {},
        "by_content_type": {},
    }
    assert report["grouped_rows"] == []
    assert report["weekly_trends"] == []
    assert report["empty_state"] == {
        "is_empty": True,
        "schema_present": False,
        "message": "No content feedback found for the selected filters.",
    }
    assert "No content feedback found" in text


def test_report_groups_mixed_feedback_types_by_reason_content_type_and_week(db):
    post = _insert_content(db, "x_post", "Post draft")
    thread = _insert_content(db, "x_thread", "Thread draft")
    blog = _insert_content(db, "blog_post", "Blog draft")
    _add_feedback(db, post, "reject", "Too generic.", days_ago=1)
    _add_feedback(db, thread, "revise", "Too generic", days_ago=1)
    _add_feedback(db, blog, "prefer", "Strong concrete story", days_ago=8)

    report = build_content_feedback_trends_report(db, days=14, now=BASE_TIME)

    assert report["totals"]["total"] == 3
    assert report["totals"]["by_feedback_type"] == {
        "prefer": 1,
        "reject": 1,
        "revise": 1,
    }
    assert report["weekly_trends"] == [
        {
            "week_start": "2026-04-13",
            "total": 1,
            "feedback_type_counts": {"prefer": 1},
        },
        {
            "week_start": "2026-04-20",
            "total": 2,
            "feedback_type_counts": {"reject": 1, "revise": 1},
        },
    ]
    assert {
        (row["feedback_type"], row["reason"], row["content_type"], row["week_start"])
        for row in report["grouped_rows"]
    } == {
        ("reject", "too generic", "x_post", "2026-04-20"),
        ("revise", "too generic", "x_thread", "2026-04-20"),
        ("prefer", "strong concrete story", "blog_post", "2026-04-13"),
    }


def test_reason_grouping_surfaces_repeated_reject_revise_reasons(db):
    first = _insert_content(db, "x_post", "First draft")
    second = _insert_content(db, "x_post", "Second draft")
    third = _insert_content(db, "x_thread", "Third draft")
    _add_feedback(db, first, "reject", "Too vague!", days_ago=2)
    _add_feedback(db, second, "revise", "too vague", days_ago=1)
    _add_feedback(db, third, "reject", "Needs sharper evidence", days_ago=1)

    report = build_content_feedback_trends_report(db, days=7, now=BASE_TIME)

    assert report["repeated_reject_revise_reasons"] == [
        {
            "reason": "too vague",
            "count": 2,
            "feedback_type_counts": {"reject": 1, "revise": 1},
            "content_type_counts": {"x_post": 2},
            "representative_content_ids": [first, second],
        }
    ]
    text = format_content_feedback_trends_text(report)
    assert "Top feedback reasons:" in text
    assert "too vague total=2" in text
    assert "Weekly trend counts:" in text


def test_feedback_type_filter_and_lookback_exclude_other_rows(db):
    recent_reject = _insert_content(db, "x_post", "Recent reject")
    old_reject = _insert_content(db, "x_post", "Old reject")
    recent_prefer = _insert_content(db, "x_post", "Recent prefer")
    _add_feedback(db, recent_reject, "reject", "Off voice", days_ago=1)
    _add_feedback(db, old_reject, "reject", "Old note", days_ago=30)
    _add_feedback(db, recent_prefer, "prefer", "Good", days_ago=1)

    report = build_content_feedback_trends_report(
        db,
        days=7,
        feedback_type="reject",
        now=BASE_TIME,
    )

    assert report["totals"]["total"] == 1
    assert report["totals"]["by_feedback_type"] == {"reject": 1}
    assert report["grouped_rows"][0]["representative_content_ids"] == [recent_reject]


def test_cli_supports_json_output_without_mutating_feedback_rows(db, capsys):
    content_id = _insert_content(db, "x_post", "CLI draft")
    feedback_id = _add_feedback(db, content_id, "reject", "Too generic", days_ago=0)
    before = db.conn.execute(
        "SELECT * FROM content_feedback WHERE id = ?",
        (feedback_id,),
    ).fetchone()

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_feedback_trends.script_context", fake_script_context):
        result = main(
            [
                "--days",
                "30",
                "--feedback-type",
                "reject",
                "--limit",
                "3",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["feedback_type"] == "reject"
    assert payload["limit"] == 3
    assert payload["totals"]["by_feedback_type"] == {"reject": 1}
    after = db.conn.execute(
        "SELECT * FROM content_feedback WHERE id = ?",
        (feedback_id,),
    ).fetchone()
    assert dict(after) == dict(before)
