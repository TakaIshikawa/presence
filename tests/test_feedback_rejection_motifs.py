"""Tests for feedback rejection motif reporting."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from synthesis.feedback_rejection_motifs import (
    build_feedback_rejection_motifs_report,
    format_feedback_rejection_motifs_text,
)


NOW = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "feedback_rejection_motifs.py"
spec = importlib.util.spec_from_file_location("feedback_rejection_motifs_script", SCRIPT_PATH)
feedback_rejection_motifs_script = importlib.util.module_from_spec(spec)
spec.loader.exec_module(feedback_rejection_motifs_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str, text: str) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=["abc123"],
        source_messages=["uuid-1"],
        content=text,
        eval_score=7.5,
        eval_feedback="ok",
    )


def _feedback(
    db,
    content_id: int,
    feedback_type: str,
    notes: str | None,
    *,
    days_ago: int = 0,
    replacement_text: str | None = None,
) -> int:
    feedback_id = db.add_content_feedback(
        content_id,
        feedback_type,
        notes or "",
        replacement_text,
    )
    created_at = (NOW - timedelta(days=days_ago)).isoformat()
    db.conn.execute(
        "UPDATE content_feedback SET created_at = ? WHERE id = ?",
        (created_at, feedback_id),
    )
    db.conn.commit()
    return feedback_id


def test_report_ranks_repeated_rejection_motifs_with_samples_and_candidates(db):
    first = _content(db, "x_post", "Today's insight: agent workflows are not about magic.")
    second = _content(db, "x_thread", "Today's insight: agent workflows are not about luck.")
    third = _content(db, "x_post", "A concrete build note.")
    _feedback(db, first, "reject", "Too generic. Needs sharper evidence.", days_ago=1)
    _feedback(db, second, "revise", "Too generic - sounds like thought leadership.", days_ago=1)
    _feedback(db, third, "reject", "Off voice.", days_ago=1)

    report = build_feedback_rejection_motifs_report(db, days=7, min_count=2, now=NOW)

    motif = report["motifs"][0]
    assert motif["motif"] == "too generic"
    assert motif["count"] == 2
    assert motif["content_ids"] == [first, second]
    assert motif["feedback_type_counts"] == {"reject": 1, "revise": 1}
    assert motif["sample_feedback"] == [
        "Too generic. Needs sharper evidence.",
        "Too generic - sounds like thought leadership.",
    ]
    assert motif["suggested_stale_pattern_candidates"] == [r"\btoo\s+generic\b"]
    text = format_feedback_rejection_motifs_text(report)
    assert "Feedback rejection motif report" in text
    assert "too generic count=2" in text


def test_report_uses_replacement_text_and_content_snippets_when_notes_are_null(db):
    first = _content(db, "x_post", "Today's insight starts with a stale announcement hook.")
    second = _content(db, "x_post", "Today's insight starts with a stale announcement hook again.")
    _feedback(
        db,
        first,
        "revise",
        None,
        replacement_text="Use a concrete builder observation instead.",
    )
    _feedback(
        db,
        second,
        "revise",
        None,
        replacement_text="Use a concrete builder observation with evidence.",
    )

    report = build_feedback_rejection_motifs_report(db, days=7, min_count=2, now=NOW)
    motifs = {item["motif"]: item for item in report["motifs"]}

    assert "use a concrete builder observation" in motifs
    assert "today s insight starts with" in motifs
    assert motifs["use a concrete builder observation"]["source_fields"] == {
        "replacement_text": 2
    }


def test_filters_by_lookback_and_content_type(db):
    included = _content(db, "x_post", "Draft one")
    old = _content(db, "x_post", "Draft two")
    other_type = _content(db, "blog_post", "Draft three")
    _feedback(db, included, "reject", "Too generic", days_ago=1)
    _feedback(db, old, "reject", "Too generic", days_ago=20)
    _feedback(db, other_type, "reject", "Too generic", days_ago=1)

    report = build_feedback_rejection_motifs_report(
        db,
        days=7,
        min_count=1,
        content_type="x_post",
        now=NOW,
    )

    assert report["summary"]["feedback_count"] == 1
    assert report["motifs"][0]["content_ids"] == [included]


def test_empty_feedback_history_returns_clear_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_feedback_rejection_motifs_report(conn, now=NOW)
    text = format_feedback_rejection_motifs_text(report)

    assert report["summary"] == {
        "feedback_count": 0,
        "content_count": 0,
        "motif_count": 0,
    }
    assert report["empty_state"] == {
        "is_empty": True,
        "schema_present": False,
        "message": "No rejected or revised content feedback found for the selected filters.",
    }
    assert report["motifs"] == []
    assert "No rejected or revised content feedback found" in text


def test_cli_supports_json_output_without_mutating_feedback(db, capsys):
    content_id = _content(db, "x_post", "CLI draft")
    feedback_id = _feedback(db, content_id, "reject", "Too generic", days_ago=0)
    before = db.conn.execute(
        "SELECT * FROM content_feedback WHERE id = ?",
        (feedback_id,),
    ).fetchone()

    with patch.object(
        feedback_rejection_motifs_script,
        "script_context",
        return_value=_script_context(db),
    ):
        result = feedback_rejection_motifs_script.main(
            ["--days", "7", "--min-count", "1", "--content-type", "x_post", "--format", "json"]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["content_type"] == "x_post"
    assert payload["summary"]["motif_count"] >= 1
    assert payload["motifs"][0]["motif"] == "too generic"
    after = db.conn.execute(
        "SELECT * FROM content_feedback WHERE id = ?",
        (feedback_id,),
    ).fetchone()
    assert dict(after) == dict(before)
