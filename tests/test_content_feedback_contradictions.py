"""Tests for contradictory content feedback reporting."""

from __future__ import annotations

import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from content_feedback_contradictions import main  # noqa: E402
from evaluation.content_feedback_contradictions import (  # noqa: E402
    build_content_feedback_contradictions_report,
    format_content_feedback_contradictions_text,
)


NOW = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)


def _content(db, text: str = "draft") -> int:
    return db.insert_generated_content("x_post", [], [], text, 7.0, "ok")


def _feedback(db, content_id: int, feedback_type: str, notes: str, tags=None, days_ago: int = 0) -> int:
    feedback_id = db.add_content_feedback(content_id, feedback_type, notes, tags=tags)
    db.conn.execute(
        "UPDATE content_feedback SET created_at = ? WHERE id = ?",
        ((NOW - timedelta(days=days_ago)).isoformat(), feedback_id),
    )
    db.conn.commit()
    return feedback_id


def test_groups_by_content_id_and_surfaces_counts_tags_and_notes(db):
    content_id = _content(db)
    _feedback(db, content_id, "prefer", "Strong evidence", tags=["evidence", "tone"])
    _feedback(db, content_id, "reject", "Evidence was misleading", tags=[" evidence "])
    _feedback(db, _content(db), "prefer", "good", tags=["voice"])

    report = build_content_feedback_contradictions_report(db, days=7, now=NOW)

    assert report["totals"]["contradictory_content_count"] == 1
    group = report["contradictions"][0]
    assert group["content_id"] == content_id
    assert group["feedback_type_counts"] == {"prefer": 1, "reject": 1}
    assert group["conflicting_tags"] == ["evidence"]
    assert group["latest_feedback_at"] == NOW.isoformat()
    assert group["sample_notes"] == ["Evidence was misleading", "Strong evidence"]
    assert group["normalized_tag_summary"][0] == {
        "tag": "evidence",
        "count": 2,
        "feedback_types": ["prefer", "reject"],
    }
    assert "content_id=" in format_content_feedback_contradictions_text(report)


def test_type_conflict_without_tags_and_malformed_tags_do_not_crash(db):
    content_id = _content(db)
    _feedback(db, content_id, "prefer", "ship it", tags=None)
    bad_id = _feedback(db, content_id, "revise", "needs work", tags=["clarity"])
    db.conn.execute("UPDATE content_feedback SET tags = ? WHERE id = ?", ("not-json", bad_id))
    db.conn.commit()

    report = build_content_feedback_contradictions_report(db, days=7, now=NOW)

    assert report["totals"]["malformed_tag_rows"] == 1
    assert report["contradictions"][0]["feedback_type_counts"] == {"prefer": 1, "revise": 1}
    assert report["contradictions"][0]["conflicting_tags"] == []


def test_tag_filter_only_returns_matching_conflicting_tag(db):
    first = _content(db)
    second = _content(db)
    _feedback(db, first, "prefer", "good", tags=["voice"])
    _feedback(db, first, "reject", "bad", tags=["voice"])
    _feedback(db, second, "prefer", "good", tags=["evidence"])
    _feedback(db, second, "reject", "bad", tags=["evidence"])

    report = build_content_feedback_contradictions_report(db, tag="evidence", now=NOW)

    assert [row["content_id"] for row in report["contradictions"]] == [second]


def test_empty_missing_schema_returns_stable_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_content_feedback_contradictions_report(conn, now=NOW)

    assert report["empty_state"]["schema_present"] is False
    assert report["contradictions"] == []


def test_cli_json_output(db, capsys):
    content_id = _content(db)
    _feedback(db, content_id, "prefer", "good", tags=["voice"])
    _feedback(db, content_id, "reject", "bad", tags=["voice"])

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("content_feedback_contradictions.script_context", fake_script_context):
        result = main(["--days", "30", "--limit", "5", "--tag", "voice", "--format", "json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["tag"] == "voice"
    assert payload["contradictions"][0]["content_id"] == content_id
