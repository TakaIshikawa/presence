"""Tests for commit-to-Claude-session link quality auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from unittest.mock import patch

from ingestion.commit_session_audit import (
    build_commit_session_audit_report,
    format_commit_session_audit_json,
    format_commit_session_audit_text,
)


SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "audit_commit_session_links.py"
)
spec = importlib.util.spec_from_file_location("audit_commit_session_links_script", SCRIPT_PATH)
audit_commit_session_links_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_commit_session_links_script)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield None, db


def _insert_commit(
    db,
    sha: str,
    timestamp: str,
    *,
    message: str | None = None,
) -> int:
    return db.insert_commit(
        repo_name="taka/presence",
        commit_sha=sha,
        commit_message=message or f"feat: {sha}",
        timestamp=timestamp,
        author="taka",
    )


def _insert_message(
    db,
    uuid: str,
    timestamp: str,
    *,
    session: str = "sess-1",
    text: str | None = None,
) -> int:
    return db.insert_claude_message(
        session_id=session,
        message_uuid=uuid,
        project_path="/work/presence",
        timestamp=timestamp,
        prompt_text=text or f"Work on {uuid}",
    )


def _link(db, commit_id: int, message_id: int, confidence: float | None = 0.9) -> int:
    cursor = db.conn.execute(
        """INSERT INTO commit_prompt_links (commit_id, message_id, confidence)
           VALUES (?, ?, ?)""",
        (commit_id, message_id, confidence),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_classifies_orphans_low_confidence_duplicates_and_large_gaps(db):
    good_commit = _insert_commit(db, "good-sha", "2026-05-01T09:00:00+00:00")
    good_message = _insert_message(db, "good-msg", "2026-05-01T09:05:00+00:00")
    low_commit = _insert_commit(db, "low-sha", "2026-05-01T10:00:00+00:00")
    low_message = _insert_message(db, "low-msg", "2026-05-01T10:15:00+00:00")
    gap_commit = _insert_commit(db, "gap-sha", "2026-05-01T11:00:00+00:00")
    gap_message = _insert_message(db, "gap-msg", "2026-05-01T03:00:00+00:00")
    dup_commit = _insert_commit(db, "dup-sha", "2026-05-01T08:00:00+00:00")
    dup_message = _insert_message(db, "dup-msg", "2026-05-01T08:10:00+00:00")
    _insert_commit(db, "orphan-sha", "2026-05-01T07:00:00+00:00")
    _insert_message(db, "orphan-msg", "2026-05-01T07:10:00+00:00")

    _link(db, good_commit, good_message, 0.9)
    low_link = _link(db, low_commit, low_message, 0.4)
    gap_link = _link(db, gap_commit, gap_message, 0.95)
    first_dup = _link(db, dup_commit, dup_message, 0.8)
    second_dup = _link(db, dup_commit, dup_message, 0.7)

    report = build_commit_session_audit_report(
        db,
        days=2,
        min_confidence=0.5,
        max_gap_hours=2,
        now=NOW,
    )

    assert [item.commit_sha for item in report.orphan_commits] == ["orphan-sha"]
    assert [item.message_uuid for item in report.orphan_messages] == ["orphan-msg"]
    assert [item.id for item in report.low_confidence_links] == [low_link]
    assert [item.id for item in report.large_gap_links] == [gap_link]
    assert [item.id for item in report.duplicate_links] == [first_dup, second_dup]
    assert report.totals == {
        "commits": 5,
        "claude_messages": 5,
        "links": 5,
        "orphan_commits": 1,
        "orphan_messages": 1,
        "low_confidence_links": 1,
        "duplicate_links": 2,
        "large_gap_links": 1,
        "flagged_links": 4,
    }


def test_json_output_is_deterministic_and_contains_actionable_reasons(db):
    commit_id = _insert_commit(db, "low-sha", "2026-05-01T10:00:00+00:00")
    message_id = _insert_message(db, "low-msg", "2026-05-01T10:15:00+00:00")
    _link(db, commit_id, message_id, 0.25)

    report = build_commit_session_audit_report(
        db,
        days=2,
        min_confidence=0.5,
        max_gap_hours=2,
        now=NOW,
    )
    first = format_commit_session_audit_json(report)
    second = format_commit_session_audit_json(report)

    assert first == second
    payload = json.loads(first)
    assert payload["filters"]["min_confidence"] == 0.5
    assert payload["low_confidence_links"][0]["commit_sha"] == "low-sha"
    assert payload["low_confidence_links"][0]["message_uuid"] == "low-msg"
    assert payload["low_confidence_links"][0]["reasons"] == [
        "confidence 0.25 below threshold 0.50"
    ]


def test_text_output_summarizes_sections(db):
    _insert_commit(db, "orphan-sha", "2026-05-01T07:00:00+00:00")
    _insert_message(db, "orphan-msg", "2026-05-01T07:10:00+00:00")

    text = format_commit_session_audit_text(
        build_commit_session_audit_report(db, days=2, now=NOW)
    )

    assert "Commit Session Link Audit" in text
    assert "Summary: commits=1 claude_messages=1 links=0 flagged=0" in text
    assert "Orphan commits:" in text
    assert "orphan-sha" in text
    assert "Orphan Claude messages:" in text
    assert "orphan-msg" in text
    assert "Low-confidence links:" in text
    assert "  - none" in text


def test_cli_outputs_json_with_threshold_options(db, capsys):
    commit_id = _insert_commit(db, "cli-sha", "2026-05-01T10:00:00+00:00")
    message_id = _insert_message(db, "cli-msg", "2026-05-01T06:00:00+00:00")
    _link(db, commit_id, message_id, 0.6)

    with patch.object(
        audit_commit_session_links_script,
        "script_context",
        wraps=lambda: _script_context(db),
    ), patch.object(
        audit_commit_session_links_script,
        "build_commit_session_audit_report",
        wraps=lambda db, **kwargs: build_commit_session_audit_report(
            db,
            now=NOW,
            **kwargs,
        ),
    ):
        assert audit_commit_session_links_script.main(
            [
                "--days",
                "2",
                "--min-confidence",
                "0.7",
                "--max-gap-hours",
                "3",
                "--json",
            ]
        ) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["min_confidence"] == 0.7
    assert payload["totals"]["low_confidence_links"] == 1
    assert payload["totals"]["large_gap_links"] == 1


def test_rejects_invalid_thresholds(db):
    try:
        build_commit_session_audit_report(db, min_confidence=1.5, now=NOW)
    except ValueError as exc:
        assert "min-confidence must be between 0 and 1" in str(exc)
    else:
        raise AssertionError("expected ValueError")
