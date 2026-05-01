"""Tests for newsletter subject candidate selection."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.newsletter_subject_selection import (  # noqa: E402
    apply_newsletter_subject_selection,
    format_newsletter_subject_selection_text,
    list_candidates_for_send,
    select_candidate_for_send,
)
from select_newsletter_subject import main  # noqa: E402


NOW = datetime(2026, 4, 30, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_pending_send(db, *, subject: str = "Draft subject") -> int:
    send_id = db.conn.execute(
        """INSERT INTO newsletter_sends
           (issue_id, subject, source_content_ids, subscriber_count, status, sent_at)
           VALUES (?, ?, ?, ?, 'draft', NULL)""",
        ("issue-draft", subject, "[]", 100),
    ).lastrowid
    db.conn.commit()
    return int(send_id)


def _insert_candidate(db, send_id: int, subject: str, score: float, **kwargs) -> int:
    metadata = kwargs.pop("metadata", {})
    cursor = db.conn.execute(
        """INSERT INTO newsletter_subject_candidates
           (newsletter_send_id, issue_id, subject, score, rationale, source,
            rank, selected, metadata)
           VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?)""",
        (
            send_id,
            kwargs.pop("issue_id", "issue-draft"),
            subject,
            score,
            kwargs.pop("rationale", ""),
            kwargs.pop("source", "heuristic"),
            kwargs.pop("rank", None),
            json.dumps(metadata),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _subject_for_send(db, send_id: int) -> str:
    return db.conn.execute(
        "SELECT subject FROM newsletter_sends WHERE id = ?",
        (send_id,),
    ).fetchone()["subject"]


def test_best_selection_picks_highest_scored_non_rejected_deterministically(db):
    send_id = _insert_pending_send(db)
    _insert_candidate(db, send_id, "Rejected winner", 10.0, metadata={"rejected": True})
    later_id = _insert_candidate(db, send_id, "Tie B", 8.5, rank=2)
    winning_id = _insert_candidate(db, send_id, "Tie A", 8.5, rank=1)
    _insert_candidate(db, send_id, "Lower score", 7.0, rank=1)

    selected = select_candidate_for_send(db, send_id=send_id, best=True)

    assert selected["id"] == winning_id
    assert selected["subject"] == "Tie A"
    assert selected["rejected"] is False
    assert later_id != winning_id


def test_explicit_selection_validates_candidate_belongs_to_send(db):
    send_id = _insert_pending_send(db)
    other_send_id = _insert_pending_send(db)
    candidate_id = _insert_candidate(db, other_send_id, "Other send", 9.0)

    report = apply_newsletter_subject_selection(
        db,
        send_id=send_id,
        candidate_id=candidate_id,
        now=NOW,
    )

    assert report["status"] == "blocked"
    assert report["reason"] == f"candidate {candidate_id} does not belong to send {send_id}"
    assert _subject_for_send(db, send_id) == "Draft subject"


def test_dry_run_reports_proposed_subject_without_update(db):
    send_id = _insert_pending_send(db)
    candidate_id = _insert_candidate(db, send_id, "Better subject", 9.0)

    report = apply_newsletter_subject_selection(
        db,
        send_id=send_id,
        candidate_id=candidate_id,
        dry_run=True,
        now=NOW,
    )

    assert report["status"] == "dry_run"
    assert report["applied"] is False
    assert report["proposed_subject"] == "Better subject"
    assert _subject_for_send(db, send_id) == "Draft subject"
    assert "Proposed subject: Better subject" in format_newsletter_subject_selection_text(
        report
    )


def test_invalid_candidate_id_returns_blocked_status(db):
    send_id = _insert_pending_send(db)
    _insert_candidate(db, send_id, "Only candidate", 7.0)

    report = apply_newsletter_subject_selection(
        db,
        send_id=send_id,
        candidate_id=9999,
        now=NOW,
    )

    assert report["status"] == "blocked"
    assert report["reason"] == f"candidate 9999 does not belong to send {send_id}"
    assert _subject_for_send(db, send_id) == "Draft subject"


def test_sent_newsletter_is_blocked_and_not_modified(db):
    send_id = db.insert_newsletter_send(
        issue_id="issue-sent",
        subject="Already delivered",
        content_ids=[],
        subscriber_count=100,
        status="sent",
    )
    _insert_candidate(db, send_id, "Too late", 9.0)

    report = apply_newsletter_subject_selection(db, send_id=send_id, best=True, now=NOW)

    assert report["status"] == "blocked"
    assert report["reason"] == "newsletter_already_sent"
    assert _subject_for_send(db, send_id) == "Already delivered"


def test_apply_updates_subject_marks_selected_and_preserves_candidates(db):
    send_id = _insert_pending_send(db)
    first_id = _insert_candidate(db, send_id, "Old candidate", 6.0)
    selected_id = _insert_candidate(db, send_id, "Applied subject", 9.0)

    report = apply_newsletter_subject_selection(db, send_id=send_id, best=True, now=NOW)

    assert report["status"] == "applied"
    assert report["applied"] is True
    assert _subject_for_send(db, send_id) == "Applied subject"
    candidates = list_candidates_for_send(db, send_id)
    assert {candidate["id"] for candidate in candidates} == {first_id, selected_id}
    assert [candidate["id"] for candidate in candidates if candidate["selected"]] == [
        selected_id
    ]


def test_cli_json_output_supports_best_and_dry_run(db, capsys):
    send_id = _insert_pending_send(db)
    _insert_candidate(db, send_id, "CLI subject", 9.0)

    with patch("select_newsletter_subject.script_context", return_value=_script_context(db)):
        result = main(
            [
                "--send-id",
                str(send_id),
                "--best",
                "--dry-run",
                "--format",
                "json",
            ]
        )

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "dry_run"
    assert payload["proposed_subject"] == "CLI subject"
    assert _subject_for_send(db, send_id) == "Draft subject"
