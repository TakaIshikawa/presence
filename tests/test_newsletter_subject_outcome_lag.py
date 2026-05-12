"""Tests for newsletter subject outcome lag reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from newsletter_subject_outcome_lag import main  # noqa: E402
from evaluation.newsletter_subject_outcome_lag import (  # noqa: E402
    build_newsletter_subject_outcome_lag_report,
    format_newsletter_subject_outcome_lag_text,
)


NOW = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)


def _send(db, hours_ago: int, status: str = "sent") -> int:
    cursor = db.conn.execute(
        """INSERT INTO newsletter_sends (issue_id, subject, status, sent_at)
           VALUES (?, ?, ?, ?)""",
        ("issue-1", "Subject", status, (NOW - timedelta(hours=hours_ago)).isoformat()),
    )
    db.conn.commit()
    return cursor.lastrowid


def _candidate(db, send_id: int, *, selected: int = 1, subject: str = "Chosen") -> int:
    cursor = db.conn.execute(
        """INSERT INTO newsletter_subject_candidates
           (newsletter_send_id, issue_id, subject, score, source, rank, selected)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (send_id, "issue-1", subject, 0.82, "llm", 1, selected),
    )
    db.conn.commit()
    return cursor.lastrowid


def _engagement(db, send_id: int, fetched_hours_ago: int) -> None:
    db.conn.execute(
        """INSERT INTO newsletter_engagement
           (newsletter_send_id, issue_id, opens, clicks, fetched_at)
           VALUES (?, ?, 10, 2, ?)""",
        (send_id, "issue-1", (NOW - timedelta(hours=fetched_hours_ago)).isoformat()),
    )
    db.conn.commit()


def test_flags_missing_and_stale_engagement_for_old_sent_newsletters(db):
    missing_send = _send(db, 50)
    stale_send = _send(db, 60)
    fresh_send = _send(db, 70)
    young_send = _send(db, 4)
    _candidate(db, missing_send, subject="Missing")
    _candidate(db, stale_send, subject="Stale")
    _candidate(db, fresh_send, subject="Fresh")
    _candidate(db, young_send, subject="Young")
    _engagement(db, stale_send, fetched_hours_ago=49)
    _engagement(db, fresh_send, fetched_hours_ago=2)

    report = build_newsletter_subject_outcome_lag_report(
        db,
        min_age_hours=24,
        stale_after_hours=24,
        now=NOW,
    )

    statuses = {item["subject"]: item["lag_status"] for item in report["items"]}
    assert statuses == {"Stale": "stale", "Missing": "absent"}
    stale = next(item for item in report["items"] if item["subject"] == "Stale")
    assert stale["newsletter_send_id"] == stale_send
    assert stale["candidate_score"] == 0.82
    assert stale["source"] == "llm"
    assert stale["latest_metrics_at"] == (NOW - timedelta(hours=49)).isoformat()
    assert "Lagged subjects:" in format_newsletter_subject_outcome_lag_text(report)


def test_excludes_unsent_and_unselected_candidates(db):
    draft_send = _send(db, 50, status="draft")
    sent = _send(db, 50)
    _candidate(db, draft_send, subject="Draft")
    _candidate(db, sent, selected=0, subject="Unselected")

    report = build_newsletter_subject_outcome_lag_report(db, now=NOW)

    assert report["items"] == []


def test_cli_supports_json_output(db, capsys):
    send_id = _send(db, 50)
    candidate_id = _candidate(db, send_id, subject="CLI")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("newsletter_subject_outcome_lag.script_context", fake_script_context):
        result = main(["--min-age-hours", "24", "--stale-after-hours", "24", "--limit", "5", "--format", "json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["items"][0]["candidate_id"] == candidate_id
    assert payload["items"][0]["lag_status"] == "absent"
