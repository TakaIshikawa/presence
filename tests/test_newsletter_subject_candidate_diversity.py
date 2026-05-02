"""Tests for newsletter subject candidate diversity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.newsletter_subject_candidate_diversity import (
    DUPLICATE_NORMALIZED_SUBJECT,
    REPEATED_OPENING_TOKEN,
    SINGLE_SOURCE_POOL,
    build_newsletter_subject_candidate_diversity_report,
    format_newsletter_subject_candidate_diversity_json,
    format_newsletter_subject_candidate_diversity_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "newsletter_subject_candidate_diversity.py"
)
spec = importlib.util.spec_from_file_location(
    "newsletter_subject_candidate_diversity_script",
    SCRIPT_PATH,
)
newsletter_subject_candidate_diversity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_subject_candidate_diversity_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _send(db, *, issue_id: str = "issue-1", sent_at: datetime | None = None) -> int:
    cursor = db.conn.execute(
        """INSERT INTO newsletter_sends
           (issue_id, subject, source_content_ids, subscriber_count, status, sent_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            issue_id,
            "Draft subject",
            "[]",
            100,
            "draft",
            (sent_at or NOW).isoformat(),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _candidate(
    db,
    send_id: int,
    subject: str,
    *,
    issue_id: str = "issue-1",
    source: str = "heuristic",
    rank: int = 1,
    created_at: datetime | None = None,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO newsletter_subject_candidates
           (newsletter_send_id, issue_id, subject, score, rationale, source, rank, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            send_id,
            issue_id,
            subject,
            8.0,
            "ok",
            source,
            rank,
            (created_at or NOW).isoformat(),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_duplicate_normalization_is_grouped_by_newsletter_send_id(db):
    send_id = _send(db)
    first = _candidate(db, send_id, "Ship Faster!")
    second = _candidate(db, send_id, "ship faster", rank=2)
    _candidate(db, send_id, "A calmer release checklist", source="llm", rank=3)

    report = build_newsletter_subject_candidate_diversity_report(db, now=NOW)

    assert report.to_dict()["artifact_type"] == "newsletter_subject_candidate_diversity"
    finding = report.findings[0]
    assert finding.newsletter_send_id == send_id
    assert DUPLICATE_NORMALIZED_SUBJECT in finding.issue_codes
    assert finding.duplicate_groups[0].normalized_subject == "ship faster"
    assert finding.duplicate_groups[0].candidate_ids == (first, second)
    assert finding.candidate_count == 3


def test_opening_token_concentration_is_detected_independently(db):
    send_id = _send(db)
    _candidate(db, send_id, "Build better release notes", source="heuristic")
    _candidate(db, send_id, "Build calmer review loops", source="llm", rank=2)
    _candidate(db, send_id, "Practice fewer handoffs", source="archive", rank=3)

    report = build_newsletter_subject_candidate_diversity_report(db, now=NOW)

    finding = report.findings[0]
    assert finding.issue_codes == (REPEATED_OPENING_TOKEN,)
    assert finding.dominant_opening is not None
    assert finding.dominant_opening.token == "build"
    assert finding.dominant_opening.count == 2
    assert finding.dominant_opening.share == pytest.approx(0.6667)
    assert finding.duplicate_groups == ()


def test_single_source_candidate_pool_is_detected_independently(db):
    send_id = _send(db)
    _candidate(db, send_id, "Small fixes with useful edges", source="heuristic")
    _candidate(db, send_id, "Release notes that carry context", source="heuristic", rank=2)
    _candidate(db, send_id, "Sharper defaults for review", source="heuristic", rank=3)

    report = build_newsletter_subject_candidate_diversity_report(db, now=NOW)

    finding = report.findings[0]
    assert finding.issue_codes == (SINGLE_SOURCE_POOL,)
    assert finding.source_counts == {"heuristic": 3}
    assert "another source" in finding.recommended_action


def test_filters_text_and_json_output_are_stable(db, monkeypatch, capsys):
    old_send = _send(db, issue_id="old", sent_at=NOW - timedelta(days=40))
    target_send = _send(db, issue_id="target")
    other_send = _send(db, issue_id="other")
    _candidate(db, old_send, "Old repeated", created_at=NOW - timedelta(days=40))
    _candidate(db, old_send, "Old repeated!", rank=2, created_at=NOW - timedelta(days=40))
    _candidate(db, target_send, "Focus the launch", source="llm")
    _candidate(db, target_send, "Focus the launch!", source="archive", rank=2)
    _candidate(db, other_send, "Other subject", source="heuristic")
    _candidate(db, other_send, "Other subject!", source="llm", rank=2)

    report = build_newsletter_subject_candidate_diversity_report(
        db,
        days=14,
        newsletter_send_id=target_send,
        now=NOW,
    )
    payload = json.loads(format_newsletter_subject_candidate_diversity_json(report))
    text = format_newsletter_subject_candidate_diversity_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["filters"]["newsletter_send_id"] == target_send
    assert payload["totals"] == {
        "candidate_count": 2,
        "issue_send_count": 1,
        "send_count": 1,
    }
    assert payload["findings"][0]["newsletter_send_id"] == target_send
    assert "Newsletter Subject Candidate Diversity Report" in text
    assert f"send={target_send}" in text
    assert f"send={other_send}" not in text

    monkeypatch.setattr(
        newsletter_subject_candidate_diversity_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        newsletter_subject_candidate_diversity_script,
        "build_newsletter_subject_candidate_diversity_report",
        lambda db, **kwargs: build_newsletter_subject_candidate_diversity_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert newsletter_subject_candidate_diversity_script.main(
        ["--newsletter-send-id", str(target_send), "--format", "json"]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["findings"][0]["newsletter_send_id"] == target_send

    assert newsletter_subject_candidate_diversity_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_empty_schema_behavior_and_invalid_args():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_subject_candidate_diversity_report(conn, now=NOW)

    assert report.findings == ()
    assert report.missing_tables == (
        "newsletter_subject_candidates",
        "newsletter_sends",
    )
    assert report.totals == {
        "candidate_count": 0,
        "issue_send_count": 0,
        "send_count": 0,
    }
    assert "Missing tables: newsletter_subject_candidates, newsletter_sends" in (
        format_newsletter_subject_candidate_diversity_text(report)
    )

    with pytest.raises(ValueError, match="days must be positive"):
        build_newsletter_subject_candidate_diversity_report(conn, days=0, now=NOW)
    with pytest.raises(ValueError, match="newsletter_send_id must be positive"):
        build_newsletter_subject_candidate_diversity_report(
            conn,
            newsletter_send_id=0,
            now=NOW,
        )
    conn.close()
