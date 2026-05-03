"""Tests for reply draft question coverage auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_question_coverage import (
    REASON_EVASIVE_GENERIC_REPLY,
    build_reply_question_coverage_audit,
    explicit_question_text,
    format_reply_question_coverage_json,
    format_reply_question_coverage_text,
    inspect_reply_question_coverage_row,
    missing_question_coverage_reason,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_question_coverage.py"
spec = importlib.util.spec_from_file_location("reply_question_coverage_script", SCRIPT_PATH)
reply_question_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_question_coverage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, inbound_text: str, draft_text: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text=inbound_text,
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text=draft_text,
        platform="x",
        status="pending",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", (detected_at, reply_id))
    db.conn.commit()


def test_direct_answers_are_counted_as_covered():
    row = {
        "id": 1,
        "inbound_tweet_id": "mention-1",
        "inbound_text": "Should I use retries for transient failures?",
        "draft_text": "Yes, use retries with exponential backoff for transient failures.",
    }

    is_question, finding = inspect_reply_question_coverage_row(row)

    assert is_question is True
    assert finding is None
    assert missing_question_coverage_reason(row) is None


def test_evasive_generic_replies_are_flagged_with_required_fields():
    row = {
        "id": 2,
        "inbound_tweet_id": "mention-2",
        "inbound_author_handle": "alice",
        "platform": "x",
        "inbound_text": "How should I debug worker timeouts?",
        "draft_text": "Good question, thanks for asking.",
    }

    is_question, finding = inspect_reply_question_coverage_row(row)

    assert is_question is True
    assert finding is not None
    assert finding.mention_id == "mention-2"
    assert finding.draft_id == 2
    assert finding.question_text == "How should I debug worker timeouts?"
    assert finding.reason == REASON_EVASIVE_GENERIC_REPLY


def test_no_question_mentions_are_not_flagged_or_counted():
    row = {
        "id": 3,
        "inbound_tweet_id": "mention-3",
        "inbound_text": "Thanks, this helped a lot.",
        "draft_text": "Glad it helped.",
    }

    is_question, finding = inspect_reply_question_coverage_row(row)
    report = build_reply_question_coverage_audit(reply_records=[row], now=NOW)

    assert explicit_question_text(row["inbound_text"]) == ""
    assert is_question is False
    assert finding is None
    assert report.summary == {"total_questions": 0, "covered_count": 0, "uncovered_count": 0}
    assert report.findings == ()


def test_uncertainty_replies_are_counted_as_covered():
    row = {
        "id": 4,
        "inbound_tweet_id": "mention-4",
        "inbound_text": "What version causes this startup crash?",
        "draft_text": "I'm not sure from the details here; I would need more context.",
    }

    is_question, finding = inspect_reply_question_coverage_row(row)

    assert is_question is True
    assert finding is None
    assert missing_question_coverage_reason(row) is None


def test_keyword_overlap_covers_explicit_questions_without_direct_phrase():
    row = {
        "id": 5,
        "inbound_tweet_id": "mention-5",
        "inbound_text": "Could deployment retries cause duplicate jobs",
        "draft_text": "Deployment retries can duplicate jobs unless the worker is idempotent.",
    }

    report = build_reply_question_coverage_audit(reply_records=[row], now=NOW)

    assert report.summary == {"total_questions": 1, "covered_count": 1, "uncovered_count": 0}
    assert report.findings == ()


def test_report_summary_and_formatters_are_stable():
    report = build_reply_question_coverage_audit(
        reply_records=[
            {
                "id": 1,
                "inbound_tweet_id": "covered",
                "inbound_text": "Can you explain retries?",
                "draft_text": "Use exponential backoff.",
            },
            {
                "id": 2,
                "inbound_tweet_id": "uncovered",
                "inbound_text": "Why did the deploy fail?",
                "draft_text": "Thanks for sharing this.",
            },
            {
                "id": 3,
                "inbound_tweet_id": "non-question",
                "inbound_text": "Nice writeup.",
                "draft_text": "Thanks.",
            },
        ],
        now=NOW,
    )
    payload = json.loads(format_reply_question_coverage_json(report))
    text = format_reply_question_coverage_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "reply_question_coverage"
    assert payload["summary"] == {
        "covered_count": 1,
        "total_questions": 2,
        "uncovered_count": 1,
    }
    assert payload["findings"][0]["mention_id"] == "uncovered"
    assert payload["findings"][0]["draft_id"] == 2
    assert payload["findings"][0]["question_text"] == "Why did the deploy fail?"
    assert "Reply Question Coverage Audit" in text
    assert "total_questions=2 covered=1 uncovered=1" in text


def test_sqlite_days_and_limit_filters_are_applied(db):
    old = _insert_reply(db, "old", "How do I debug old failures?", "Thanks for sharing.")
    newer_uncovered = _insert_reply(db, "new", "How do I debug new failures?", "Thanks for sharing.")
    newest_covered = _insert_reply(db, "covered", "How do I debug covered failures?", "Use logs.")
    _set_detected_at(db, old, "2026-04-01 10:00:00")
    _set_detected_at(db, newer_uncovered, "2026-05-02 10:00:00")
    _set_detected_at(db, newest_covered, "2026-05-03 10:00:00")

    report = build_reply_question_coverage_audit(db, days=7, limit=2, now=NOW)

    assert report.filters == {"days": 7, "limit": 2}
    assert report.summary == {"total_questions": 2, "covered_count": 1, "uncovered_count": 1}
    assert [finding.draft_id for finding in report.findings] == [newer_uncovered]


def test_cli_emits_json_and_respects_days_and_limit(db, monkeypatch, capsys):
    _insert_reply(db, "old", "How do I debug old failures?", "Thanks for sharing.")
    recent = _insert_reply(db, "recent", "How do I debug recent failures?", "Thanks for sharing.")
    _set_detected_at(db, recent, "2026-05-03 10:00:00")
    monkeypatch.setattr(
        reply_question_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_question_coverage_script,
        "build_reply_question_coverage_audit",
        lambda db, **kwargs: build_reply_question_coverage_audit(db, now=NOW, **kwargs),
    )

    exit_code = reply_question_coverage_script.main(["--days", "1", "--limit", "1", "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"] == {"days": 1, "limit": 1}
    assert payload["summary"] == {
        "covered_count": 0,
        "total_questions": 1,
        "uncovered_count": 1,
    }
    assert payload["findings"][0]["draft_id"] == recent


def test_cli_validation_errors_return_argparse_status(capsys):
    exit_code = reply_question_coverage_script.main(["--days", "0"])

    assert exit_code == 2
    assert "value must be positive" in capsys.readouterr().err
