"""Tests for reply answer coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_answer_coverage import (
    REASON_EMPTY_DRAFT,
    REASON_EVASIVE_DRAFT,
    REASON_MISSING_ANSWER_SIGNAL,
    build_reply_answer_coverage_report,
    format_reply_answer_coverage_json,
    format_reply_answer_coverage_text,
    is_question_like,
    missing_answer_reason,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_answer_coverage.py"
spec = importlib.util.spec_from_file_location("reply_answer_coverage_script", SCRIPT_PATH)
reply_answer_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_answer_coverage_script)


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
        intent="other",
        priority="normal",
        status="pending",
        platform="x",
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_question_detection_uses_marks_openers_phrases_and_intent():
    assert is_question_like({"inbound_text": "Can you explain the retry policy"})
    assert is_question_like({"inbound_text": "Would love your take on this"})
    assert is_question_like({"inbound_text": "This crashed again", "intent": "bug_report"})
    assert is_question_like({"inbound_text": "Any advice?"})
    assert not is_question_like({"inbound_text": "Thanks, this helped a lot"})


def test_draft_coverage_flags_empty_evasive_and_missing_answer_signal():
    question = {"inbound_text": "How should I handle retry timeouts?"}

    assert missing_answer_reason({**question, "draft_text": ""}) == REASON_EMPTY_DRAFT
    assert (
        missing_answer_reason(
            {**question, "draft_text": "Good question. It depends, I would need more context."}
        )
        == REASON_EVASIVE_DRAFT
    )
    assert (
        missing_answer_reason({**question, "draft_text": "Thanks for sharing the detail."})
        == REASON_MISSING_ANSWER_SIGNAL
    )
    assert (
        missing_answer_reason(
            {**question, "draft_text": "Use exponential backoff and check the timeout budget."}
        )
        is None
    )


def test_report_flags_question_drafts_and_counts_non_questions_without_failures(db):
    empty_id = _insert_reply(
        db,
        "empty",
        "How should I debug the worker timeout?",
        "",
        priority="high",
        intent="question",
    )
    _insert_reply(
        db,
        "answered",
        "Why does the queue retry?",
        "Because transient failures are retried before surfacing an error.",
        intent="question",
    )
    _insert_reply(
        db,
        "thanks",
        "Thanks for the writeup.",
        "",
        intent="praise",
    )

    report = build_reply_answer_coverage_report(db, now=NOW)

    assert report["counts"] == {
        "rows_scanned": 3,
        "question_replies": 2,
        "non_question_replies": 1,
        "unresolved_questions": 1,
    }
    assert report["items"][0]["reply_queue_id"] == empty_id
    assert report["items"][0]["inbound_id"] == "empty"
    assert report["items"][0]["author_handle"] == "alice"
    assert report["items"][0]["priority"] == "high"
    assert report["items"][0]["missing_answer_reason"] == REASON_EMPTY_DRAFT


def test_platform_status_and_limit_filters_are_applied_before_scoring(db):
    x_pending = _insert_reply(
        db,
        "x-pending",
        "How do I tune retries?",
        "Not sure.",
        platform="x",
        status="pending",
    )
    bluesky_pending = _insert_reply(
        db,
        "bsky-pending",
        "How do I tune retries?",
        "Not sure.",
        platform="bluesky",
        status="pending",
    )
    _insert_reply(
        db,
        "x-reviewed",
        "How do I tune retries?",
        "Not sure.",
        platform="x",
        status="reviewed",
    )
    _set_detected_at(db, x_pending, "2026-05-01 01:00:00")
    _set_detected_at(db, bluesky_pending, "2026-05-01 02:00:00")

    x_report = build_reply_answer_coverage_report(
        db,
        platform="x",
        status="pending",
        limit=5,
        now=NOW,
    )
    all_limited = build_reply_answer_coverage_report(
        db,
        status=None,
        limit=1,
        now=NOW,
    )

    assert [item["inbound_id"] for item in x_report["items"]] == ["x-pending"]
    assert x_report["counts"]["rows_scanned"] == 1
    assert all_limited["counts"]["rows_scanned"] == 1
    assert all_limited["items"][0]["reply_queue_id"] == x_pending


def test_json_and_text_formatting_are_stable(db):
    reply_id = _insert_reply(
        db,
        "fmt",
        "Could you explain why deploys fail?",
        "Thanks for flagging this.",
        intent="question",
    )

    report = build_reply_answer_coverage_report(db, now=NOW)
    payload = json.loads(format_reply_answer_coverage_json(report))
    text = format_reply_answer_coverage_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "reply_answer_coverage"
    assert payload["items"][0]["reply_queue_id"] == reply_id
    assert "Reply Answer Coverage Report" in text
    assert f"reply={reply_id}" in text
    assert f"reason={REASON_MISSING_ANSWER_SIGNAL}" in text


def test_cli_supports_filters_and_json_output(db, monkeypatch, capsys):
    _insert_reply(
        db,
        "cli-x",
        "How should I handle retries?",
        "Not sure.",
        platform="x",
    )
    _insert_reply(
        db,
        "cli-bsky",
        "How should I handle retries?",
        "Not sure.",
        platform="bluesky",
    )
    monkeypatch.setattr(
        reply_answer_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_answer_coverage_script,
        "build_reply_answer_coverage_report",
        lambda db, **kwargs: build_reply_answer_coverage_report(db, now=NOW, **kwargs),
    )

    exit_code = reply_answer_coverage_script.main(
        ["--platform", "bluesky", "--status", "pending", "--limit", "10", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["platform"] == "bluesky"
    assert payload["filters"]["status"] == "pending"
    assert [item["inbound_id"] for item in payload["items"]] == ["cli-bsky"]


def test_cli_text_output_uses_script_context(db, monkeypatch, capsys):
    reply_id = _insert_reply(
        db,
        "cli-text",
        "What should I check first?",
        "",
        intent="question",
    )
    monkeypatch.setattr(
        reply_answer_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = reply_answer_coverage_script.main(["--format", "text"])
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "Reply Answer Coverage Report" in output
    assert f"reply={reply_id}" in output
