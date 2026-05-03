"""Tests for reply unanswered follow-up prompt report."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_unanswered_followup_prompt import (
    build_reply_unanswered_followup_prompt_report,
    extract_questions,
    format_reply_unanswered_followup_prompt_json,
    format_reply_unanswered_followup_prompt_text,
    inspect_reply_unanswered_followup_prompt,
    is_question_answered,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "reply_unanswered_followup_prompt.py"
)
spec = importlib.util.spec_from_file_location("reply_unanswered_followup_prompt_script", SCRIPT_PATH)
reply_unanswered_followup_prompt_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_unanswered_followup_prompt_script)


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


def test_extract_questions_finds_explicit_question_marks():
    text = "How do retries work? What about exponential backoff?"
    questions = extract_questions(text)

    assert len(questions) == 2
    assert "How do retries work" in questions[0]
    assert "What about exponential backoff" in questions[1]


def test_extract_questions_finds_question_openers_without_marks():
    text = "Could you explain how this works"
    questions = extract_questions(text)

    assert len(questions) == 1
    assert "Could you explain how this works" in questions[0]


def test_extract_questions_returns_empty_for_statements():
    text = "Thanks for the help. This is great."
    questions = extract_questions(text)

    assert questions == []


def test_is_question_answered_with_direct_answer_signal():
    question = "Should I use retries for transient failures?"
    draft = "Yes, use retries with exponential backoff."

    assert is_question_answered(question, draft) is True


def test_is_question_answered_with_semantic_overlap():
    question = "How should I handle deployment retries?"
    draft = "Deployment retries should use exponential backoff to avoid overwhelming the system."

    assert is_question_answered(question, draft) is True


def test_is_question_answered_returns_false_for_empty_draft():
    question = "How do I debug worker timeouts?"
    draft = ""

    assert is_question_answered(question, draft) is False


def test_is_question_answered_returns_false_for_unrelated_draft():
    question = "How do I configure database retries?"
    draft = "Thanks for asking."

    assert is_question_answered(question, draft) is False


def test_inspect_single_unanswered_question():
    row = {
        "id": 1,
        "inbound_tweet_id": "mention-1",
        "inbound_author_handle": "alice",
        "inbound_text": "How do retries work?",
        "draft_text": "Thanks for asking.",
        "detected_at": "2026-05-03T10:00:00Z",
    }

    finding = inspect_reply_unanswered_followup_prompt(row)

    assert finding is not None
    assert finding.mention_id == "mention-1"
    assert finding.draft_id == 1
    assert finding.author_handle == "alice"
    assert finding.question_count == 1
    assert finding.answered_question_count == 0
    assert finding.unanswered_question_count == 1
    assert finding.drafted_at == "2026-05-03T10:00:00Z"
    assert finding.warnings == ()


def test_inspect_multiple_questions_partially_answered():
    row = {
        "id": 2,
        "inbound_tweet_id": "mention-2",
        "inbound_author_handle": "bob",
        "inbound_text": "How do retries work? What about exponential backoff?",
        "draft_text": "Retries help handle transient failures by repeating failed operations.",
        "detected_at": "2026-05-03T11:00:00Z",
    }

    finding = inspect_reply_unanswered_followup_prompt(row)

    assert finding is not None
    assert finding.question_count == 2
    # Both questions share "retries" but second question lacks context in draft
    assert finding.unanswered_question_count >= 1  # At least one unanswered


def test_inspect_all_questions_answered():
    row = {
        "id": 3,
        "inbound_tweet_id": "mention-3",
        "inbound_author_handle": "charlie",
        "inbound_text": "Should I use retries?",
        "draft_text": "Yes, use retries with exponential backoff for transient failures.",
        "detected_at": "2026-05-03T12:00:00Z",
    }

    finding = inspect_reply_unanswered_followup_prompt(row)

    assert finding is not None
    assert finding.question_count == 1
    assert finding.answered_question_count == 1
    assert finding.unanswered_question_count == 0


def test_inspect_no_questions_returns_none():
    row = {
        "id": 4,
        "inbound_tweet_id": "mention-4",
        "inbound_text": "Thanks, this helped a lot.",
        "draft_text": "Glad it helped.",
    }

    finding = inspect_reply_unanswered_followup_prompt(row)

    assert finding is None


def test_inspect_missing_source_text_warning():
    row = {
        "id": 5,
        "inbound_tweet_id": "mention-5",
        "inbound_text": "",
        "draft_text": "Thanks.",
    }

    finding = inspect_reply_unanswered_followup_prompt(row)

    assert finding is None


def test_inspect_missing_draft_text_warning():
    row = {
        "id": 6,
        "inbound_tweet_id": "mention-6",
        "inbound_text": "How do I debug this?",
        "draft_text": "",
        "detected_at": "2026-05-03T13:00:00Z",
    }

    finding = inspect_reply_unanswered_followup_prompt(row)

    assert finding is not None
    assert finding.question_count == 1
    assert finding.answered_question_count == 0
    assert finding.unanswered_question_count == 1
    assert "missing_draft_text" in finding.warnings


def test_report_only_includes_unanswered_questions(db):
    _insert_reply(
        db,
        "mention-1",
        "How do retries work?",
        "Retries help handle transient failures.",
    )
    _insert_reply(
        db,
        "mention-2",
        "Should I use exponential backoff?",
        "Yes, use exponential backoff.",
    )
    _insert_reply(
        db,
        "mention-3",
        "What about circuit breakers?",
        "Thanks for asking.",
    )

    report = build_reply_unanswered_followup_prompt_report(db, days=7, limit=100, now=NOW)

    assert report.summary["total_drafts_with_questions"] == 3
    # mention-1: retries/work answered via overlap, mention-2: answered via "yes", mention-3: unanswered
    assert report.summary["drafts_with_unanswered_questions"] >= 1
    assert len(report.findings) >= 1
    # Find mention-3 in findings
    mention_3_findings = [f for f in report.findings if f.mention_id == "mention-3"]
    assert len(mention_3_findings) == 1


def test_report_deterministic_sorting(db):
    id1 = _insert_reply(db, "mention-1", "How do retries work?", "Thanks.")
    id2 = _insert_reply(db, "mention-2", "What about backoff?", "Good question.")
    id3 = _insert_reply(db, "mention-3", "Should I retry?", "Interesting.")

    _set_detected_at(db, id1, "2026-05-03T10:00:00Z")
    _set_detected_at(db, id2, "2026-05-03T09:00:00Z")
    _set_detected_at(db, id3, "2026-05-03T11:00:00Z")

    report = build_reply_unanswered_followup_prompt_report(db, days=7, limit=100, now=NOW)

    assert len(report.findings) == 3
    # Should be sorted by detected_at ASC for deterministic output
    assert report.findings[0].drafted_at == "2026-05-03T09:00:00Z"
    assert report.findings[1].drafted_at == "2026-05-03T10:00:00Z"
    assert report.findings[2].drafted_at == "2026-05-03T11:00:00Z"


def test_report_json_is_deterministic(db):
    _insert_reply(db, "mention-1", "How do retries work?", "Thanks.")

    report = build_reply_unanswered_followup_prompt_report(db, days=7, limit=100, now=NOW)
    json_output = format_reply_unanswered_followup_prompt_json(report)
    parsed = json.loads(json_output)

    assert parsed["artifact_type"] == "reply_unanswered_followup_prompt"
    assert "generated_at" in parsed
    assert "filters" in parsed
    assert "summary" in parsed
    assert "findings" in parsed
    assert isinstance(parsed["findings"], list)


def test_report_text_format(db):
    _insert_reply(db, "mention-1", "How do retries work?", "Thanks.")

    report = build_reply_unanswered_followup_prompt_report(db, days=7, limit=100, now=NOW)
    text_output = format_reply_unanswered_followup_prompt_text(report)

    assert "Reply Unanswered Follow-up Prompt Report" in text_output
    assert "Generated:" in text_output
    assert "Filters:" in text_output
    assert "Summary:" in text_output
    assert "Findings:" in text_output


def test_report_handles_missing_table(db):
    db.conn.execute("DROP TABLE IF EXISTS reply_queue")
    db.conn.commit()

    report = build_reply_unanswered_followup_prompt_report(db, days=7, limit=100, now=NOW)

    assert report.missing_tables == ("reply_queue",)
    assert report.summary["total_drafts_with_questions"] == 0
    assert len(report.findings) == 0


def test_report_handles_missing_columns(db):
    # Save existing data
    rows = db.conn.execute("SELECT * FROM reply_queue").fetchall()

    # Recreate table without required columns
    db.conn.execute("DROP TABLE IF EXISTS reply_queue")
    db.conn.execute(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inbound_tweet_id TEXT
        )
        """
    )
    db.conn.commit()

    report = build_reply_unanswered_followup_prompt_report(db, days=7, limit=100, now=NOW)

    assert report.missing_columns is not None
    assert "reply_queue" in report.missing_columns
    assert "inbound_text" in report.missing_columns["reply_queue"]
    assert "draft_text" in report.missing_columns["reply_queue"]
    assert report.summary["total_drafts_with_questions"] == 0


def test_script_main_with_json_format(db, monkeypatch, capsys):
    _insert_reply(db, "mention-1", "How do retries work?", "Thanks.")

    monkeypatch.setattr(
        reply_unanswered_followup_prompt_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = reply_unanswered_followup_prompt_script.main(["--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    parsed = json.loads(captured.out)
    assert parsed["artifact_type"] == "reply_unanswered_followup_prompt"


def test_script_main_with_text_format(db, monkeypatch, capsys):
    _insert_reply(db, "mention-1", "How do retries work?", "Thanks.")

    monkeypatch.setattr(
        reply_unanswered_followup_prompt_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = reply_unanswered_followup_prompt_script.main(["--format", "text"])
    captured = capsys.readouterr()

    assert exit_code == 0
    assert "Reply Unanswered Follow-up Prompt Report" in captured.out


def test_script_main_with_custom_days(db, monkeypatch, capsys):
    _insert_reply(db, "mention-1", "How do retries work?", "Thanks.")

    monkeypatch.setattr(
        reply_unanswered_followup_prompt_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = reply_unanswered_followup_prompt_script.main(["--days", "14", "--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    parsed = json.loads(captured.out)
    assert parsed["filters"]["days"] == 14


def test_script_main_with_custom_limit(db, monkeypatch, capsys):
    _insert_reply(db, "mention-1", "How do retries work?", "Thanks.")

    monkeypatch.setattr(
        reply_unanswered_followup_prompt_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = reply_unanswered_followup_prompt_script.main(["--limit", "50", "--format", "json"])
    captured = capsys.readouterr()

    assert exit_code == 0
    parsed = json.loads(captured.out)
    assert parsed["filters"]["limit"] == 50
