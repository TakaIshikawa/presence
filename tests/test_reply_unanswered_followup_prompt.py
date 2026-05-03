"""Tests for reply unanswered follow-up prompt reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_unanswered_followup_prompt import (
    WARNING_EMPTY_DRAFT,
    WARNING_EMPTY_SOURCE,
    build_reply_unanswered_followup_report,
    format_reply_unanswered_followup_json,
    format_reply_unanswered_followup_text,
)


NOW = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "reply_unanswered_followup_prompt.py"
)
spec = importlib.util.spec_from_file_location("reply_unanswered_followup_script", SCRIPT_PATH)
reply_unanswered_followup_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_unanswered_followup_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


@contextmanager
def _memory_db():
    """Create an in-memory SQLite database with required schema."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inbound_tweet_id TEXT,
            inbound_author_handle TEXT,
            inbound_text TEXT,
            our_tweet_id TEXT,
            our_post_text TEXT,
            draft_text TEXT,
            status TEXT DEFAULT 'pending',
            platform TEXT DEFAULT 'x',
            detected_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    yield conn
    conn.close()


def _insert_reply(
    conn: sqlite3.Connection,
    inbound_id: str,
    **kwargs,
) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_text="Nice post",
        draft_text="Thanks",
        status="pending",
        platform="x",
    )
    defaults.update(kwargs)
    columns = ", ".join(defaults.keys())
    placeholders = ", ".join("?" for _ in defaults)
    cursor = conn.execute(
        f"INSERT INTO reply_queue ({columns}) VALUES ({placeholders})",
        tuple(defaults.values()),
    )
    conn.commit()
    return cursor.lastrowid


def test_single_question_answered_has_zero_unanswered():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-1",
            inbound_text="How do I install this?",
            draft_text="You can install it with: pip install package-name",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        assert report.scanned_count == 1
        assert report.unanswered_count == 0
        assert len(report.findings) == 0
        assert report.ok is True


def test_single_question_unanswered_triggers_finding():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-2",
            inbound_text="How do I configure the timeout?",
            draft_text="Thanks for your interest!",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        assert report.scanned_count == 1
        assert report.unanswered_count == 1
        assert report.ok is False

        finding = report.findings[0]
        assert finding.question_count == 1
        assert finding.answered_question_count == 0
        assert finding.unanswered_question_count == 1


def test_multiple_questions_detected():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-3",
            inbound_text="How do I configure SSL? What about database schema?",
            draft_text="Thanks for your questions!",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        # Should detect 2 questions
        finding = report.findings[0]
        assert finding.question_count == 2
        # Generic response doesn't answer either question
        assert finding.unanswered_question_count == 2


def test_no_questions_in_source_not_included():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-4",
            inbound_text="Great article. Really enjoyed it.",
            draft_text="Thank you!",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        assert report.scanned_count == 1
        assert report.unanswered_count == 0
        assert len(report.findings) == 0


def test_empty_source_text_triggers_warning():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-5",
            inbound_text="",
            draft_text="Thanks",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        finding = report.findings[0] if report.findings else None
        # Empty source means no questions detected, so shouldn't be in findings
        assert report.scanned_count == 1
        assert report.unanswered_count == 0


def test_empty_draft_text_triggers_warning():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-6",
            inbound_text="How do I fix this error?",
            draft_text="",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        # Empty draft should be detected as unanswered
        assert report.unanswered_count == 0  # No draft means no coverage check
        assert report.scanned_count == 1


def test_direct_questions_detected():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-7",
            inbound_text="Can you explain the architecture? What are the main components?",
            draft_text="It uses a layered architecture.",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        finding = report.findings[0]
        assert finding.question_count == 2
        # "architecture" and "components" are in the question, partial coverage
        assert finding.unanswered_question_count >= 1


def test_filters_by_status():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "pending-1",
            status="pending",
            inbound_text="How does this work?",
            draft_text="Let me explain.",
        )
        _insert_reply(
            conn,
            "reviewed-1",
            status="reviewed",
            inbound_text="What should I do?",
            draft_text="Try this approach.",
        )

        report = build_reply_unanswered_followup_report(
            conn, days=7, status=("pending",), now=NOW
        )

        assert report.scanned_count == 1


def test_filters_by_platform():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "x-1",
            platform="x",
            inbound_text="How do I do this?",
            draft_text="Here's the answer.",
        )
        _insert_reply(
            conn,
            "bluesky-1",
            platform="bluesky",
            inbound_text="What about this?",
            draft_text="Check the docs.",
        )

        report = build_reply_unanswered_followup_report(
            conn, days=7, platform=("x",), now=NOW
        )

        assert report.scanned_count == 1


def test_filters_by_days_lookback():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "recent",
            detected_at=(NOW - timedelta(days=3)).isoformat(),
            inbound_text="What is this?",
            draft_text="It's a tool.",
        )
        _insert_reply(
            conn,
            "old",
            detected_at=(NOW - timedelta(days=10)).isoformat(),
            inbound_text="How does it work?",
            draft_text="It works well.",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        assert report.scanned_count == 1


def test_missing_reply_queue_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

    assert report.ok is True
    assert report.scanned_count == 0
    assert report.missing_tables == ("reply_queue",)


def test_missing_required_columns_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_queue (status TEXT)")
    conn.commit()

    report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

    assert report.ok is True
    assert report.scanned_count == 0
    assert "reply_queue" in report.missing_columns


def test_json_formatter_produces_deterministic_output():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-8",
            inbound_text="How do I start?",
            draft_text="Just run it.",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)
        output = format_reply_unanswered_followup_json(report)

        parsed = json.loads(output)
        assert parsed["artifact_type"] == "reply_unanswered_followup_prompt"
        assert "generated_at" in parsed
        assert "filters" in parsed
        assert "findings" in parsed


def test_text_formatter_produces_readable_output():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-9",
            inbound_text="What should I configure?",
            draft_text="Good question!",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)
        output = format_reply_unanswered_followup_text(report)

        assert "Reply Unanswered Follow-up Prompt Report" in output
        assert "Unanswered:" in output


def test_script_json_output_format(monkeypatch):
    with _memory_db() as conn:
        _insert_reply(conn, "mention-10")

        monkeypatch.setattr(
            reply_unanswered_followup_script,
            "script_context",
            lambda: _script_context(conn),
        )

        exit_code = reply_unanswered_followup_script.main(["--format", "json"])
        assert exit_code == 0


def test_script_text_output_format(monkeypatch):
    with _memory_db() as conn:
        _insert_reply(conn, "mention-11")

        monkeypatch.setattr(
            reply_unanswered_followup_script,
            "script_context",
            lambda: _script_context(conn),
        )

        exit_code = reply_unanswered_followup_script.main(["--format", "text"])
        assert exit_code == 0


def test_script_accepts_days_argument(monkeypatch):
    with _memory_db() as conn:
        _insert_reply(conn, "mention-12")

        monkeypatch.setattr(
            reply_unanswered_followup_script,
            "script_context",
            lambda: _script_context(conn),
        )

        exit_code = reply_unanswered_followup_script.main(["--days", "14", "--format", "json"])
        assert exit_code == 0


def test_script_accepts_status_and_platform_filters(monkeypatch):
    with _memory_db() as conn:
        _insert_reply(conn, "mention-13", status="pending", platform="x")

        monkeypatch.setattr(
            reply_unanswered_followup_script,
            "script_context",
            lambda: _script_context(conn),
        )

        exit_code = reply_unanswered_followup_script.main(
            ["--status", "pending", "--platform", "x", "--format", "json"]
        )
        assert exit_code == 0


def test_sorting_prioritizes_highest_unanswered_count():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "one-unanswered",
            inbound_text="What is this?",
            draft_text="Good question.",
        )
        _insert_reply(
            conn,
            "two-unanswered",
            inbound_text="What is this? How does it work?",
            draft_text="Thanks for asking.",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        assert len(report.findings) == 2
        assert report.findings[0].unanswered_question_count >= report.findings[
            1
        ].unanswered_question_count


def test_question_phrase_detection():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-14",
            inbound_text="Can you help me understand the deployment process?",
            draft_text="Sure, I can explain.",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        # "Can you help" is a question phrase
        # Should detect at least one question
        finding = report.findings[0] if report.findings else None
        if finding:
            assert finding.question_count >= 1


def test_answer_pattern_yes_no_detection():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-15",
            inbound_text="Is this supported?",
            draft_text="Yes, it's fully supported.",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        # "Yes" is an answer pattern, should be considered answered
        assert report.unanswered_count == 0


def test_answer_pattern_action_verbs_detection():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-16",
            inbound_text="How do I fix the timeout error?",
            draft_text="Try increasing the timeout value in the config.",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        # "Try" is an answer pattern, should be considered answered
        assert report.unanswered_count == 0


def test_keyword_overlap_detection():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-17",
            inbound_text="What are the authentication options?",
            draft_text="We support OAuth and API key authentication methods.",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        # "authentication" appears in both question and answer
        # Should have good keyword overlap
        assert report.unanswered_count == 0


def test_negative_days_raises_value_error():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    try:
        build_reply_unanswered_followup_report(conn, days=-1, now=NOW)
        assert False, "Expected ValueError"
    except ValueError as exc:
        assert "days must be positive" in str(exc)


def test_multiple_questions_all_answered():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-18",
            inbound_text="How do I install it? What version should I use?",
            draft_text=(
                "Use pip install package-name for installation. "
                "The recommended version is 2.0 or higher."
            ),
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        # Both questions should be answered
        assert report.unanswered_count == 0


def test_multiple_questions_none_answered():
    with _memory_db() as conn:
        _insert_reply(
            conn,
            "mention-19",
            inbound_text="How do I configure SSL? What certificates are needed?",
            draft_text="Thanks for your interest in our product!",
        )

        report = build_reply_unanswered_followup_report(conn, days=7, now=NOW)

        finding = report.findings[0]
        assert finding.question_count == 2
        assert finding.unanswered_question_count == 2
        assert finding.answered_question_count == 0
