"""Tests for reply draft platform-fit linting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_platform_fit import (
    RULE_CHARACTER_BUDGET,
    RULE_DUPLICATE_GREETING,
    RULE_EXCESSIVE_HEDGING,
    RULE_MISSING_DIRECT_ANSWER,
    RULE_TOO_MANY_LINKS,
    RULE_UNSUPPORTED_THREAD_FORMATTING,
    build_reply_platform_fit_report,
    format_reply_platform_fit_json,
    format_reply_platform_fit_text,
    lint_reply_platform_fit,
    lint_reply_platform_fit_row,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "lint_reply_platform_fit.py"
spec = importlib.util.spec_from_file_location("lint_reply_platform_fit_script", SCRIPT_PATH)
lint_reply_platform_fit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(lint_reply_platform_fit_script)

NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, tweet_id: str, draft_text: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Thanks for this.",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text=draft_text,
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (detected_at, reply_id),
    )
    db.conn.commit()


def test_character_budget_includes_measured_and_allowed_lengths():
    finding = lint_reply_platform_fit_row(
        {
            "id": 10,
            "inbound_tweet_id": "too-long",
            "platform": "x",
            "status": "pending",
            "inbound_text": "Thanks.",
            "draft_text": "a" * 281,
        }
    )

    assert finding is not None
    assert finding.severity == "error"
    assert finding.rule_ids == [RULE_CHARACTER_BUDGET]
    assert finding.measured_length == 281
    assert finding.allowed_length == 280
    assert "281 exceeds x budget 280" in finding.reasons[0]


def test_question_like_inbound_with_evasive_draft_is_flagged():
    finding = lint_reply_platform_fit_row(
        {
            "id": 11,
            "inbound_tweet_id": "question",
            "platform": "bluesky",
            "status": "pending",
            "inbound_text": "Could you explain why the worker times out?",
            "intent": "question",
            "draft_text": "Good question, it depends. I would need more context.",
        }
    )

    assert finding is not None
    assert finding.severity == "error"
    assert RULE_MISSING_DIRECT_ANSWER in finding.rule_ids
    assert finding.suggested_action == "rewrite the draft to answer the inbound question directly"


def test_warn_rules_group_reasons_and_action_for_one_reply():
    finding = lint_reply_platform_fit_row(
        {
            "id": 12,
            "inbound_tweet_id": "warns",
            "platform": "x",
            "status": "pending",
            "inbound_text": "Thanks.",
            "draft_text": (
                "Hi hi thanks for the note.\n"
                "1/ Maybe this might probably help: https://a.test https://b.test"
            ),
        }
    )

    assert finding is not None
    assert finding.severity == "warn"
    assert finding.rule_ids == [
        RULE_EXCESSIVE_HEDGING,
        RULE_DUPLICATE_GREETING,
        RULE_TOO_MANY_LINKS,
        RULE_UNSUPPORTED_THREAD_FORMATTING,
    ]
    assert len(finding.reasons) == 4
    assert finding.suggested_action == "convert the draft into a single reply without thread markers"


def test_filters_by_platform_status_and_min_severity(db):
    error_id = _insert_reply(db, "error", "a" * 281, platform="x", status="pending")
    warn_id = _insert_reply(
        db,
        "warn",
        "Maybe this might probably help.",
        platform="bluesky",
        status="pending",
    )
    _insert_reply(db, "posted", "a" * 281, platform="x", status="posted")
    _set_detected_at(db, error_id, "2026-04-24 08:00:00")
    _set_detected_at(db, warn_id, "2026-04-24 08:05:00")

    errors = lint_reply_platform_fit(db, platform="x", status="pending", min_severity="error")
    warnings = lint_reply_platform_fit(db, status="pending", min_severity="warn")

    assert [finding.mention_id for finding in errors] == ["error"]
    assert {finding.mention_id for finding in warnings} == {"error", "warn"}


def test_report_formatters_are_deterministic(db):
    reply_id = _insert_reply(
        db,
        "ask-1",
        "Not sure, it depends.",
        platform="x",
        inbound_text="How should I handle retries?",
        intent="question",
    )

    report = build_reply_platform_fit_report(db, min_severity="warn", now=NOW)
    payload = json.loads(format_reply_platform_fit_json(report))
    text = format_reply_platform_fit_text(report)

    assert payload["artifact_type"] == "reply_platform_fit_lint"
    assert payload["findings"][0]["reply_queue_id"] == reply_id
    assert payload["counts"]["by_rule"] == {RULE_MISSING_DIRECT_ANSWER: 1}
    assert "Reply Platform Fit Lint" in text
    assert f"reply={reply_id}" in text
    assert "suggested_action:" in text


def test_partial_or_absent_reply_queue_schema_does_not_crash():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    assert lint_reply_platform_fit(conn) == []

    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, inbound_text TEXT)")
    conn.execute("INSERT INTO reply_queue (id, inbound_text) VALUES (1, 'Can you help?')")

    assert lint_reply_platform_fit(conn) == []


def test_cli_json_output_uses_db_path_and_filters(file_db, capsys):
    reply_id = _insert_reply(
        file_db,
        "cli-long",
        "a" * 301,
        platform="bluesky",
        status="pending",
    )

    exit_code = lint_reply_platform_fit_script.main(
        [
            "--db",
            str(file_db.db_path),
            "--platform",
            "bluesky",
            "--status",
            "pending",
            "--min-severity",
            "error",
            "--format",
            "json",
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"]["platform"] == "bluesky"
    assert payload["findings"][0]["reply_queue_id"] == reply_id
    assert payload["findings"][0]["rule_ids"] == [RULE_CHARACTER_BUDGET]


def test_cli_text_output_uses_script_context(db, monkeypatch, capsys):
    reply_id = _insert_reply(db, "ctx-long", "a" * 281)
    monkeypatch.setattr(
        lint_reply_platform_fit_script,
        "script_context",
        lambda: _script_context(db),
    )

    exit_code = lint_reply_platform_fit_script.main(["--min-severity", "error"])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "Reply Platform Fit Lint" in output
    assert f"reply={reply_id}" in output
