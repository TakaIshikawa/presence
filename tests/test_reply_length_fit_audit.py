"""Tests for reply draft length-fit auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_length_fit_audit import (
    ISSUE_EMPTY_DRAFT,
    ISSUE_MISSING_DRAFT,
    ISSUE_NEAR_LIMIT,
    ISSUE_OVER_LIMIT,
    build_reply_length_fit_audit,
    format_reply_length_fit_audit_json,
    format_reply_length_fit_audit_text,
    inspect_reply_length_fit_row,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_length_fit_audit.py"
spec = importlib.util.spec_from_file_location("reply_length_fit_audit_script", SCRIPT_PATH)
reply_length_fit_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_length_fit_audit_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, tweet_id: str, draft_text: str | None, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=tweet_id,
        inbound_author_handle="alice",
        inbound_author_id="user-a",
        inbound_text="Can you help with this?",
        our_tweet_id="our-1",
        our_content_id=None,
        our_post_text="Original post",
        draft_text=draft_text,
        platform="x",
        status="pending",
    )
    defaults.update(kwargs)
    reply_id = db.insert_reply_draft(**defaults)
    if draft_text is None:
        db.conn.execute("UPDATE reply_queue SET draft_text = NULL WHERE id = ?", (reply_id,))
        db.conn.commit()
    return reply_id


def test_x_and_bluesky_default_limits_are_applied():
    x_finding = inspect_reply_length_fit_row(
        {"id": 1, "platform": "x", "inbound_text": "How?", "draft_text": "a" * 281}
    )
    bluesky_finding = inspect_reply_length_fit_row(
        {"id": 2, "platform": "bluesky", "inbound_text": "How?", "draft_text": "b" * 300}
    )

    assert x_finding is not None
    assert x_finding.issue_type == ISSUE_OVER_LIMIT
    assert x_finding.measured_length == 281
    assert x_finding.allowed_length == 280
    assert bluesky_finding is not None
    assert bluesky_finding.issue_type == ISSUE_NEAR_LIMIT
    assert bluesky_finding.measured_length == 300
    assert bluesky_finding.allowed_length == 300


def test_custom_platform_limits_and_thresholds_are_configurable():
    report = build_reply_length_fit_audit(
        reply_records=[
            {
                "id": 1,
                "platform": "x",
                "inbound_tweet_id": "custom",
                "inbound_text": "How?",
                "draft_text": "a" * 260,
            }
        ],
        platform_limits={"x": 250, "bluesky": 500},
        near_threshold=0.8,
        now=NOW,
    )

    assert report.findings[0].issue_type == ISSUE_OVER_LIMIT
    assert report.findings[0].allowed_length == 250
    assert report.filters["platform_limits"]["bluesky"] == 500
    assert report.filters["near_threshold"] == 0.8


def test_missing_empty_over_and_near_findings_with_totals():
    rows = [
        {"id": 1, "inbound_tweet_id": "ok", "inbound_text": "Hi", "draft_text": "short"},
        {"id": 2, "inbound_tweet_id": "over", "inbound_text": "Hi", "draft_text": "a" * 281},
        {"id": 3, "inbound_tweet_id": "near", "inbound_text": "Hi", "draft_text": "b" * 252},
        {"id": 4, "inbound_tweet_id": "empty", "inbound_text": "Hi", "draft_text": " \n\t "},
        {"id": 5, "inbound_tweet_id": "missing", "inbound_text": "Hi", "draft_text": None},
        {"id": 6, "inbound_tweet_id": "no-inbound", "inbound_text": "", "draft_text": None},
    ]

    report = build_reply_length_fit_audit(rows, now=NOW)

    assert report.totals == {
        "checked_replies": 6,
        "over_limit_count": 1,
        "near_limit_count": 1,
        "missing_draft_count": 1,
        "empty_draft_count": 1,
    }
    assert [finding.issue_type for finding in report.findings] == [
        ISSUE_OVER_LIMIT,
        ISSUE_MISSING_DRAFT,
        ISSUE_EMPTY_DRAFT,
        ISSUE_NEAR_LIMIT,
    ]


def test_sqlite_reply_queue_input_and_limit(db):
    over_id = _insert_reply(db, "over", "a" * 281, platform="x")
    _insert_reply(db, "ok", "b" * 250, platform="bluesky")
    db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", ("2026-05-03 09:00:00", over_id))
    db.conn.commit()

    report = build_reply_length_fit_audit(db, now=NOW, limit=10)

    assert report.totals["checked_replies"] == 2
    assert [finding.reply_queue_id for finding in report.findings] == [over_id]
    assert report.findings[0].platform == "x"


def test_schema_gaps_are_reported_without_crashing():
    missing_table = build_reply_length_fit_audit(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table.missing_tables == ("reply_queue",)
    assert missing_table.totals["checked_replies"] == 0

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, inbound_text TEXT)")
    missing_columns = build_reply_length_fit_audit(conn, now=NOW)

    assert missing_columns.missing_columns == {"reply_queue": ("draft_text",)}


def test_formatters_are_deterministic():
    report = build_reply_length_fit_audit(
        reply_records=[
            {
                "id": 7,
                "platform": "x",
                "inbound_tweet_id": "fmt",
                "inbound_author_handle": "alice",
                "inbound_text": "How?",
                "draft_text": "a" * 281,
            }
        ],
        now=NOW,
    )

    payload = json.loads(format_reply_length_fit_audit_json(report))
    text = format_reply_length_fit_audit_text(report)

    assert payload["artifact_type"] == "reply_length_fit_audit"
    assert payload["generated_at"] == "2026-05-03T12:00:00+00:00"
    assert payload["totals"]["over_limit_count"] == 1
    assert payload["findings"][0]["issue_type"] == ISSUE_OVER_LIMIT
    assert "Reply Length Fit Audit" in text
    assert "checked=1 over_limit=1 near_limit=0 missing_draft=0 empty=0" in text
    assert "reply=7 inbound=fmt platform=x @alice issue=over_limit length=281/280" in text


def test_cli_parsing_db_json_and_script_context(file_db, db, monkeypatch, capsys):
    file_reply_id = _insert_reply(file_db, "cli-file", "a" * 281)

    exit_code = reply_length_fit_audit_script.main(
        [
            "--db",
            str(file_db.db_path),
            "--format",
            "json",
            "--platform-limit",
            "x=200",
            "--near-threshold",
            "0.75",
            "--limit",
            "5",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"]["platform_limits"]["x"] == 200
    assert payload["filters"]["near_threshold"] == 0.75
    assert payload["findings"][0]["reply_queue_id"] == file_reply_id

    ctx_reply_id = _insert_reply(db, "cli-context", None)
    monkeypatch.setattr(
        reply_length_fit_audit_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = reply_length_fit_audit_script.main(["--format", "text"])
    output = capsys.readouterr().out

    assert exit_code == 1
    assert f"reply={ctx_reply_id}" in output
    assert "issue=missing_draft" in output


def test_cli_parse_errors_return_argparse_status(capsys):
    exit_code = reply_length_fit_audit_script.main(["--platform-limit", "x:not-a-limit"])

    assert exit_code == 2
    assert "value must be PLATFORM=LIMIT" in capsys.readouterr().err
