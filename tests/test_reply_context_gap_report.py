"""Tests for reply relationship context gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_context_gap_report import (
    analyze_reply_context_record,
    build_reply_context_gap_report,
    format_reply_context_gap_report_json,
    format_reply_context_gap_report_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "report_reply_context_gaps.py"
spec = importlib.util.spec_from_file_location("report_reply_context_gaps_script", SCRIPT_PATH)
report_reply_context_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(report_reply_context_gaps_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_reply(db, inbound_id: str, **kwargs) -> int:
    defaults = dict(
        inbound_tweet_id=inbound_id,
        inbound_author_handle="alice",
        inbound_author_id="author-a",
        inbound_text="Can you clarify this?",
        our_tweet_id="our-1",
        our_content_id=123,
        our_post_text="Original post",
        draft_text="Thanks for asking.",
        status="pending",
        platform="x",
        relationship_context=json.dumps(
            {
                "display_name": "Alice",
                "bio": "Builds internal tools.",
                "relationship_notes": "Usually asks precise implementation questions.",
                "last_interaction_at": "2026-04-30T12:00:00+00:00",
            },
            sort_keys=True,
        ),
    )
    defaults.update(kwargs)
    return db.insert_reply_draft(**defaults)


def _set_detected_at(db, reply_id: int, detected_at: str) -> None:
    db.conn.execute("UPDATE reply_queue SET detected_at = ? WHERE id = ?", (detected_at, reply_id))
    db.conn.commit()


def test_plain_record_flags_missing_profile_stale_interaction_and_absent_notes():
    item = analyze_reply_context_record(
        {
            "id": 7,
            "inbound_tweet_id": "mention-7",
            "inbound_author_handle": "bob",
            "platform": "x",
            "last_interaction_at": "2026-03-01T12:00:00+00:00",
        },
        max_interaction_age_days=30,
        now=NOW,
    )

    assert item.reply_id == 7
    assert item.mention_id == "mention-7"
    assert item.author_handle == "bob"
    assert item.last_interaction_age_days == 62.0
    assert [finding.reason_code for finding in item.findings] == [
        "missing_profile_context",
        "absent_relationship_notes",
        "stale_last_interaction",
    ]
    assert all(finding.suggested_action for finding in item.findings)


def test_records_without_cultivate_metadata_do_not_crash_and_include_review_fields():
    report = build_reply_context_gap_report(
        [
            {
                "reply_queue_id": 8,
                "mention_id": "mention-8",
                "author_handle": "carol",
                "inbound_text": "What do you mean here?",
                "draft_text": "Good question.",
            }
        ],
        now=NOW,
    )
    payload = json.loads(format_reply_context_gap_report_json(report))

    assert payload["artifact_type"] == "reply_context_gap_report"
    assert payload["scanned_count"] == 1
    assert payload["items"][0]["reply_id"] == 8
    assert payload["items"][0]["mention_id"] == "mention-8"
    assert payload["items"][0]["author_handle"] == "carol"
    assert payload["items"][0]["inbound_preview"] == "What do you mean here?"
    assert payload["by_reason"] == {
        "absent_relationship_notes": 1,
        "missing_last_interaction": 1,
        "missing_profile_context": 1,
    }


def test_complete_cultivate_context_is_not_flagged():
    report = build_reply_context_gap_report(
        [
            {
                "id": 1,
                "inbound_tweet_id": "clean",
                "inbound_author_handle": "alice",
                "relationship_context": json.dumps(
                    {
                        "display_name": "Alice",
                        "bio": "Writes about developer tools.",
                        "relationship_notes": "Prefers concrete implementation details.",
                        "recent_interactions": [
                            {"created_at": "2026-05-01T12:00:00+00:00", "text": "Thanks"}
                        ],
                    }
                ),
            }
        ],
        now=NOW,
    )

    assert report.ok is True
    assert report.findings == ()
    assert report.items == ()


def test_malformed_context_and_severity_filtering_are_deterministic():
    rows = [
        {
            "id": 1,
            "inbound_tweet_id": "bad",
            "inbound_author_handle": "alice",
            "relationship_context": "{bad-json",
        },
        {
            "id": 2,
            "inbound_tweet_id": "thin",
            "inbound_author_handle": "bob",
            "profile_summary": "Bob builds SDKs.",
        },
    ]

    all_report = build_reply_context_gap_report(rows, now=NOW)
    high_report = build_reply_context_gap_report(rows, min_severity="high", now=NOW)
    payload = json.loads(format_reply_context_gap_report_json(high_report))
    text = format_reply_context_gap_report_text(all_report)

    assert list(payload) == sorted(payload)
    assert payload["finding_count"] == 2
    assert payload["by_severity"] == {"high": 2}
    assert {finding["reason_code"] for finding in payload["findings"]} == {
        "malformed_relationship_context",
        "missing_profile_context",
    }
    assert "Reply Context Gap Report" in text
    assert "absent_relationship_notes" in text


def test_cli_loads_reply_queue_rows_and_supports_severity_filtering(db, monkeypatch, capsys):
    missing = _insert_reply(db, "missing", relationship_context=None)
    stale = _insert_reply(
        db,
        "stale",
        relationship_context=json.dumps(
            {
                "display_name": "Stale",
                "bio": "Works on queues.",
                "relationship_notes": "Known for careful reviews.",
                "last_interaction_at": "2026-03-01T12:00:00+00:00",
            },
            sort_keys=True,
        ),
    )
    _insert_reply(db, "reviewed", status="reviewed", relationship_context=None)
    _set_detected_at(db, missing, "2026-05-02 10:00:00")
    _set_detected_at(db, stale, "2026-05-02 09:00:00")
    monkeypatch.setattr(
        report_reply_context_gaps_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        report_reply_context_gaps_script,
        "build_reply_context_gap_report",
        lambda rows, **kwargs: build_reply_context_gap_report(rows, now=NOW, **kwargs),
    )

    exit_code = report_reply_context_gaps_script.main(["--min-severity", "high"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"]["min_severity"] == "high"
    assert payload["scanned_count"] == 2
    assert payload["finding_count"] == 1
    assert payload["findings"][0]["reply_id"] == missing
    assert payload["findings"][0]["mention_id"] == "missing"
    assert payload["findings"][0]["reason_code"] == "missing_profile_context"

    text_exit = report_reply_context_gaps_script.main(["--format", "text"])
    text = capsys.readouterr().out
    assert text_exit == 1
    assert "Reply Context Gap Report" in text
    assert "stale_last_interaction" in text


def test_cli_handles_missing_reply_queue_and_validation(capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    rows, missing = report_reply_context_gaps_script.list_reply_context_gap_records(
        conn,
        days=7,
        platforms=(),
        statuses=("pending",),
        limit=10,
    )

    assert rows == []
    assert missing == ("reply_queue",)

    invalid = report_reply_context_gaps_script.main(["--min-severity", "urgent"])
    captured = capsys.readouterr()
    assert invalid == 2
    assert "severity must be one of" in captured.err
