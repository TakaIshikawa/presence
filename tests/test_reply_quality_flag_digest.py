"""Tests for reply_queue quality flag digest reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.reply_quality_flag_digest import (
    build_reply_quality_flag_digest_report,
    format_reply_quality_flag_digest_json,
    format_reply_quality_flag_digest_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_quality_flag_digest.py"
spec = importlib.util.spec_from_file_location("reply_quality_flag_digest_script", SCRIPT_PATH)
reply_quality_flag_digest_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(reply_quality_flag_digest_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_reply(
    db,
    *,
    handle: str,
    score: float | None,
    flags: object = None,
    status: str = "pending",
    intent: str = "question",
    platform: str = "x",
    detected_at: datetime | None = None,
) -> int:
    if flags is None:
        encoded_flags = None
    elif isinstance(flags, str):
        encoded_flags = flags
    else:
        encoded_flags = json.dumps(flags)
    reply_id = db.insert_reply_draft(
        inbound_tweet_id=f"{platform}-{handle}-{status}-{intent}-{score}-{encoded_flags}",
        platform=platform,
        inbound_author_handle=handle,
        inbound_author_id=f"id-{handle}",
        inbound_text=f"inbound from {handle}",
        our_tweet_id=f"our-{handle}",
        our_content_id=None,
        our_post_text="our post",
        draft_text=f"draft to {handle}",
        intent=intent,
        quality_score=score,
        quality_flags=encoded_flags,
        status=status,
    )
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        ((detected_at or NOW - timedelta(days=1)).isoformat(), reply_id),
    )
    db.conn.commit()
    return int(reply_id)


def test_report_groups_totals_and_lists_low_score_or_actionable_flag_rows(db):
    low_id = _add_reply(
        db,
        handle="low",
        score=4.5,
        flags=["specificity_gap"],
        status="pending",
        intent="question",
        platform="x",
    )
    syco_id = _add_reply(
        db,
        handle="syco",
        score=8.0,
        flags=["sycophantic"],
        status="approved",
        intent="appreciation",
        platform="bluesky",
    )
    _add_reply(
        db,
        handle="ok",
        score=9.0,
        flags=[],
        status="dismissed",
        intent="bug_report",
        platform="x",
    )
    _add_reply(
        db,
        handle="posted",
        score=2.0,
        flags=["generic"],
        status="posted",
        intent="other",
        platform="x",
    )

    report = build_reply_quality_flag_digest_report(db, days=7, max_score=6.0, now=NOW)
    payload = json.loads(format_reply_quality_flag_digest_json(report))
    text = format_reply_quality_flag_digest_text(report)

    assert payload["artifact_type"] == "reply_quality_flag_digest"
    assert payload["totals"]["row_count"] == 3
    assert payload["totals"]["low_score_count"] == 1
    assert payload["counts"]["by_status"] == {"approved": 1, "dismissed": 1, "pending": 1}
    assert payload["counts"]["by_intent"] == {
        "appreciation": 1,
        "bug_report": 1,
        "question": 1,
    }
    assert payload["counts"]["by_platform"] == {"bluesky": 1, "x": 2}
    assert payload["counts"]["by_quality_flag"] == {"specificity_gap": 1, "sycophantic": 1}
    assert [item["reply_queue_id"] for item in payload["actionable_replies"]] == [
        low_id,
        syco_id,
    ]
    assert payload["actionable_replies"][0]["author_handle"] == "low"
    assert payload["actionable_replies"][0]["status"] == "pending"
    assert payload["actionable_replies"][0]["intent"] == "question"
    assert payload["actionable_replies"][0]["recommended_action"] == "review or rewrite before approving"
    assert "reply_queue:" in text


def test_malformed_quality_flags_are_reported_with_repair_recommendation(db):
    invalid_id = _add_reply(db, handle="badjson", score=7.0, flags="[bad")
    non_array_id = _add_reply(db, handle="object", score=7.0, flags='{"flag":"generic"}')
    _add_reply(db, handle="valid", score=7.0, flags=["generic"])

    report = build_reply_quality_flag_digest_report(db, days=7, now=NOW)
    malformed = report["malformed_quality_flags"]

    assert [item["reply_queue_id"] for item in malformed] == [invalid_id, non_array_id]
    assert [item["classification"] for item in malformed] == ["invalid_json", "not_json_array"]
    assert all("JSON array of strings" in item["repair_recommendation"] for item in malformed)
    assert report["totals"]["malformed_quality_flags_count"] == 2
    assert report["has_issues"] is True


def test_status_days_and_partial_schema_are_resilient():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            status TEXT,
            quality_score REAL,
            quality_flags TEXT,
            detected_at TEXT
        );
        INSERT INTO reply_queue (id, status, quality_score, quality_flags, detected_at)
        VALUES
            (1, 'pending', 5.0, '["generic"]', '2026-05-01T12:00:00+00:00'),
            (2, 'dismissed', 1.0, '["sycophantic"]', '2026-04-01T12:00:00+00:00'),
            (3, 'posted', 1.0, '["generic"]', '2026-05-01T12:00:00+00:00');
        """
    )

    report = build_reply_quality_flag_digest_report(
        conn,
        days=7,
        statuses=("pending",),
        now=NOW,
    )

    assert report["totals"]["row_count"] == 1
    assert report["counts"]["by_status"] == {"pending": 1}
    assert report["counts"]["by_intent"] == {"other": 1}
    assert report["counts"]["by_platform"] == {"x": 1}
    assert report["missing_columns"]["reply_queue"] == [
        "inbound_author_handle",
        "intent",
        "platform",
        "reviewed_at",
        "posted_at",
    ]


def test_missing_reply_queue_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_reply_quality_flag_digest_report(conn, now=NOW)

    assert report["totals"]["row_count"] == 0
    assert report["missing_tables"] == ["reply_queue"]
    assert report["has_issues"] is False
    assert "Missing tables: reply_queue" in format_reply_quality_flag_digest_text(report)


def test_cli_outputs_text_json_and_supports_fail_on_issues(db, monkeypatch, capsys):
    _add_reply(db, handle="cli", score=4.0, flags=["generic"])
    monkeypatch.setattr(
        reply_quality_flag_digest_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        reply_quality_flag_digest_script,
        "build_reply_quality_flag_digest_report",
        lambda db, **kwargs: build_reply_quality_flag_digest_report(db, now=NOW, **kwargs),
    )

    assert reply_quality_flag_digest_script.main(["--days", "7"]) == 0
    assert "Reply Quality Flag Digest" in capsys.readouterr().out

    assert reply_quality_flag_digest_script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["totals"]["actionable_count"] == 1

    assert reply_quality_flag_digest_script.main(["--fail-on-issues"]) == 2
    capsys.readouterr()

    assert reply_quality_flag_digest_script.main(["--max-score", "11"]) == 2
    assert "score must be between 0 and 10" in capsys.readouterr().err
