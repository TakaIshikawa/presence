"""Tests for X thread hook diversity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from synthesis.thread_hook_diversity import (
    build_thread_hook_diversity_report,
    classify_thread_hook,
    extract_first_tweet_hook,
    format_thread_hook_diversity_json,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "thread_hook_diversity.py"
spec = importlib.util.spec_from_file_location("thread_hook_diversity_script", SCRIPT_PATH)
thread_hook_diversity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(thread_hook_diversity_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _thread(
    db,
    content: str,
    *,
    created_at: datetime | None = None,
    queued: bool = True,
    status: str = "queued",
) -> int:
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )
    created = created_at or (NOW - timedelta(days=1))
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created.isoformat(), content_id),
    )
    if queued:
        db.conn.execute(
            """INSERT INTO publish_queue
               (content_id, scheduled_at, platform, status, created_at)
               VALUES (?, ?, 'x', ?, ?)""",
            (
                content_id,
                (created + timedelta(hours=2)).isoformat(),
                status,
                created.isoformat(),
            ),
        )
    db.conn.commit()
    return content_id


def test_classifies_supported_hook_categories_deterministically():
    cases = {
        "Why did this queue stop moving?": "question",
        "Most teams add retries, but the real issue is visibility.": "contrast",
        "3 lessons from fixing a silent publish failure": "lesson",
        "I was wrong about retry queues": "confession",
        "I built a queue monitor this morning": "build-log",
        "Announcing a smaller publisher audit": "announcement",
        "": "empty",
        "OK": "plain",
    }

    assert {text: classify_thread_hook(text) for text in cases} == cases


def test_extracts_first_tweet_from_stored_thread_shapes():
    assert (
        extract_first_tweet_hook("TWEET 1:\nWhy did deploys slow down?\nTWEET 2:\nDetails")
        == "Why did deploys slow down?"
    )
    assert extract_first_tweet_hook('["I built a queue monitor", "Details"]') == (
        "I built a queue monitor"
    )
    assert extract_first_tweet_hook('{"tweets": [{"text": "Announcing the audit"}]}') == (
        "Announcing the audit"
    )
    assert extract_first_tweet_hook("Plain opening\n\nSecond line") == "Plain opening"
    assert extract_first_tweet_hook("TWEET 1:\n\nTWEET 2:\nDetails") == ""


def test_report_flags_categories_over_max_share_and_includes_duplicate_counts(db):
    question_one = _thread(db, "TWEET 1:\nWhy did retries stall?\nTWEET 2:\nDetails")
    question_two = _thread(db, '{"thread": ["Why did the queue hold posts?", "Details"]}')
    _thread(db, "TWEET 1:\nI built a retry ledger\nTWEET 2:\nDetails")
    _thread(db, "TWEET 1:\nAnnouncing the new audit\nTWEET 2:\nDetails")

    report = build_thread_hook_diversity_report(
        db,
        days=7,
        max_share=0.4,
        now=NOW,
    )

    assert report.totals["record_count"] == 4
    assert report.totals["by_category"]["question"] == 2
    assert report.totals["finding_count"] == 2
    assert {finding.thread_id for finding in report.findings} == {
        question_one,
        question_two,
    }
    assert {finding.duplicate_count for finding in report.findings} == {1}
    assert {finding.category_share for finding in report.findings} == {0.5}
    assert all("rewrite" in finding.recommendation for finding in report.findings)


def test_report_respects_lookback_status_limit_and_recent_generated_rows(db):
    _thread(db, "TWEET 1:\nWhy queued?\nTWEET 2:\nDetails", status="queued")
    _thread(db, "TWEET 1:\nWhy held?\nTWEET 2:\nDetails", status="held")
    _thread(db, "TWEET 1:\nWhy failed?\nTWEET 2:\nDetails", status="failed")
    _thread(
        db,
        "TWEET 1:\nWhy old?\nTWEET 2:\nDetails",
        created_at=NOW - timedelta(days=30),
    )
    generated_only = _thread(
        db,
        "TWEET 1:\nI built an unqueued draft\nTWEET 2:\nDetails",
        queued=False,
    )

    report = build_thread_hook_diversity_report(
        db,
        days=7,
        status=("queued",),
        limit=2,
        max_share=0.5,
        now=NOW,
    )

    assert report.totals["record_count"] == 2
    assert {record.status for record in report.records} == {"queued", "generated"}
    assert generated_only in {record.thread_id for record in report.records}
    assert all(record.status != "failed" for record in report.records)


def test_formatters_cli_and_missing_schema(db, monkeypatch, capsys):
    _thread(db, "TWEET 1:\nWhy this hook?\nTWEET 2:\nDetails")
    report = build_thread_hook_diversity_report(db, days=7, max_share=0.5, now=NOW)
    payload = json.loads(format_thread_hook_diversity_json(report))

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "thread_hook_diversity"
    assert payload["records"][0]["hook_category"] == "question"

    monkeypatch.setattr(
        thread_hook_diversity_script,
        "script_context",
        lambda: _script_context(db),
    )
    result = thread_hook_diversity_script.main(["--days", "7", "--max-share", "0.5"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert cli_payload["filters"]["max_share"] == 0.5

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    missing_report = build_thread_hook_diversity_report(conn, now=NOW)

    assert missing_report.records == ()
    assert missing_report.findings == ()
    assert missing_report.missing_tables == ("generated_content",)
