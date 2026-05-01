"""Tests for thread hook performance reporting."""

from __future__ import annotations

import importlib.util
import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.thread_hook_performance import (
    build_thread_hook_performance_report,
    classify_hook_style,
    extract_thread_opening,
    format_thread_hook_performance_json,
    format_thread_hook_performance_table,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "thread_hook_performance.py"
spec = importlib.util.spec_from_file_location("thread_hook_performance", SCRIPT_PATH)
thread_hook_performance = importlib.util.module_from_spec(spec)
sys.modules["thread_hook_performance"] = thread_hook_performance
spec.loader.exec_module(thread_hook_performance)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _thread(
    db,
    content: str,
    *,
    published_at: str = "2026-04-30T12:00:00+00:00",
    score: float = 0.0,
    auto_quality: str | None = None,
    content_type: str = "x_thread",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET auto_quality = ? WHERE id = ?",
        (auto_quality, content_id),
    )
    db.upsert_publication_success(
        content_id,
        "x",
        platform_post_id=f"tw-{content_id}",
        published_at=published_at,
    )
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, ?, 0, 0, 0, 0, ?, ?)""",
        (content_id, f"tw-{content_id}", score, "2026-05-01T11:00:00+00:00"),
    )
    db.conn.commit()
    return content_id


def test_extracts_openings_from_common_stored_content_shapes():
    assert (
        extract_thread_opening("TWEET 1:\nWhy did deploys get slower?\nTWEET 2:\nDetails")
        == "Why did deploys get slower?"
    )
    assert extract_thread_opening('["I built a queue monitor", "Details"]') == (
        "I built a queue monitor"
    )
    assert extract_thread_opening('{"tweets": [{"text": "42% fewer retries"}]}') == (
        "42% fewer retries"
    )
    assert extract_thread_opening("Plain opening line\n\nSecond line") == "Plain opening line"


def test_hook_style_classification_is_deterministic():
    cases = {
        "Why did this worker stop retrying?": "question",
        "I was wrong about retry queues": "confession",
        "I built a publish ledger this morning": "build-log",
        "Most teams think retries are enough. Actually, they hide drift.": "contrarian",
        "42% of failed posts shared one missing field": "metric-led",
        "A small reliability note from today's pipeline": "plain-summary",
    }

    assert {text: classify_hook_style(text) for text in cases} == cases


def test_report_aggregates_engagement_resonance_and_examples(db):
    question_high = _thread(
        db,
        "TWEET 1:\nWhy did the hook win?\nTWEET 2:\nBecause it named the risk.",
        score=12.0,
        auto_quality="resonated",
    )
    _thread(db, '{"thread": ["Why do retries fail quietly?", "Details"]}', score=6.0)
    _thread(
        db,
        '["I built a retry dashboard", "Here is what changed"]',
        score=5.0,
        auto_quality="resonated",
    )

    rows = build_thread_hook_performance_report(
        db,
        days=7,
        min_count=1,
        examples=1,
        now=NOW,
    )
    by_style = {row.style: row for row in rows}

    assert by_style["question"].count == 2
    assert by_style["question"].average_engagement == 9.0
    assert by_style["question"].resonated_count == 1
    assert by_style["question"].resonance_rate == 0.5
    assert by_style["question"].examples[0].content_id == question_high
    assert by_style["build-log"].count == 1
    assert rows[0].style == "question"


def test_min_count_excludes_under_sampled_styles_and_ignores_non_threads(db):
    _thread(db, "TWEET 1:\nWhy this changed?", score=9.0)
    _thread(db, "TWEET 1:\nWhy this worked?", score=7.0)
    _thread(db, "I built one thing", score=100.0)
    _thread(db, "Why single posts are ignored?", score=100.0, content_type="x_post")

    rows = build_thread_hook_performance_report(
        db,
        days=7,
        min_count=2,
        examples=0,
        now=NOW,
    )

    assert [row.style for row in rows] == ["question"]
    assert rows[0].count == 2


def test_formatters_and_cli_json_output(db, capsys):
    _thread(db, "TWEET 1:\n3 hooks changed the result\nTWEET 2:\nDetails", score=4.0)

    rows = build_thread_hook_performance_report(db, days=7, examples=1, now=NOW)
    payload = json.loads(format_thread_hook_performance_json(rows))
    table = format_thread_hook_performance_table(rows, days=7, min_count=1)

    assert payload[0]["style"] == "metric-led"
    assert list(payload[0]) == sorted(payload[0])
    assert payload[0]["examples"][0]["opening"] == "3 hooks changed the result"
    assert "Thread Hook Performance (last 7 days)" in table
    assert "metric-led" in table
    assert "example #" in table

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(thread_hook_performance, "script_context", fake_script_context):
        thread_hook_performance.main(["--days", "7", "--examples", "1", "--json"])

    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload[0]["style"] == "metric-led"
