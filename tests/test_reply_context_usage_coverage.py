"""Tests for reply context usage coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from engagement.reply_context_usage_coverage import (
    build_reply_context_usage_coverage_report,
    build_reply_context_usage_coverage_report_from_db,
    format_reply_context_usage_coverage_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_context_usage_coverage.py"
spec = importlib.util.spec_from_file_location("reply_context_usage_coverage_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_reports_available_used_missing_fields_and_ratio():
    report = build_reply_context_usage_coverage_report(
        [
            {
                "id": "r1",
                "draft_text": "Thanks Ada. The latency benchmark detail helps, and I agree an clarify action fits here.",
                "author_handle": "@ada",
                "relationship_notes": "Ada prefers latency benchmark detail.",
                "prior_interaction_summary": "Previously discussed rollout risk.",
                "target_tweet_text": "Can you clarify the benchmark?",
                "strategic_action_metadata": {"action_type": "clarify", "goal": "answer the benchmark question"},
            }
        ]
    )

    item = report["replies"][0]

    assert item["available_context_fields"] == [
        "relationship_notes",
        "prior_interaction_summary",
        "target_tweet_text",
        "strategic_action_metadata",
    ]
    assert item["used_context_fields"] == [
        "relationship_notes",
        "target_tweet_text",
        "strategic_action_metadata",
    ]
    assert item["missing_context_fields"] == ["prior_interaction_summary"]
    assert item["coverage_ratio"] == 0.75
    assert item["is_low_coverage"] is False


def test_flags_low_coverage_with_configurable_threshold():
    report = build_reply_context_usage_coverage_report(
        [
            {
                "id": "r1",
                "draft_text": "Thanks for the question.",
                "relationship_notes": "Prefers concrete latency details.",
                "target_tweet_text": "Can you share the benchmark?",
            }
        ],
        low_coverage_threshold=0.75,
    )

    item = report["replies"][0]
    assert item["coverage_ratio"] == 0.0
    assert item["is_low_coverage"] is True
    assert "ratio" in format_reply_context_usage_coverage_text(report)


def test_summarizes_average_coverage_by_author_and_action_type():
    report = build_reply_context_usage_coverage_report(
        [
            {
                "id": "1",
                "draft_text": "Latency benchmark answer.",
                "author_handle": "@ada",
                "target_tweet_text": "Latency benchmark?",
                "strategic_action_metadata": {"action_type": "answer"},
            },
            {
                "id": "2",
                "draft_text": "Thanks.",
                "author_handle": "@ada",
                "target_tweet_text": "What about rollout risk?",
                "strategic_action_metadata": {"action_type": "clarify"},
            },
        ]
    )

    by_author = report["aggregates"]["by_author_handle"][0]
    by_action = {item["strategic_action_type"]: item for item in report["aggregates"]["by_strategic_action_type"]}

    assert by_author["author_handle"] == "@ada"
    assert by_author["count"] == 2
    assert by_author["average_coverage_ratio"] == 0.5
    assert by_action["answer"]["average_coverage_ratio"] == 1.0
    assert by_action["clarify"]["low_coverage"] == 1


def test_no_available_context_counts_as_full_coverage_not_low():
    report = build_reply_context_usage_coverage_report([{"id": "r1", "draft_text": "Thanks."}])

    item = report["replies"][0]

    assert item["available_context_fields"] == []
    assert item["coverage_ratio"] == 1.0
    assert item["is_low_coverage"] is False
    assert report["totals"]["no_available_context"] == 1


def test_empty_dataset_has_empty_state():
    report = build_reply_context_usage_coverage_report([])

    assert report["replies"] == []
    assert report["empty_state"]["message"] == "No reply drafts found."


def test_db_loader_and_cli_json_output(monkeypatch, capsys, tmp_path):
    import sqlite3

    conn = sqlite3.connect(tmp_path / "replies.db")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_drafts (
           id INTEGER PRIMARY KEY,
           draft_text TEXT,
           author_handle TEXT,
           target_tweet_text TEXT,
           strategic_action_metadata TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO reply_drafts
           (draft_text, author_handle, target_tweet_text, strategic_action_metadata)
           VALUES (?, ?, ?, ?)""",
        (
            "Benchmark answer for Ada.",
            "@ada",
            "Can you answer the benchmark question?",
            json.dumps({"action_type": "answer"}),
        ),
    )
    conn.commit()

    report = build_reply_context_usage_coverage_report_from_db(conn)
    assert report["replies"][0]["reply_id"] == "1"

    monkeypatch.setattr(script, "script_context", lambda: _script_context(conn))
    assert script.main(["--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "reply_context_usage_coverage"

    assert script.main(["--table"]) == 0
    assert "Reply Context Usage Coverage" in capsys.readouterr().out
