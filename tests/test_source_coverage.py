"""Tests for source material coverage reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.source_coverage import summarize_source_coverage
from source_coverage import main

NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _seed_sources(db) -> None:
    db.insert_commit("acme/app", "sha-covered", "feat: covered", "2026-04-20T10:00:00+00:00", "taka")
    db.insert_commit("acme/app", "sha-uncovered", "fix: uncovered", "2026-04-19T10:00:00+00:00", "taka")
    db.insert_commit("acme/other", "sha-other", "chore: other", "2026-04-18T10:00:00+00:00", "taka")
    db.insert_claude_message("s1", "msg-covered", "/repo", "2026-04-20T09:00:00+00:00", "Covered prompt")
    db.insert_claude_message("s2", "msg-uncovered", "/repo", "2026-04-19T09:00:00+00:00", "Uncovered prompt")
    db.upsert_github_activity(
        repo_name="acme/app",
        activity_type="pull_request",
        number=12,
        title="Covered PR",
        state="open",
        author="taka",
        url="https://example.test/pr/12",
        updated_at="2026-04-20T08:00:00+00:00",
    )
    db.upsert_github_activity(
        repo_name="acme/app",
        activity_type="issue",
        number=13,
        title="Uncovered issue",
        state="open",
        author="taka",
        url="https://example.test/issues/13",
        updated_at="2026-04-19T08:00:00+00:00",
    )
    db.upsert_github_activity(
        repo_name="acme/other",
        activity_type="release",
        number=1,
        title="Other release",
        state="published",
        author="taka",
        url="https://example.test/releases/1",
        updated_at="2026-04-18T08:00:00+00:00",
    )


def test_fully_covered_sources(db):
    _seed_sources(db)
    db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-covered", "sha-uncovered", "sha-other"],
        source_messages=["msg-covered", "msg-uncovered"],
        source_activity_ids=[
            "acme/app#12:pull_request",
            "acme/app#13:issue",
            "acme/other#1:release",
        ],
        content="Everything is used.",
        eval_score=8.0,
        eval_feedback="ok",
    )

    report = summarize_source_coverage(db, days=30, limit=5, now=NOW)

    assert report["summary"]["uncovered_total"] == 0
    assert report["commits"]["uncovered_count"] == 0
    assert report["messages"]["uncovered_count"] == 0
    assert report["github_activity"]["uncovered_count"] == 0
    assert report["warnings"] == []


def test_partially_covered_sources_are_detected_independently(db):
    _seed_sources(db)
    db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-covered"],
        source_messages=["msg-covered"],
        source_activity_ids=["acme/app#12:pull_request"],
        content="Some source material is used.",
        eval_score=8.0,
        eval_feedback="ok",
    )

    report = summarize_source_coverage(db, days=30, repo="acme/app", limit=1, now=NOW)

    assert report["commits"]["uncovered_count"] == 1
    assert report["commits"]["uncovered_items"][0]["commit_sha"] == "sha-uncovered"
    assert report["messages"]["uncovered_count"] == 1
    assert report["messages"]["uncovered_items"][0]["message_uuid"] == "msg-uncovered"
    assert report["github_activity"]["uncovered_count"] == 1
    assert report["github_activity"]["uncovered_items"][0]["activity_id"] == "acme/app#13:issue"
    assert report["commits"]["ingested_count"] == 2
    assert report["github_activity"]["ingested_count"] == 2


def test_empty_generated_content_sources_mark_ingested_items_uncovered(db):
    _seed_sources(db)
    db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        source_activity_ids=[],
        content="No source refs.",
        eval_score=5.0,
        eval_feedback="thin",
    )

    report = summarize_source_coverage(db, days=30, repo="acme/app", limit=10, now=NOW)

    assert report["commits"]["uncovered_count"] == 2
    assert report["messages"]["uncovered_count"] == 2
    assert report["github_activity"]["uncovered_count"] == 2


def test_empty_ingestion_tables_return_zero_counts(db):
    report = summarize_source_coverage(db, days=30, limit=10, now=NOW)

    assert report["summary"] == {"uncovered_total": 0, "ingested_total": 0}
    assert report["commits"]["uncovered_items"] == []
    assert report["messages"]["uncovered_items"] == []
    assert report["github_activity"]["uncovered_items"] == []


def test_malformed_generated_source_json_is_warning_not_crash(db):
    _seed_sources(db)
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=["msg-covered"],
        source_activity_ids=[],
        content="Malformed refs will be patched below.",
        eval_score=6.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET source_commits = ?, source_activity_ids = ? WHERE id = ?",
        ("[not-json", '{"activity": "not-a-list"}', content_id),
    )
    db.conn.commit()

    report = summarize_source_coverage(db, days=30, repo="acme/app", limit=10, now=NOW)

    assert report["commits"]["uncovered_count"] == 2
    assert report["messages"]["uncovered_count"] == 1
    assert report["github_activity"]["uncovered_count"] == 2
    assert any("malformed source_commits" in warning for warning in report["warnings"])
    assert any("non-list source_activity_ids" in warning for warning in report["warnings"])


def test_cli_json_output_is_deterministic(db, capsys):
    _seed_sources(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("source_coverage.script_context", fake_script_context):
        main(["--days", "30", "--repo", "acme/app", "--limit", "1", "--json"])

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert output.index('"commits"') < output.index('"generated_at"')
    assert payload["window"]["repo"] == "acme/app"
    assert len(payload["commits"]["uncovered_items"]) == 1
