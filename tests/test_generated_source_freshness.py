"""Tests for generated-content source freshness reporting."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from evaluation.generated_source_freshness import (
    build_generated_source_freshness_report,
    format_generated_source_freshness_json,
    format_generated_source_freshness_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "generated_source_freshness.py"
spec = importlib.util.spec_from_file_location("generated_source_freshness_script", SCRIPT_PATH)
generated_source_freshness_script = importlib.util.module_from_spec(spec)
sys.modules["generated_source_freshness_script"] = generated_source_freshness_script
assert spec and spec.loader
spec.loader.exec_module(generated_source_freshness_script)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)


def _content(
    db,
    *,
    content_type: str = "x_post",
    commits: list[str] | None = None,
    messages: list[str] | None = None,
    activity_ids: list[str] | None = None,
    created_at: str = "2026-05-01T00:00:00+00:00",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=commits or [],
        source_messages=messages or [],
        source_activity_ids=activity_ids or [],
        content="Generated copy",
        eval_score=8.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at, content_id),
    )
    db.conn.commit()
    return content_id


def test_classifies_generated_content_from_newest_resolved_source(db):
    db.insert_commit("repo/app", "fresh-sha", "fresh", "2026-04-30T12:00:00+00:00", "dev")
    db.insert_commit("repo/app", "aging-sha", "aging", "2026-04-12T12:00:00+00:00", "dev")
    db.insert_commit("repo/app", "stale-sha", "stale", "2026-03-15T12:00:00+00:00", "dev")
    db.conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text)
           VALUES (?, ?, ?, ?, ?)""",
        ("s1", "msg-stale", "/repo", "2026-03-10T12:00:00+00:00", "old"),
    )
    db.conn.commit()
    activity_id = db.upsert_github_activity(
        repo_name="repo/app",
        activity_type="pull_request",
        number=12,
        title="Fresh PR",
        state="closed",
        author="dev",
        url="https://example.test/pr/12",
        updated_at="2026-04-29T12:00:00+00:00",
    )

    fresh_id = _content(db, commits=["fresh-sha"])
    aging_id = _content(db, commits=["aging-sha"])
    stale_id = _content(db, commits=["stale-sha"], messages=["msg-stale"])
    missing_id = _content(db)
    activity_content_id = _content(
        db,
        activity_ids=[str(activity_id), "repo/app#12:pull_request"],
    )

    report = build_generated_source_freshness_report(
        db,
        now=NOW,
        aging_days=14,
        stale_days=30,
    )
    by_id = {row.content_id: row for row in report.rows}

    assert by_id[fresh_id].status == "fresh"
    assert by_id[fresh_id].age_days == 1
    assert by_id[aging_id].status == "aging"
    assert by_id[stale_id].status == "stale"
    assert by_id[stale_id].oldest_source_timestamp == "2026-03-10T12:00:00+00:00"
    assert by_id[missing_id].status == "missing_sources"
    assert by_id[activity_content_id].status == "fresh"
    assert by_id[activity_content_id].resolved_source_count == 2
    assert report.summary == {
        "total": 5,
        "fresh": 2,
        "aging": 1,
        "stale": 1,
        "missing_sources": 1,
        "by_status": {
            "fresh": 2,
            "aging": 1,
            "stale": 1,
            "missing_sources": 1,
        },
    }


def test_json_and_text_output_are_deterministic_and_highlight_problem_rows(db):
    db.insert_commit("repo/app", "old-sha", "old", "2026-03-01T12:00:00+00:00", "dev")
    stale_id = _content(db, commits=["old-sha"])
    missing_id = _content(db)

    report = build_generated_source_freshness_report(db, now=NOW)
    payload = json.loads(format_generated_source_freshness_json(report))
    text = format_generated_source_freshness_text(report)

    assert list(payload) == sorted(payload)
    assert payload["summary"]["stale"] == 1
    assert payload["summary"]["missing_sources"] == 1
    assert f"content_id={stale_id}" in text
    assert f"content_id={missing_id}" in text
    assert "source_age=61 days" in text
    assert "GENERATED SOURCE FRESHNESS" in text


def test_handles_legacy_schema_missing_columns_and_tables_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
               id INTEGER PRIMARY KEY,
               content_type TEXT,
               content TEXT
           )"""
    )
    conn.execute(
        "INSERT INTO generated_content (id, content_type, content) VALUES (1, 'x_post', 'copy')"
    )

    report = build_generated_source_freshness_report(conn, now=NOW)

    assert report.summary["missing_sources"] == 1
    assert set(report.missing_tables) == {"github_activity", "github_commits", "claude_messages"}
    assert report.missing_columns["generated_content"] == [
        "created_at",
        "source_activity_ids",
        "source_commits",
        "source_messages",
    ]
    assert report.rows[0].status == "missing_sources"
    assert "generated_content.source_commits column is missing" in report.rows[0].warnings


def test_malformed_source_json_is_reported_as_warning(db):
    content_id = _content(db, commits=["valid-sha"])
    db.conn.execute(
        "UPDATE generated_content SET source_commits = ? WHERE id = ?",
        ("[not-json", content_id),
    )
    db.conn.commit()

    report = build_generated_source_freshness_report(db, now=NOW)

    assert report.rows[0].status == "missing_sources"
    assert "malformed source_commits" in report.warnings[0]
    assert "malformed source_commits" in report.rows[0].warnings[0]


def test_filters_by_content_type_and_lookback_days(db):
    db.insert_commit("repo/app", "sha", "fresh", "2026-04-30T12:00:00+00:00", "dev")
    included = _content(
        db,
        content_type="blog_post",
        commits=["sha"],
        created_at="2026-04-28T12:00:00+00:00",
    )
    _content(
        db,
        content_type="blog_post",
        commits=["sha"],
        created_at="2026-03-01T12:00:00+00:00",
    )
    _content(
        db,
        content_type="x_post",
        commits=["sha"],
        created_at="2026-04-30T12:00:00+00:00",
    )

    report = build_generated_source_freshness_report(
        db,
        now=NOW,
        days=7,
        content_type="blog_post",
    )

    assert [row.content_id for row in report.rows] == [included]
    assert report.content_type == "blog_post"
    assert report.days == 7


def test_cli_outputs_json_for_patched_script_context(db, capsys):
    db.insert_commit("repo/app", "sha", "fresh", "2026-04-30T12:00:00+00:00", "dev")
    _content(db, commits=["sha"])

    @contextmanager
    def fake_script_context():
        yield SimpleNamespace(), db

    with patch.object(generated_source_freshness_script, "script_context", fake_script_context):
        result = generated_source_freshness_script.main(["--format", "json"])

    payload = json.loads(capsys.readouterr().out)
    assert result == 0
    assert payload["summary"]["total"] == 1

    result = generated_source_freshness_script.main(["--aging-days", "31", "--stale-days", "30"])
    captured = capsys.readouterr()
    assert result == 1
    assert "aging_days must be less than or equal to stale_days" in captured.err
