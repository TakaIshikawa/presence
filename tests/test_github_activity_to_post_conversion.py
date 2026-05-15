"""Tests for GitHub activity to post conversion reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.github_activity_to_post_conversion import (
    build_github_activity_to_post_conversion_report,
    build_github_activity_to_post_conversion_report_from_db,
    format_github_activity_to_post_conversion_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "github_activity_to_post_conversion.py"
spec = importlib.util.spec_from_file_location("github_activity_to_post_conversion_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _activity(activity_id: str, kind: str, days: int, sha: str = "") -> dict:
    return {
        "activity_id": activity_id,
        "activity_type": kind,
        "repository": "owner/repo",
        "commit_sha": sha,
        "activity_at": (NOW - timedelta(days=days)).isoformat(),
    }


def test_matches_by_explicit_source_ids_and_commit_sha_references():
    activities = [
        _activity("owner/repo#1:issue", "issue", 10),
        _activity("commit-1", "commit", 3, "abcdef1234567890"),
    ]
    content = [
        {"id": "post-1", "source_activity_ids": ["owner/repo#1:issue"], "published": True},
        {"id": "post-2", "content": "Shipped in abcdef1", "published_status": "scheduled"},
    ]

    report = build_github_activity_to_post_conversion_report(activities, content, now=NOW)

    by_id = {item["activity_id"]: item for item in report["activities"]}
    assert by_id["owner/repo#1:issue"]["matched_content_ids"] == ["post-1"]
    assert by_id["commit-1"]["matched_content_ids"] == ["post-2"]
    assert by_id["commit-1"]["published_statuses"] == ["scheduled"]


def test_unconverted_activity_is_bucketed_by_age_and_type():
    report = build_github_activity_to_post_conversion_report(
        [_activity("old-pr", "pr", 120), _activity("new-issue", "issue", 2)],
        [],
        now=NOW,
    )

    assert [item["activity_id"] for item in report["activities"]] == ["old-pr", "new-issue"]
    assert report["activities"][0]["conversion_status"] == "unconverted"
    assert report["activities"][0]["age_bucket"] == "stale"
    assert report["totals"]["by_activity_type"]["pr"]["unconverted"] == 1
    assert "age_bucket" in format_github_activity_to_post_conversion_text(report)


def test_converted_activity_includes_matched_content_ids_and_published_status():
    report = build_github_activity_to_post_conversion_report(
        [_activity("a1", "issue", 4)],
        [{"id": "c1", "source_activity_ids": ["a1"], "published_at": NOW.isoformat()}],
        now=NOW,
    )

    item = report["activities"][0]
    assert item["conversion_status"] == "converted"
    assert item["matched_content_ids"] == ["c1"]
    assert item["published_statuses"] == ["published"]


def test_db_loader_and_cli_json_output(monkeypatch, capsys, tmp_path):
    import sqlite3

    conn = sqlite3.connect(tmp_path / "github.db")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE github_activity (
           id INTEGER PRIMARY KEY,
           repo_name TEXT,
           activity_type TEXT,
           number INTEGER,
           commit_sha TEXT,
           updated_at TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE generated_content (
           id INTEGER PRIMARY KEY,
           content TEXT,
           source_activity_ids TEXT,
           source_commits TEXT,
           published INTEGER,
           published_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO github_activity (repo_name, activity_type, number, updated_at) VALUES (?, ?, ?, ?)",
        ("owner/repo", "issue", 7, NOW.isoformat()),
    )
    conn.execute(
        "INSERT INTO generated_content (content, source_activity_ids, published) VALUES (?, ?, 1)",
        ("post", json.dumps(["owner/repo#7:issue"])),
    )
    conn.commit()

    report = build_github_activity_to_post_conversion_report_from_db(conn, now=NOW)
    assert report["activities"][0]["conversion_status"] == "converted"

    monkeypatch.setattr(script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        script,
        "build_github_activity_to_post_conversion_report_from_db",
        lambda db, **kwargs: build_github_activity_to_post_conversion_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "github_activity_to_post_conversion"

    assert script.main(["--table"]) == 0
    assert "GitHub Activity To Post Conversion" in capsys.readouterr().out
