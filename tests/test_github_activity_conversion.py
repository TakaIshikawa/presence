"""Tests for GitHub activity conversion reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from github_activity_conversion import main  # noqa: E402
from evaluation.github_activity_conversion import (  # noqa: E402
    build_github_activity_conversion_report,
    format_github_activity_conversion_text,
)


NOW = datetime(2026, 5, 12, 12, 0, tzinfo=timezone.utc)


def _activity(db, repo: str, number: str, activity_type: str, days_ago: int = 1) -> int:
    cursor = db.conn.execute(
        """INSERT INTO github_activity
           (repo_name, activity_type, number, title, updated_at)
           VALUES (?, ?, ?, ?, ?)""",
        (repo, activity_type, number, "Title", (NOW - timedelta(days=days_ago)).isoformat()),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_groups_conversion_by_activity_type_and_repository(db):
    published_activity = _activity(db, "acme/repo", "1", "issue")
    unpublished_activity = _activity(db, "acme/repo", "2", "issue")
    _activity(db, "acme/repo", "3", "issue")
    pr_activity = _activity(db, "acme/repo", "4", "pull_request")

    published_content = db.insert_generated_content(
        "x_post", [], [], "Published", 7.0, "ok", source_activity_ids=[str(published_activity)]
    )
    db.upsert_publication_success(published_content, "x", "tw1", "https://x.com/u/status/tw1", NOW.isoformat())
    db.insert_generated_content(
        "x_post", [], [], "Draft", 7.0, "ok", source_activity_ids=["acme/repo#2:issue"]
    )
    db.insert_generated_content(
        "x_post", [], [], "PR", 7.0, "ok", source_activity_ids=[str(pr_activity)]
    )

    report = build_github_activity_conversion_report(db, days=7, now=NOW)

    issue = next(row for row in report["groups"] if row["activity_type"] == "issue")
    assert issue["repository"] == "acme/repo"
    assert issue["ingested"] == 3
    assert issue["linked_to_content"] == 2
    assert issue["published"] == 1
    assert issue["unpublished"] == 1
    assert issue["conversion_rate"] == 0.3333
    pr = next(row for row in report["groups"] if row["activity_type"] == "pull_request")
    assert pr["ingested"] == 1
    assert pr["linked_to_content"] == 1
    assert pr["published"] == 0
    assert "Groups:" in format_github_activity_conversion_text(report)


def test_filters_repository_and_activity_type_include_unlinked_rows(db):
    _activity(db, "acme/repo", "1", "issue")
    _activity(db, "other/repo", "2", "issue")
    _activity(db, "acme/repo", "3", "release")

    report = build_github_activity_conversion_report(
        db,
        repository="acme/repo",
        activity_type="issue",
        now=NOW,
    )

    assert len(report["groups"]) == 1
    assert report["groups"][0]["ingested"] == 1
    assert report["groups"][0]["linked_to_content"] == 0


def test_cli_supports_json_output(db, capsys):
    _activity(db, "acme/repo", "1", "issue")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("github_activity_conversion.script_context", fake_script_context):
        result = main(["--days", "7", "--repository", "acme/repo", "--activity-type", "issue", "--format", "json"])

    assert result == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["groups"][0]["ingested"] == 1
