"""Tests for GitHub activity response backlog reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from ingestion.github_activity_response_backlog import (
    MALFORMED_LABELS,
    MALFORMED_METADATA,
    build_github_activity_response_backlog_report,
    format_github_activity_response_backlog_json,
    format_github_activity_response_backlog_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "github_activity_response_backlog.py"
)
spec = importlib.util.spec_from_file_location(
    "github_activity_response_backlog_script",
    SCRIPT_PATH,
)
github_activity_response_backlog_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(github_activity_response_backlog_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _activity(
    db,
    *,
    repo_name: str = "alpha/app",
    activity_type: str = "issue",
    number: int | str = 1,
    title: str = "Needs a reply",
    state: str = "open",
    author: str = "external-user",
    days_ago: float = 3,
    created_days_ago: float | None = None,
    labels: list[str] | str | None = None,
    metadata: dict | str | None = None,
) -> int:
    return int(
        db.upsert_github_activity(
            repo_name=repo_name,
            activity_type=activity_type,
            number=number,
            title=title,
            state=state,
            author=author,
            url=f"https://github.com/{repo_name}/{activity_type}/{number}",
            updated_at=(NOW - timedelta(days=days_ago)).isoformat(),
            created_at=(
                NOW - timedelta(days=created_days_ago if created_days_ago is not None else days_ago)
            ).isoformat(),
            labels=labels or [],
            metadata=metadata or {},
        )
    )


def _cover(db, *activity_ids: str) -> int:
    return int(
        db.insert_generated_content(
            content_type="newsletter_brief",
            source_commits=[],
            source_messages=[],
            source_activity_ids=list(activity_ids),
            content="Covered content",
            eval_score=8.0,
            eval_feedback="ok",
        )
    )


def test_report_ranks_uncovered_actionable_activity_before_covered_items(db):
    covered_id = "alpha/app#1:issue"
    _activity(
        db,
        number=1,
        title="Covered severe bug",
        created_days_ago=40,
        labels=["bug", "help wanted"],
    )
    _cover(db, covered_id)
    _activity(
        db,
        number=2,
        title="Uncovered question",
        created_days_ago=5,
        labels=["question"],
        metadata={"comments_count": 2},
    )
    _activity(
        db,
        number=3,
        title="Old closed item",
        state="closed",
        days_ago=90,
        created_days_ago=100,
        labels=["bug"],
    )

    report = build_github_activity_response_backlog_report(db, days=14, now=NOW)
    payload = json.loads(format_github_activity_response_backlog_json(report))
    text = format_github_activity_response_backlog_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "github_activity_response_backlog"
    assert payload["totals"]["candidate_count"] == 2
    assert payload["items"][0]["activity_id"] == "alpha/app#2:issue"
    assert payload["items"][0]["covered"] is False
    assert payload["items"][0]["labels"] == ["question"]
    assert payload["items"][1]["activity_id"] == covered_id
    assert payload["items"][1]["covered"] is True
    assert "GitHub Activity Response Backlog" in text
    assert "alpha/app issue #2" in text
    assert "Old closed item" not in text


def test_repo_activity_type_limit_and_cli_json_filters(db, monkeypatch, capsys):
    _activity(db, repo_name="alpha/app", activity_type="issue", number=1, labels=["bug"])
    _activity(
        db,
        repo_name="alpha/app",
        activity_type="discussion",
        number=2,
        labels=["question"],
        metadata={"answer_state": "open", "comments_count": 4},
    )
    _activity(db, repo_name="beta/api", activity_type="discussion", number=3, labels=["question"])

    report = build_github_activity_response_backlog_report(
        db,
        repo="alpha/app",
        activity_type="discussion",
        limit=1,
        now=NOW,
    )

    assert report.filters["repo"] == "alpha/app"
    assert [item.activity_id for item in report.items] == ["alpha/app#2:discussion"]

    monkeypatch.setattr(
        github_activity_response_backlog_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        github_activity_response_backlog_script,
        "build_github_activity_response_backlog_report",
        lambda db, **kwargs: build_github_activity_response_backlog_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert github_activity_response_backlog_script.main(
        [
            "--repo",
            "alpha/app",
            "--activity-type",
            "discussion",
            "--limit",
            "1",
            "--format",
            "json",
        ]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["items"][0]["activity_id"] == "alpha/app#2:discussion"

    assert github_activity_response_backlog_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_malformed_labels_and_metadata_warn_without_crashing(db):
    bad_id = _activity(db, number=10, labels=["bug"], metadata={"comments_count": 1})
    db.conn.execute(
        "UPDATE github_activity SET labels = ?, metadata = ? WHERE id = ?",
        ("not-json", "not-json", bad_id),
    )
    content_id = _cover(db, "alpha/app#missing:issue")
    db.conn.execute(
        "UPDATE generated_content SET source_activity_ids = ? WHERE id = ?",
        ("not-json", content_id),
    )
    db.conn.commit()

    report = build_github_activity_response_backlog_report(db, now=NOW)

    assert report.items[0].activity_id == "alpha/app#10:issue"
    assert any(MALFORMED_LABELS in warning for warning in report.items[0].warnings)
    assert any(MALFORMED_METADATA in warning for warning in report.items[0].warnings)
    assert any("source_activity_ids" in warning for warning in report.warnings)
    assert report.totals["warning_count"] == 3


def test_recent_comment_is_included_but_old_comment_is_filtered(db):
    _activity(
        db,
        activity_type="issue_comment",
        number=1001,
        title="Recent issue comment",
        state="commented",
        days_ago=1,
        metadata={"parent_number": 11},
    )
    _activity(
        db,
        activity_type="issue_comment",
        number=1002,
        title="Old issue comment",
        state="commented",
        days_ago=60,
        metadata={"parent_number": 12},
    )

    report = build_github_activity_response_backlog_report(db, days=14, now=NOW)

    assert [item.number for item in report.items] == ["1001"]


def test_missing_tables_and_invalid_builder_args_return_stable_errors():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_github_activity_response_backlog_report(conn, now=NOW)

    assert report.items == ()
    assert "github_activity" in report.missing_tables
    assert report.totals["candidate_count"] == 0
    assert "Missing tables: github_activity" in format_github_activity_response_backlog_text(report)

    with pytest.raises(ValueError, match="days must be positive"):
        build_github_activity_response_backlog_report(conn, days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_github_activity_response_backlog_report(conn, limit=0, now=NOW)
    with pytest.raises(ValueError, match="activity_type must be one of"):
        build_github_activity_response_backlog_report(conn, activity_type="release", now=NOW)
    conn.close()
