"""Tests for Claude work artifact coverage reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from ingestion.claude_work_coverage import (  # noqa: E402
    build_claude_work_coverage_report,
    format_claude_work_coverage_text,
)
from claude_work_coverage import main as coverage_cli  # noqa: E402


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)


def _insert_commit(
    db,
    sha: str,
    timestamp: str,
    *,
    repo: str = "taka/presence",
    message: str | None = None,
) -> int:
    return db.insert_commit(
        repo_name=repo,
        commit_sha=sha,
        commit_message=message or f"feat: commit {sha}",
        timestamp=timestamp,
        author="taka",
    )


def _insert_message(
    db,
    uuid: str,
    timestamp: str,
    *,
    session: str = "sess-1",
    project: str = "/work/presence",
    text: str | None = None,
) -> int:
    return db.insert_claude_message(
        session_id=session,
        message_uuid=uuid,
        project_path=project,
        timestamp=timestamp,
        prompt_text=text or f"Work on {uuid}",
    )


def _link(db, commit_id: int, message_id: int, confidence: float = 0.9) -> None:
    db.conn.execute(
        "INSERT INTO commit_prompt_links (commit_id, message_id, confidence) VALUES (?, ?, ?)",
        (commit_id, message_id, confidence),
    )
    db.conn.commit()


def test_groups_daily_coverage_with_deterministic_ordering(db):
    msg_id = _insert_message(db, "msg-1", "2026-04-23T09:00:00+00:00")
    first_commit = _insert_commit(db, "abc111", "2026-04-23T10:00:00+00:00")
    _insert_commit(db, "abc222", "2026-04-23T11:00:00+00:00")
    _insert_message(
        db,
        "msg-2",
        "2026-04-24T09:00:00+00:00",
        session="sess-2",
    )
    _link(db, first_commit, msg_id)

    report = build_claude_work_coverage_report(db, days=3, now=NOW)

    assert [row["day"] for row in report["daily"]] == ["2026-04-23", "2026-04-24"]
    assert report["daily"][0] == {
        "day": "2026-04-23",
        "commit_count": 2,
        "linked_commit_count": 1,
        "unlinked_commit_count": 1,
        "claude_message_count": 1,
        "linked_claude_message_count": 1,
        "unlinked_claude_message_count": 0,
        "claude_session_count": 1,
    }
    assert report["summary"]["commit_count"] == 2
    assert report["summary"]["claude_session_count"] == 2


def test_flags_commit_heavy_days_and_claude_heavy_days_without_links(db):
    _insert_commit(db, "sha-1", "2026-04-23T10:00:00+00:00")
    _insert_commit(db, "sha-2", "2026-04-23T11:00:00+00:00")
    _insert_commit(db, "sha-3", "2026-04-23T12:00:00+00:00")
    _insert_message(db, "msg-1", "2026-04-24T09:00:00+00:00", session="sess-a")
    _insert_message(db, "msg-2", "2026-04-24T10:00:00+00:00", session="sess-a")
    _insert_message(db, "msg-3", "2026-04-24T11:00:00+00:00", session="sess-b")

    report = build_claude_work_coverage_report(
        db,
        days=3,
        min_commits=3,
        now=NOW,
    )

    assert [row["day"] for row in report["unlinked_commit_heavy_days"]] == [
        "2026-04-23"
    ]
    assert [row["day"] for row in report["claude_heavy_days_without_commits"]] == [
        "2026-04-24"
    ]
    assert report["summary"]["unlinked_commit_heavy_day_count"] == 1
    assert report["summary"]["claude_heavy_day_without_commits_count"] == 1


def test_repo_filter_limits_commits_and_matching_project_messages(db):
    linked_msg = _insert_message(
        db,
        "presence-msg",
        "2026-04-23T09:00:00+00:00",
        project="/work/presence",
    )
    hidden_msg = _insert_message(
        db,
        "other-msg",
        "2026-04-23T09:30:00+00:00",
        project="/work/other",
    )
    presence_commit = _insert_commit(
        db,
        "presence-sha",
        "2026-04-23T10:00:00+00:00",
        repo="taka/presence",
    )
    _insert_commit(
        db,
        "other-sha",
        "2026-04-23T11:00:00+00:00",
        repo="taka/other",
    )
    _link(db, presence_commit, linked_msg)

    report = build_claude_work_coverage_report(
        db,
        days=3,
        repo="taka/presence",
        now=NOW,
    )

    assert report["summary"]["commit_count"] == 1
    assert report["summary"]["claude_message_count"] == 1
    assert report["top_unlinked_commits"] == []
    assert [item["message_uuid"] for item in report["top_unlinked_messages"]] == []
    assert hidden_msg not in {item["id"] for item in report["top_unlinked_messages"]}


def test_limits_top_unlinked_items_by_recent_timestamp(db):
    _insert_commit(db, "old", "2026-04-23T10:00:00+00:00")
    _insert_commit(db, "new", "2026-04-24T10:00:00+00:00")
    _insert_message(db, "old-msg", "2026-04-23T09:00:00+00:00")
    _insert_message(db, "new-msg", "2026-04-24T09:00:00+00:00")

    report = build_claude_work_coverage_report(
        db,
        days=3,
        limit=1,
        now=NOW,
    )

    assert [item["commit_sha"] for item in report["top_unlinked_commits"]] == ["new"]
    assert [item["message_uuid"] for item in report["top_unlinked_messages"]] == [
        "new-msg"
    ]


def test_text_output_contains_flagged_sections(db):
    _insert_commit(db, "sha-1", "2026-04-23T10:00:00+00:00")
    _insert_commit(db, "sha-2", "2026-04-23T11:00:00+00:00")

    output = format_claude_work_coverage_text(
        build_claude_work_coverage_report(
            db,
            days=3,
            min_commits=2,
            now=NOW,
        )
    )

    assert "Claude Work Coverage" in output
    assert "Commit-heavy days with unlinked commits" in output
    assert "2026-04-23: commits=2 linked=0" in output
    assert "sha-2" in output


def test_cli_supports_json_repo_filter_and_limit(db, capsys):
    _insert_commit(db, "presence-sha", "2026-04-24T10:00:00+00:00")
    _insert_commit(db, "other-sha", "2026-04-24T10:00:00+00:00", repo="taka/other")

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("claude_work_coverage.script_context", fake_script_context):
        exit_code = coverage_cli(
            [
                "--repo",
                "taka/presence",
                "--days",
                "30",
                "--min-commits",
                "1",
                "--limit",
                "1",
                "--json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["window"]["repo"] == "taka/presence"
    assert payload["summary"]["commit_count"] == 1
    assert [item["commit_sha"] for item in payload["top_unlinked_commits"]] == [
        "presence-sha"
    ]
