"""Tests for Claude session outcome coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_session_outcome_coverage import (
    build_claude_session_outcome_coverage_report,
    format_claude_session_outcome_coverage_json,
    format_claude_session_outcome_coverage_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "claude_session_outcome_coverage.py"
)
spec = importlib.util.spec_from_file_location("claude_session_outcome_coverage_script", SCRIPT_PATH)
claude_session_outcome_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_outcome_coverage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_message(
    db,
    *,
    message_uuid: str,
    session_id: str,
    project_path: str = "/repo/presence",
    timestamp: datetime | None = None,
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=(timestamp or NOW).isoformat(),
        prompt_text=f"Work on {message_uuid}",
    )


def _add_commit(db, sha: str, timestamp: datetime | None = None) -> int:
    return db.insert_commit(
        repo_name="taka/presence",
        commit_sha=sha,
        commit_message=f"feat: {sha}",
        timestamp=(timestamp or NOW).isoformat(),
        author="taka",
    )


def _link(db, commit_id: int, message_id: int) -> None:
    db.conn.execute(
        "INSERT INTO commit_prompt_links (commit_id, message_id, confidence) VALUES (?, ?, ?)",
        (commit_id, message_id, 0.9),
    )
    db.conn.commit()


def _content(db, source_messages: list[str]) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=source_messages,
        content="Generated post",
        eval_score=0.8,
        eval_feedback="ok",
    )


def test_groups_sessions_and_classifies_downstream_outcomes(db):
    msg_a1 = _add_message(
        db,
        message_uuid="msg-a1",
        session_id="sess-a",
        timestamp=NOW - timedelta(hours=5),
    )
    _add_message(
        db,
        message_uuid="msg-a2",
        session_id="sess-a",
        timestamp=NOW - timedelta(hours=4),
    )
    msg_b = _add_message(
        db,
        message_uuid="msg-b1",
        session_id="sess-b",
        timestamp=NOW - timedelta(hours=3),
    )
    _add_message(
        db,
        message_uuid="msg-c1",
        session_id="sess-c",
        timestamp=NOW - timedelta(hours=2),
    )
    _add_message(
        db,
        message_uuid="msg-d1",
        session_id="sess-d",
        timestamp=NOW - timedelta(hours=1),
    )
    _link(db, _add_commit(db, "sha-a"), msg_a1)
    _link(db, _add_commit(db, "sha-b"), msg_b)
    _content(db, ["msg-b1"])
    db.add_content_idea(
        note="Turn Claude question into a post",
        source="claude",
        source_metadata={"source_messages": ["msg-c1"], "session_id": "sess-c"},
    )

    report = build_claude_session_outcome_coverage_report(db, days=7, now=NOW)
    payload = json.loads(format_claude_session_outcome_coverage_json(report))
    text = format_claude_session_outcome_coverage_text(report)
    by_session = {session["session_id"]: session for session in payload["sessions"]}

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_session_outcome_coverage"
    assert payload["totals"] == {
        "commit_no_content": 1,
        "content_generated": 1,
        "idea_only": 1,
        "messages_scanned": 5,
        "no_commit": 1,
        "sessions_scanned": 4,
    }
    assert by_session["sess-a"]["message_count"] == 2
    assert by_session["sess-a"]["linked_commit_count"] == 1
    assert by_session["sess-a"]["generated_content_count"] == 0
    assert by_session["sess-a"]["status"] == "commit_no_content"
    assert by_session["sess-b"]["generated_content_count"] == 1
    assert by_session["sess-b"]["status"] == "content_generated"
    assert by_session["sess-c"]["idea_count"] == 1
    assert by_session["sess-c"]["status"] == "idea_only"
    assert by_session["sess-d"]["status"] == "no_commit"
    assert "Claude Session Outcome Coverage" in text
    assert "session=sess-a" in text
    assert "status=commit_no_content" in text


def test_project_status_filter_and_limit_are_deterministic(db):
    _add_message(
        db,
        message_uuid="presence-old",
        session_id="sess-presence-old",
        project_path="/repo/presence",
        timestamp=NOW - timedelta(hours=2),
    )
    _add_message(
        db,
        message_uuid="presence-new",
        session_id="sess-presence-new",
        project_path="/repo/presence",
        timestamp=NOW - timedelta(hours=1),
    )
    _add_message(
        db,
        message_uuid="other-new",
        session_id="sess-other-new",
        project_path="/repo/other",
        timestamp=NOW - timedelta(minutes=30),
    )

    report = build_claude_session_outcome_coverage_report(
        db,
        days=7,
        project_path="/repo/presence",
        status="no_commit",
        limit=1,
        now=NOW,
    )
    payload = report.to_dict()

    assert payload["filters"]["project_path_filter_applied"] is True
    assert payload["filters"]["status"] == "no_commit"
    assert payload["totals"]["sessions_scanned"] == 2
    assert [session["session_id"] for session in payload["sessions"]] == [
        "sess-presence-old"
    ]


def test_generated_content_source_messages_are_parsed_defensively(db):
    _add_message(db, message_uuid="msg-covered", session_id="sess-covered")
    _content(db, ["msg-covered"])
    db.conn.execute(
        """INSERT INTO generated_content
           (content_type, source_messages, content, eval_score, eval_feedback)
           VALUES (?, ?, ?, ?, ?)""",
        ("x_post", "{bad json", "Bad", 0.1, "bad"),
    )
    db.conn.execute(
        """INSERT INTO generated_content
           (content_type, source_messages, content, eval_score, eval_feedback)
           VALUES (?, ?, ?, ?, ?)""",
        ("x_post", json.dumps({"message": "msg-covered"}), "Object", 0.1, "bad"),
    )
    db.conn.commit()

    report = build_claude_session_outcome_coverage_report(db, days=7, now=NOW)
    payload = report.to_dict()

    assert payload["sessions"][0]["generated_content_count"] == 1
    assert any("malformed source_messages" in warning for warning in payload["warnings"])
    assert any("non-list source_messages" in warning for warning in payload["warnings"])
    assert "Warnings:" in format_claude_session_outcome_coverage_text(report)


def test_missing_optional_content_ideas_still_reports_commit_and_content_coverage():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY,
            session_id TEXT,
            message_uuid TEXT,
            project_path TEXT,
            timestamp TEXT
        );
        CREATE TABLE github_commits (
            id INTEGER PRIMARY KEY,
            commit_sha TEXT
        );
        CREATE TABLE commit_prompt_links (
            commit_id INTEGER,
            message_id INTEGER
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            source_messages TEXT
        );
        INSERT INTO claude_messages
            (id, session_id, message_uuid, project_path, timestamp)
        VALUES
            (1, 'sess-linked', 'msg-linked', '/repo/presence', '2026-05-02T10:00:00+00:00'),
            (2, 'sess-content', 'msg-content', '/repo/presence', '2026-05-02T11:00:00+00:00');
        INSERT INTO github_commits (id, commit_sha) VALUES (10, 'sha-linked');
        INSERT INTO commit_prompt_links (commit_id, message_id) VALUES (10, 1);
        INSERT INTO generated_content (id, source_messages) VALUES (20, '["msg-content"]');
        """
    )

    report = build_claude_session_outcome_coverage_report(conn, days=7, now=NOW)
    by_session = {session.session_id: session for session in report.sessions}

    assert report.missing_tables == ("content_ideas",)
    assert by_session["sess-linked"].status == "commit_no_content"
    assert by_session["sess-linked"].linked_commit_count == 1
    assert by_session["sess-content"].status == "content_generated"
    assert by_session["sess-content"].generated_content_count == 1
    assert "Missing optional tables: content_ideas" in (
        format_claude_session_outcome_coverage_text(report)
    )


def test_cli_json_text_and_invalid_args_are_stable(db, monkeypatch, capsys):
    _add_message(db, message_uuid="msg-cli", session_id="sess-cli")
    monkeypatch.setattr(
        claude_session_outcome_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_session_outcome_coverage_script,
        "build_claude_session_outcome_coverage_report",
        lambda db, **kwargs: build_claude_session_outcome_coverage_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_session_outcome_coverage_script.main(
        ["--days", "7", "--limit", "5", "--status", "no_commit", "--format", "json"]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["sessions"][0]["session_id"] == "sess-cli"
    assert cli_payload["filters"]["status"] == "no_commit"

    assert claude_session_outcome_coverage_script.main(["--format", "text"]) == 0
    assert "session=sess-cli" in capsys.readouterr().out

    assert claude_session_outcome_coverage_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert claude_session_outcome_coverage_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
