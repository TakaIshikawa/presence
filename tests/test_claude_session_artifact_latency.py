"""Tests for Claude session artifact latency reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from ingestion.claude_session_artifact_latency import (
    build_claude_session_artifact_latency_report,
    format_claude_session_artifact_latency_json,
    format_claude_session_artifact_latency_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "claude_session_artifact_latency.py"
)
spec = importlib.util.spec_from_file_location("claude_session_artifact_latency_script", SCRIPT_PATH)
claude_session_artifact_latency_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(claude_session_artifact_latency_script)


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


def _content(db, source_messages: list[str], created_at: datetime) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=source_messages,
        content="Generated post",
        eval_score=0.8,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), content_id),
    )
    db.conn.commit()
    return content_id


def test_groups_sessions_and_computes_first_artifact_latencies(db):
    first = NOW - timedelta(hours=5)
    msg_a1 = _add_message(db, message_uuid="msg-a1", session_id="sess-a", timestamp=first)
    _add_message(
        db,
        message_uuid="msg-a2",
        session_id="sess-a",
        timestamp=first + timedelta(minutes=15),
    )
    msg_b = _add_message(
        db,
        message_uuid="msg-b1",
        session_id="sess-b",
        timestamp=NOW - timedelta(hours=4),
    )
    _add_message(
        db,
        message_uuid="msg-c1",
        session_id="sess-c",
        timestamp=NOW - timedelta(hours=3),
    )
    msg_d = _add_message(
        db,
        message_uuid="msg-d1",
        session_id="sess-d",
        timestamp=NOW - timedelta(hours=2),
    )

    _link(db, _add_commit(db, "sha-a-later", first + timedelta(hours=3)), msg_a1)
    _link(db, _add_commit(db, "sha-a-first", first + timedelta(hours=1)), msg_a1)
    _content(db, ["msg-b1"], NOW - timedelta(hours=3, minutes=30))
    _link(db, _add_commit(db, "sha-d", NOW - timedelta(hours=1, minutes=30)), msg_d)
    _content(db, ["msg-d1"], NOW - timedelta(hours=1))

    report = build_claude_session_artifact_latency_report(db, days=7, now=NOW)
    payload = json.loads(format_claude_session_artifact_latency_json(report))
    text = format_claude_session_artifact_latency_text(report)
    by_session = {session["session_id"]: session for session in payload["sessions"]}

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "claude_session_artifact_latency"
    assert payload["totals"] == {
        "commit_only": 1,
        "content_and_commit": 1,
        "content_only": 1,
        "messages_scanned": 5,
        "no_artifact": 1,
        "sessions_scanned": 4,
    }
    assert by_session["sess-a"]["message_count"] == 2
    assert by_session["sess-a"]["first_commit_sha"] == "sha-a-first"
    assert by_session["sess-a"]["first_commit_latency_seconds"] == 3600
    assert by_session["sess-a"]["status"] == "commit_only"
    assert by_session["sess-b"]["first_generated_content_latency_seconds"] == 1800
    assert by_session["sess-b"]["status"] == "content_only"
    assert by_session["sess-c"]["first_artifact_latency_seconds"] is None
    assert by_session["sess-c"]["status"] == "no_artifact"
    assert by_session["sess-d"]["first_artifact_latency_seconds"] == 1800
    assert by_session["sess-d"]["status"] == "content_and_commit"
    assert "Claude Session Artifact Latency" in text
    assert "session=sess-a" in text
    assert "commit_latency_s=3600" in text


def test_project_status_filter_limit_and_sorting_are_deterministic(db):
    msg_fast = _add_message(
        db,
        message_uuid="presence-fast",
        session_id="sess-presence-fast",
        project_path="/repo/presence",
        timestamp=NOW - timedelta(hours=3),
    )
    msg_slow = _add_message(
        db,
        message_uuid="presence-slow",
        session_id="sess-presence-slow",
        project_path="/repo/presence",
        timestamp=NOW - timedelta(hours=3),
    )
    msg_other = _add_message(
        db,
        message_uuid="other-fast",
        session_id="sess-other-fast",
        project_path="/repo/other",
        timestamp=NOW - timedelta(hours=3),
    )
    _link(db, _add_commit(db, "fast", NOW - timedelta(hours=2, minutes=45)), msg_fast)
    _link(db, _add_commit(db, "slow", NOW - timedelta(hours=2)), msg_slow)
    _link(db, _add_commit(db, "other", NOW - timedelta(hours=2, minutes=50)), msg_other)

    report = build_claude_session_artifact_latency_report(
        db,
        days=7,
        project_path="/repo/presence",
        status="commit_only",
        limit=1,
        now=NOW,
    )
    payload = report.to_dict()

    assert payload["filters"]["project_path_filter_applied"] is True
    assert payload["filters"]["status"] == "commit_only"
    assert payload["totals"]["sessions_scanned"] == 2
    assert [session["session_id"] for session in payload["sessions"]] == [
        "sess-presence-fast"
    ]


def test_generated_content_source_messages_are_parsed_defensively(db):
    _add_message(db, message_uuid="msg-covered", session_id="sess-covered")
    _content(db, ["msg-covered"], NOW + timedelta(minutes=5))
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

    report = build_claude_session_artifact_latency_report(db, days=7, now=NOW)
    payload = report.to_dict()

    assert payload["sessions"][0]["first_generated_content_latency_seconds"] == 300
    assert any("malformed source_messages" in warning for warning in payload["warnings"])
    assert any("non-list source_messages" in warning for warning in payload["warnings"])
    assert "Warnings:" in format_claude_session_artifact_latency_text(report)


def test_missing_optional_tables_and_columns_are_reported_gracefully():
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
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            source_messages TEXT
        );
        INSERT INTO claude_messages
            (id, session_id, message_uuid, project_path, timestamp)
        VALUES
            (1, 'sess-missing', 'msg-missing', '/repo/presence', '2026-05-02T10:00:00+00:00');
        INSERT INTO generated_content (id, source_messages) VALUES (20, '["msg-missing"]');
        """
    )

    report = build_claude_session_artifact_latency_report(conn, days=7, now=NOW)
    payload = report.to_dict()

    assert report.missing_tables == ("commit_prompt_links", "github_commits")
    assert payload["missing_columns"] == {"generated_content": ["created_at"]}
    assert payload["sessions"][0]["status"] == "no_artifact"
    assert "Missing optional tables: commit_prompt_links, github_commits" in (
        format_claude_session_artifact_latency_text(report)
    )
    assert "generated_content(created_at)" in format_claude_session_artifact_latency_text(report)


def test_cli_json_text_invalid_args_and_fail_on_no_artifacts(db, monkeypatch, capsys):
    _add_message(db, message_uuid="msg-cli", session_id="sess-cli")
    monkeypatch.setattr(
        claude_session_artifact_latency_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        claude_session_artifact_latency_script,
        "build_claude_session_artifact_latency_report",
        lambda db, **kwargs: build_claude_session_artifact_latency_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert claude_session_artifact_latency_script.main(
        ["--days", "7", "--limit", "5", "--status", "no_artifact", "--format", "json"]
    ) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["sessions"][0]["session_id"] == "sess-cli"
    assert cli_payload["filters"]["status"] == "no_artifact"

    assert claude_session_artifact_latency_script.main(["--format", "text"]) == 0
    assert "session=sess-cli" in capsys.readouterr().out

    assert claude_session_artifact_latency_script.main(["--fail-on-no-artifacts"]) == 1
    assert "no_artifact=1" in capsys.readouterr().out

    assert claude_session_artifact_latency_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert claude_session_artifact_latency_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
