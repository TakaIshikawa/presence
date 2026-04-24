"""Tests for persisted redaction audit helpers and CLI formatting."""

from __future__ import annotations

import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from ingestion.redaction_audit import (
    SUPPORTED_TABLES,
    audit_redaction_leaks,
    build_audit_payload,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "redaction_audit.py"
SCRIPT_SPEC = importlib.util.spec_from_file_location("redaction_audit_script", SCRIPT_PATH)
redaction_audit_script = importlib.util.module_from_spec(SCRIPT_SPEC)
assert SCRIPT_SPEC and SCRIPT_SPEC.loader
SCRIPT_SPEC.loader.exec_module(redaction_audit_script)


NOW = datetime(2026, 4, 25, 12, 0, tzinfo=timezone.utc)
RECENT = (NOW - timedelta(days=1)).isoformat()
OLD = (NOW - timedelta(days=45)).isoformat()


def _insert_supported_rows(db) -> dict[str, str]:
    secrets = {
        "github_token": "ghp_abcdefghijklmnopqrstuvwxyz123456",
        "email": "dev@example.com",
        "path": "/Users/taka/Project/app/.env",
        "bearer": "Bearer abcdefghijklmnopqrstuvwxyz",
        "assignment": "password=super-secret-value",
    }
    db.insert_claude_message(
        "sess-1",
        "uuid-1",
        "/Users/taka/Project/app",
        RECENT,
        f"Rotate {secrets['github_token']}",
    )
    db.insert_claude_message("sess-old", "uuid-old", "/tmp/project", OLD, secrets["email"])
    db.upsert_github_activity(
        repo_name="acme/widget",
        activity_type="issue",
        number=1,
        title="Investigate private path",
        body=f"Log came from {secrets['path']}",
        state="open",
        author="octocat",
        url="https://github.example/1",
        updated_at=RECENT,
    )
    db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=f"Do not publish {secrets['bearer']}",
        eval_score=8,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (RECENT, 1),
    )
    db.insert_reply_draft(
        inbound_tweet_id="reply-1",
        inbound_author_handle="alice",
        inbound_author_id="a1",
        inbound_text=f"Here is {secrets['assignment']}",
        our_tweet_id="tweet-1",
        our_content_id=None,
        our_post_text="safe original",
        draft_text="safe draft",
    )
    db.conn.execute(
        "UPDATE reply_queue SET detected_at = ? WHERE id = ?",
        (RECENT, 1),
    )
    db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, author, content, insight, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "own_conversation",
            "k1",
            "dev@example.com",
            "Safe content",
            "Safe insight",
            RECENT,
        ),
    )
    db.conn.commit()
    return secrets


def test_audit_scans_supported_tables_and_redacts_previews(db):
    secrets = _insert_supported_rows(db)

    matches = audit_redaction_leaks(db.conn, days=30, now=NOW)
    payload = build_audit_payload(matches, days=30)

    assert payload["total_matches"] >= 5
    assert set(payload["matches"]) == set(SUPPORTED_TABLES)
    assert {match.table for match in matches} == set(SUPPORTED_TABLES)
    assert not any(match.row_id == 2 for match in matches if match.table == "claude_messages")

    serialized = json.dumps(payload)
    for raw_value in secrets.values():
        assert raw_value not in serialized
    assert "[REDACTED_SECRET]" in serialized
    assert "[REDACTED_PATH]" in serialized
    assert "[REDACTED_EMAIL]" in serialized
    assert "[REDACTED_BEARER]" in serialized


def test_audit_can_limit_to_one_table(db):
    _insert_supported_rows(db)

    matches = audit_redaction_leaks(db.conn, days=30, tables=("knowledge",), now=NOW)

    assert matches
    assert {match.table for match in matches} == {"knowledge"}


def test_cli_json_payload_and_fail_on_match_do_not_include_raw_values(db, monkeypatch, capsys):
    secrets = _insert_supported_rows(db)
    config = SimpleNamespace(privacy=SimpleNamespace(redaction_patterns=None))

    class Context:
        def __enter__(self):
            return config, db

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(redaction_audit_script, "script_context", lambda: Context())

    exit_code = redaction_audit_script.main(
        ["--days", "30", "--table", "claude_messages", "--format", "json", "--fail-on-match"]
    )

    assert exit_code == 1
    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["total_matches"] == 2
    assert list(payload["matches"]) == ["claude_messages"]
    for raw_value in secrets.values():
        assert raw_value not in output


def test_cli_returns_zero_without_matches(db, monkeypatch, capsys):
    config = SimpleNamespace(privacy=SimpleNamespace(redaction_patterns=None))

    class Context:
        def __enter__(self):
            return config, db

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(redaction_audit_script, "script_context", lambda: Context())

    exit_code = redaction_audit_script.main(["--days", "30", "--format", "text", "--fail-on-match"])

    assert exit_code == 0
    assert "No redaction pattern matches found." in capsys.readouterr().out
