"""Tests for Claude Code blocker idea seeding."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from synthesis.claude_blocker_idea_seeder import (
    SOURCE_NAME,
    build_claude_blocker_idea_candidates,
    extract_claude_blocker_candidates_from_rows,
    seed_claude_blocker_ideas,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "seed_claude_blocker_ideas.py"
spec = importlib.util.spec_from_file_location("seed_claude_blocker_ideas_script", SCRIPT_PATH)
seed_claude_blocker_ideas_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(seed_claude_blocker_ideas_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _add_message(
    db,
    *,
    message_uuid: str,
    prompt_text: str,
    session_id: str = "sess-1",
    project_path: str = "/repo/presence",
    timestamp: str = "2026-05-01T12:00:00+00:00",
) -> int:
    return db.insert_claude_message(
        session_id=session_id,
        message_uuid=message_uuid,
        project_path=project_path,
        timestamp=timestamp,
        prompt_text=prompt_text,
    )


def test_extracts_blocker_candidates_with_required_review_fields():
    rows = [
        {
            "id": 10,
            "session_id": "sess-a",
            "message_uuid": "uuid-a",
            "project_path": "/repo/presence",
            "timestamp": "2026-05-01T10:00:00+00:00",
            "prompt_text": "The export is blocked waiting on a schema decision for content ideas.",
        }
    ]

    candidates = extract_claude_blocker_candidates_from_rows(rows)

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate.session_id == "sess-a"
    assert "blocked waiting on a schema decision" in candidate.blocker_excerpt
    assert candidate.confidence >= 0.8
    assert "decision" in candidate.suggested_angle.lower() or "dependency" in candidate.suggested_angle.lower()
    assert candidate.source_key.startswith("claude_blocker_")
    assert candidate.source_metadata["source"] == SOURCE_NAME
    assert candidate.source_metadata["source_key"] == candidate.source_key


def test_build_candidates_scans_recent_claude_rows_and_filters_resolved_items(db):
    kept_id = _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="TODO: unresolved work to capture failing publish retries before release.",
    )
    _add_message(
        db,
        message_uuid="uuid-2",
        prompt_text="Resolved: failing publish retries are fixed and verified.",
    )

    candidates = build_claude_blocker_idea_candidates(db, days=7, now=NOW)

    assert len(candidates) == 1
    assert candidates[0].message_id == kept_id
    assert candidates[0].message_uuid == "uuid-1"
    assert candidates[0].signal_type == "failing"


def test_dry_run_reports_candidates_without_writing_to_database(db):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Cannot reproduce the queue timeout locally; waiting on production logs.",
    )

    results = seed_claude_blocker_ideas(db, days=7, dry_run=True, now=NOW)

    assert [result.status for result in results] == ["proposed"]
    assert results[0].idea_id is None
    assert results[0].source_metadata["source_key"] == results[0].source_key
    assert db.get_content_ideas(status="open") == []


def test_non_dry_run_inserts_only_when_content_ideas_table_exists():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            message_uuid TEXT UNIQUE NOT NULL,
            project_path TEXT,
            timestamp TEXT NOT NULL,
            prompt_text TEXT NOT NULL
        )"""
    )
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text)
           VALUES ('sess-raw', 'uuid-raw', '/repo', '2026-05-01T12:00:00+00:00',
                   'Needs decision on whether the digest should skip archived blockers.')"""
    )
    conn.commit()

    results = seed_claude_blocker_ideas(conn, days=7, dry_run=False, now=NOW)

    assert [result.status for result in results] == ["skipped"]
    assert results[0].reason == "missing content_ideas table"
    assert conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'content_ideas'"
    ).fetchone() is None
    conn.close()


def test_repeated_runs_do_not_duplicate_same_session_blocker(db):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Blocked on API contract review before enabling the newsletter exporter.",
    )

    first = seed_claude_blocker_ideas(db, days=7, now=NOW)
    second = seed_claude_blocker_ideas(db, days=7, now=NOW)

    assert [result.status for result in first] == ["created"]
    assert [result.status for result in second] == ["skipped"]
    assert second[0].idea_id == first[0].idea_id
    assert second[0].reason == "open duplicate"
    ideas = db.get_content_ideas(status="open")
    assert len(ideas) == 1
    metadata = json.loads(ideas[0]["source_metadata"])
    assert metadata["source"] == SOURCE_NAME
    assert metadata["source_key"] == first[0].source_key


def test_same_blocker_text_in_different_sessions_gets_distinct_source_key(db):
    prompt = "Waiting on design review for the campaign brief export."
    _add_message(db, message_uuid="uuid-1", session_id="sess-a", prompt_text=prompt)
    _add_message(db, message_uuid="uuid-2", session_id="sess-b", prompt_text=prompt)

    candidates = build_claude_blocker_idea_candidates(db, days=7, now=NOW)

    assert len(candidates) == 2
    assert len({candidate.source_key for candidate in candidates}) == 2


def test_cli_supports_db_dry_run_and_json_format(tmp_path, capsys):
    db_path = tmp_path / "claude.db"
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE claude_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT NOT NULL,
            message_uuid TEXT UNIQUE NOT NULL,
            project_path TEXT,
            timestamp TEXT NOT NULL,
            prompt_text TEXT NOT NULL
        )"""
    )
    conn.execute(
        """INSERT INTO claude_messages
           (session_id, message_uuid, project_path, timestamp, prompt_text)
           VALUES ('sess-cli', 'uuid-cli', '/repo', '2026-05-01T12:00:00+00:00',
                   'Still failing in CI and cannot reproduce locally.')"""
    )
    conn.commit()
    conn.close()

    exit_code = seed_claude_blocker_ideas_script.main(
        ["--db", str(db_path), "--days", "7", "--limit", "3", "--dry-run", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload[0]["status"] == "proposed"
    assert payload[0]["session_id"] == "sess-cli"
    assert payload[0]["source_metadata"]["source"] == SOURCE_NAME


def test_cli_uses_configured_database_context(db, monkeypatch, capsys):
    _add_message(
        db,
        message_uuid="uuid-1",
        prompt_text="Waiting on product decision before publishing the release notes.",
    )
    monkeypatch.setattr(
        seed_claude_blocker_ideas_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        seed_claude_blocker_ideas_script,
        "seed_claude_blocker_ideas",
        lambda db, **kwargs: seed_claude_blocker_ideas(db, now=NOW, **kwargs),
    )

    exit_code = seed_claude_blocker_ideas_script.main(
        ["--days", "7", "--limit", "5", "--dry-run", "--format", "text"]
    )
    output = capsys.readouterr().out

    assert exit_code == 0
    assert "created=0 proposed=1 skipped=0" in output
    assert "sess-1" in output
    assert db.get_content_ideas(status="open") == []
