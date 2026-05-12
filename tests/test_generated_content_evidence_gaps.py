"""Tests for generated content evidence gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from storage.db import Database

from evaluation.generated_content_evidence_gaps import (
    build_generated_content_evidence_gaps_report,
    format_generated_content_evidence_gaps_json,
    format_generated_content_evidence_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "generated_content_evidence_gaps.py"
)
spec = importlib.util.spec_from_file_location(
    "generated_content_evidence_gaps_script",
    SCRIPT_PATH,
)
generated_content_evidence_gaps_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(generated_content_evidence_gaps_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _set_created_at(db, content_id: int, created_at: datetime = NOW) -> None:
    db.conn.execute(
        "UPDATE generated_content SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), content_id),
    )
    db.conn.commit()


def _complete_content(db) -> int:
    db.insert_commit(
        "presence",
        "sha-complete",
        "feat: complete evidence",
        "2026-05-01T11:00:00+00:00",
        "taka",
    )
    db.insert_claude_message(
        "session-complete",
        "msg-complete",
        "/repo",
        "2026-05-01T10:55:00+00:00",
        "Generate complete evidence",
    )
    db.upsert_github_activity(
        repo_name="presence",
        activity_type="issue",
        number=7,
        title="Complete evidence",
        state="closed",
        author="taka",
        url="https://github.test/presence/issues/7",
        updated_at="2026-05-01T10:50:00+00:00",
    )
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-complete"],
        source_messages=["msg-complete"],
        source_activity_ids=["presence#7:issue"],
        content="Complete evidence copy.",
        eval_score=8.0,
        eval_feedback="grounded",
        claim_check_summary={"supported_count": 1, "unsupported_count": 0},
        persona_guard_summary={"checked": True, "passed": True, "status": "passed"},
    )
    db.add_content_feedback(content_id, "prefer", "Strong source coverage.")
    _set_created_at(db, content_id)
    return content_id


def test_empty_db_has_no_gap_groups(db):
    report = build_generated_content_evidence_gaps_report(db, days=7, now=NOW)

    assert report.gap_groups == ()
    assert report.totals["rows_scanned"] == 0
    assert report.to_dict()["artifact_type"] == "generated_content_evidence_gaps"


def test_complete_evidence_has_no_gaps(db):
    _complete_content(db)

    report = build_generated_content_evidence_gaps_report(db, days=7, now=NOW)

    assert report.has_gaps is False
    assert report.totals["rows_scanned"] == 1
    assert "No generated content evidence gaps" in format_generated_content_evidence_gaps_text(report)


def test_missing_evidence_groups_by_content_row(db):
    content_id = db.insert_generated_content(
        content_type="x_thread",
        source_commits=[],
        source_messages=[],
        source_activity_ids=[],
        content="Unsupported copy.",
        eval_score=5.0,
        eval_feedback="thin",
    )
    _set_created_at(db, content_id)

    report = build_generated_content_evidence_gaps_report(db, days=7, now=NOW)
    payload = json.loads(format_generated_content_evidence_gaps_json(report))
    text = format_generated_content_evidence_gaps_text(report)

    assert payload["totals"]["gap_group_count"] == 1
    assert payload["gap_groups"][0]["content_id"] == content_id
    assert payload["gap_groups"][0]["content_type"] == "x_thread"
    assert payload["gap_groups"][0]["created_at"] == NOW.isoformat()
    assert payload["gap_groups"][0]["missing_areas"] == [
        "source_commits",
        "source_messages",
        "github_activity",
        "claim_check",
        "persona_guard",
        "feedback",
    ]
    assert f"content_id={content_id}" in text
    assert "missing=source_commits, source_messages, github_activity" in text


def test_malformed_optional_data_is_tolerated():
    db = Database(":memory:")
    db.connect()
    db.conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            content_type TEXT,
            source_commits TEXT,
            source_messages TEXT,
            source_activity_ids TEXT,
            content TEXT,
            created_at TEXT
        )"""
    )
    db.conn.execute(
        """INSERT INTO generated_content
           (content_type, source_commits, source_messages, source_activity_ids, content, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("x_post", "{bad-json", "[]", "{}", "Partial evidence.", NOW.isoformat()),
    )
    db.conn.commit()

    try:
        report = build_generated_content_evidence_gaps_report(db, days=7, now=NOW)
    finally:
        db.close()

    assert report.totals["malformed_source_field_count"] == 2
    assert "github_commits" in report.missing_tables
    assert report.gap_groups[0].content_id == 1
    assert "claim_check" in report.gap_groups[0].missing_areas


def test_cli_uses_database_context_and_json_output(db, monkeypatch, capsys):
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        source_activity_ids=[],
        content="CLI gap.",
        eval_score=5.0,
        eval_feedback="thin",
    )
    _set_created_at(db, content_id)
    monkeypatch.setattr(
        generated_content_evidence_gaps_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        generated_content_evidence_gaps_script,
        "build_generated_content_evidence_gaps_report",
        lambda db, **kwargs: build_generated_content_evidence_gaps_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = generated_content_evidence_gaps_script.main(
        ["--days", "7", "--limit", "5", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["limit"] == 5
    assert payload["gap_groups"][0]["content_id"] == content_id


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(
        generated_content_evidence_gaps_script,
        "script_context",
        lambda: _script_context(SimpleNamespace()),
    )
    monkeypatch.setattr(
        generated_content_evidence_gaps_script,
        "build_generated_content_evidence_gaps_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    exit_code = generated_content_evidence_gaps_script.main([])

    assert exit_code == 1
    assert "error: db failed" in capsys.readouterr().err
