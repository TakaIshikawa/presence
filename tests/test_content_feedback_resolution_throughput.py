"""Tests for content feedback resolution throughput reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.content_feedback_resolution_throughput import (
    build_content_feedback_resolution_throughput_report,
    format_content_feedback_resolution_throughput_json,
    format_content_feedback_resolution_throughput_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_feedback_resolution_throughput.py"
spec = importlib.util.spec_from_file_location("content_feedback_resolution_throughput_script", SCRIPT_PATH)
content_feedback_resolution_throughput_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_feedback_resolution_throughput_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, *, published: int = 0, curation_quality: str | None = None) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="copy",
        eval_score=7,
        eval_feedback="ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = ?, curation_quality = ? WHERE id = ?",
        (published, curation_quality, content_id),
    )
    db.conn.commit()
    return content_id


def _feedback(db, content_id: int, feedback_type: str, created_at: datetime, tags=None, replacement_text=None) -> int:
    feedback_id = db.add_content_feedback(
        content_id,
        feedback_type,
        "notes",
        replacement_text=replacement_text,
        tags=tags,
    )
    db.conn.execute(
        "UPDATE content_feedback SET created_at = ? WHERE id = ?",
        (created_at.isoformat(), feedback_id),
    )
    db.conn.commit()
    return feedback_id


def test_groups_resolution_by_feedback_type_tag_age_and_state(db):
    unresolved_content = _content(db)
    resolved_replacement = _content(db)
    resolved_curation = _content(db, curation_quality="good")
    resolved_published = _content(db, published=1)
    old_unresolved = _feedback(
        db,
        unresolved_content,
        "reject",
        NOW - timedelta(days=8),
        tags=["too_generic", "evidence"],
    )
    _feedback(
        db,
        resolved_replacement,
        "revise",
        NOW - timedelta(hours=5),
        tags=["evidence"],
        replacement_text="new copy",
    )
    _feedback(db, resolved_curation, "prefer", NOW - timedelta(days=2), tags=["hook"])
    _feedback(db, resolved_published, "reject", NOW - timedelta(days=4), tags=["too_generic"])

    report = build_content_feedback_resolution_throughput_report(db, days=30, now=NOW)
    payload = json.loads(format_content_feedback_resolution_throughput_json(report))
    text = format_content_feedback_resolution_throughput_text(report)

    assert payload["totals"]["feedback_count"] == 4
    assert payload["totals"]["resolved_count"] == 3
    assert payload["totals"]["unresolved_count"] == 1
    assert payload["tag_counts"] == {"evidence": 2, "hook": 1, "too_generic": 2}
    assert payload["oldest_unresolved"][0]["feedback_id"] == old_unresolved
    assert any(
        group["feedback_type"] == "reject"
        and group["tag"] == "evidence"
        and group["age_bucket"] == "7-14d"
        and group["resolution_state"] == "unresolved"
        for group in payload["groups"]
    )
    assert "Unresolved feedback by type and tag:" in text
    assert "type=reject tag=too_generic" in text


def test_malformed_tags_are_safe_and_reported(db):
    content_id = _content(db)
    feedback_id = _feedback(db, content_id, "revise", NOW - timedelta(days=1), tags=["valid"])
    db.conn.execute("UPDATE content_feedback SET tags = ? WHERE id = ?", ("not-json", feedback_id))
    db.conn.commit()

    report = build_content_feedback_resolution_throughput_report(db, now=NOW)

    assert report["totals"]["malformed_tag_rows"] == 1
    assert report["tag_counts"] == {"malformed": 1}
    assert report["groups"][0]["tag"] == "malformed"


def test_partial_schema_without_generated_content_does_not_crash():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE content_feedback (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            feedback_type TEXT,
            tags TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO content_feedback VALUES (?, ?, ?, ?, ?)",
        (1, 42, "reject", '["tag"]', NOW.isoformat()),
    )
    conn.commit()
    try:
        report = build_content_feedback_resolution_throughput_report(conn, now=NOW)
    finally:
        conn.close()

    assert report["missing_tables"] == ["generated_content"]
    assert report["totals"]["unresolved_count"] == 1


def test_cli_supports_json_output(db, monkeypatch, capsys):
    feedback_id = _feedback(db, _content(db), "reject", NOW - timedelta(hours=1), tags=["tone"])
    monkeypatch.setattr(content_feedback_resolution_throughput_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        content_feedback_resolution_throughput_script,
        "build_content_feedback_resolution_throughput_report",
        lambda db, **kwargs: build_content_feedback_resolution_throughput_report(db, now=NOW, **kwargs),
    )

    exit_code = content_feedback_resolution_throughput_script.main(["--format", "json", "--days", "7"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["oldest_unresolved"][0]["feedback_id"] == feedback_id


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(
        content_feedback_resolution_throughput_script,
        "script_context",
        lambda: _script_context(SimpleNamespace()),
    )
    monkeypatch.setattr(
        content_feedback_resolution_throughput_script,
        "build_content_feedback_resolution_throughput_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    assert content_feedback_resolution_throughput_script.main([]) == 1
    assert "error: db failed" in capsys.readouterr().err
