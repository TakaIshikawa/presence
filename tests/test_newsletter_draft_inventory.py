"""Tests for newsletter draft inventory reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.newsletter_draft_inventory import (
    build_newsletter_draft_inventory_report,
    format_newsletter_draft_inventory_json,
    format_newsletter_draft_inventory_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_draft_inventory.py"
spec = importlib.util.spec_from_file_location("newsletter_draft_inventory_script", SCRIPT_PATH)
newsletter_draft_inventory_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_draft_inventory_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "newsletter",
    source_commits=None,
    source_messages=None,
    source_activity_ids=None,
    eval_score=7,
    published: int = 0,
    created_at: datetime | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=source_commits if source_commits is not None else ["abc"],
        source_messages=source_messages if source_messages is not None else [],
        content="newsletter copy",
        eval_score=eval_score,
        eval_feedback="ok",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET source_activity_ids = ?, published = ?, created_at = ?
           WHERE id = ?""",
        (
            json.dumps(source_activity_ids if source_activity_ids is not None else []),
            published,
            (created_at or NOW).isoformat(),
            content_id,
        ),
    )
    db.conn.commit()
    return content_id


def test_classifies_newsletter_inventory_and_source_coverage(db):
    ready = _content(db, created_at=NOW - timedelta(days=1))
    needs_sources = _content(db, source_commits=[], source_messages=[], source_activity_ids=[], created_at=NOW)
    needs_eval = _content(db, eval_score=None, created_at=NOW - timedelta(days=2))
    stale = _content(db, created_at=NOW - timedelta(days=20))
    sent = _content(db, published=1, created_at=NOW - timedelta(days=3))
    abandoned = _content(db, published=-1, created_at=NOW - timedelta(days=4))
    variant = _content(db, content_type="x_post", created_at=NOW - timedelta(days=1))
    db.conn.execute(
        "INSERT INTO content_variants (content_id, platform, variant_type, content) VALUES (?, ?, ?, ?)",
        (variant, "newsletter", "summary", "variant"),
    )
    db.conn.commit()

    report = build_newsletter_draft_inventory_report(db, stale_days=14, now=NOW)
    payload = json.loads(format_newsletter_draft_inventory_json(report))
    text = format_newsletter_draft_inventory_text(report)
    statuses = {item["content_id"]: item["inventory_status"] for item in payload["items"]}

    assert statuses[ready] == "draft_ready"
    assert statuses[needs_sources] == "needs_sources"
    assert statuses[needs_eval] == "needs_eval"
    assert statuses[stale] == "stale_draft"
    assert statuses[sent] == "sent"
    assert statuses[abandoned] == "abandoned"
    assert statuses[variant] == "draft_ready"
    assert payload["totals"]["status_counts"]["draft_ready"] == 2
    assert payload["representative_content_ids"]["needs_sources"] == [needs_sources]
    assert "content_id=" in text


def test_malformed_source_fields_are_reported(db):
    content_id = _content(db)
    db.conn.execute(
        "UPDATE generated_content SET source_commits = ?, source_messages = ? WHERE id = ?",
        ("not-json", '{"bad": true}', content_id),
    )
    db.conn.commit()

    report = build_newsletter_draft_inventory_report(db, now=NOW)

    assert report["items"][0]["inventory_status"] == "needs_sources"
    assert report["totals"]["malformed_source_fields"] == {
        "source_commits": 1,
        "source_messages": 1,
    }


def test_missing_content_variants_is_optional_metadata():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            source_commits TEXT,
            source_messages TEXT,
            source_activity_ids TEXT,
            content TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO generated_content VALUES (?, ?, ?, ?, ?, ?)",
        (1, "newsletter", '["abc"]', "[]", "[]", "copy"),
    )
    conn.commit()
    try:
        report = build_newsletter_draft_inventory_report(conn, now=NOW)
    finally:
        conn.close()

    assert report["missing_optional_table_metadata"] == ["content_variants"]
    assert report["items"][0]["inventory_status"] == "needs_eval"


def test_cli_supports_json_output(db, monkeypatch, capsys):
    content_id = _content(db)
    monkeypatch.setattr(newsletter_draft_inventory_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        newsletter_draft_inventory_script,
        "build_newsletter_draft_inventory_report",
        lambda db, **kwargs: build_newsletter_draft_inventory_report(db, now=NOW, **kwargs),
    )

    exit_code = newsletter_draft_inventory_script.main(["--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["items"][0]["content_id"] == content_id


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(newsletter_draft_inventory_script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        newsletter_draft_inventory_script,
        "build_newsletter_draft_inventory_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    assert newsletter_draft_inventory_script.main([]) == 1
    assert "error: db failed" in capsys.readouterr().err
