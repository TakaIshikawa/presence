"""Tests for blog/newsletter timing alignment."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.blog_newsletter_timing_alignment import (
    build_blog_newsletter_timing_alignment_report,
    build_blog_newsletter_timing_alignment_report_from_db,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_newsletter_timing_alignment.py"
spec = importlib.util.spec_from_file_location("blog_newsletter_timing_alignment_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_classifies_early_aligned_late_and_unmatched():
    blogs = [
        {"id": 1, "title": "Agent Evaluation Notes", "url": "https://e.test/blog/agent-evaluation", "published_at": NOW.isoformat()},
        {"id": 2, "title": "Prompt Replay Bundle", "slug": "prompt-replay", "published_at": NOW.isoformat()},
        {"id": 3, "title": "Source Evidence", "published_at": NOW.isoformat()},
    ]
    newsletters = [
        {"id": "early", "subject": "Agent Evaluation Notes", "body": "https://e.test/blog/agent-evaluation", "sent_at": (NOW - timedelta(days=2)).isoformat()},
        {"id": "aligned", "subject": "Prompt Replay Bundle", "body": "prompt-replay", "sent_at": (NOW + timedelta(days=3)).isoformat()},
        {"id": "late", "subject": "Source Evidence", "sent_at": (NOW + timedelta(days=20)).isoformat()},
        {"id": "unmatched", "subject": "Totally Different", "sent_at": NOW.isoformat()},
    ]

    report = build_blog_newsletter_timing_alignment_report(blogs, newsletters, late_after_days=14, now=NOW)

    assert report["totals"]["early_count"] == 1
    assert report["totals"]["aligned_count"] == 1
    assert report["totals"]["late_count"] == 1
    assert report["totals"]["unmatched_count"] == 1
    assert {item["newsletter_id"]: item["status"] for item in report["findings"]} == {
        "early": "early",
        "aligned": "aligned",
        "late": "late",
        "unmatched": "unmatched",
    }


def test_db_loader_and_cli_outputs(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE blog_posts (id INTEGER, title TEXT, url TEXT, status TEXT, published_at TEXT)")
    conn.execute("CREATE TABLE newsletter_sends (id TEXT, subject TEXT, metadata TEXT, sent_at TEXT, status TEXT)")
    conn.execute(
        "INSERT INTO blog_posts VALUES (?, ?, ?, ?, ?)",
        (7, "Release Notes", "https://e.test/blog/release-notes", "published", NOW.isoformat()),
    )
    conn.execute(
        "INSERT INTO newsletter_sends VALUES (?, ?, ?, ?, ?)",
        ("n1", "Release Notes", json.dumps({"body": "Read https://e.test/blog/release-notes"}), (NOW + timedelta(days=1)).isoformat(), "sent"),
    )
    conn.commit()
    db = SimpleNamespace(conn=conn)

    report = build_blog_newsletter_timing_alignment_report_from_db(db, now=NOW)
    assert report["findings"][0]["status"] == "aligned"

    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_blog_newsletter_timing_alignment_report_from_db",
        lambda db, **kwargs: build_blog_newsletter_timing_alignment_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "blog_newsletter_timing_alignment"
    assert script.main(["--table"]) == 0
    assert "Blog Newsletter Timing Alignment" in capsys.readouterr().out
