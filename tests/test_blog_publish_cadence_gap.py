"""Tests for blog publish cadence gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from evaluation.blog_publish_cadence_gap import (
    build_blog_publish_cadence_gap_report,
    build_blog_publish_cadence_gap_report_from_db,
    format_blog_publish_cadence_gap_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_publish_cadence_gap.py"
spec = importlib.util.spec_from_file_location("blog_publish_cadence_gap_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_regular_publication_history_is_ok():
    posts = [
        {"id": 1, "title": "One", "published_at": (NOW - timedelta(days=21)).isoformat()},
        {"id": 2, "title": "Two", "published_at": (NOW - timedelta(days=14)).isoformat()},
        {"id": 3, "title": "Three", "published_at": (NOW - timedelta(days=7)).isoformat()},
    ]

    report = build_blog_publish_cadence_gap_report(posts, warning_days=10, critical_days=20, now=NOW)

    assert report["totals"]["longest_gap_days"] == 7.0
    assert report["totals"]["average_gap_days"] == 7.0
    assert report["totals"]["overdue_status"] == "ok"
    assert report["flagged_gaps"] == []


def test_sparse_history_flags_gaps_and_threshold_changes():
    posts = [
        {"id": "a", "title": "Alpha", "published_at": (NOW - timedelta(days=60)).isoformat()},
        {"id": "b", "title": "Beta", "published_at": (NOW - timedelta(days=25)).isoformat()},
        {"id": "c", "title": "Gamma", "published_at": (NOW - timedelta(days=5)).isoformat()},
    ]

    report = build_blog_publish_cadence_gap_report(posts, warning_days=14, critical_days=30, now=NOW)
    assert report["totals"]["longest_gap_days"] == 35.0
    assert report["totals"]["average_gap_days"] == 27.5
    assert [gap["status"] for gap in report["gaps"]] == ["critical", "warning"]

    stricter = build_blog_publish_cadence_gap_report(posts, warning_days=7, critical_days=14, now=NOW)
    assert [gap["status"] for gap in stricter["gaps"]] == ["critical", "critical"]


def test_empty_history_and_cli_outputs(monkeypatch, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE blog_posts (id INTEGER PRIMARY KEY, title TEXT, status TEXT, published_at TEXT)"
    )
    db = SimpleNamespace(conn=conn)

    report = build_blog_publish_cadence_gap_report_from_db(db, now=NOW)
    assert report["empty_state"]["is_empty"] is True
    assert report["totals"]["overdue_status"] == "no_posts"
    assert "No published blog posts found." in format_blog_publish_cadence_gap_text(report)

    conn.execute(
        "INSERT INTO blog_posts (title, status, published_at) VALUES (?, ?, ?)",
        ("Cadence", "published", (NOW - timedelta(days=20)).isoformat()),
    )
    conn.commit()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        script,
        "build_blog_publish_cadence_gap_report_from_db",
        lambda db, **kwargs: build_blog_publish_cadence_gap_report_from_db(db, now=NOW, **kwargs),
    )

    assert script.main(["--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "blog_publish_cadence_gap"

    assert script.main(["--table"]) == 0
    assert "Blog Publish Cadence Gap" in capsys.readouterr().out
