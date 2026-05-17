"""Tests for publication content age at publish reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.publication_content_age_at_publish import (
    build_publication_content_age_at_publish_report,
    format_publication_content_age_at_publish_json,
    format_publication_content_age_at_publish_text,
)


NOW = datetime(2026, 5, 18, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_content_age_at_publish.py"
spec = importlib.util.spec_from_file_location("publication_content_age_at_publish_script", SCRIPT_PATH)
publication_content_age_at_publish_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_content_age_at_publish_script)


@contextmanager
def _script_context(conn):
    yield SimpleNamespace(), conn


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY, content_type TEXT, content_format TEXT, created_at TEXT
        );
        CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, status TEXT, published_at TEXT
        );"""
    )
    return conn


def _publication(conn: sqlite3.Connection, content_id: int, created_at: datetime, published_at: datetime, platform: str = "blog", fmt: str = "post") -> None:
    conn.execute("INSERT INTO generated_content VALUES (?, 'blog_post', ?, ?)", (content_id, fmt, created_at.isoformat()))
    conn.execute("INSERT INTO content_publications VALUES (?, ?, ?, 'published', ?)", (content_id, content_id, platform, published_at.isoformat()))
    conn.commit()


def test_stale_publications_and_group_summaries():
    conn = _conn()
    _publication(conn, 1, NOW - timedelta(hours=100), NOW - timedelta(hours=1), "blog", "long")
    _publication(conn, 2, NOW - timedelta(hours=20), NOW - timedelta(hours=1), "x", "short")
    conn.execute("INSERT INTO content_publications VALUES (3, 2, 'x', 'queued', ?)", ((NOW - timedelta(hours=1)).isoformat(),))

    report = build_publication_content_age_at_publish_report(conn, now=NOW, stale_hours=72)

    assert report["totals"]["publication_count"] == 2
    assert report["stale_publications"][0]["content_id"] == 1
    assert [group["platform"] for group in report["grouped_summaries"]] == ["blog", "x"]


def test_json_text_cli_and_missing_schema(monkeypatch, capsys):
    conn = _conn()
    _publication(conn, 1, NOW - timedelta(hours=10), NOW - timedelta(hours=1))
    report = build_publication_content_age_at_publish_report(conn, now=NOW)

    assert json.loads(format_publication_content_age_at_publish_json(report))["artifact_type"] == "publication_content_age_at_publish"
    assert "Publication Content Age At Publish" in format_publication_content_age_at_publish_text(report)
    monkeypatch.setattr(publication_content_age_at_publish_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        publication_content_age_at_publish_script,
        "build_publication_content_age_at_publish_report",
        lambda db, **kwargs: build_publication_content_age_at_publish_report(db, now=NOW, **kwargs),
    )
    assert publication_content_age_at_publish_script.main(["--format", "text"]) == 0
    assert "Totals: publications=1" in capsys.readouterr().out

    missing = build_publication_content_age_at_publish_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["generated_content", "content_publications"]
