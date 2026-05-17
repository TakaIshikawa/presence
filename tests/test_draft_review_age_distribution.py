"""Tests for pending draft review age distribution."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.draft_review_age_distribution import (
    build_draft_review_age_distribution_report,
    format_draft_review_age_distribution_json,
    format_draft_review_age_distribution_text,
)


NOW = datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "draft_review_age_distribution.py"
spec = importlib.util.spec_from_file_location("draft_review_age_distribution_script", SCRIPT_PATH)
draft_review_age_distribution_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(draft_review_age_distribution_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            source_kind TEXT,
            title TEXT,
            created_at TEXT,
            review_status TEXT,
            published INTEGER
        )"""
    )
    return conn


def _draft(conn: sqlite3.Connection, content_type: str, source_kind: str, title: str, created_at: str, status: str = "pending_review", published: int = 0) -> None:
    conn.execute(
        """INSERT INTO generated_content
           (content_type, source_kind, title, created_at, review_status, published)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (content_type, source_kind, title, created_at, status, published),
    )
    conn.commit()


def test_buckets_pending_drafts_by_age_content_type_and_source_kind():
    conn = _conn()
    _draft(conn, "blog_post", "github", "Recent", "2026-05-18T00:00:00+00:00")
    _draft(conn, "blog_post", "github", "Three day", "2026-05-16T00:00:00+00:00")
    _draft(conn, "newsletter", "curated", "Old", "2026-05-10T00:00:00+00:00")
    _draft(conn, "newsletter", "curated", "Approved", "2026-05-01T00:00:00+00:00", status="approved")

    report = build_draft_review_age_distribution_report(conn, now=NOW)

    assert report["summary"]["pending_count"] == 3
    rows = {(row["content_type"], row["source_kind"], row["bucket"]): row for row in report["rows"]}
    assert rows[("blog_post", "github", "0-24h")]["item_count"] == 1
    assert rows[("blog_post", "github", "1-3d")]["item_count"] == 1
    assert rows[("newsletter", "curated", "7d+")]["oldest_item_age_hours"] == 204.0
    assert report["oldest_items"][0]["title"] == "Old"


def test_json_text_and_cli_output(monkeypatch, capsys):
    conn = _conn()
    _draft(conn, "blog_post", "github", "CLI", "2026-05-17T12:00:00+00:00")
    monkeypatch.setattr(draft_review_age_distribution_script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        draft_review_age_distribution_script,
        "build_draft_review_age_distribution_report",
        lambda db, **kwargs: build_draft_review_age_distribution_report(db, now=NOW, **kwargs),
    )

    report = build_draft_review_age_distribution_report(conn, now=NOW)
    payload = json.loads(format_draft_review_age_distribution_json(report))
    text = format_draft_review_age_distribution_text(report)
    exit_code = draft_review_age_distribution_script.main(["--oldest-limit", "1", "--format", "json"])
    cli_payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "draft_review_age_distribution"
    assert "Draft Review Age Distribution" in text
    assert cli_payload["filters"]["oldest_limit"] == 1
    assert exit_code == 0
