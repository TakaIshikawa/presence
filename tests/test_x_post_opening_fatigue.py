"""Tests for X post opening fatigue reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.x_post_opening_fatigue import (
    analyze_x_post_opening_fatigue,
    build_x_post_opening_fatigue_report,
    extract_x_post_opening_clause,
    format_x_post_opening_fatigue_json,
    format_x_post_opening_fatigue_markdown,
    normalize_x_post_opening,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "x_post_opening_fatigue.py"
spec = importlib.util.spec_from_file_location("x_post_opening_fatigue_script", SCRIPT_PATH)
x_post_opening_fatigue_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(x_post_opening_fatigue_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _row(post_id: int, content: str, published_at: datetime | str):
    timestamp = published_at if isinstance(published_at, str) else published_at.isoformat()
    return {
        "post_id": post_id,
        "content": content,
        "published_at": timestamp,
    }


def _publication_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT NOT NULL,
            content TEXT NOT NULL
        );
        CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER NOT NULL,
            platform TEXT NOT NULL,
            status TEXT NOT NULL,
            platform_post_id TEXT,
            platform_url TEXT,
            published_at TEXT
        );
        """
    )
    return conn


def _insert_post(
    conn: sqlite3.Connection,
    post_id: int,
    content: str,
    published_at: datetime,
    *,
    platform: str = "x",
    status: str = "published",
    content_type: str = "x_post",
) -> None:
    conn.execute(
        "INSERT INTO generated_content (id, content_type, content) VALUES (?, ?, ?)",
        (post_id, content_type, content),
    )
    conn.execute(
        """INSERT INTO content_publications
           (content_id, platform, status, platform_post_id, platform_url, published_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            post_id,
            platform,
            status,
            f"x-{post_id}",
            f"https://x.example/{post_id}",
            published_at.isoformat(),
        ),
    )
    conn.commit()


def test_opening_extraction_normalizes_urls_case_and_punctuation():
    opening = extract_x_post_opening_clause(
        "Here is the thing: https://example.com Extra context follows."
    )

    assert opening == "Here is the thing"
    assert (
        normalize_x_post_opening("HERE is the thing!!! https://example.com")
        == "here is the thing"
    )


def test_analyze_groups_similar_recent_openings_and_formats_markdown():
    report = analyze_x_post_opening_fatigue(
        [
            _row(
                1,
                "Here is the thing: retries hide queue pressure.",
                NOW - timedelta(hours=1),
            ),
            _row(
                2,
                "Here is the thing - retries hide real incidents.",
                NOW - timedelta(hours=2),
            ),
            _row(3, "Here is the catch: dashboards reward silence.", NOW - timedelta(hours=3)),
            _row(4, "A different opening wins today.", NOW - timedelta(hours=4)),
            _row(5, "Here is the thing: too old to count.", NOW - timedelta(days=40)),
        ],
        days=7,
        limit=10,
        now=NOW,
    )
    payload = json.loads(format_x_post_opening_fatigue_json(report))
    markdown = format_x_post_opening_fatigue_markdown(report)

    assert payload["artifact_type"] == "x_post_opening_fatigue"
    assert list(payload) == sorted(payload)
    assert payload["filters"]["days"] == 7
    assert payload["totals"] == {
        "clusters": 1,
        "posts_scanned": 5,
        "posts_with_opening": 4,
        "repeated_posts": 3,
    }
    cluster = payload["clusters"][0]
    assert cluster["normalized_opening"] in {"here is the thing", "here is the catch"}
    assert cluster["count"] == 3
    assert cluster["example_post_ids"] == [1, 2, 3]
    assert "# X Post Opening Fatigue" in markdown
    assert "## Repeated Opening Clusters" in markdown
    assert "post #1 at 2026-05-02T11:00:00+00:00" in markdown
    assert "`here is the thing`" in markdown


def test_database_report_reads_published_x_posts_and_skips_other_rows():
    conn = _publication_db()
    _insert_post(
        conn,
        1,
        "Let me say this: queues need boring metrics.",
        NOW - timedelta(hours=1),
    )
    _insert_post(
        conn,
        2,
        "Let me say this, retries are product work.",
        NOW - timedelta(hours=2),
    )
    _insert_post(
        conn,
        3,
        "Let me say this: this belongs elsewhere.",
        NOW - timedelta(hours=3),
        platform="bluesky",
    )
    _insert_post(
        conn,
        4,
        "Let me say this: drafts are not published.",
        NOW - timedelta(hours=4),
        status="queued",
    )
    _insert_post(
        conn,
        5,
        "Let me say this: threads are separate.",
        NOW - timedelta(hours=5),
        content_type="x_thread",
    )

    report = build_x_post_opening_fatigue_report(conn, days=7, now=NOW)

    assert report.missing_tables == ()
    assert report.missing_columns == {}
    assert report.totals["posts_scanned"] == 2
    assert len(report.clusters) == 1
    assert report.clusters[0].example_post_ids == (1, 2)
    assert report.clusters[0].examples[0].platform_post_id == "x-1"


def test_schema_gaps_return_empty_report_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_x_post_opening_fatigue_report(conn, now=NOW)

    assert report.clusters == ()
    assert report.missing_tables == ("content_publications", "generated_content")
    assert report.totals["posts_scanned"] == 0
    assert (
        "Missing tables: content_publications, generated_content"
        in format_x_post_opening_fatigue_markdown(report)
    )

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")
    partial.execute("CREATE TABLE content_publications (content_id INTEGER, platform TEXT)")
    column_report = build_x_post_opening_fatigue_report(partial, now=NOW)

    assert column_report.missing_columns == {
        "content_publications": ("published_at",),
        "generated_content": ("content",),
    }


def test_cli_json_and_invalid_positive_integer_args(db, monkeypatch, capsys):
    monkeypatch.setattr(
        x_post_opening_fatigue_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        x_post_opening_fatigue_script,
        "build_x_post_opening_fatigue_report",
        lambda db, **kwargs: analyze_x_post_opening_fatigue(
            [
                _row(1, "Watch the queue: one post.", NOW - timedelta(minutes=2)),
                _row(2, "Watch the queue: another post.", NOW - timedelta(minutes=1)),
            ],
            now=NOW,
            **kwargs,
        ),
    )

    assert (
        x_post_opening_fatigue_script.main(
            ["--days", "7", "--limit", "5", "--format", "json"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["clusters"][0]["example_post_ids"] == [2, 1]

    assert x_post_opening_fatigue_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert x_post_opening_fatigue_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
