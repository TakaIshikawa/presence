"""Tests for X post opening-hook diversity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from synthesis.hook_diversity import (
    build_hook_diversity_report,
    extract_opening_hook,
    format_hook_diversity_json,
    format_hook_diversity_text,
    hook_similarity,
    normalize_opening_hook,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "hook_diversity.py"
spec = importlib.util.spec_from_file_location("hook_diversity_script", SCRIPT_PATH)
hook_diversity_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(hook_diversity_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _row(post_id: int, content: str, seen_at: datetime | str):
    timestamp = seen_at if isinstance(seen_at, str) else seen_at.isoformat()
    return {"post_id": post_id, "content": content, "created_at": timestamp}


def _publication_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT NOT NULL,
            content TEXT NOT NULL,
            created_at TEXT,
            published_at TEXT
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
    created_at: datetime,
    *,
    published_at: datetime | None = None,
    platform: str = "x",
    content_type: str = "x_post",
) -> None:
    conn.execute(
        """INSERT INTO generated_content
           (id, content_type, content, created_at, published_at)
           VALUES (?, ?, ?, ?, ?)""",
        (
            post_id,
            content_type,
            content,
            created_at.isoformat(),
            published_at.isoformat() if published_at else None,
        ),
    )
    if published_at is not None:
        conn.execute(
            """INSERT INTO content_publications
               (content_id, platform, status, platform_post_id, platform_url, published_at)
               VALUES (?, ?, 'published', ?, ?, ?)""",
            (
                post_id,
                platform,
                f"x-{post_id}",
                f"https://x.example/{post_id}",
                published_at.isoformat(),
            ),
        )
    conn.commit()


def test_opening_hook_extraction_normalizes_urls_case_and_punctuation():
    hook = extract_opening_hook(
        "Watch the queue pressure: https://example.com more details follow."
    )

    assert hook == "Watch the queue pressure"
    assert normalize_opening_hook("WATCH the queue!!! https://example.com") == "watch the queue"


def test_unique_hooks_are_not_flagged():
    report = build_hook_diversity_report(
        post_records=[
            _row(1, "Queue retries hide outages when dashboards only show success.", NOW),
            _row(2, "A quiet deploy can still erase the signal engineers need.", NOW),
            _row(3, "Metrics should explain user pain before paging anyone.", NOW),
        ],
        now=NOW,
    )

    assert report.ok is True
    assert report.blocking_issue_count == 0
    assert report.totals["posts_with_hook"] == 3
    assert report.clusters == ()


def test_repeated_first_clauses_are_flagged_with_required_cluster_fields():
    report = build_hook_diversity_report(
        post_records=[
            _row(1, "Here is the thing: retries hide queue pressure.", NOW - timedelta(minutes=3)),
            _row(2, "Here is the thing - retries hide real incidents.", NOW - timedelta(minutes=1)),
            _row(3, "Different opener for a different point.", NOW - timedelta(minutes=2)),
            _row(4, "Here is the thing: this old post is outside the window.", NOW - timedelta(days=40)),
        ],
        days=7,
        threshold=0.9,
        now=NOW,
    )
    payload = json.loads(format_hook_diversity_json(report))
    text = format_hook_diversity_text(report)

    assert report.ok is False
    assert payload["artifact_type"] == "hook_diversity"
    assert payload["blocking_issue_count"] == 1
    assert payload["totals"] == {
        "cluster_count": 1,
        "posts_scanned": 4,
        "posts_with_hook": 3,
        "repeated_posts": 2,
    }
    cluster = payload["clusters"][0]
    assert cluster["cluster_size"] == 2
    assert cluster["representative_hook"] == "Here is the thing"
    assert cluster["normalized_representative_hook"] == "here is the thing"
    assert cluster["affected_post_ids"] == [2, 1]
    assert "Hook Diversity Report" in text
    assert "post_ids=2,1" in text


def test_similarity_threshold_controls_near_repeat_clusters():
    rows = [
        _row(1, "Watch the queue pressure: retries can hide outages.", NOW - timedelta(minutes=2)),
        _row(2, "Watch the queue backlog: retries can hide incidents.", NOW - timedelta(minutes=1)),
    ]

    loose = build_hook_diversity_report(
        post_records=rows,
        threshold=0.65,
        now=NOW,
    )
    strict = build_hook_diversity_report(
        post_records=rows,
        threshold=0.9,
        now=NOW,
    )

    assert hook_similarity(
        normalize_opening_hook("Watch the queue pressure"),
        normalize_opening_hook("Watch the queue backlog"),
    ) >= 0.65
    assert len(loose.clusters) == 1
    assert strict.clusters == ()


def test_database_report_reads_generated_and_published_x_posts():
    conn = _publication_db()
    _insert_post(
        conn,
        1,
        "Let me say this: generated posts should count.",
        NOW - timedelta(hours=3),
    )
    _insert_post(
        conn,
        2,
        "Let me say this, published posts should count too.",
        NOW - timedelta(hours=4),
        published_at=NOW - timedelta(hours=1),
    )
    _insert_post(
        conn,
        3,
        "Let me say this: other platforms do not prove an X post.",
        NOW - timedelta(hours=2),
        published_at=NOW - timedelta(hours=1),
        platform="bluesky",
        content_type="blog_post",
    )

    report = build_hook_diversity_report(conn, days=7, threshold=0.9, now=NOW)

    assert report.missing_tables == ()
    assert report.missing_columns is None
    assert report.totals["posts_scanned"] == 2
    assert len(report.clusters) == 1
    assert report.clusters[0].affected_post_ids == (2, 1)
    assert report.clusters[0].examples[0].platform_post_id == "x-2"


def test_schema_gaps_return_empty_report_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_hook_diversity_report(conn, now=NOW)

    assert report.clusters == ()
    assert report.missing_tables == ("generated_content",)
    assert report.totals["posts_scanned"] == 0

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE generated_content (id INTEGER PRIMARY KEY)")
    column_report = build_hook_diversity_report(partial, now=NOW)

    assert column_report.missing_columns == {"generated_content": ("content",)}


def test_cli_emits_json_and_validates_threshold_limit_and_days(db, monkeypatch, capsys):
    monkeypatch.setattr(
        hook_diversity_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        hook_diversity_script,
        "build_hook_diversity_report",
        lambda db, **kwargs: build_hook_diversity_report(
            post_records=[
                _row(1, "Watch the queue pressure: one post.", NOW - timedelta(minutes=2)),
                _row(2, "Watch the queue pressure: another post.", NOW - timedelta(minutes=1)),
            ],
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = hook_diversity_script.main(
        ["--days", "7", "--threshold", "0.9", "--limit", "5", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["filters"]["threshold"] == 0.9
    assert payload["clusters"][0]["affected_post_ids"] == [2, 1]

    assert hook_diversity_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert hook_diversity_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert hook_diversity_script.main(["--threshold", "2"]) == 2
    assert "threshold must be greater than 0 and at most 1" in capsys.readouterr().err
