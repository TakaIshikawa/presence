"""Tests for cross-platform publication lag reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.cross_platform_publication_lag import (
    build_cross_platform_publication_lag_report,
    format_cross_platform_publication_lag_json,
    format_cross_platform_publication_lag_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "cross_platform_publication_lag.py"
spec = importlib.util.spec_from_file_location("cross_platform_publication_lag_script", SCRIPT_PATH)
cross_platform_publication_lag_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(cross_platform_publication_lag_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db) -> int:
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="copy",
        eval_score=7,
        eval_feedback="ok",
    )


def _publication(db, content_id: int, platform: str, status: str, **kwargs) -> None:
    columns = ["content_id", "platform", "status"]
    values = [content_id, platform, status]
    for key, value in kwargs.items():
        columns.append(key)
        values.append(value)
    placeholders = ", ".join("?" for _ in columns)
    db.conn.execute(
        f"INSERT INTO content_publications ({', '.join(columns)}) VALUES ({placeholders})",
        values,
    )
    db.conn.commit()


def test_reports_lagging_queued_and_failed_platforms_sorted_by_lag(db):
    old = _content(db)
    newer = _content(db)
    near = _content(db)
    _publication(db, old, "x", "published", published_at=(NOW - timedelta(hours=10)).isoformat())
    _publication(db, old, "bluesky", "queued", updated_at=(NOW - timedelta(hours=1)).isoformat())
    _publication(db, newer, "x", "published", published_at=(NOW - timedelta(hours=3)).isoformat())
    _publication(
        db,
        newer,
        "linkedin",
        "failed",
        error_category="auth",
        error="expired token",
        last_error_at=(NOW - timedelta(hours=1)).isoformat(),
    )
    _publication(db, near, "x", "published", published_at=(NOW - timedelta(minutes=20)).isoformat())
    _publication(db, near, "bluesky", "queued")

    report = build_cross_platform_publication_lag_report(db, threshold_hours=1, now=NOW)
    payload = json.loads(format_cross_platform_publication_lag_json(report))
    text = format_cross_platform_publication_lag_text(report)

    assert [item["content_id"] for item in payload["lagging_items"]] == [old, newer]
    assert payload["lagging_items"][0]["lag_hours"] == 10
    assert payload["by_platform"] == {"bluesky": 1, "linkedin": 1}
    assert payload["by_status"] == {"failed": 1, "queued": 1}
    assert payload["by_platform_pair_status"] == {
        "x->bluesky:queued": 1,
        "x->linkedin:failed": 1,
    }
    assert "lagging=linkedin status=failed" in text


def test_published_rows_below_threshold_are_excluded(db):
    content_id = _content(db)
    _publication(db, content_id, "x", "published", published_at=(NOW - timedelta(hours=2)).isoformat())
    _publication(db, content_id, "bluesky", "published", published_at=(NOW - timedelta(hours=1, minutes=45)).isoformat())

    report = build_cross_platform_publication_lag_report(db, threshold_hours=1, now=NOW)

    assert report["lagging_items"] == []
    assert report["totals"]["lagging_count"] == 0


def test_missing_optional_metadata_is_reported_without_crashing():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE content_publications (
            content_id INTEGER,
            platform TEXT,
            status TEXT,
            published_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO content_publications VALUES (?, ?, ?, ?)",
        (1, "x", "published", (NOW - timedelta(hours=2)).isoformat()),
    )
    conn.execute("INSERT INTO content_publications VALUES (?, ?, ?, ?)", (1, "bluesky", "queued", None))
    conn.commit()
    try:
        report = build_cross_platform_publication_lag_report(conn, now=NOW)
    finally:
        conn.close()

    assert report["missing_tables"] == ["generated_content"]
    assert report["missing_columns"] == {}
    assert report["lagging_items"][0]["lagging_platform"] == "bluesky"


def test_cli_supports_json_output(db, monkeypatch, capsys):
    content_id = _content(db)
    _publication(db, content_id, "x", "published", published_at=(NOW - timedelta(hours=4)).isoformat())
    _publication(db, content_id, "bluesky", "queued")
    monkeypatch.setattr(cross_platform_publication_lag_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        cross_platform_publication_lag_script,
        "build_cross_platform_publication_lag_report",
        lambda db, **kwargs: build_cross_platform_publication_lag_report(db, now=NOW, **kwargs),
    )

    exit_code = cross_platform_publication_lag_script.main(["--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["lagging_items"][0]["content_id"] == content_id


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(cross_platform_publication_lag_script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        cross_platform_publication_lag_script,
        "build_cross_platform_publication_lag_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    assert cross_platform_publication_lag_script.main([]) == 1
    assert "error: db failed" in capsys.readouterr().err
