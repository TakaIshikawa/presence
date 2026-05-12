"""Tests for publication retry ETA reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.publication_retry_eta import (
    build_publication_retry_eta_report,
    format_publication_retry_eta_json,
    format_publication_retry_eta_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_retry_eta.py"
spec = importlib.util.spec_from_file_location("publication_retry_eta_script", SCRIPT_PATH)
publication_retry_eta_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publication_retry_eta_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, *, published: int = 0) -> int:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="copy",
        eval_score=7,
        eval_feedback="ok",
    )
    db.conn.execute("UPDATE generated_content SET published = ? WHERE id = ?", (published, content_id))
    db.conn.commit()
    return content_id


def _publication(db, content_id: int, platform: str, status: str, **kwargs) -> int:
    columns = ["content_id", "platform", "status"]
    values = [content_id, platform, status]
    for key, value in kwargs.items():
        columns.append(key)
        values.append(value)
    placeholders = ", ".join("?" for _ in columns)
    cursor = db.conn.execute(
        f"INSERT INTO content_publications ({', '.join(columns)}) VALUES ({placeholders})",
        values,
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_buckets_retry_eta_rows_and_groups_by_platform_status_error(db):
    due = _publication(
        db,
        _content(db),
        "x",
        "failed",
        attempt_count=1,
        next_retry_at=(NOW - timedelta(minutes=5)).isoformat(),
        last_error_at=(NOW - timedelta(hours=1)).isoformat(),
        error_category="network",
    )
    scheduled = _publication(
        db,
        _content(db),
        "bluesky",
        "queued",
        attempt_count=0,
        next_retry_at=(NOW + timedelta(hours=2)).isoformat(),
    )
    missing = _publication(db, _content(db), "x", "failed", attempt_count=1, error_category="auth")
    risk = _publication(
        db,
        _content(db),
        "linkedin",
        "failed",
        attempt_count=2,
        next_retry_at=(NOW + timedelta(hours=4)).isoformat(),
        error_category="media",
    )
    blocked = _publication(db, _content(db, published=-1), "x", "failed", attempt_count=1)
    _publication(db, _content(db), "x", "published", published_at=NOW.isoformat())

    report = build_publication_retry_eta_report(db, max_attempts=3, now=NOW)
    payload = json.loads(format_publication_retry_eta_json(report))
    text = format_publication_retry_eta_text(report)

    assert payload["totals"]["bucket_counts"] == {
        "blocked_by_terminal_content_state": 1,
        "retry_due_now": 1,
        "retry_exhaustion_risk": 1,
        "retry_missing_eta": 1,
        "retry_scheduled": 1,
    }
    assert {row["publication_id"]: row["retry_bucket"] for row in payload["rows"]} == {
        due: "retry_due_now",
        scheduled: "retry_scheduled",
        missing: "retry_missing_eta",
        risk: "retry_exhaustion_risk",
        blocked: "blocked_by_terminal_content_state",
    }
    assert payload["groups"]
    assert "bucket=retry_missing_eta" in text


def test_rows_sort_by_next_retry_then_last_error_then_id(db):
    late = _publication(
        db,
        _content(db),
        "x",
        "failed",
        attempt_count=1,
        next_retry_at=(NOW + timedelta(hours=5)).isoformat(),
    )
    early = _publication(
        db,
        _content(db),
        "x",
        "failed",
        attempt_count=1,
        next_retry_at=(NOW + timedelta(hours=1)).isoformat(),
    )
    missing = _publication(
        db,
        _content(db),
        "x",
        "failed",
        attempt_count=1,
        last_error_at=(NOW - timedelta(days=1)).isoformat(),
    )

    report = build_publication_retry_eta_report(db, now=NOW)

    assert [row["publication_id"] for row in report["rows"]] == [early, late, missing]


def test_partial_schema_without_next_retry_reports_missing_columns():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """CREATE TABLE content_publications (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            status TEXT
        )"""
    )
    conn.execute("INSERT INTO content_publications VALUES (?, ?, ?, ?)", (1, 10, "x", "failed"))
    conn.commit()
    try:
        report = build_publication_retry_eta_report(conn, now=NOW)
    finally:
        conn.close()

    assert report["missing_tables"] == ["generated_content"]
    assert report["missing_columns"]["content_publications"] == ["attempt_count", "next_retry_at"]
    assert report["rows"][0]["retry_bucket"] == "retry_missing_eta"


def test_cli_supports_json_output(db, monkeypatch, capsys):
    publication_id = _publication(db, _content(db), "x", "failed", attempt_count=1)
    monkeypatch.setattr(publication_retry_eta_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        publication_retry_eta_script,
        "build_publication_retry_eta_report",
        lambda db, **kwargs: build_publication_retry_eta_report(db, now=NOW, **kwargs),
    )

    exit_code = publication_retry_eta_script.main(["--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["rows"][0]["publication_id"] == publication_id


def test_cli_returns_nonzero_on_database_error(monkeypatch, capsys):
    monkeypatch.setattr(publication_retry_eta_script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        publication_retry_eta_script,
        "build_publication_retry_eta_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    assert publication_retry_eta_script.main([]) == 1
    assert "error: db failed" in capsys.readouterr().err
