"""Tests for publication media failure pattern reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

from evaluation.publication_media_failure_patterns import (
    build_publication_media_failure_patterns_report,
    build_publication_media_failure_patterns_report_from_db,
    format_publication_media_failure_patterns_text,
    normalize_media_error_signature,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_media_failure_patterns.py"
spec = importlib.util.spec_from_file_location("publication_media_failure_patterns_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_groups_failures_by_platform_media_signature_and_retry_outcome():
    rows = [
        {"id": "1", "content_id": "c1", "platform": "x", "media_type": "image", "error": "Image 123 too large", "status": "failed", "retry_count": 1},
        {"id": "2", "content_id": "c2", "platform": "x", "media_type": "image", "error": "Image 999 too large", "status": "failed", "retry_count": 1},
        {"id": "3", "content_id": "c3", "platform": "bluesky", "media_type": "video", "error": "Unsupported codec", "status": "failed", "retry_count": 0},
    ]

    report = build_publication_media_failure_patterns_report(rows, now=NOW)

    pattern = report["patterns"][0]
    assert pattern["platform"] == "x"
    assert pattern["media_type"] == "image"
    assert pattern["error_signature"] == "image <num> too large"
    assert pattern["failure_count"] == 2
    assert pattern["affected_content_ids"] == ["c1", "c2"]
    assert "retry_rate" in format_publication_media_failure_patterns_text(report)


def test_normalizes_similar_error_messages_to_stable_signature():
    assert normalize_media_error_signature("Upload https://x/y failed for asset abcdef123456 size 123") == (
        "upload <url> failed for asset <token> size <num>"
    )


def test_retry_success_rate_and_severity_are_reported():
    rows = [
        {"id": "1", "content_id": "c1", "platform": "x", "media_type": "image", "error": "Timeout 1", "status": "failed", "retry_count": 1},
        {"id": "2", "content_id": "c1", "platform": "x", "media_type": "image", "status": "published", "parent_attempt_id": "1"},
    ]

    report = build_publication_media_failure_patterns_report(rows, now=NOW)

    pattern = report["patterns"][0]
    assert pattern["retry_outcome"] == "succeeded_after_retry"
    assert pattern["retry_success_rate"] == 1.0
    assert pattern["severity"] == "low"


def test_db_loader_and_cli_json_output(monkeypatch, capsys, tmp_path):
    import sqlite3

    conn = sqlite3.connect(tmp_path / "pub.db")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publication_attempts (
           id INTEGER PRIMARY KEY,
           content_id TEXT,
           platform TEXT,
           media_type TEXT,
           error TEXT,
           status TEXT,
           retry_count INTEGER,
           parent_attempt_id TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO publication_attempts (content_id, platform, media_type, error, status, retry_count) VALUES (?, ?, ?, ?, ?, ?)",
        ("c1", "x", "image", "Image 123 too large", "failed", 0),
    )
    conn.commit()

    report = build_publication_media_failure_patterns_report_from_db(conn, now=NOW)
    assert report["patterns"][0]["error_signature"] == "image <num> too large"

    monkeypatch.setattr(script, "script_context", lambda: _script_context(conn))
    monkeypatch.setattr(
        script,
        "build_publication_media_failure_patterns_report_from_db",
        lambda db, **kwargs: build_publication_media_failure_patterns_report_from_db(db, now=NOW, **kwargs),
    )
    assert script.main(["--limit", "5"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "publication_media_failure_patterns"

    assert script.main(["--table"]) == 0
    assert "Publication Media Failure Patterns" in capsys.readouterr().out
