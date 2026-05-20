"""Tests for publication attempt error recurrence reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publication_attempt_error_recurrence import (
    build_publication_attempt_error_recurrence_report,
    build_publication_attempt_error_recurrence_report_from_db,
    format_publication_attempt_error_recurrence_json,
    format_publication_attempt_error_recurrence_text,
    normalize_publication_attempt_error,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_error_recurrence.py"
spec = importlib.util.spec_from_file_location("publication_attempt_error_recurrence_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_groups_repeated_failures_and_excludes_successes():
    report = build_publication_attempt_error_recurrence_report(
        [
            {"id": 1, "content_id": 7, "platform": "x", "status": "failed", "error": "Request 123 failed for https://e.test/a at 2026-05-20T10:00:00Z"},
            {"id": 2, "content_id": 7, "platform": "x", "status": "error", "error": "Request 456 failed for https://e.test/b at 2026-05-20T11:00:00Z"},
            {"id": 3, "content_id": 7, "platform": "x", "status": "success", "error": "Request 789 failed for https://e.test/c"},
        ],
        min_count=2,
        now=NOW,
    )

    assert report["artifact_type"] == "publication_attempt_error_recurrence"
    assert len(report["recurring_errors"]) == 1
    assert report["recurring_errors"][0]["recurrence_count"] == 2
    assert report["platform_summary"]["x"]["failed_attempts"] == 2


def test_normalization_replaces_volatile_fragments_consistently():
    left = 'Bad payload "abc-123" for UUID 550e8400-e29b-41d4-a716-446655440000 and id 99 at https://x.test/path'
    right = 'Bad payload "different" for UUID 6ba7b810-9dad-11d1-80b4-00c04fd430c8 and id 100 at https://y.test/path'
    assert normalize_publication_attempt_error(left) == normalize_publication_attempt_error(right)
    assert "<quoted>" in normalize_publication_attempt_error(left)
    assert "<url>" in normalize_publication_attempt_error(left)


def test_db_loader_and_missing_schema_and_cli(tmp_path, capsys):
    empty = sqlite3.connect(":memory:")
    missing = build_publication_attempt_error_recurrence_report_from_db(empty, now=NOW)
    assert missing["missing_schema"]["missing_tables"] == ["publication_attempts"]

    db_path = tmp_path / "attempts.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE publication_attempts (id INTEGER PRIMARY KEY, content_id INTEGER, platform TEXT, status TEXT, error_message TEXT, created_at TEXT)")
    conn.execute("INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?)", (1, 1, "bluesky", "failed", "HTTP 500 post 123", "2026-05-20"))
    conn.execute("INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?)", (2, 1, "bluesky", "failed", "HTTP 500 post 456", "2026-05-21"))
    conn.commit()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["recurring_errors"][0]["platform"] == "bluesky"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publication Attempt Error Recurrence" in capsys.readouterr().out


def test_formatters_and_invalid_arguments():
    report = build_publication_attempt_error_recurrence_report([], now=NOW)
    assert json.loads(format_publication_attempt_error_recurrence_json(report))["artifact_type"] == "publication_attempt_error_recurrence"
    assert "Recurring errors" in format_publication_attempt_error_recurrence_text(report)
    with pytest.raises(ValueError, match="min_count must be positive"):
        build_publication_attempt_error_recurrence_report([], min_count=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publication_attempt_error_recurrence_report([], limit=0)
