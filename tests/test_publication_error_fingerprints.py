"""Tests for publication error fingerprint reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_error_fingerprints import (
    build_publication_error_fingerprints_report,
    build_publication_error_fingerprints_report_from_db,
    format_publication_error_fingerprints_json,
    format_publication_error_fingerprints_text,
    normalize_publication_error_fingerprint,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_error_fingerprints.py"
spec = importlib.util.spec_from_file_location("publication_error_fingerprints_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_normalization_removes_volatile_urls_timestamps_ids_and_numbers():
    first = normalize_publication_error_fingerprint(
        "Request id req-123 failed for https://example.test/a/456 at 2026-05-01T12:01:00Z with code 98765"
    )
    second = normalize_publication_error_fingerprint(
        "request id req-999 failed for https://example.test/b/777 at 2026-05-02T09:15:00+00:00 with code 11111"
    )

    assert first == second
    assert first == "request id <id> failed for <url> at <timestamp> with code <number>"


def test_groups_equivalent_errors_by_platform_and_tracks_latest_queue_id():
    rows = [
        {
            "id": 1,
            "queue_id": 10,
            "content_id": 101,
            "platform": "x",
            "attempted_at": (NOW - timedelta(hours=3)).isoformat(),
            "last_error": "Upload failed for media id 123 at https://cdn.test/a.png",
        },
        {
            "id": 2,
            "queue_id": 20,
            "content_id": 102,
            "platform": "x",
            "attempted_at": (NOW - timedelta(hours=1)).isoformat(),
            "error_message": "upload failed for media id 999 at https://cdn.test/b.png",
        },
        {
            "id": 3,
            "queue_id": 30,
            "content_id": 103,
            "platform": "bluesky",
            "attempted_at": (NOW - timedelta(minutes=30)).isoformat(),
            "error": "upload failed for media id 555 at https://cdn.test/c.png",
        },
    ]

    report = build_publication_error_fingerprints_report(rows, days=1, now=NOW)

    assert len(report["fingerprints"]) == 2
    by_platform = {row["platform"]: row for row in report["fingerprints"]}
    assert by_platform["x"]["attempt_count"] == 2
    assert by_platform["x"]["affected_content_ids"] == [101, 102]
    assert by_platform["x"]["first_seen"] == (NOW - timedelta(hours=3)).isoformat()
    assert by_platform["x"]["last_seen"] == (NOW - timedelta(hours=1)).isoformat()
    assert by_platform["x"]["latest_queue_id"] == 20
    assert by_platform["x"]["latest_error"] == "upload failed for media id 999 at https://cdn.test/b.png"
    assert by_platform["bluesky"]["attempt_count"] == 1


def test_db_adapter_uses_failed_publication_attempts_and_json_is_deterministic():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publication_attempts (
            id INTEGER, queue_id INTEGER, content_id INTEGER, platform TEXT, attempted_at TEXT,
            success INTEGER, error TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO publication_attempts VALUES (1, 100, 1, 'x', ?, 0, '429 request id aaa at 2026-05-01T10:00:00Z')",
        ((NOW - timedelta(hours=2)).isoformat(),),
    )
    conn.execute(
        "INSERT INTO publication_attempts VALUES (2, 200, 2, 'x', ?, 0, '429 request id bbb at 2026-05-01T11:00:00Z')",
        ((NOW - timedelta(hours=1)).isoformat(),),
    )
    conn.execute(
        "INSERT INTO publication_attempts VALUES (3, 300, 3, 'x', ?, 1, '429 request id ccc at 2026-05-01T11:30:00Z')",
        ((NOW - timedelta(minutes=30)).isoformat(),),
    )

    report = build_publication_error_fingerprints_report_from_db(conn, days=1, now=NOW)
    payload = json.loads(format_publication_error_fingerprints_json(report))

    assert list(payload.keys()) == sorted(payload.keys())
    assert payload["artifact_type"] == "publication_error_fingerprints"
    assert payload["fingerprints"][0]["attempt_count"] == 2
    assert payload["fingerprints"][0]["latest_queue_id"] == 200


def test_text_empty_state_cli_and_invalid_args(monkeypatch, capsys):
    empty = build_publication_error_fingerprints_report([], now=NOW)
    assert "No failed publication attempts found." in format_publication_error_fingerprints_text(empty)
    with pytest.raises(ValueError, match="days must be positive"):
        build_publication_error_fingerprints_report([], days=0, now=NOW)
    with pytest.raises(SystemExit):
        script.parse_args(["--days", "0"])

    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_publication_error_fingerprints_report_from_db",
        lambda _db, **kwargs: build_publication_error_fingerprints_report(
            [{"content_id": 1, "platform": "x", "attempted_at": NOW.isoformat(), "error": "failure 123"}],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--days", "1", "--format", "text"]) == 0
    assert "Publication Error Fingerprints" in capsys.readouterr().out
