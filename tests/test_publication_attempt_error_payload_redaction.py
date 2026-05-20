"""Tests for publication attempt error payload redaction reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publication_attempt_error_payload_redaction import (
    build_publication_attempt_error_payload_redaction_report,
    build_publication_attempt_error_payload_redaction_report_from_db,
    format_publication_attempt_error_payload_redaction_json,
    format_publication_attempt_error_payload_redaction_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_error_payload_redaction.py"
spec = importlib.util.spec_from_file_location("publication_attempt_error_payload_redaction_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            attempted_at TEXT,
            success INTEGER,
            error TEXT,
            error_category TEXT,
            response_metadata TEXT
        )"""
    )
    return conn


def test_builder_detects_secret_values_keys_oauth_email_and_raw_metadata():
    report = build_publication_attempt_error_payload_redaction_report(
        [
            {
                "attempt_id": 1,
                "content_id": 10,
                "platform": "x",
                "attempted_at": NOW.isoformat(),
                "success": 0,
                "error_category": "auth",
                "error": "Authorization failed Bearer sk-test1234567890 for user@example.com",
                "response_metadata": json.dumps(
                    {
                        "access_token": "tok_abcdefghijklmnopqrstuvwxyz",
                        "profile": {"email": "owner@example.com"},
                    }
                ),
            },
            {
                "attempt_id": 2,
                "platform": "x",
                "attempted_at": NOW.isoformat(),
                "success": 0,
                "error_category": "provider",
                "error": "",
                "response_metadata": "x" * 64,
            },
            {
                "attempt_id": 3,
                "platform": "x",
                "attempted_at": NOW.isoformat(),
                "success": 1,
                "error_category": "auth",
                "error": "Bearer should_not_count123456",
                "response_metadata": "{}",
            },
        ],
        now=NOW,
        metadata_bytes=32,
    )
    payload = json.loads(format_publication_attempt_error_payload_redaction_json(report))

    assert payload["artifact_type"] == "publication_attempt_error_payload_redaction"
    assert payload["totals"]["attempt_count"] == 3
    assert payload["totals"]["failed_attempt_count"] == 2
    assert payload["totals"]["by_finding_type"] == {
        "email_address": 2,
        "malformed_metadata": 1,
        "oauth_field": 1,
        "oversized_metadata": 2,
        "secret_key": 1,
        "secret_value": 2,
    }
    assert {"platform", "error_category", "finding_type", "count"}.issubset(payload["groups"][0])
    assert "user@example.com" not in format_publication_attempt_error_payload_redaction_text(report)


def test_db_loader_cli_and_schema_gaps(tmp_path, capsys):
    conn = _conn()
    conn.execute(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 1, "bluesky", NOW.isoformat(), 0, "api_key=abc123456789012345", "auth", '{"ok":true}'),
    )
    conn.execute(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (2, 2, "bluesky", NOW.isoformat(), 1, "Bearer abc123456789012345", "auth", '{"ok":true}'),
    )

    report = build_publication_attempt_error_payload_redaction_report_from_db(conn, now=NOW)
    assert report["totals"]["finding_count"] == 1
    assert report["findings"][0]["source"] == "error"

    db_path = tmp_path / "attempts.sqlite"
    conn.commit()
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat(), "--days", "2", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["filters"] == {"days": 2, "limit": 5, "metadata_bytes": 2048}
    assert script.main(["--db", str(db_path), "--format", "text", "--now", NOW.isoformat()]) == 0
    assert "Publication Attempt Error Payload Redaction" in capsys.readouterr().out

    missing = build_publication_attempt_error_payload_redaction_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["publication_attempts"]
    partial = sqlite3.connect(":memory:")
    partial.execute("CREATE TABLE publication_attempts (success INTEGER)")
    gaps = build_publication_attempt_error_payload_redaction_report_from_db(partial, now=NOW)
    assert gaps["missing_columns"] == {"publication_attempts": ["response_metadata"]}


def test_validation_errors():
    with pytest.raises(ValueError, match="days must be positive"):
        build_publication_attempt_error_payload_redaction_report([], days=0)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publication_attempt_error_payload_redaction_report([], limit=0)
    assert script.main(["--limit", "0"]) == 2
