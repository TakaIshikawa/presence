"""Tests for publication attempt idempotency key reuse reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.publication_attempt_idempotency_key_reuse import (
    build_publication_attempt_idempotency_key_reuse_report,
    build_publication_attempt_idempotency_key_reuse_report_from_db,
    format_publication_attempt_idempotency_key_reuse_json,
    format_publication_attempt_idempotency_key_reuse_text,
)


NOW = datetime(2026, 5, 24, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_idempotency_key_reuse.py"
spec = importlib.util.spec_from_file_location("publication_attempt_idempotency_key_reuse_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_builder_flags_missing_and_reused_keys():
    rows = [
        {"attempt_id": 1, "content_id": 10, "provider": "buffer", "platform": "x", "idempotency_key": "", "request_payload": "{}", "attempted_at": "2026-05-24T10:00:00+00:00"},
        {"attempt_id": 2, "content_id": 10, "provider": "buffer", "platform": "x", "idempotency_key": "same", "request_payload": '{"a":1}', "attempted_at": "2026-05-24T10:01:00+00:00"},
        {"attempt_id": 3, "content_id": 11, "provider": "buffer", "platform": "x", "idempotency_key": "same", "request_payload": '{"a":2}', "attempted_at": "2026-05-24T10:02:00+00:00"},
        {"attempt_id": 4, "content_id": 12, "provider": "native", "platform": "x", "idempotency_key": "same", "request_payload": '{"a":3}', "attempted_at": "2026-05-24T10:03:00+00:00"},
    ]
    report = build_publication_attempt_idempotency_key_reuse_report(rows, provider="buffer", now=NOW)

    assert report["artifact_type"] == "publication_attempt_idempotency_key_reuse"
    assert report["totals"] == {"attempts": 3, "missing_keys": 1, "reused_keys": 1, "shown_findings": 2}
    assert report["findings"][0]["reason"] == "reused_idempotency_key"
    assert report["findings"][0]["content_ids"] == ["10", "11"]
    assert set(report["findings"][0]["reuse_reasons"]) == {"different_content_id", "different_request_payload"}
    assert report["findings"][1]["reason"] == "missing_idempotency_key"


def test_db_loader_handles_schema_gaps_and_filters(tmp_path):
    conn = sqlite3.connect(":memory:")
    assert build_publication_attempt_idempotency_key_reuse_report_from_db(conn, now=NOW)["missing_tables"] == ["publication_attempts"]
    conn.execute("CREATE TABLE publication_attempts (id INTEGER PRIMARY KEY, provider TEXT, platform TEXT, content_id INTEGER, attempted_at TEXT)")
    report = build_publication_attempt_idempotency_key_reuse_report_from_db(conn, now=NOW)
    assert report["missing_columns"] == {"publication_attempts": ["idempotency_key", "request_payload"]}

    db_path = tmp_path / "attempts.sqlite"
    db = sqlite3.connect(db_path)
    db.executescript(
        """
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY, content_id INTEGER, provider TEXT, platform TEXT,
            idempotency_key TEXT, request_payload TEXT, attempted_at TEXT
        );
        INSERT INTO publication_attempts VALUES (1, 1, 'buffer', 'x', 'k', '{"a":1}', '2026-05-20T12:00:00+00:00');
        INSERT INTO publication_attempts VALUES (2, 2, 'buffer', 'x', 'k', '{"a":2}', '2026-05-24T11:00:00+00:00');
        """
    )
    db.close()
    loaded = build_publication_attempt_idempotency_key_reuse_report_from_db(sqlite3.connect(db_path), lookback_days=2, now=NOW)
    assert loaded["totals"]["attempts"] == 1


def test_formatters_and_cli(tmp_path, monkeypatch, capsys):
    report = build_publication_attempt_idempotency_key_reuse_report([], now=NOW)
    assert json.loads(format_publication_attempt_idempotency_key_reuse_json(report))["artifact_type"] == "publication_attempt_idempotency_key_reuse"
    assert "Publication Attempt Idempotency Key Reuse" in format_publication_attempt_idempotency_key_reuse_text(report)

    db_path = tmp_path / "attempts.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE publication_attempts (
            id INTEGER PRIMARY KEY, content_id INTEGER, provider TEXT, platform TEXT,
            idempotency_key TEXT, request_payload TEXT, attempted_at TEXT
        );
        INSERT INTO publication_attempts VALUES (1, 1, 'buffer', 'x', '', '{}', '2026-05-24T12:00:00+00:00');
        """
    )
    conn.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--provider", "buffer"]) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["missing_keys"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "missing_keys=1" in capsys.readouterr().out
    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["publication_attempts"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
    with pytest.raises(SystemExit):
        script.parse_args(["--lookback-days", "0"])
