"""Tests for reply queue platform metadata integrity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.reply_queue_platform_metadata_integrity import (
    build_reply_queue_platform_metadata_integrity_report,
    format_reply_queue_platform_metadata_integrity_json,
    format_reply_queue_platform_metadata_integrity_text,
)


NOW = datetime(2026, 5, 1, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_queue_platform_metadata_integrity.py"
spec = importlib.util.spec_from_file_location("reply_queue_platform_metadata_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE reply_queue (
            id INTEGER PRIMARY KEY,
            platform TEXT,
            platform_metadata TEXT,
            tweet_id TEXT,
            uri TEXT,
            cid TEXT
        );
        """
    )
    return conn


def test_report_flags_missing_malformed_mismatch_and_missing_identifiers_with_fallbacks():
    conn = _conn()
    conn.executescript(
        """
        INSERT INTO reply_queue VALUES (1, 'x', NULL, NULL, NULL, NULL);
        INSERT INTO reply_queue VALUES (2, 'bluesky', '{bad', NULL, NULL, NULL);
        INSERT INTO reply_queue VALUES (3, 'x', '{"platform":"bluesky","uri":"at://post"}', NULL, NULL, NULL);
        INSERT INTO reply_queue VALUES (4, 'x', '{"platform":"x"}', NULL, NULL, NULL);
        INSERT INTO reply_queue VALUES (5, 'bluesky', '{"platform":"bluesky"}', NULL, NULL, NULL);
        INSERT INTO reply_queue VALUES (6, 'x', '{"platform":"x"}', 'tweet-1', NULL, NULL);
        INSERT INTO reply_queue VALUES (7, 'bluesky', '{"platform":"bluesky"}', NULL, 'at://post', 'cid-1');
        """
    )

    report = build_reply_queue_platform_metadata_integrity_report(conn, now=NOW)

    assert report["artifact_type"] == "reply_queue_platform_metadata_integrity"
    gap_types = [item["gap_type"] for item in report["items"]]
    assert gap_types == [
        "missing_metadata",
        "malformed_metadata",
        "platform_mismatch",
        "missing_native_identifier",
        "missing_native_identifier",
    ]
    assert {item["reply_id"] for item in report["items"]} == {1, 2, 3, 4, 5}


def test_schema_gaps_limit_formatters_and_cli(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute("INSERT INTO reply_queue VALUES (1, 'x', '', NULL, NULL, NULL)")
    report = build_reply_queue_platform_metadata_integrity_report(conn, limit=1, now=NOW)

    assert len(report["items"]) == 1
    assert json.loads(format_reply_queue_platform_metadata_integrity_json(report))["artifact_type"] == "reply_queue_platform_metadata_integrity"
    assert "reply_id | platform | gap_type" in format_reply_queue_platform_metadata_integrity_text(report)

    missing = build_reply_queue_platform_metadata_integrity_report(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["reply_queue"]
    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE reply_queue (id INTEGER, platform TEXT)")
    schema_report = build_reply_queue_platform_metadata_integrity_report(bad, now=NOW)
    assert schema_report["missing_columns"] == {"reply_queue": ["platform_metadata"]}

    db_path = tmp_path / "reply.sqlite"
    disk = sqlite3.connect(db_path)
    disk.executescript(
        """
        CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, platform TEXT, platform_metadata TEXT);
        INSERT INTO reply_queue VALUES (1, 'x', '');
        """
    )
    disk.close()
    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["items"][0]["gap_type"] == "missing_metadata"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Queue Platform Metadata Integrity" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["reply_queue"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
