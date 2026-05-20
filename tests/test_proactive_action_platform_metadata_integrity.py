"""Tests for proactive action platform metadata integrity reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.proactive_action_platform_metadata_integrity import (
    build_proactive_action_platform_metadata_integrity_report_from_db,
    format_proactive_action_platform_metadata_integrity_json,
    format_proactive_action_platform_metadata_integrity_text,
)


NOW = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_platform_metadata_integrity.py"
spec = importlib.util.spec_from_file_location("proactive_action_platform_metadata_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE proactive_actions (
            id INTEGER PRIMARY KEY,
            action_type TEXT,
            status TEXT,
            target_tweet_id TEXT,
            target_url TEXT,
            posted_tweet_id TEXT,
            posted_platform_id TEXT,
            platform_metadata TEXT,
            created_at TEXT,
            posted_at TEXT
        )"""
    )
    return conn


def _action(
    conn: sqlite3.Connection,
    action_id: int,
    *,
    action_type: str = "reply",
    status: str = "pending",
    target_tweet_id: str | None = "target-1",
    target_url: str | None = None,
    posted_tweet_id: str | None = None,
    posted_platform_id: str | None = None,
    platform_metadata: object = None,
    created_at: str = "2026-05-20T10:00:00+00:00",
) -> None:
    raw_metadata = (
        json.dumps(platform_metadata if platform_metadata is not None else {"platform": "x", "url": "https://x.test/target", "cid": "cid-1", "target_tweet_id": target_tweet_id})
        if not isinstance(platform_metadata, str)
        else platform_metadata
    )
    conn.execute(
        """INSERT INTO proactive_actions
           (id, action_type, status, target_tweet_id, target_url, posted_tweet_id,
            posted_platform_id, platform_metadata, created_at, posted_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
        (action_id, action_type, status, target_tweet_id, target_url, posted_tweet_id, posted_platform_id, raw_metadata, created_at),
    )


def test_report_detects_platform_metadata_integrity_gaps():
    conn = _conn()
    _action(conn, 1, platform_metadata="{bad-json")
    _action(conn, 2, status="approved", platform_metadata={"target_tweet_id": "target-1"})
    _action(conn, 3, status="approved", platform_metadata={"platform": "x", "url": "https://x.test/other", "cid": "cid-1", "target_tweet_id": "different"})
    _action(conn, 4, status="posted", platform_metadata={"platform": "x", "url": "https://x.test/target", "cid": "cid-1", "target_tweet_id": "target-1"})
    _action(conn, 5, status="pending", platform_metadata={"platform": "x", "url": "https://x.test/target", "cid": "cid-1", "deleted": True})

    report = build_proactive_action_platform_metadata_integrity_report_from_db(conn, now=NOW)

    assert report["artifact_type"] == "proactive_action_platform_metadata_integrity"
    assert [finding["gap_type"] for finding in report["findings"]] == [
        "malformed_platform_metadata",
        "missing_platform",
        "missing_url",
        "missing_cid",
        "target_tweet_id_mismatch",
        "posted_missing_identifier",
        "stale_unavailable_target",
    ]
    assert report["summary"]["by_gap_type"]["posted_missing_identifier"] == 1
    assert report["groups"][0] == {"gap_type": "malformed_platform_metadata", "finding_count": 1}


def test_filters_limit_schema_and_formatters():
    conn = _conn()
    _action(conn, 1, status="approved", action_type="reply", platform_metadata={"target_tweet_id": "target-1"})
    _action(conn, 2, status="approved", action_type="like", platform_metadata={"target_tweet_id": "target-1"})
    _action(conn, 3, status="approved", action_type="reply", platform_metadata={"target_tweet_id": "target-1"}, created_at="2026-04-01T10:00:00+00:00")

    report = build_proactive_action_platform_metadata_integrity_report_from_db(
        conn,
        status="approved",
        action_type="reply",
        days=10,
        limit=2,
        now=NOW,
    )

    assert report["summary"]["action_count"] == 1
    assert report["summary"]["finding_count"] == 3
    assert len(report["findings"]) == 2
    assert json.loads(format_proactive_action_platform_metadata_integrity_json(report))["artifact_type"] == "proactive_action_platform_metadata_integrity"
    assert "action_id | status | action_type | gap_type" in format_proactive_action_platform_metadata_integrity_text(report)

    missing = build_proactive_action_platform_metadata_integrity_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["proactive_actions"]

    bad = sqlite3.connect(":memory:")
    bad.execute("CREATE TABLE proactive_actions (id INTEGER, status TEXT)")
    schema_report = build_proactive_action_platform_metadata_integrity_report_from_db(bad, now=NOW)
    assert schema_report["missing_columns"] == {"proactive_actions": ["action_type", "platform_metadata"]}


def test_cli_supports_db_json_text_context_and_invalid_args(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "actions.sqlite"
    conn = _conn()
    _action(conn, 1, status="approved", platform_metadata={"target_tweet_id": "target-1"})
    conn.commit()
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    conn.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--status", "approved", "--action-type", "reply"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] == 3

    assert script.main(["--db", str(db_path), "--format", "text", "--days", "7", "--limit", "1"]) == 0
    assert "Proactive Action Platform Metadata Integrity" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["proactive_actions"]

    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
