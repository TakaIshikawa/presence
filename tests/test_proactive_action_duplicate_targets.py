"""Tests for proactive action duplicate target reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.proactive_action_duplicate_targets import (
    build_proactive_action_duplicate_targets_report,
    build_proactive_action_duplicate_targets_report_from_db,
    format_proactive_action_duplicate_targets_json,
    format_proactive_action_duplicate_targets_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_duplicate_targets.py"
spec = importlib.util.spec_from_file_location("proactive_action_duplicate_targets_script", SCRIPT_PATH)
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
            target_url TEXT,
            target_tweet_id TEXT,
            target_author_handle TEXT,
            discovery_source TEXT,
            status TEXT,
            created_at TEXT
        )"""
    )
    return conn


def _action(
    conn: sqlite3.Connection,
    action_id: int,
    *,
    action_type: str = "reply",
    target_url: str | None = None,
    target_tweet_id: str | None = "tweet-1",
    target_author_handle: str | None = None,
    discovery_source: str = "search",
    status: str = "pending",
    days_ago: float = 1,
) -> None:
    conn.execute(
        """INSERT INTO proactive_actions
           (id, action_type, target_url, target_tweet_id, target_author_handle,
            discovery_source, status, created_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            action_id,
            action_type,
            target_url,
            target_tweet_id,
            target_author_handle,
            discovery_source,
            status,
            (NOW - timedelta(days=days_ago)).isoformat(),
        ),
    )
    conn.commit()


def test_builder_groups_duplicate_targets_by_target_and_action_type():
    rows = [
        {
            "id": 1,
            "action_type": "reply",
            "target_url": "https://X.com/Alice/status/1?utm_source=test",
            "status": "pending",
            "discovery_source": "search",
            "created_at": (NOW - timedelta(hours=6)).isoformat(),
        },
        {
            "id": 2,
            "action_type": "reply",
            "target_url": "https://x.com/Alice/status/1",
            "status": "approved",
            "discovery_source": "timeline",
            "created_at": (NOW - timedelta(hours=4)).isoformat(),
        },
        {
            "id": 3,
            "action_type": "like",
            "target_url": "https://x.com/Alice/status/1",
            "status": "pending",
            "discovery_source": "search",
            "created_at": (NOW - timedelta(hours=3)).isoformat(),
        },
        {
            "id": 4,
            "action_type": "reply",
            "target_url": "https://x.com/old/status/2",
            "status": "pending",
            "discovery_source": "search",
            "created_at": (NOW - timedelta(days=10)).isoformat(),
        },
    ]

    report = build_proactive_action_duplicate_targets_report(rows, days=7, now=NOW)

    assert report["artifact_type"] == "proactive_action_duplicate_targets"
    assert report["totals"]["duplicate_group_count"] == 1
    group = report["groups"][0]
    assert group["target_key"] == "url:https://x.com/alice/status/1"
    assert group["action_type"] == "reply"
    assert group["action_ids"] == [1, 2]
    assert group["statuses"] == {"approved": 1, "pending": 1}
    assert group["discovery_sources"] == {"search": 1, "timeline": 1}
    assert group["pending_duplicate_count"] == 1
    assert group["has_pending_review_duplicates"] is True


def test_db_adapter_finds_tweet_and_account_duplicates_with_limit():
    conn = _conn()
    _action(conn, 1, target_tweet_id="tweet-1", status="pending", discovery_source="search")
    _action(conn, 2, target_tweet_id="tweet-1", status="posted", discovery_source="timeline")
    _action(conn, 3, action_type="quote_tweet", target_tweet_id=None, target_author_handle="@Alice", status="pending")
    _action(conn, 4, action_type="quote_tweet", target_tweet_id=None, target_author_handle="alice", status="pending")
    _action(conn, 5, target_tweet_id="old", days_ago=30)

    report = build_proactive_action_duplicate_targets_report_from_db(conn, days=7, limit=1, now=NOW)

    assert report["totals"]["duplicate_group_count"] == 2
    assert report["totals"]["shown_count"] == 1
    assert report["totals"]["pending_duplicate_count"] == 3
    assert report["groups"][0]["target_key"] == "account:alice"
    assert report["groups"][0]["action_ids"] == [3, 4]


def test_min_count_filter_and_missing_schema_metadata():
    conn = _conn()
    _action(conn, 1, target_tweet_id="tweet-1")
    _action(conn, 2, target_tweet_id="tweet-1")

    report = build_proactive_action_duplicate_targets_report_from_db(conn, min_count=3, now=NOW)
    assert report["groups"] == []
    assert report["empty_state"]["is_empty"] is True

    missing_table = build_proactive_action_duplicate_targets_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing_table["missing_tables"] == ["proactive_actions"]

    partial = sqlite3.connect(":memory:")
    partial.row_factory = sqlite3.Row
    partial.execute("CREATE TABLE proactive_actions (id INTEGER PRIMARY KEY, action_type TEXT)")
    missing_columns = build_proactive_action_duplicate_targets_report_from_db(partial, now=NOW)
    assert missing_columns["missing_columns"]["proactive_actions"] == [
        "created_at",
        "discovery_source",
        "status",
        "target_url|target_tweet_id|target_author_handle|target_author_id",
    ]


def test_json_and_text_formatters_are_stable():
    report = build_proactive_action_duplicate_targets_report(
        [
            {
                "id": 1,
                "action_type": "reply",
                "target_tweet_id": "tweet-1",
                "status": "pending",
                "discovery_source": "search",
                "created_at": (NOW - timedelta(hours=1)).isoformat(),
            },
            {
                "id": 2,
                "action_type": "reply",
                "target_tweet_id": "tweet-1",
                "status": "posted",
                "discovery_source": "timeline",
                "created_at": (NOW - timedelta(minutes=30)).isoformat(),
            },
        ],
        now=NOW,
    )

    payload = json.loads(format_proactive_action_duplicate_targets_json(report))
    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "proactive_action_duplicate_targets"
    text = format_proactive_action_duplicate_targets_text(report)
    assert "Proactive Action Duplicate Targets" in text
    assert "tweet:tweet-1 action_type=reply count=2 pending=1 actions=1,2" in text


def test_cli_supports_db_days_min_count_limit_json_text_and_invalid_numbers(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "actions.sqlite"
    conn = _conn()
    _action(conn, 1, target_tweet_id="tweet-1")
    _action(conn, 2, target_tweet_id="tweet-1")
    conn.backup(sqlite3.connect(db_path))
    conn.close()

    assert script.main(["--db", str(db_path), "--days", "30", "--min-count", "2", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["duplicate_group_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Proactive Action Duplicate Targets" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["missing_tables"] == ["proactive_actions"]
    with pytest.raises(SystemExit):
        script.parse_args(["--min-count", "1"])
