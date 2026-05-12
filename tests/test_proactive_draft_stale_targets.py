"""Tests for proactive draft stale target reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.proactive_draft_stale_targets import (
    build_proactive_draft_stale_targets_report,
    format_proactive_draft_stale_targets_json,
    format_proactive_draft_stale_targets_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "proactive_draft_stale_targets.py"
)
spec = importlib.util.spec_from_file_location(
    "proactive_draft_stale_targets_script",
    SCRIPT_PATH,
)
proactive_draft_stale_targets_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(proactive_draft_stale_targets_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_action(
    db,
    *,
    target_tweet_id: str,
    created_at: datetime,
    status: str = "pending",
    action_type: str = "reply",
    platform_metadata: dict | None = None,
    draft_text: str | None = "Draft reply.",
) -> int:
    action_id = db.insert_proactive_action(
        action_type=action_type,
        target_tweet_id=target_tweet_id,
        target_tweet_text="Target text",
        target_author_handle="alice",
        target_author_id="author-a",
        discovery_source="search",
        relevance_score=0.8,
        draft_text=draft_text,
        relationship_context=json.dumps({"profile_summary": "Builder"}),
        knowledge_ids=json.dumps([[123, 0.91]]),
        platform_metadata=json.dumps(platform_metadata or {}, sort_keys=True),
    )
    db.conn.execute(
        "UPDATE proactive_actions SET status = ?, created_at = ? WHERE id = ?",
        (status, created_at.isoformat(), action_id),
    )
    db.conn.commit()
    return action_id


def test_classifies_missing_stale_old_and_needs_refresh(db):
    old_snapshot = _insert_action(
        db,
        target_tweet_id="old-snapshot",
        created_at=NOW - timedelta(days=2),
        platform_metadata={
            "platform": "x",
            "target_url": "https://x.test/old",
            "target_fetched_at": (NOW - timedelta(days=10)).isoformat(),
        },
    )
    missing_url = _insert_action(
        db,
        target_tweet_id="missing-url",
        created_at=NOW - timedelta(days=1),
        platform_metadata={
            "platform": "bluesky",
            "target_fetched_at": NOW.isoformat(),
        },
    )
    old_draft = _insert_action(
        db,
        target_tweet_id="old-draft",
        created_at=NOW - timedelta(days=8),
        action_type="quote_tweet",
        platform_metadata={
            "platform": "x",
            "target_url": "https://x.test/fresh",
            "target_fetched_at": NOW.isoformat(),
        },
    )
    _insert_action(
        db,
        target_tweet_id="fresh",
        created_at=NOW - timedelta(hours=2),
        platform_metadata={
            "platform": "x",
            "target_url": "https://x.test/freshest",
            "target_fetched_at": NOW.isoformat(),
        },
    )
    _insert_action(
        db,
        target_tweet_id="posted",
        created_at=NOW - timedelta(days=20),
        status="posted",
        platform_metadata={},
    )

    report = build_proactive_draft_stale_targets_report(db, days=7, now=NOW)
    payload = json.loads(format_proactive_draft_stale_targets_json(report))
    by_id = {item["draft_id"]: item for item in payload["stale_targets"]}
    text = format_proactive_draft_stale_targets_text(report)

    assert set(by_id) == {old_snapshot, missing_url, old_draft}
    assert by_id[old_snapshot]["reasons"] == ["stale_target_snapshot", "needs_refresh"]
    assert by_id[missing_url]["reasons"] == ["missing_target_url", "needs_refresh"]
    assert by_id[old_draft]["reasons"] == ["old_draft", "needs_refresh"]
    assert payload["reason_counts"] == {
        "missing_target_url": 1,
        "needs_refresh": 3,
        "old_draft": 1,
        "stale_target_snapshot": 1,
    }
    assert payload["by_platform"] == {"bluesky": 1, "x": 2}
    assert payload["representative_draft_ids"] == [old_draft, old_snapshot, missing_url]
    assert f"draft_id={old_snapshot}" in text
    assert "Representative draft ids:" in text


def test_handles_absent_proactive_table_with_stable_empty_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        report = build_proactive_draft_stale_targets_report(conn, now=NOW)
    finally:
        conn.close()

    assert report.to_dict()["artifact_type"] == "proactive_draft_stale_targets"
    assert report.missing_tables == ("proactive_actions",)
    assert report.reason_counts == {
        "missing_target_url": 0,
        "stale_target_snapshot": 0,
        "old_draft": 0,
        "needs_refresh": 0,
    }
    assert report.stale_targets == ()


def test_uses_direct_optional_target_columns_when_present():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE proactive_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            action_type TEXT,
            status TEXT,
            draft_text TEXT,
            created_at TEXT,
            updated_at TEXT,
            target_url TEXT,
            target_fetched_at TEXT,
            target_tweet_id TEXT,
            target_author_handle TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO proactive_actions
           (action_type, status, draft_text, created_at, updated_at,
            target_url, target_fetched_at, target_tweet_id, target_author_handle)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            "reply",
            "pending",
            "Draft",
            (NOW - timedelta(days=1)).isoformat(),
            (NOW - timedelta(days=1)).isoformat(),
            "https://x.test/direct",
            (NOW - timedelta(days=9)).isoformat(),
            "target-direct",
            "alice",
        ),
    )
    conn.commit()

    try:
        report = build_proactive_draft_stale_targets_report(conn, days=7, now=NOW)
    finally:
        conn.close()

    assert report.stale_targets[0].target_url == "https://x.test/direct"
    assert report.stale_targets[0].target_age_days == 9.0
    assert report.stale_targets[0].reasons == ("stale_target_snapshot", "needs_refresh")


def test_limit_and_cli_json_output(db, monkeypatch, capsys):
    first = _insert_action(
        db,
        target_tweet_id="first",
        created_at=NOW - timedelta(days=10),
        platform_metadata={},
    )
    _insert_action(
        db,
        target_tweet_id="second",
        created_at=NOW - timedelta(days=9),
        platform_metadata={},
    )
    monkeypatch.setattr(
        proactive_draft_stale_targets_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        proactive_draft_stale_targets_script,
        "build_proactive_draft_stale_targets_report",
        lambda db, **kwargs: build_proactive_draft_stale_targets_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    exit_code = proactive_draft_stale_targets_script.main(
        ["--days", "7", "--limit", "1", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["filters"]["limit"] == 1
    assert payload["stale_targets"][0]["draft_id"] == first


def test_cli_validation_and_database_errors(monkeypatch, capsys):
    with pytest.raises(SystemExit):
        proactive_draft_stale_targets_script.parse_args(["--days", "0"])
    with pytest.raises(SystemExit):
        proactive_draft_stale_targets_script.parse_args(["--limit", "-1"])

    monkeypatch.setattr(
        proactive_draft_stale_targets_script,
        "script_context",
        lambda: _script_context(SimpleNamespace()),
    )
    monkeypatch.setattr(
        proactive_draft_stale_targets_script,
        "build_proactive_draft_stale_targets_report",
        lambda *args, **kwargs: (_ for _ in ()).throw(sqlite3.Error("db failed")),
    )

    exit_code = proactive_draft_stale_targets_script.main([])

    assert exit_code == 1
    assert "error: db failed" in capsys.readouterr().err
