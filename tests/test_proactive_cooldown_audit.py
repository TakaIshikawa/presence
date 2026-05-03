"""Tests for proactive engagement cooldown auditing."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.proactive_cooldown_audit import (
    build_proactive_cooldown_audit,
    format_proactive_cooldown_audit_json,
    format_proactive_cooldown_audit_text,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_cooldown_audit.py"
spec = importlib.util.spec_from_file_location("proactive_cooldown_audit_script", SCRIPT_PATH)
proactive_cooldown_audit_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(proactive_cooldown_audit_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _insert_action(db, *, tweet_id: str, handle: str = "alice", **kwargs) -> int:
    defaults = dict(
        action_type="reply",
        target_tweet_id=tweet_id,
        target_tweet_text="Target post",
        target_author_handle=handle,
        target_author_id=None,
        discovery_source="search",
        relevance_score=0.8,
        draft_text="Useful point.",
        relationship_context=None,
        knowledge_ids=None,
        platform_metadata=None,
    )
    defaults.update(kwargs)
    return db.insert_proactive_action(**defaults)


def _set_time(db, action_id: int, timestamp: str, *, status: str = "posted") -> None:
    db.conn.execute(
        """UPDATE proactive_actions
           SET status = ?, created_at = ?, reviewed_at = NULL, posted_at = ?
           WHERE id = ?""",
        (status, timestamp, timestamp if status == "posted" else None, action_id),
    )
    db.conn.commit()


def test_under_limit_target_is_not_flagged(db):
    first = _insert_action(db, tweet_id="alice-1", action_type="reply")
    second = _insert_action(db, tweet_id="alice-2", action_type="quote_tweet")
    _set_time(db, first, "2026-05-03T09:00:00+00:00")
    _set_time(db, second, "2026-05-03T10:00:00+00:00")

    report = build_proactive_cooldown_audit(db, days=7, max_actions=2, now=NOW)

    assert report.ok is True
    assert report.totals["audited_actions"] == 2
    assert report.violations == ()


def test_over_limit_target_reports_count_latest_timestamp_and_action_ids(db):
    first = _insert_action(db, tweet_id="alice-1", action_type="reply")
    second = _insert_action(db, tweet_id="alice-2", action_type="quote_tweet")
    third = _insert_action(db, tweet_id="alice-3", action_type="like")
    _set_time(db, first, "2026-05-03T08:00:00+00:00")
    _set_time(db, second, "2026-05-03T10:00:00+00:00")
    _set_time(db, third, "2026-05-03T09:00:00+00:00")

    report = build_proactive_cooldown_audit(db, days=7, max_actions=2, now=NOW)
    payload = json.loads(format_proactive_cooldown_audit_json(report))

    assert report.ok is False
    assert payload["artifact_type"] == "proactive_cooldown_audit"
    assert payload["violation_count"] == 1
    assert payload["violations"][0]["target_id"] == "handle:alice"
    assert payload["violations"][0]["action_count"] == 3
    assert payload["violations"][0]["most_recent_action_at"] == "2026-05-03T10:00:00+00:00"
    assert payload["violations"][0]["action_ids"] == [second, third, first]


def test_multiple_targets_are_reported_deterministically(db):
    alice_ids = [
        _insert_action(db, tweet_id="alice-1", handle="@Alice", action_type="reply"),
        _insert_action(db, tweet_id="alice-2", handle="alice", action_type="quote_tweet"),
        _insert_action(db, tweet_id="alice-3", handle="ALICE", action_type="like"),
    ]
    bob_ids = [
        _insert_action(db, tweet_id="bob-1", handle="bob", action_type="reply"),
        _insert_action(db, tweet_id="bob-2", handle="@Bob", action_type="quote_tweet"),
        _insert_action(db, tweet_id="bob-3", handle="BOB", action_type="like"),
    ]
    for index, action_id in enumerate([*alice_ids, *bob_ids], start=1):
        _set_time(db, action_id, f"2026-05-03T0{index}:00:00+00:00")

    report = build_proactive_cooldown_audit(db, days=7, max_actions=2, now=NOW)

    assert [violation.target_id for violation in report.violations] == [
        "handle:bob",
        "handle:alice",
    ]
    assert [violation.action_count for violation in report.violations] == [3, 3]


def test_target_normalization_links_id_handle_and_url_forms(db):
    first = _insert_action(
        db,
        tweet_id="alice-id",
        handle="@Alice",
        target_author_id="user-42",
        action_type="reply",
    )
    second = _insert_action(
        db,
        tweet_id="alice-url",
        handle="",
        platform_metadata=json.dumps({"target_url": "https://x.com/Alice/status/123"}),
        action_type="quote_tweet",
    )
    third = _insert_action(
        db,
        tweet_id="alice-direct",
        handle="",
        target_author_id="user-42",
        action_type="like",
    )
    _set_time(db, first, "2026-05-03T08:00:00+00:00")
    _set_time(db, second, "2026-05-03T09:00:00+00:00")
    _set_time(db, third, "2026-05-03T10:00:00+00:00")

    report = build_proactive_cooldown_audit(db, days=7, max_actions=2, now=NOW)

    assert len(report.violations) == 1
    assert report.violations[0].target_id == "id:user-42"
    assert report.violations[0].aliases == ("handle:alice", "id:user-42")


def test_boundary_timestamp_is_inclusive_at_cutoff(db):
    included = _insert_action(db, tweet_id="boundary-1", action_type="reply")
    recent = _insert_action(db, tweet_id="boundary-2", action_type="quote_tweet")
    excluded = _insert_action(db, tweet_id="boundary-3", action_type="like")
    _set_time(db, included, "2026-04-26T12:00:00+00:00")
    _set_time(db, recent, "2026-05-03T11:00:00+00:00")
    _set_time(db, excluded, "2026-04-26T11:59:59+00:00")

    report = build_proactive_cooldown_audit(db, days=7, max_actions=1, now=NOW)

    assert report.totals["audited_actions"] == 2
    assert report.violations[0].action_count == 2
    assert set(report.violations[0].action_ids) == {included, recent}


def test_resolved_cultivate_action_records_are_included():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE people (
               id TEXT PRIMARY KEY,
               x_handle TEXT,
               x_user_id TEXT
           )"""
    )
    conn.execute(
        """CREATE TABLE actions (
               id TEXT PRIMARY KEY,
               action_type TEXT NOT NULL,
               target_person_id TEXT NOT NULL,
               status TEXT NOT NULL,
               created_at TEXT NOT NULL,
               completed_at TEXT,
               payload TEXT
           )"""
    )
    conn.execute("INSERT INTO people (id, x_handle, x_user_id) VALUES ('p1', 'Alice', '42')")
    conn.executemany(
        """INSERT INTO actions
           (id, action_type, target_person_id, status, created_at, completed_at, payload)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            ("a1", "reply", "p1", "completed", "2026-05-03T08:00:00+00:00", None, "{}"),
            (
                "a2",
                "quote_tweet",
                "p1",
                "completed",
                "2026-05-03T09:00:00+00:00",
                None,
                json.dumps({"execution_type": "quote_tweet", "resolved_at": "2026-05-03T09:30:00+00:00"}),
            ),
        ],
    )

    report = build_proactive_cooldown_audit(conn, days=7, max_actions=1, now=NOW)

    assert len(report.violations) == 1
    assert report.violations[0].target_id == "id:42"
    assert report.violations[0].action_ids == ("a2", "a1")
    assert report.violations[0].sources == ("actions",)


def test_formatters_are_deterministic(db):
    first = _insert_action(db, tweet_id="fmt-1", action_type="reply")
    second = _insert_action(db, tweet_id="fmt-2", action_type="quote_tweet")
    _set_time(db, first, "2026-05-03T08:00:00+00:00")
    _set_time(db, second, "2026-05-03T09:00:00+00:00")

    report = build_proactive_cooldown_audit(db, days=7, max_actions=1, now=NOW)
    payload = json.loads(format_proactive_cooldown_audit_json(report))
    text = format_proactive_cooldown_audit_text(report)

    assert payload["generated_at"] == "2026-05-03T12:00:00+00:00"
    assert payload["filters"] == {"days": 7, "max_actions": 1}
    assert "Proactive Cooldown Audit" in text
    assert "Totals: audited=2 targets=1 violations=1 missing_target=0" in text
    assert f"ids={second},{first}" in text


def test_cli_supports_db_json_text_and_parse_errors(file_db, db, monkeypatch, capsys):
    file_first = _insert_action(file_db, tweet_id="cli-file-1", action_type="reply")
    file_second = _insert_action(file_db, tweet_id="cli-file-2", action_type="quote_tweet")
    _set_time(file_db, file_first, "2026-05-03T08:00:00+00:00")
    _set_time(file_db, file_second, "2026-05-03T09:00:00+00:00")

    exit_code = proactive_cooldown_audit_script.main(
        ["--db", str(file_db.db_path), "--days", "36500", "--max-actions", "1", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["violations"][0]["action_ids"] == [file_second, file_first]

    ctx_first = _insert_action(db, tweet_id="cli-ctx-1", action_type="reply")
    ctx_second = _insert_action(db, tweet_id="cli-ctx-2", action_type="quote_tweet")
    _set_time(db, ctx_first, "2026-05-03T08:00:00+00:00")
    _set_time(db, ctx_second, "2026-05-03T09:00:00+00:00")
    monkeypatch.setattr(
        proactive_cooldown_audit_script,
        "script_context",
        lambda: _script_context(db),
    )
    exit_code = proactive_cooldown_audit_script.main(["--days", "36500", "--max-actions", "1"])
    text = capsys.readouterr().out

    assert exit_code == 1
    assert "Proactive Cooldown Audit" in text
    assert f"ids={ctx_second},{ctx_first}" in text

    invalid = proactive_cooldown_audit_script.main(["--days", "0"])
    assert invalid == 2
    assert "value must be positive" in capsys.readouterr().err
