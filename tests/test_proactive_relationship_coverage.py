"""Tests for proactive relationship coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from engagement.proactive_relationship_coverage import (
    build_proactive_relationship_coverage_report,
    format_proactive_relationship_coverage_json,
    format_proactive_relationship_coverage_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
_ACTION_SEQUENCE = 0
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_relationship_coverage.py"
spec = importlib.util.spec_from_file_location("proactive_relationship_coverage_script", SCRIPT_PATH)
proactive_relationship_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(proactive_relationship_coverage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _action(
    db,
    *,
    handle: str,
    context: str | None,
    status: str = "pending",
    action_type: str = "reply",
    source: str = "search",
) -> int:
    global _ACTION_SEQUENCE
    _ACTION_SEQUENCE += 1
    cursor = db.conn.execute(
        """INSERT INTO proactive_actions
           (action_type, target_tweet_id, target_tweet_text, target_author_handle,
            discovery_source, relevance_score, draft_text, status,
            relationship_context, created_at)
           VALUES (?, ?, 'Post', ?, ?, 0.8, 'Draft', ?, ?, '2026-05-02T10:00:00+00:00')""",
        (action_type, f"{handle}-{action_type}-{_ACTION_SEQUENCE}", handle, source, status, context),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_classifies_relationship_context_and_totals(db):
    has_context = _action(db, handle="alice", context='{"stage": "warm"}')
    missing = _action(db, handle="bob", context="")
    malformed = _action(db, handle="cam", context="{bad")
    posted_missing = _action(db, handle="dee", context=None, status="posted")

    report = build_proactive_relationship_coverage_report(db, now=NOW)

    by_id = {item["id"]: item for item in report["items"]}
    assert by_id[has_context]["context_classification"] == "has_context"
    assert by_id[missing]["context_classification"] == "missing_context"
    assert by_id[malformed]["context_classification"] == "malformed_context"
    assert by_id[posted_missing]["context_classification"] == "posted_without_context"
    assert report["totals"]["by_context_classification"] == {
        "has_context": 1,
        "malformed_context": 1,
        "missing_context": 1,
        "posted_without_context": 1,
    }
    assert report["totals"]["malformed_context_count"] == 1
    assert report["totals"]["posted_without_context_count"] == 1


def test_groups_by_action_status_source_and_author(db):
    _action(db, handle="alice", context='{"stage": "warm"}', action_type="reply", source="search")
    _action(db, handle="alice", context=None, action_type="reply", source="search")
    _action(db, handle="bob", context="{}", action_type="quote_tweet", source="curated_timeline")

    report = build_proactive_relationship_coverage_report(db, now=NOW)

    group = next(
        row
        for row in report["groups"]
        if row["action_type"] == "reply"
        and row["status"] == "pending"
        and row["discovery_source"] == "search"
        and row["target_author_handle"] == "alice"
    )
    assert group["count"] == 2
    assert group["by_context_classification"]["has_context"] == 1
    assert group["by_context_classification"]["missing_context"] == 1
    assert report["totals"]["by_action_type"]["reply"] == 2
    assert report["totals"]["by_target_author_handle"]["alice"] == 2


def test_limit_and_required_item_fields(db):
    first = _action(db, handle="alice", context="{bad", source="search")
    _action(db, handle="bob", context='{"stage": "new"}', source="cultivate")

    report = build_proactive_relationship_coverage_report(db, limit=1, now=NOW)

    assert len(report["items"]) == 1
    item = report["items"][0]
    assert item["id"] == first
    assert item["action_type"] == "reply"
    assert item["status"] == "pending"
    assert item["discovery_source"] == "search"
    assert item["target_author_handle"] == "alice"
    assert item["context_classification"] == "malformed_context"


def test_json_text_and_cli_are_stable(db, monkeypatch, capsys):
    _action(db, handle="alice", context='{"stage": "warm"}')

    report = build_proactive_relationship_coverage_report(db, limit=5, now=NOW)
    payload = json.loads(format_proactive_relationship_coverage_json(report))
    text = format_proactive_relationship_coverage_text(report)

    assert list(payload.keys()) == sorted(payload.keys())
    assert "Proactive Relationship Coverage" in text
    assert "has_context=1" in text

    monkeypatch.setattr(
        proactive_relationship_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        proactive_relationship_coverage_script,
        "build_proactive_relationship_coverage_report",
        lambda db, **kwargs: build_proactive_relationship_coverage_report(db, now=NOW, **kwargs),
    )
    assert proactive_relationship_coverage_script.main(["--lookback-days", "7", "--limit", "5", "--format", "json"]) == 0
    cli_payload = json.loads(capsys.readouterr().out)
    assert cli_payload["filters"]["lookback_days"] == 7
    assert proactive_relationship_coverage_script.main(["--limit", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err


def test_missing_schema_and_invalid_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_proactive_relationship_coverage_report(conn, now=NOW)
    assert report["missing_tables"] == ["proactive_actions"]

    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_proactive_relationship_coverage_report(conn, lookback_days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_proactive_relationship_coverage_report(conn, limit=0, now=NOW)
    conn.close()
