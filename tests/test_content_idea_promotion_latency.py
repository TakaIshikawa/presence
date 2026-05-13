"""Tests for content idea promotion latency reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.content_idea_promotion_latency import (
    build_content_idea_promotion_latency_report,
    format_content_idea_promotion_latency_json,
    format_content_idea_promotion_latency_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_idea_promotion_latency.py"
spec = importlib.util.spec_from_file_location("content_idea_promotion_latency_script", SCRIPT_PATH)
content_idea_promotion_latency_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(content_idea_promotion_latency_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _idea(db, *, status: str, priority: str, topic: str, created: str, updated: str) -> int:
    cursor = db.conn.execute(
        """INSERT INTO content_ideas
           (note, status, priority, topic, created_at, updated_at)
           VALUES ('Idea note', ?, ?, ?, ?, ?)""",
        (status, priority, topic, created, updated),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def test_classifies_open_and_closed_ideas_with_deterministic_latency(db):
    fresh = _idea(db, status="open", priority="high", topic="ai", created="2026-05-19T12:00:00+00:00", updated="2026-05-19T12:00:00+00:00")
    stale = _idea(db, status="open", priority="normal", topic="dx", created="2026-05-01T12:00:00+00:00", updated="2026-05-01T12:00:00+00:00")
    promoted = _idea(db, status="promoted", priority="low", topic="ai", created="2026-05-10T12:00:00+00:00", updated="2026-05-13T12:00:00+00:00")
    dismissed = _idea(db, status="dismissed", priority="normal", topic="ops", created="2026-05-10T12:00:00+00:00", updated="2026-05-18T12:00:00+00:00")

    report = build_content_idea_promotion_latency_report(db, stale_days=14, now=NOW)

    by_id = {item["id"]: item for item in report["items"]}
    assert by_id[fresh]["classification"] == "fresh_open"
    assert by_id[stale]["classification"] == "stale_open"
    assert by_id[promoted]["classification"] == "promoted"
    assert by_id[promoted]["latency_days"] == 3
    assert by_id[promoted]["latency_bucket"] == "2-3d"
    assert by_id[dismissed]["classification"] == "dismissed"
    assert by_id[dismissed]["latency_bucket"] == "8-14d"


def test_totals_include_status_priority_topic_and_stale_open_count(db):
    _idea(db, status="open", priority="high", topic="ai", created="2026-05-01T12:00:00+00:00", updated="2026-05-01T12:00:00+00:00")
    _idea(db, status="promoted", priority="high", topic="ai", created="2026-05-10T12:00:00+00:00", updated="2026-05-11T12:00:00+00:00")
    _idea(db, status="dismissed", priority="low", topic="dx", created="2026-05-10T12:00:00+00:00", updated="2026-05-20T12:00:00+00:00")

    report = build_content_idea_promotion_latency_report(db, stale_days=7, now=NOW)

    assert report["totals"]["status_counts"]["stale_open"] == 1
    assert report["totals"]["status_counts"]["promoted"] == 1
    assert report["totals"]["priority_counts"] == {"high": 2, "low": 1}
    assert report["totals"]["topic_counts"] == {"ai": 2, "dx": 1}
    assert report["totals"]["stale_open_count"] == 1


def test_limit_json_text_and_cli(db, monkeypatch, capsys):
    _idea(db, status="open", priority="high", topic="ai", created="2026-05-01T12:00:00+00:00", updated="2026-05-01T12:00:00+00:00")
    _idea(db, status="promoted", priority="low", topic="dx", created="2026-05-10T12:00:00+00:00", updated="2026-05-12T12:00:00+00:00")

    report = build_content_idea_promotion_latency_report(db, limit=1, now=NOW)
    assert len(report["items"]) == 1
    assert list(json.loads(format_content_idea_promotion_latency_json(report)).keys()) == sorted(report.keys())
    assert "Content Idea Promotion Latency" in format_content_idea_promotion_latency_text(report)

    monkeypatch.setattr(content_idea_promotion_latency_script, "script_context", lambda: _script_context(db))
    monkeypatch.setattr(
        content_idea_promotion_latency_script,
        "build_content_idea_promotion_latency_report",
        lambda db, **kwargs: build_content_idea_promotion_latency_report(db, now=NOW, **kwargs),
    )
    assert content_idea_promotion_latency_script.main(["--lookback-days", "30", "--stale-days", "7", "--limit", "2", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["stale_days"] == 7
    assert content_idea_promotion_latency_script.main(["--stale-days", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err


def test_missing_schema_and_invalid_args_are_reported():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_content_idea_promotion_latency_report(conn, now=NOW)
    assert report["missing_tables"] == ["content_ideas"]

    with pytest.raises(ValueError, match="lookback_days must be positive"):
        build_content_idea_promotion_latency_report(conn, lookback_days=0, now=NOW)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_idea_promotion_latency_report(conn, limit=0, now=NOW)
    conn.close()
