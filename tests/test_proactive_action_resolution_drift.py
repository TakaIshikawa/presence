"""Tests for proactive action resolution drift reporting."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.proactive_action_resolution_drift import (
    build_proactive_action_resolution_drift_report,
    build_proactive_action_resolution_drift_report_from_db,
    format_proactive_action_resolution_drift_json,
    format_proactive_action_resolution_drift_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_resolution_drift.py"
spec = importlib.util.spec_from_file_location("proactive_action_resolution_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _ts(hours_ago: int) -> str:
    return (NOW - timedelta(hours=hours_ago)).isoformat()


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE proactive_actions (
               id INTEGER PRIMARY KEY,
               status TEXT,
               action_type TEXT,
               target_author_handle TEXT,
               target_tweet_id TEXT,
               posted_tweet_id TEXT,
               posted_at TEXT,
               approved_at TEXT,
               created_at TEXT,
               draft_text TEXT,
               relevance_score REAL,
               reviewed_at TEXT,
               review_status TEXT,
               reviewer_id TEXT,
               knowledge_ids TEXT
           );"""
    )
    return conn


def test_builder_detects_resolution_drift_and_malformed_json():
    report = build_proactive_action_resolution_drift_report(
        [
            {"proactive_action_id": 1, "status": "posted", "action_type": "reply", "target_author_handle": "a"},
            {"proactive_action_id": 2, "status": "approved", "action_type": "reply", "target_author_handle": "b", "approved_at": _ts(30)},
            {
                "proactive_action_id": 3,
                "status": "pending",
                "action_type": "quote",
                "target_author_handle": "c",
                "draft_text": "Ready",
                "relevance_score": 0.9,
                "knowledge_ids": "[bad",
            },
            {"proactive_action_id": 4, "status": "pending", "action_type": "reply", "target_author_handle": "d", "target_tweet_id": ""},
            {"proactive_action_id": 5, "status": "pending", "action_type": "reply", "target_author_handle": "d", "target_tweet_id": "tw-1"},
        ],
        sla_hours=24,
        now=NOW,
    )

    assert report["artifact_type"] == "proactive_action_resolution_drift"
    assert report["summary"]["by_issue_type"] == {
        "approved_stale_without_posting": 1,
        "duplicate_blank_target_tweet_id": 1,
        "malformed_knowledge_ids_json": 1,
        "pending_strong_relevance_without_review": 1,
        "posted_missing_posted_at": 1,
        "posted_missing_posted_tweet_id": 1,
    }
    first = report["drift_items"][0]
    assert {"proactive_action_id", "action_type", "target_author_handle", "issue_type", "age_hours"} <= set(first)


def test_db_loader_and_configurable_thresholds():
    conn = _conn()
    conn.execute(
        "INSERT INTO proactive_actions (id, status, action_type, target_author_handle, draft_text, relevance_score) VALUES (1, 'pending', 'reply', 'a', 'Draft', 0.75)"
    )
    conn.execute(
        "INSERT INTO proactive_actions (id, status, action_type, target_author_handle, approved_at) VALUES (2, 'approved', 'reply', 'b', ?)",
        (_ts(12),),
    )

    report = build_proactive_action_resolution_drift_report_from_db(conn, sla_hours=8, relevance_threshold=0.7, now=NOW)

    assert [item["proactive_action_id"] for item in report["drift_items"]] == [1, 2]
    assert report["drift_items"][1]["age_hours"] == 12.0


def test_missing_table_cli_and_validation(tmp_path, capsys):
    missing = build_proactive_action_resolution_drift_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["proactive_actions"]

    conn = _conn()
    conn.execute("INSERT INTO proactive_actions (id, status, action_type, target_author_handle) VALUES (1, 'posted', 'reply', 'a')")
    conn.commit()
    db_path = tmp_path / "proactive.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["drift_count"] == 2
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Proactive Action Resolution Drift" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--sla-hours", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert script.main(["--db", str(db_path), "--relevance-threshold", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err


def test_formatters_and_invalid_thresholds():
    report = build_proactive_action_resolution_drift_report([], now=NOW)
    assert json.loads(format_proactive_action_resolution_drift_json(report))["artifact_type"] == "proactive_action_resolution_drift"
    assert "No proactive action resolution drift found" in format_proactive_action_resolution_drift_text(report)
    with pytest.raises(ValueError, match="sla_hours must be positive"):
        build_proactive_action_resolution_drift_report([], sla_hours=0)
    with pytest.raises(ValueError, match="relevance_threshold must be non-negative"):
        build_proactive_action_resolution_drift_report([], relevance_threshold=-0.1)
    with pytest.raises(ValueError, match="limit must be positive"):
        build_proactive_action_resolution_drift_report([], limit=0)
