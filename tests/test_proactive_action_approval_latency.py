"""Tests for proactive action approval latency reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
import sqlite3

from engagement.proactive_action_approval_latency import (
    build_proactive_action_approval_latency_report_from_db,
    format_proactive_action_approval_latency_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_approval_latency.py"
spec = importlib.util.spec_from_file_location("proactive_action_approval_latency_script", SCRIPT_PATH)
proactive_action_approval_latency_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(proactive_action_approval_latency_script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE proactive_actions (
            id INTEGER PRIMARY KEY,
            action_type TEXT,
            discovery_source TEXT,
            status TEXT,
            created_at TEXT,
            reviewed_at TEXT,
            posted_at TEXT
        );
        """
    )
    return conn


def test_report_flags_pending_reviewed_and_posted_latency():
    conn = _conn()
    conn.executemany(
        "INSERT INTO proactive_actions VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (1, "reply", "search", "pending", "2026-05-18T00:00:00+00:00", None, None),
            (2, "reply", "search", "approved", "2026-05-17T00:00:00+00:00", "2026-05-20T00:00:00+00:00", None),
            (3, "post", "rss", "posted", "2026-05-19T00:00:00+00:00", "2026-05-19T01:00:00+00:00", "2026-05-20T10:00:00+00:00"),
            (4, "post", "rss", "posted", "2026-05-01T00:00:00+00:00", "2026-05-01T01:00:00+00:00", "2026-05-01T02:00:00+00:00"),
        ],
    )

    report = build_proactive_action_approval_latency_report_from_db(
        conn, now=NOW, window_days=7, pending_sla_hours=24, review_sla_hours=24, limit=10
    )

    assert report["artifact_type"] == "proactive_action_approval_latency"
    assert [item["action_id"] for item in report["overdue_pending_actions"]] == [1]
    assert [item["action_id"] for item in report["late_reviewed_actions"]] == [2]
    assert [item["action_id"] for item in report["late_posted_actions"]] == [3]
    assert report["grouped_summaries"][0]["action_type"] == "reply"
    assert "Late posted actions" in format_proactive_action_approval_latency_text(report)


def test_missing_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_proactive_action_approval_latency_report_from_db(conn, now=NOW)
    assert report["missing_tables"] == ["proactive_actions"]
    assert report["summary"]["rows_scanned"] == 0


def test_cli_outputs_text_and_json(tmp_path, capsys):
    db_path = tmp_path / "db.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE proactive_actions (
            id INTEGER PRIMARY KEY,
            action_type TEXT,
            discovery_source TEXT,
            status TEXT,
            created_at TEXT,
            reviewed_at TEXT,
            posted_at TEXT
        );
        INSERT INTO proactive_actions VALUES (1, 'reply', 'search', 'pending', '2026-05-18T00:00:00+00:00', NULL, NULL);
        """
    )
    conn.close()

    assert proactive_action_approval_latency_script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert '"artifact_type": "proactive_action_approval_latency"' in capsys.readouterr().out
    assert proactive_action_approval_latency_script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Proactive Action Approval Latency" in capsys.readouterr().out
