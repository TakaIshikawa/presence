"""Tests for proactive action conversion by source reporting."""

from __future__ import annotations

from contextlib import contextmanager
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from engagement.proactive_action_conversion_by_source import (
    build_proactive_action_conversion_by_source_report_from_db,
    format_proactive_action_conversion_by_source_json,
    format_proactive_action_conversion_by_source_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "proactive_action_conversion_by_source.py"
spec = importlib.util.spec_from_file_location("proactive_action_conversion_by_source_script", SCRIPT_PATH)
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
        """CREATE TABLE proactive_actions (
             id INTEGER PRIMARY KEY,
             discovery_source TEXT,
             action_type TEXT,
             status TEXT,
             created_at TEXT,
             reviewed_at TEXT,
             posted_at TEXT
           );"""
    )
    return conn


def test_report_groups_counts_rates_and_latencies():
    conn = _conn()
    conn.executemany(
        """INSERT INTO proactive_actions
           (id, discovery_source, action_type, status, created_at, reviewed_at, posted_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [
            (1, "Mention", "Reply", "pending", "2026-05-20T00:00:00+00:00", None, None),
            (2, "mention", "reply", "approved", "2026-05-20T00:00:00+00:00", "2026-05-20T02:00:00+00:00", None),
            (3, "mention", "reply", "posted", "2026-05-20T00:00:00+00:00", "2026-05-20T04:00:00+00:00", "2026-05-20T07:00:00+00:00"),
            (4, "search", "quote", "dismissed", "2026-05-20T00:00:00+00:00", "2026-05-20T01:00:00+00:00", None),
        ],
    )

    report = build_proactive_action_conversion_by_source_report_from_db(conn)
    mention = report["grouped_summaries"][0]

    assert report["artifact_type"] == "proactive_action_conversion_by_source"
    assert mention["discovery_source"] == "mention"
    assert mention["action_type"] == "reply"
    assert mention["pending_count"] == 1
    assert mention["approved_count"] == 2
    assert mention["posted_count"] == 1
    assert mention["approval_rate"] == 0.6667
    assert mention["post_rate"] == 0.3333
    assert mention["median_review_hours"] == 3.0
    assert mention["median_post_hours"] == 7.0


def test_formatters_cli_and_missing_schema(tmp_path, monkeypatch, capsys):
    conn = _conn()
    conn.execute(
        """INSERT INTO proactive_actions
           (id, discovery_source, action_type, status, created_at, reviewed_at)
           VALUES (1, 'mention', 'reply', 'approved', '2026-05-20T00:00:00+00:00', '2026-05-20T02:00:00+00:00')"""
    )
    report = build_proactive_action_conversion_by_source_report_from_db(conn)
    assert json.loads(format_proactive_action_conversion_by_source_json(report))["summary"]["approved_count"] == 1
    assert "approved=1" in format_proactive_action_conversion_by_source_text(report)

    db_path = tmp_path / "actions.sqlite"
    conn.commit()
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["grouped_summaries"][0]["approved_count"] == 1
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Proactive Action Conversion By Source" in capsys.readouterr().out

    missing = sqlite3.connect(":memory:")
    assert build_proactive_action_conversion_by_source_report_from_db(missing)["missing_tables"] == ["proactive_actions"]

    memory = _conn()
    monkeypatch.setattr(script, "script_context", lambda: _script_context(memory))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["rows_scanned"] == 0
