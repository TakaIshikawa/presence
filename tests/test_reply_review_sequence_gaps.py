"""Tests for reply review sequence gap reporting."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.reply_review_sequence_gaps import build_reply_review_sequence_gaps_report_from_db


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "reply_review_sequence_gaps.py"
spec = importlib.util.spec_from_file_location("reply_review_sequence_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE reply_queue (
               id INTEGER PRIMARY KEY,
               status TEXT,
               reviewed_at TEXT,
               posted_at TEXT,
               inbound_author_handle TEXT
           )"""
    )
    conn.execute(
        """CREATE TABLE reply_review_events (
               id INTEGER PRIMARY KEY,
               reply_queue_id INTEGER,
               event_type TEXT,
               created_at TEXT
           )"""
    )
    return conn


def _reply(conn: sqlite3.Connection, reply_id: int, status: str, reviewed_at: str | None = None, posted_at: str | None = None) -> None:
    conn.execute("INSERT INTO reply_queue VALUES (?, ?, ?, ?, ?)", (reply_id, status, reviewed_at, posted_at, f"author{reply_id}"))


def _event(conn: sqlite3.Connection, event_id: int, reply_id: int, event_type: str, created_at: str) -> None:
    conn.execute("INSERT INTO reply_review_events VALUES (?, ?, ?, ?)", (event_id, reply_id, event_type, created_at))


def test_detects_missing_approval_dismissal_stale_latest_and_no_events():
    conn = _conn()
    _reply(conn, 1, "approved", reviewed_at="2026-05-20T10:00:00+00:00")
    _reply(conn, 2, "dismissed", reviewed_at="2026-05-20T10:00:00+00:00")
    _event(conn, 1, 2, "approved", "2026-05-20T10:00:00+00:00")
    _reply(conn, 3, "posted", reviewed_at="2026-05-20T10:00:00+00:00", posted_at="2026-05-20T12:00:00+00:00")
    _event(conn, 2, 3, "approved", "2026-05-20T09:00:00+00:00")
    _reply(conn, 4, "posted", reviewed_at="2026-05-20T10:00:00+00:00", posted_at="2026-05-20T12:00:00+00:00")
    _reply(conn, 5, "approved", reviewed_at="2026-05-20T10:00:00+00:00")
    _event(conn, 3, 5, "approved", "2026-05-20T10:00:00+00:00")

    report = build_reply_review_sequence_gaps_report_from_db(conn, stale_tolerance_hours=1)

    by_id = {item["reply_queue_id"]: item for item in report["gaps"]}
    assert by_id[1]["gap_type"] == "missing_review_event"
    assert by_id[2]["gap_type"] == "missing_dismissal_event"
    assert by_id[3]["gap_type"] == "stale_latest_event"
    assert by_id[4]["gap_type"] == "missing_review_event"
    assert 5 not in by_id
    assert by_id[1]["inbound_author_handle"] == "author1"


def test_cli_status_filter_and_validation(tmp_path, capsys):
    conn = _conn()
    _reply(conn, 1, "approved", reviewed_at="2026-05-20T10:00:00+00:00")
    _reply(conn, 2, "dismissed", reviewed_at="2026-05-20T10:00:00+00:00")
    conn.commit()
    db_path = tmp_path / "reply.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)

    assert script.main(["--db", str(db_path), "--status", "dismissed"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert [item["reply_queue_id"] for item in payload["gaps"]] == [2]

    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Reply Review Sequence Gaps" in capsys.readouterr().out

    assert script.main(["--db", str(db_path), "--stale-tolerance-hours", "-1"]) == 2
    assert "value must be non-negative" in capsys.readouterr().err


def test_missing_events_table_reports_no_event_gap():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE reply_queue (id INTEGER PRIMARY KEY, status TEXT, reviewed_at TEXT, posted_at TEXT)")
    conn.execute("INSERT INTO reply_queue VALUES (1, 'approved', '2026-05-20T10:00:00+00:00', NULL)")

    report = build_reply_review_sequence_gaps_report_from_db(conn)

    assert report["missing_tables"] == ["reply_review_events"]
    assert report["gaps"][0]["gap_type"] == "missing_review_event"
