from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path

from evaluation.newsletter_segment_retention_drop import (
    build_newsletter_segment_retention_drop_report,
    build_newsletter_segment_retention_drop_report_from_db,
    format_newsletter_segment_retention_drop_json,
    format_newsletter_segment_retention_drop_text,
)


NOW = datetime(2026, 5, 25, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_segment_retention_drop.py"
spec = importlib.util.spec_from_file_location("script_newsletter_segment_retention_drop", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_in_memory_rows_sort_by_loss_rate_then_segment_id():
    rows = [
        {"subscriber_id": f"a{i}", "segment_id": "beta", "subscriber_count": 10 if i == 0 else 0}
        for i in range(10)
    ] + [
        {"subscriber_id": "a1", "segment_id": "beta", "unsubscribed_at": "2026-05-23T00:00:00+00:00"},
        {"subscriber_id": "b1", "segment_id": "alpha", "subscriber_count": 10, "unsubscribed_at": "2026-05-23T00:00:00+00:00"},
        {"subscriber_id": "b2", "segment_id": "alpha", "unsubscribed_at": "2026-05-22T00:00:00+00:00"},
    ]
    report = build_newsletter_segment_retention_drop_report(rows, now=NOW, min_loss_rate=0.0)
    assert report["artifact_type"] == "newsletter_segment_retention_drop"
    assert report["drops"][0]["segment_id"] == "alpha"
    assert {"generated_at", "filters", "summary", "drops", "missing_tables", "missing_columns", "empty_state"} <= set(report)
    assert json.loads(format_newsletter_segment_retention_drop_json(report))["artifact_type"] == "newsletter_segment_retention_drop"
    assert "Newsletter Segment Retention Drop" in format_newsletter_segment_retention_drop_text(report)


def test_db_with_segment_join_and_cli(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE newsletter_subscribers (id TEXT, email TEXT, status TEXT, unsubscribed_at TEXT);
        CREATE TABLE newsletter_subscriber_segments (subscriber_id TEXT, segment_id TEXT);
        CREATE TABLE newsletter_segments (id TEXT, name TEXT);
        INSERT INTO newsletter_segments VALUES ('s1', 'Founders');
        """
    )
    for i in range(10):
        conn.execute("INSERT INTO newsletter_subscribers VALUES (?, ?, ?, ?)", (f"u{i}", f"u{i}@e.test", "active", None))
        conn.execute("INSERT INTO newsletter_subscriber_segments VALUES (?, ?)", (f"u{i}", "s1"))
    for i in range(3):
        conn.execute(
            "UPDATE newsletter_subscribers SET status='unsubscribed', unsubscribed_at=? WHERE id=?",
            ("2026-05-24T00:00:00+00:00", f"u{i}"),
        )
    conn.commit()
    report = build_newsletter_segment_retention_drop_report_from_db(conn, now=NOW, min_loss_rate=0.01)
    assert report["drops"][0]["segment_id"] == "s1"
    assert report["drops"][0]["segment_name"] == "Founders"
    db = tmp_path / "db.sqlite"
    out = sqlite3.connect(db)
    conn.backup(out)
    out.close()
    assert script.main(["--db", str(db), "--format", "text", "--window-days", "7", "--baseline-days", "14"]) == 0
    assert "s1" in capsys.readouterr().out
    assert script.main(["--db", str(db), "--window-days", "0"]) == 2


def test_missing_segment_tables_falls_back_to_subscriber_segment_column():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE newsletter_subscribers (email TEXT, segment TEXT, status TEXT, unsubscribed_at TEXT)")
    conn.execute("INSERT INTO newsletter_subscribers VALUES ('a@test', 'trial', 'unsubscribed', '2026-05-24T00:00:00+00:00')")
    report = build_newsletter_segment_retention_drop_report_from_db(conn, now=NOW, min_loss_rate=0.0)
    assert report["missing_tables"] == ["newsletter_subscriber_segments|newsletter_segments"]
    assert report["drops"][0]["segment_id"] == "trial"


def test_missing_schema_reports_empty_state():
    report = build_newsletter_segment_retention_drop_report_from_db(sqlite3.connect(":memory:"))
    assert report["missing_tables"] == ["newsletter_subscribers"]
    assert report["empty_state"]["reason"] == "missing_schema"
