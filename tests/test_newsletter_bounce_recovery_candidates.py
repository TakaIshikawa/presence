from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.newsletter_bounce_recovery_candidates import (
    build_newsletter_bounce_recovery_candidates_report_from_db,
    format_newsletter_bounce_recovery_candidates_json,
    format_newsletter_bounce_recovery_candidates_text,
)


NOW = datetime(2026, 5, 24, 12, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_bounce_recovery_candidates.py"
spec = importlib.util.spec_from_file_location("newsletter_bounce_recovery_candidates_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """CREATE TABLE newsletter_subscribers (
               id INTEGER PRIMARY KEY, email TEXT, status TEXT, updated_at TEXT
           );
           CREATE TABLE newsletter_events (
               subscriber_id INTEGER, event_type TEXT, event_at TEXT, bounce_type TEXT
           );
           CREATE TABLE newsletter_provider_events (
               subscriber_id INTEGER, type TEXT, occurred_at TEXT, bounce_class TEXT
           );"""
    )
    return conn


def test_recoverable_candidates_rank_by_newest_signal():
    conn = _db()
    conn.execute("INSERT INTO newsletter_subscribers VALUES (1, 'a@example.com', 'bounced', '2026-05-20T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_subscribers VALUES (2, 'b@example.net', 'active', '2026-05-23T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_subscribers VALUES (3, 'c@example.org', 'bounced', '2026-05-23T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_events VALUES (1, 'bounce', '2026-05-20T00:00:00+00:00', 'soft')")
    conn.execute("INSERT INTO newsletter_events VALUES (1, 'open', '2026-05-22T00:00:00+00:00', NULL)")
    conn.execute("INSERT INTO newsletter_provider_events VALUES (2, 'hard_bounce', '2026-05-21T00:00:00+00:00', 'hard')")
    conn.execute("INSERT INTO newsletter_events VALUES (3, 'bounce', '2026-05-23T00:00:00+00:00', 'hard')")
    report = build_newsletter_bounce_recovery_candidates_report_from_db(conn, now=NOW)
    assert [row["subscriber_id"] for row in report["candidates"]] == ["2", "1"]
    assert report["candidates"][0]["recommended_action"] == "verify engagement and remove from suppression"
    assert json.loads(format_newsletter_bounce_recovery_candidates_json(report))["artifact_type"] == "newsletter_bounce_recovery_candidates"


def test_missing_table_and_text_output(tmp_path, capsys):
    missing = build_newsletter_bounce_recovery_candidates_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert "newsletter_subscribers" in missing["missing_tables"]
    text = format_newsletter_bounce_recovery_candidates_text(missing)
    assert "Missing tables:" in text
    conn = _db()
    conn.execute("INSERT INTO newsletter_subscribers VALUES (1, 'a@example.com', 'bounced', NULL)")
    conn.execute("INSERT INTO newsletter_events VALUES (1, 'bounce', '2026-05-20T00:00:00+00:00', 'soft')")
    conn.commit()
    path = tmp_path / "db.sqlite"
    with sqlite3.connect(path) as out:
        conn.backup(out)
    assert script.main(["--db", str(path), "--format", "text", "--limit", "1"]) == 0
    assert "Newsletter Bounce Recovery Candidates" in capsys.readouterr().out
    assert script.main(["--db", str(path), "--limit", "0"]) == 2
