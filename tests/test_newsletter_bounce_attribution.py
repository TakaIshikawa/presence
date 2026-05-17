"""Tests for newsletter bounce attribution."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path

from evaluation.newsletter_bounce_attribution import (
    build_newsletter_bounce_attribution_report_from_db,
    format_newsletter_bounce_attribution_json,
    format_newsletter_bounce_attribution_text,
)


NOW = datetime(2026, 5, 15, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_bounce_attribution.py"
spec = importlib.util.spec_from_file_location("newsletter_bounce_attribution_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_groups_bounce_signals_by_issue_campaign_and_reason():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE newsletter_metrics (id TEXT, issue_id TEXT, campaign TEXT, recipient_email TEXT, event_type TEXT, reason TEXT, raw_metrics TEXT, occurred_at TEXT)"
    )
    conn.executemany(
        "INSERT INTO newsletter_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("m1", "issue-1", "launch", "a@example.com", "bounce", "mailbox full", "{}", NOW.isoformat()),
            ("m2", "issue-1", "launch", "b@example.com", "failed_delivery", "mailbox full", "{}", NOW.isoformat()),
            (
                "m3",
                "issue-2",
                "nurture",
                "c@other.test",
                "open",
                "",
                json.dumps({"delivery_error": True, "reason": "domain blocked"}),
                NOW.isoformat(),
            ),
        ],
    )

    report = build_newsletter_bounce_attribution_report_from_db(conn, now=NOW)

    assert report["summary"]["delivery_issue_events"] == 3
    assert report["findings"][0]["issue_id"] == "issue-1"
    assert report["findings"][0]["reason"] == "mailbox_full"
    assert report["findings"][0]["affected_recipient_count"] == 2
    assert report["findings"][0]["top_domains"] == [{"value": "example.com", "count": 2}]
    payload = json.loads(format_newsletter_bounce_attribution_json(report))
    assert list(payload) == ["artifact_type", "filters", "findings", "generated_at", "schema_gaps", "summary"]
    text = format_newsletter_bounce_attribution_text(report)
    assert "issue=issue-1" in text
    assert "reason=mailbox_full" in text
    assert "recipients=2 domains=1" in text


def test_missing_schema_and_cli_db_do_not_crash(tmp_path, capsys):
    db_path = tmp_path / "report.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE unrelated (id INTEGER)")
    conn.commit()
    conn.close()

    report = build_newsletter_bounce_attribution_report_from_db(sqlite3.connect(db_path), now=NOW)
    assert report["findings"] == []
    assert report["schema_gaps"]["missing_tables"]

    assert script.main(["--db", str(db_path), "--days", "7", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_bounce_attribution"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "No newsletter bounce attribution findings" in capsys.readouterr().out
