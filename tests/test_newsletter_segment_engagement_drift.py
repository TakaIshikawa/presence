from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from evaluation.newsletter_segment_engagement_drift import build_newsletter_segment_engagement_drift_report


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_segment_engagement_drift.py"
spec = importlib.util.spec_from_file_location("newsletter_segment_engagement_drift_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_segment_metrics (
            issue_id TEXT, segment TEXT, open_rate REAL, click_rate REAL,
            unsubscribe_rate REAL, sent_at TEXT
        )"""
    )
    return conn


def test_click_open_decline():
    conn = _conn()
    conn.execute("INSERT INTO newsletter_segment_metrics VALUES ('old', 'pro', 0.60, 0.30, 0.01, '2026-03-01T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_segment_metrics VALUES ('new', 'pro', 0.40, 0.10, 0.01, '2026-04-25T00:00:00+00:00')")

    report = build_newsletter_segment_engagement_drift_report(conn, days=14, baseline_days=90, min_delta_pct=20, now=NOW)

    assert report.drifts[0].drift_type == "click_decline"
    assert report.drifts[0].severity == "high"
    assert report.drifts[0].sample_issue_ids == ("new",)


def test_unsubscribe_increase():
    conn = _conn()
    conn.execute("INSERT INTO newsletter_segment_metrics VALUES ('old', 'free', 0.50, 0.20, 0.01, '2026-03-01T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_segment_metrics VALUES ('new', 'free', 0.50, 0.20, 0.03, '2026-04-25T00:00:00+00:00')")

    report = build_newsletter_segment_engagement_drift_report(conn, days=14, baseline_days=90, min_delta_pct=50, now=NOW)

    assert report.drifts[0].drift_type == "unsubscribe_increase"


def test_segment_filtering():
    conn = _conn()
    conn.execute("INSERT INTO newsletter_segment_metrics VALUES ('old1', 'a', 0.60, 0.30, 0.01, '2026-03-01T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_segment_metrics VALUES ('new1', 'a', 0.40, 0.10, 0.01, '2026-04-25T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_segment_metrics VALUES ('old2', 'b', 0.60, 0.30, 0.01, '2026-03-01T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_segment_metrics VALUES ('new2', 'b', 0.40, 0.10, 0.01, '2026-04-25T00:00:00+00:00')")

    report = build_newsletter_segment_engagement_drift_report(conn, segment="b", days=14, baseline_days=90, now=NOW)

    assert [drift.segment for drift in report.drifts] == ["b"]


def test_missing_schema_empty_state():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_segment_engagement_drift_report(conn, now=NOW)

    assert report.empty_state["is_empty"] is True
    assert "newsletter_segment_metrics" in report.missing_tables


def test_cli_json_output(capsys, tmp_path):
    db_path = tmp_path / "newsletter.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """CREATE TABLE newsletter_segment_metrics (
            issue_id TEXT, segment TEXT, open_rate REAL, click_rate REAL, unsubscribe_rate REAL, sent_at TEXT
        );
        INSERT INTO newsletter_segment_metrics VALUES ('old', 'pro', 0.60, 0.30, 0.01, '2026-03-01T00:00:00+00:00');
        INSERT INTO newsletter_segment_metrics VALUES ('new', 'pro', 0.40, 0.10, 0.01, '2026-04-25T00:00:00+00:00');"""
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--days", "30", "--baseline-days", "90", "--format", "json"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["artifact_type"] == "newsletter_segment_engagement_drift"
    assert payload["drifts"][0]["segment"] == "pro"
