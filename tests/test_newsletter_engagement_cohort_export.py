from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from output.newsletter_engagement_cohort_export import build_newsletter_engagement_cohort_export_from_db, format_newsletter_engagement_cohort_export_csv, format_newsletter_engagement_cohort_export_json

SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_newsletter_engagement_cohorts.py"
spec = importlib.util.spec_from_file_location("export_newsletter_engagement_cohorts_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE newsletter_subscribers (id INTEGER PRIMARY KEY, signup_source TEXT, status TEXT, created_at TEXT)")
    conn.execute("CREATE TABLE newsletter_engagement_events (subscriber_id INTEGER, event_type TEXT)")
    return conn


def test_cohort_json_csv_and_cli(tmp_path, capsys):
    conn = _conn()
    conn.executemany("INSERT INTO newsletter_subscribers VALUES (?, ?, ?, ?)", [(1, "blog", "active", "2026-01-03T00:00:00+00:00"), (2, "blog", "unsubscribed", "2026-01-15T00:00:00+00:00")])
    conn.executemany("INSERT INTO newsletter_engagement_events VALUES (?, ?)", [(1, "open"), (1, "click"), (2, "unsubscribe")])
    export = build_newsletter_engagement_cohort_export_from_db(conn)
    assert export["rows"][0]["cohort_month"] == "2026-01"
    assert export["rows"][0]["engagement_rate"] == 1.0
    assert json.loads(format_newsletter_engagement_cohort_export_json(export))["artifact_type"] == "newsletter_engagement_cohort_export"
    assert "cohort_month,signup_source" in format_newsletter_engagement_cohort_export_csv(export)
    conn.commit()
    dest = sqlite3.connect(tmp_path / "cohorts.sqlite")
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(tmp_path / "cohorts.sqlite"), "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["rows"]


def test_missing_optional_and_validation():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("CREATE TABLE newsletter_subscribers (id INTEGER PRIMARY KEY, source TEXT, created_at TEXT)")
    export = build_newsletter_engagement_cohort_export_from_db(conn)
    assert "missing optional engagement events table" in export["diagnostics"]
    assert format_newsletter_engagement_cohort_export_csv(export).startswith("cohort_month")
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
