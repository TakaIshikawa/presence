from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.newsletter_signup_source_quality import (
    build_newsletter_signup_source_quality_report,
    build_newsletter_signup_source_quality_report_from_db,
    format_newsletter_signup_source_quality_json,
    format_newsletter_signup_source_quality_text,
)


SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_signup_source_quality.py"
spec = importlib.util.spec_from_file_location("newsletter_signup_source_quality_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_reports_source_quality_findings():
    report = build_newsletter_signup_source_quality_report(
        [
            {"subscriber_id": 1, "signup_source": "ad", "email": "a@mailinator.com", "created_at": "2026-05-01T00:00:00+00:00"},
            {"subscriber_id": 2, "signup_source": "ad", "email": "b@example.com", "campaign": "spring", "consented_at": "2026-05-01T00:00:00+00:00"},
            {"subscriber_id": 3, "signup_source": "ad", "email": "c@example.com", "campaign": "spring", "consented_at": "2026-05-01T00:00:00+00:00"},
            {"subscriber_id": 4, "signup_source": "ad", "email": "d@example.com", "campaign": "spring", "consented_at": "2026-05-01T00:00:00+00:00"},
        ],
        burst_threshold=3,
    )
    assert report["artifact_type"] == "newsletter_signup_source_quality"
    assert report["totals"]["subscriber_count"] == 4
    assert report["source_breakdown"][0]["disposable_domain_count"] == 1
    assert {f["reason"] for f in report["findings"]} >= {"missing_consent_timestamp", "missing_campaign_attribution", "disposable_email_domain", "repeated_email_domain_burst"}


def test_db_loader_rendering_and_cli(tmp_path, capsys):
    path = tmp_path / "subscribers.sqlite"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE newsletter_subscribers (
            id INTEGER, signup_source TEXT, campaign TEXT, consented_at TEXT, created_at TEXT, email TEXT
        );
        INSERT INTO newsletter_subscribers VALUES (1, 'ad', NULL, NULL, '2026-05-01T00:00:00+00:00', 'a@mailinator.com');
        """
    )
    conn.close()
    report = build_newsletter_signup_source_quality_report_from_db(sqlite3.connect(path), burst_threshold=2)
    assert report["findings"][0]["source"] == "ad"
    assert json.loads(format_newsletter_signup_source_quality_json(report))["artifact_type"] == "newsletter_signup_source_quality"
    assert "Newsletter Signup Source Quality" in format_newsletter_signup_source_quality_text(report)
    assert script.main(["--db", str(path), "--format", "json", "--limit", "5", "--burst-threshold", "2"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_signup_source_quality"
    with pytest.raises(SystemExit):
        script.parse_args(["--burst-threshold", "0"])


def test_missing_table():
    assert build_newsletter_signup_source_quality_report_from_db(sqlite3.connect(":memory:"))["missing_tables"] == ["newsletter_subscribers"]
