from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

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


def test_signup_source_quality_findings_and_rendering(tmp_path, capsys):
    rows = [
        {"id": 1, "source": "Ad", "email": "a@mailinator.com", "created_at": "2026-05-01T10:00:00Z"},
        {"id": 2, "source": "Ad", "email": "b@example.com", "created_at": "2026-05-01T11:00:00Z", "consented_at": "2026-05-01T11:01:00Z"},
        {"id": 3, "source": "Ad", "email": "c@example.com", "created_at": "2026-05-01T12:00:00Z", "campaign": "spring"},
        {"id": 4, "source": "Organic", "email": "d@example.org", "created_at": "2026-05-02T12:00:00Z", "campaign": "may", "consented_at": "2026-05-02T12:01:00Z"},
    ]
    report = build_newsletter_signup_source_quality_report(rows, burst_threshold=2)

    assert report["artifact_type"] == "newsletter_signup_source_quality"
    assert report["totals"]["missing_consent"] == 2
    assert report["totals"]["missing_campaign"] == 2
    assert report["totals"]["disposable_domains"] == 1
    assert any("email_domain_burst" in item["reasons"] for item in report["findings"])
    assert json.loads(format_newsletter_signup_source_quality_json(report))["artifact_type"] == "newsletter_signup_source_quality"
    assert "source | total" in format_newsletter_signup_source_quality_text(report)

    conn = sqlite3.connect(":memory:")
    conn.executescript("CREATE TABLE newsletter_subscribers (id INTEGER, source TEXT, campaign TEXT, consented_at TEXT, created_at TEXT, email TEXT);")
    conn.executemany("INSERT INTO newsletter_subscribers VALUES (:id, :source, :campaign, :consented_at, :created_at, :email)", [{**row, "campaign": row.get("campaign"), "consented_at": row.get("consented_at")} for row in rows])
    conn.commit()
    db_path = tmp_path / "signups.sqlite"
    with sqlite3.connect(db_path) as target:
        conn.backup(target)
    assert script.main(["--db", str(db_path), "--format", "text", "--burst-threshold", "2"]) == 0
    assert "Newsletter Signup Source Quality" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--burst-threshold", "0"]) == 2


def test_missing_table_and_empty_state():
    missing = build_newsletter_signup_source_quality_report_from_db(sqlite3.connect(":memory:"))
    assert missing["missing_tables"] == ["newsletter_subscribers"]
    report = build_newsletter_signup_source_quality_report([])
    assert report["empty_state"]["is_empty"] is True
