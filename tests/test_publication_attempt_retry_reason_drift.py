from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path

from evaluation.publication_attempt_retry_reason_drift import (
    build_publication_attempt_retry_reason_drift_report,
    build_publication_attempt_retry_reason_drift_report_from_db,
    format_publication_attempt_retry_reason_drift_json,
    format_publication_attempt_retry_reason_drift_text,
)


NOW = datetime(2026, 5, 24, 12, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_retry_reason_drift.py"
spec = importlib.util.spec_from_file_location("publication_attempt_retry_reason_drift_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_builder_computes_counts_shares_deltas_and_findings_by_platform_reason():
    rows = [
        {"attempted_at": "2026-05-05T00:00:00+00:00", "platform": "x", "retry_reason": "rate_limit"},
        {"attempted_at": "2026-05-06T00:00:00+00:00", "platform": "x", "retry_reason": "auth"},
        {"attempted_at": "2026-05-20T00:00:00+00:00", "platform": "x", "retry_reason": "auth"},
        {"attempted_at": "2026-05-21T00:00:00+00:00", "platform": "x", "retry_reason": "auth"},
        {"attempted_at": "2026-05-20T00:00:00+00:00", "platform": "blog", "retry_reason": "timeout"},
    ]

    report = build_publication_attempt_retry_reason_drift_report(
        rows,
        baseline_days=14,
        current_days=7,
        min_delta=0.4,
        min_sample=2,
        now=NOW,
    )

    auth = next(row for row in report["drift_rows"] if row["platform"] == "x" and row["reason"] == "auth")
    assert auth["baseline_count"] == 1
    assert auth["current_count"] == 2
    assert auth["delta_count"] == 1
    assert auth["baseline_share"] == 0.5
    assert auth["current_share"] == 1.0
    assert auth["delta_share"] == 0.5
    assert report["findings"][0]["platform"] == "x"
    assert report["findings"][0]["reason"] == "auth"


def test_builder_falls_back_to_error_category_then_status_for_reason():
    report = build_publication_attempt_retry_reason_drift_report(
        [
            {"attempted_at": "2026-05-05T00:00:00+00:00", "platform": "x", "error_category": "rate_limit"},
            {"attempted_at": "2026-05-06T00:00:00+00:00", "platform": "x", "status": "failed"},
            {"attempted_at": "2026-05-20T00:00:00+00:00", "platform": "x", "error_category": "rate_limit"},
            {"attempted_at": "2026-05-21T00:00:00+00:00", "platform": "x", "error_category": "rate_limit"},
        ],
        baseline_days=14,
        current_days=7,
        min_delta=0.4,
        min_sample=2,
        now=NOW,
    )

    assert {row["reason"] for row in report["drift_rows"]} == {"failed", "rate_limit"}
    assert report["findings"][0]["reason"] == "rate_limit"


def test_min_sample_suppresses_findings_but_keeps_drift_rows():
    report = build_publication_attempt_retry_reason_drift_report(
        [{"attempted_at": "2026-05-20T00:00:00+00:00", "platform": "x", "retry_reason": "auth"}],
        min_sample=2,
        now=NOW,
    )

    assert report["findings"] == []
    assert report["drift_rows"][0]["reason"] == "auth"


def test_sqlite_loader_reads_publication_attempts_optional_reason_columns():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE publication_attempts (
            id INTEGER,
            attempted_at TEXT,
            platform TEXT,
            error_category TEXT,
            status TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?)",
        [
            (1, "2026-05-05T00:00:00+00:00", "x", "rate_limit", "failed"),
            (2, "2026-05-06T00:00:00+00:00", "x", "auth", "failed"),
            (3, "2026-05-20T00:00:00+00:00", "x", "auth", "failed"),
            (4, "2026-05-21T00:00:00+00:00", "x", "auth", "failed"),
        ],
    )

    report = build_publication_attempt_retry_reason_drift_report_from_db(
        conn,
        baseline_days=14,
        current_days=7,
        min_delta=0.4,
        now=NOW,
    )

    assert report["findings"][0]["reason"] == "auth"
    assert report["missing_tables"] == []
    assert report["missing_columns"] == {}


def test_sqlite_loader_prefers_publication_retries_and_reports_schema_gap():
    missing_report = build_publication_attempt_retry_reason_drift_report_from_db(sqlite3.connect(":memory:"))
    assert missing_report["missing_tables"] == ["publication_attempts|publication_retries"]

    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE publication_attempts (attempted_at TEXT, platform TEXT, retry_reason TEXT)")
    conn.execute("CREATE TABLE publication_retries (created_at TEXT, platform TEXT, retry_reason TEXT, status TEXT)")
    conn.execute("INSERT INTO publication_attempts VALUES ('2026-05-20T00:00:00+00:00', 'x', 'attempts_reason')")
    conn.execute("INSERT INTO publication_retries VALUES ('2026-05-20T00:00:00+00:00', 'x', 'retries_reason', 'failed')")

    report = build_publication_attempt_retry_reason_drift_report_from_db(conn, now=NOW)

    assert {row["reason"] for row in report["drift_rows"]} == {"retries_reason"}


def test_formatters_and_cli_support_json_text_db_and_validation(tmp_path, capsys):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE publication_retries (id INTEGER, attempted_at TEXT, platform TEXT, retry_reason TEXT)")
    conn.executemany(
        "INSERT INTO publication_retries VALUES (?, ?, ?, ?)",
        [
            (1, "2026-05-05T00:00:00+00:00", "x", "rate_limit"),
            (2, "2026-05-06T00:00:00+00:00", "x", "auth"),
            (3, "2026-05-20T00:00:00+00:00", "x", "auth"),
            (4, "2026-05-21T00:00:00+00:00", "x", "auth"),
        ],
    )
    conn.commit()
    db_path = tmp_path / "attempts.sqlite"
    out = sqlite3.connect(db_path)
    conn.backup(out)
    out.close()

    assert script.main(["--db", str(db_path), "--format", "json", "--baseline-days", "14", "--current-days", "7", "--min-delta", "0.4"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["findings"][0]["reason"] == "auth"

    report = build_publication_attempt_retry_reason_drift_report([], now=NOW)
    assert json.loads(format_publication_attempt_retry_reason_drift_json(report))["artifact_type"] == "publication_attempt_retry_reason_drift"
    assert "No publication attempt retry reason drift found" in format_publication_attempt_retry_reason_drift_text(report)

    assert script.main(["--db", str(db_path), "--format", "text", "--limit", "1"]) == 0
    assert "Publication Attempt Retry Reason Drift" in capsys.readouterr().out
    assert script.main(["--db", str(db_path), "--limit", "0"]) == 2
