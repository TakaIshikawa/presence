from __future__ import annotations

from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_claim_reviewer_latency import (
    build_content_claim_reviewer_latency_report_from_db,
    format_content_claim_reviewer_latency_json,
    format_content_claim_reviewer_latency_text,
)

NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_claim_reviewer_latency.py"
spec = importlib.util.spec_from_file_location("content_claim_reviewer_latency_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""CREATE TABLE content_claim_checks (id INTEGER PRIMARY KEY, status TEXT, created_at TEXT, reviewed_at TEXT, reviewer_id TEXT, reopened_at TEXT)""")
    return conn


def test_latency_findings_and_cli(tmp_path, capsys):
    conn = _conn()
    rows = [
        (1, "pending", NOW - timedelta(hours=100), None, None, None),
        (2, "approved", NOW - timedelta(hours=80), NOW - timedelta(hours=10), "u1", None),
        (3, "rejected", NOW - timedelta(hours=70), NOW - timedelta(hours=5), None, NOW - timedelta(hours=1)),
    ]
    for row in rows:
        conn.execute("INSERT INTO content_claim_checks VALUES (?, ?, ?, ?, ?, ?)", (row[0], row[1], row[2].isoformat(), row[3].isoformat() if row[3] else None, row[4], row[5].isoformat() if row[5] else None))
    report = build_content_claim_reviewer_latency_report_from_db(conn, pending_threshold_hours=72, decision_threshold_hours=24, now=NOW)
    kinds = {f["finding_type"] for f in report["findings"]}
    assert {"stale_pending_review", "slow_review_decision", "slow_approval", "slow_rejection", "missing_reviewer_identity", "reopened_after_slow_review"} <= kinds
    assert json.loads(format_content_claim_reviewer_latency_json(report))["artifact_type"] == "content_claim_reviewer_latency"
    assert "Content Claim Reviewer Latency" in format_content_claim_reviewer_latency_text(report)
    db_path = tmp_path / "claims.sqlite"
    conn.commit()
    dest = sqlite3.connect(db_path)
    conn.backup(dest)
    dest.close()
    assert script.main(["--db", str(db_path), "--format", "json", "--pending-threshold-hours", "72", "--decision-threshold-hours", "24", "--limit", "10"]) == 0
    assert json.loads(capsys.readouterr().out)["summary"]["finding_count"] >= 1


def test_schema_empty_and_validation():
    assert build_content_claim_reviewer_latency_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"]
    empty = build_content_claim_reviewer_latency_report_from_db(_conn(), now=NOW)
    assert empty["empty_state"]["is_empty"]
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
