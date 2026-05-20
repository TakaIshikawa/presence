"""Tests for content claim-check evidence URL integrity reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.content_claim_check_evidence_url_integrity import (
    build_content_claim_check_evidence_url_integrity_report,
    build_content_claim_check_evidence_url_integrity_report_from_db,
    format_content_claim_check_evidence_url_integrity_json,
    format_content_claim_check_evidence_url_integrity_text,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_claim_check_evidence_url_integrity.py"
spec = importlib.util.spec_from_file_location("content_claim_check_evidence_url_integrity_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def _conn(path: Path | str = ":memory:") -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, status TEXT);
        CREATE TABLE content_claim_checks (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            status TEXT,
            passed INTEGER,
            supported_count INTEGER,
            unsupported_count INTEGER,
            annotation_text TEXT,
            metadata TEXT,
            result TEXT,
            evidence TEXT,
            evidence_url TEXT,
            evidence_urls TEXT
        );
        """
    )
    return conn


def test_builder_flags_missing_duplicate_placeholder_non_http_and_passed_without_evidence():
    rows = [
        {"claim_check_id": 1, "content_id": 10, "status": "passed", "passed": 1, "annotation_text": ""},
        {
            "claim_check_id": 2,
            "content_id": 11,
            "status": "failed",
            "metadata": json.dumps({"evidence": [{"url": "ftp://example.com/source"}, {"url": "https://example.com/source"}]}),
        },
        {
            "claim_check_id": 3,
            "content_id": 12,
            "status": "passed",
            "result": json.dumps({"sources": ["https://real.test/a", "https://real.test/a"]}),
        },
    ]
    report = build_content_claim_check_evidence_url_integrity_report(rows, now=NOW)
    payload = json.loads(format_content_claim_check_evidence_url_integrity_json(report))

    assert payload["artifact_type"] == "content_claim_check_evidence_url_integrity"
    assert payload["totals"]["by_issue_type"] == {
        "duplicate_url": 2,
        "malformed_json": 0,
        "missing_evidence_url": 1,
        "non_http_scheme": 1,
        "passed_without_evidence": 1,
        "placeholder_domain": 2,
    }
    assert payload["status_summaries"][0]["claim_check_count"] >= 1
    assert {"status", "content_id", "issue_type", "count"}.issubset(payload["content_summaries"][0])
    assert "Content Claim Check Evidence URL Integrity" in format_content_claim_check_evidence_url_integrity_text(report)


def test_db_loader_cli_and_schema_gaps(tmp_path, capsys):
    conn = _conn()
    conn.execute("INSERT INTO generated_content VALUES (?, ?)", (1, "ready"))
    conn.execute(
        "INSERT INTO content_claim_checks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (1, 1, "passed", 1, 1, 0, "Evidence https://source.example.org/a", None, None, None, None, None),
    )
    conn.execute(
        "INSERT INTO content_claim_checks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (2, 2, "passed", 1, 1, 0, "", "{bad", None, None, None, None),
    )

    report = build_content_claim_check_evidence_url_integrity_report_from_db(conn)
    assert report["totals"]["finding_count"] == 4
    assert report["findings"][0]["issue_type"] == "missing_evidence_url"

    db_path = tmp_path / "claims.sqlite"
    conn.commit()
    conn.backup(sqlite3.connect(db_path))
    assert script.main(["--db", str(db_path), "--format", "json", "--limit", "5"]) == 0
    assert json.loads(capsys.readouterr().out)["filters"] == {"limit": 5}
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Content Claim Check Evidence URL Integrity" in capsys.readouterr().out

    missing = build_content_claim_check_evidence_url_integrity_report_from_db(sqlite3.connect(":memory:"))
    assert missing["missing_tables"] == ["content_claim_checks"]
    partial = sqlite3.connect(":memory:")
    partial.execute("CREATE TABLE content_claim_checks (status TEXT)")
    gaps = build_content_claim_check_evidence_url_integrity_report_from_db(partial)
    assert gaps["missing_columns"] == {"content_claim_checks": ["content_id"]}


def test_validation_errors():
    with pytest.raises(ValueError, match="limit must be positive"):
        build_content_claim_check_evidence_url_integrity_report([], limit=0)
    assert script.main(["--limit", "0"]) == 2
