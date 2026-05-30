from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

from evaluation.content_claim_check_reviewer_disagreement import (
    build_content_claim_check_reviewer_disagreement_report,
    build_content_claim_check_reviewer_disagreement_report_from_db,
    format_content_claim_check_reviewer_disagreement_json,
    format_content_claim_check_reviewer_disagreement_text,
)


SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "content_claim_check_reviewer_disagreement.py"
spec = importlib.util.spec_from_file_location("content_claim_check_reviewer_disagreement_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)

NOW = "2026-05-24T00:00:00+00:00"


def test_builder_reports_required_reasons_and_empty_state() -> None:
    report = build_content_claim_check_reviewer_disagreement_report(
        [
            {
                "id": 1,
                "claim_id": 7,
                "reviewer_id": "alice",
                "verdict": "approved",
                "confidence": 0.95,
                "checked_at": "2026-05-20T00:00:00+00:00",
            },
            {
                "id": 2,
                "claim_id": 7,
                "reviewer_id": "bob",
                "verdict": "rejected",
                "confidence": 0.25,
                "checked_at": "2026-05-20T01:00:00+00:00",
            },
        ],
        now=NOW,
    )

    reasons = _reasons(report)
    assert reasons == {"conflicting_verdicts", "mixed_confidence_reviews", "verdict_reversal"}
    assert report["totals"]["by_reason"]["conflicting_verdicts"] == 1
    assert json.loads(format_content_claim_check_reviewer_disagreement_json(report))["artifact_type"] == (
        "content_claim_check_reviewer_disagreement"
    )
    assert "Content Claim Check Reviewer Disagreement" in format_content_claim_check_reviewer_disagreement_text(report)

    empty = build_content_claim_check_reviewer_disagreement_report([], now=NOW)
    assert empty["empty_state"]["is_empty"] is True


def test_builder_honors_window_claim_reviewer_and_limit_filters() -> None:
    rows = [
        {
            "id": 1,
            "claim_id": 1,
            "reviewer_id": "alice",
            "verdict": "approved",
            "checked_at": "2026-05-20T00:00:00+00:00",
        },
        {
            "id": 2,
            "claim_id": 1,
            "reviewer_id": "bob",
            "verdict": "rejected",
            "checked_at": "2026-05-20T01:00:00+00:00",
        },
        {
            "id": 3,
            "claim_id": 2,
            "reviewer_id": "alice",
            "verdict": "approved",
            "checked_at": "2026-04-01T00:00:00+00:00",
        },
        {
            "id": 4,
            "claim_id": 2,
            "reviewer_id": "bob",
            "verdict": "rejected",
            "checked_at": "2026-04-01T01:00:00+00:00",
        },
    ]

    report = build_content_claim_check_reviewer_disagreement_report(
        rows,
        claim_id=1,
        min_reviewers=2,
        window_days=10,
        limit=1,
        now=NOW,
    )

    assert report["filters"]["claim_id"] == 1
    assert report["totals"]["claim_count"] == 1
    assert report["totals"]["finding_count"] == 2
    assert report["totals"]["shown_count"] == 1


def test_from_db_reads_checks_claim_text_and_content_labels() -> None:
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE content_claim_checks (
            id INTEGER PRIMARY KEY,
            content_claim_id INTEGER,
            reviewer TEXT,
            verdict TEXT,
            confidence REAL,
            checked_at TEXT
        );
        CREATE TABLE content_claims (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            claim_text TEXT
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            title TEXT
        );
        """
    )
    conn.execute("INSERT INTO content_claims VALUES (7, 42, 'Revenue grew 40 percent')")
    conn.execute("INSERT INTO generated_content VALUES (42, 'Launch recap')")
    conn.execute(
        "INSERT INTO content_claim_checks VALUES (1, 7, 'alice', 'supported', 0.9, '2026-05-22T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO content_claim_checks VALUES (2, 7, 'bob', 'unsupported', 0.3, '2026-05-22T01:00:00+00:00')"
    )

    report = build_content_claim_check_reviewer_disagreement_report_from_db(conn, now=NOW)
    first_item = report["findings"][0]["items"][0]

    assert _reasons(report) == {"conflicting_verdicts", "mixed_confidence_reviews", "verdict_reversal"}
    assert first_item["claim_text"] == "Revenue grew 40 percent"
    assert first_item["content_id"] == 42
    assert first_item["content_label"] == "Launch recap"


def test_from_db_handles_missing_table_and_count_based_verdicts() -> None:
    missing = build_content_claim_check_reviewer_disagreement_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert missing["missing_tables"] == ["content_claim_checks"]

    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE content_claim_checks (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            reviewer_id TEXT,
            supported_count INTEGER,
            unsupported_count INTEGER,
            created_at TEXT
        );
        """
    )
    conn.execute("INSERT INTO content_claim_checks VALUES (1, 99, 'alice', 2, 0, '2026-05-22T00:00:00+00:00')")
    conn.execute("INSERT INTO content_claim_checks VALUES (2, 99, 'bob', 0, 1, '2026-05-22T01:00:00+00:00')")

    report = build_content_claim_check_reviewer_disagreement_report_from_db(conn, now=NOW)

    assert "conflicting_verdicts" in _reasons(report)
    assert report["missing_columns"] == {}


def test_cli_supports_required_options_and_validation(tmp_path: Path, capsys) -> None:
    db_path = tmp_path / "claims.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE content_claim_checks (
            id INTEGER PRIMARY KEY,
            claim_id INTEGER,
            reviewer_id TEXT,
            verdict TEXT,
            confidence REAL,
            checked_at TEXT
        );
        INSERT INTO content_claim_checks VALUES (1, 5, 'alice', 'approved', 0.8, '2026-05-22T00:00:00+00:00');
        INSERT INTO content_claim_checks VALUES (2, 5, 'bob', 'rejected', 0.2, '2026-05-22T01:00:00+00:00');
        """
    )
    conn.close()

    assert (
        script.main(
            [
                "--db",
                str(db_path),
                "--format",
                "json",
                "--window-days",
                "7",
                "--min-reviewers",
                "2",
                "--claim-id",
                "5",
                "--limit",
                "2",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)
    assert payload["filters"]["claim_id"] == "5"
    assert payload["totals"]["shown_count"] == 2

    assert script.main(["--limit", "0"]) == 2


def _reasons(report: dict) -> set[str]:
    return {group["reason"] for group in report["findings"]}
