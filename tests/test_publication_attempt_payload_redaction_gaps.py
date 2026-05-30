from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

import pytest

from evaluation.publication_attempt_payload_redaction_gaps import (
    build_publication_attempt_payload_redaction_gaps_report,
    build_publication_attempt_payload_redaction_gaps_report_from_db,
)


NOW = datetime(2026, 5, 20, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publication_attempt_payload_redaction_gaps.py"
spec = importlib.util.spec_from_file_location("publication_attempt_payload_redaction_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_flags_json_headers_urls_and_diagnostics_but_not_redacted_values():
    report = build_publication_attempt_payload_redaction_gaps_report(
        [
            {
                "attempt_id": "a1",
                "platform": "x",
                "error": "failed for owner@example.com with Bearer abcdefghijklmnopqrst",
                "request_headers": "Authorization: Bearer [REDACTED]",
                "request_payload": json.dumps({"client_secret": "supersecretvalue", "note": "ok"}),
                "url": "https://api.example.test/post?access_token=tok_abcdefghijklmnopqrstuvwxyz",
            },
            {
                "attempt_id": "a2",
                "platform": "mastodon",
                "response_payload": {"email": "***", "token": "<redacted>"},
            },
        ],
        now=NOW,
    )
    rows = report["rows"]
    assert {key for row in rows for key in row} >= {"attempt_id", "platform", "field_path", "redaction_issue", "matched_kind", "severity"}
    assert {row["matched_kind"] for row in rows} == {"bearer_token", "email_address", "provider_token", "secret_field", "url_query_secret"}
    assert all("request_headers" not in row["field_path"] for row in rows)


def test_db_loader_and_cli(tmp_path, capsys):
    db_path = tmp_path / "attempts.sqlite"
    conn = sqlite3.connect(db_path)
    conn.execute("CREATE TABLE publication_attempts (id INTEGER, platform TEXT, error TEXT, response_metadata TEXT, request_url TEXT)")
    conn.execute(
        "INSERT INTO publication_attempts VALUES (?, ?, ?, ?, ?)",
        (1, "bluesky", "", '{"headers":{"authorization":"Bearer abcdefghijklmnop"}}', "https://x.test/?api_key=plainsecretvalue"),
    )
    conn.commit()

    report = build_publication_attempt_payload_redaction_gaps_report_from_db(conn, now=NOW)
    assert report["totals"]["finding_count"] == 3
    assert script.main(["--db", str(db_path), "--format", "json", "--now", NOW.isoformat()]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "publication_attempt_payload_redaction_gaps"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "Publication Attempt Payload Redaction Gaps" in capsys.readouterr().out


def test_validation_errors():
    with pytest.raises(ValueError, match="limit must be positive"):
        build_publication_attempt_payload_redaction_gaps_report([], limit=0)
    assert script.main(["--limit", "0"]) == 2
