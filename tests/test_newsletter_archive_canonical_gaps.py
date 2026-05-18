"""Tests for newsletter archive canonical gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from evaluation.newsletter_archive_canonical_gaps import (
    build_newsletter_archive_canonical_gaps_report,
    build_newsletter_archive_canonical_gaps_report_from_db,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_archive_canonical_gaps.py"
spec = importlib.util.spec_from_file_location("newsletter_archive_canonical_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_reports_missing_archive_and_canonical_urls():
    report = build_newsletter_archive_canonical_gaps_report(
        [{"id": "s1", "issue_id": "i1", "subject": "Weekly", "sent_at": "2026-04-01T00:00:00+00:00"}],
        now=NOW,
    )

    gap = report["gaps"][0]
    assert gap["issue_id"] == "i1"
    assert gap["issue_reasons"] == ["missing_archive_url", "missing_canonical_url"]
    assert gap["severity"] == "high"
    assert report["totals"]["reason_counts"]["missing_archive_url"] == 1


def test_detects_duplicate_canonical_and_archive_mismatch():
    report = build_newsletter_archive_canonical_gaps_report(
        [
            {"id": "s1", "issue_id": "i1", "archive_url": "https://news.test/archive/i1", "canonical_url": "https://news.test/canonical/shared"},
            {"id": "s2", "issue_id": "i2", "archive_url": "https://news.test/archive/i2", "canonical_url": "https://news.test/canonical/shared"},
        ],
        now=NOW,
    )

    reasons = {gap["send_id"]: gap["issue_reasons"] for gap in report["gaps"]}
    assert "duplicate_canonical_url" in reasons["s1"]
    assert "canonical_archive_mismatch" in reasons["s1"]
    assert "duplicate_canonical_url" in reasons["s2"]


def test_uses_manifest_rows_and_flags_embedded_archive_mismatch():
    report = build_newsletter_archive_canonical_gaps_report(
        [
            {
                "id": "s1",
                "issue_id": "i1",
                "body": "Read the archive at https://news.test/archive/old",
            }
        ],
        [{"issue_id": "i1", "archive_url": "https://news.test/archive/i1", "canonical_url": "https://news.test/archive/i1"}],
        now=NOW,
    )

    gap = report["gaps"][0]
    assert gap["archive_url"] == "https://news.test/archive/i1"
    assert gap["canonical_url"] == "https://news.test/archive/i1"
    assert gap["embedded_url_count"] == 1
    assert gap["issue_reasons"] == ["embedded_archive_mismatch"]


def test_empty_state_and_invalid_limit():
    report = build_newsletter_archive_canonical_gaps_report(
        [{"id": "s1", "archive_url": "https://news.test/archive/i1", "canonical_url": "https://news.test/archive/i1"}],
        now=NOW,
    )

    assert report["empty_state"]["is_empty"] is True
    assert report["totals"]["gap_count"] == 0
    with pytest.raises(ValueError):
        build_newsletter_archive_canonical_gaps_report([], limit=0)


def test_db_adapter_loads_sends_and_manifest():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE newsletter_sends (id TEXT, issue_id TEXT, subject TEXT, body TEXT, sent_at TEXT)")
    conn.execute("CREATE TABLE newsletter_archive_manifest (issue_id TEXT, archive_url TEXT, canonical_url TEXT)")
    conn.execute("INSERT INTO newsletter_sends VALUES ('s1', 'i1', 'Weekly', 'See https://news.test/archive/i1', '2026-04-01T00:00:00+00:00')")
    conn.execute("INSERT INTO newsletter_archive_manifest VALUES ('i1', 'https://news.test/archive/i1', 'https://news.test/archive/i1')")

    report = build_newsletter_archive_canonical_gaps_report_from_db(conn, now=NOW)

    assert report["totals"]["send_count"] == 1
    assert report["gaps"] == []


def test_cli_supports_json_text_table_and_invalid_limit(monkeypatch, capsys):
    monkeypatch.setattr(script, "script_context", lambda: _script_context(SimpleNamespace()))
    monkeypatch.setattr(
        script,
        "build_newsletter_archive_canonical_gaps_report_from_db",
        lambda _db, **kwargs: build_newsletter_archive_canonical_gaps_report(
            [{"id": "s1"}],
            now=NOW,
            **kwargs,
        ),
    )

    assert script.main(["--limit", "1", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "newsletter_archive_canonical_gaps"
    assert script.main(["--table"]) == 0
    assert "issue_id | send_id" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
