"""Tests for newsletter broken image reference audits."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.newsletter_image_audit import (
    audit_newsletter_image_text,
    build_newsletter_image_queue_report,
    format_newsletter_image_audit_json,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "audit_newsletter_images.py"
spec = importlib.util.spec_from_file_location("audit_newsletter_images_script", SCRIPT_PATH)
audit_newsletter_images_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(audit_newsletter_images_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def test_audits_markdown_and_html_image_references_with_locations():
    findings = audit_newsletter_image_text(
        "\n".join(
            [
                "Hero ![ok](https://cdn.example.com/hero.png)",
                "Local ![bad](/Users/taka/draft.png)",
                '<img alt="empty" src="">',
                '<img src="ftp://cdn.example.com/file.png">',
                '<img src="images/relative.png">',
                '<img src="//cdn.example.com/no-scheme.png">',
            ]
        ),
        source="draft.md",
    )

    by_issue = {finding.issue: finding for finding in findings}

    assert {finding.issue for finding in findings} == {
        "empty_src",
        "local_filesystem_path",
        "unsupported_scheme",
        "protocol_relative_url",
        "relative_path",
    }
    assert by_issue["local_filesystem_path"].reference_type == "markdown"
    assert by_issue["local_filesystem_path"].offending_reference == "/Users/taka/draft.png"
    assert by_issue["local_filesystem_path"].location == {
        "source": "draft.md",
        "line": 2,
        "column": 14,
    }
    assert by_issue["empty_src"].reference_type == "html"
    assert by_issue["empty_src"].severity == "high"
    assert "absolute HTTPS" in by_issue["relative_path"].suggested_fix


def test_relative_paths_can_be_explicitly_allowed():
    findings = audit_newsletter_image_text(
        "![asset](images/hero.png)\n<img src=\"/tmp/local.png\">",
        allow_relative=True,
    )

    assert len(findings) == 1
    assert findings[0].issue == "local_filesystem_path"


def test_queue_report_audits_recent_queued_newsletter_texts_and_metadata():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subject TEXT,
            status TEXT,
            body TEXT,
            metadata TEXT,
            updated_at TEXT
        )"""
    )
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, metadata, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            1,
            "issue-1",
            "Weekly",
            "queued",
            "![hero](relative/hero.png)",
            json.dumps({"html": '<img src="file:///tmp/hero.png">'}),
            "2026-05-01T00:00:00+00:00",
        ),
    )
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (2, "issue-2", "Sent", "sent", '<img src="">', "2026-05-01T00:00:00+00:00"),
    )
    conn.commit()

    report = build_newsletter_image_queue_report(conn, days=7, now=NOW)
    payload = json.loads(format_newsletter_image_audit_json(report))

    assert payload["totals"]["record_count"] == 1
    assert payload["totals"]["finding_count"] == 2
    assert payload["records"] == [
        {
            "newsletter_send_id": 1,
            "issue_id": "issue-1",
            "subject": "Weekly",
            "status": "queued",
        }
    ]
    assert {finding["issue"] for finding in payload["findings"]} == {
        "local_filesystem_path",
        "relative_path",
    }
    assert all("newsletter_sends:1:" in finding["location"]["source"] for finding in payload["findings"])


def test_missing_newsletter_sends_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_image_queue_report(conn, now=NOW)

    assert report.totals["record_count"] == 0
    assert report.findings == ()
    assert report.missing_tables == ("newsletter_sends",)


def test_cli_audits_draft_file_and_fails_only_when_requested(tmp_path, capsys):
    draft = tmp_path / "newsletter.md"
    draft.write_text("![hero](relative/hero.png)")

    result = audit_newsletter_images_script.main(["--draft-file", str(draft)])
    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["totals"]["finding_count"] == 1

    result = audit_newsletter_images_script.main(
        ["--draft-file", str(draft), "--fail-on-findings"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert result == 2
    assert payload["findings"][0]["issue"] == "relative_path"


def test_cli_audits_queued_records_as_json(db, monkeypatch, capsys):
    fake_report = build_newsletter_image_queue_report(
        _queue_db(),
        days=30,
        now=NOW,
    )
    monkeypatch.setattr(
        audit_newsletter_images_script,
        "script_context",
        lambda: _script_context(db),
    )
    with patch.object(
        audit_newsletter_images_script,
        "build_newsletter_image_queue_report",
        return_value=fake_report,
    ) as mock_build:
        result = audit_newsletter_images_script.main(["--days", "14", "--limit", "0"])

    payload = json.loads(capsys.readouterr().out)

    assert result == 0
    assert payload["artifact_type"] == "newsletter_image_audit"
    mock_build.assert_called_once_with(
        db,
        days=14,
        status=("draft", "pending", "queued", "scheduled"),
        limit=None,
        allow_relative=False,
    )


def _queue_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            status TEXT,
            body TEXT,
            updated_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO newsletter_sends VALUES (1, 'queued', '<img src=\"\">', ?)",
        ("2026-05-01T00:00:00+00:00",),
    )
    conn.commit()
    return conn
