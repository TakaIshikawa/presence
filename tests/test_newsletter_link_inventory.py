"""Tests for newsletter outbound link inventory export."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.newsletter_link_inventory import (
    build_newsletter_link_inventory_for_text,
    build_newsletter_link_inventory_report,
    format_newsletter_link_inventory_json,
    format_newsletter_link_inventory_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "export_newsletter_link_inventory.py"
spec = importlib.util.spec_from_file_location("export_newsletter_link_inventory_script", SCRIPT_PATH)
export_newsletter_link_inventory_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(export_newsletter_link_inventory_script)


@contextmanager
def _script_context(conn: sqlite3.Connection):
    yield SimpleNamespace(), SimpleNamespace(conn=conn)


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE newsletter_sends (
            id INTEGER PRIMARY KEY,
            issue_id TEXT,
            subject TEXT,
            status TEXT,
            body TEXT,
            html TEXT,
            metadata TEXT,
            created_at TEXT,
            sent_at TEXT
        );
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            title TEXT,
            content TEXT,
            created_at TEXT,
            curation_quality TEXT
        );
        """
    )
    return conn


def test_extracts_markdown_html_groups_domains_and_flags_repeated_weak_links():
    item = build_newsletter_link_inventory_for_text(
        """
See [the API migration guide](https://Docs.Example.com/migrate) before launch.
This repeats [read more](https://docs.example.com/migrate).
<p>Partner detail: <a href="https://partner.example/case-study">case study</a></p>
Raw follow-up https://blog.example.com/post.
""",
        newsletter_id="issue-1",
        source="newsletter_send",
        subject="API notes",
    )
    payload = item.to_dict()

    assert payload["total_links"] == 4
    assert payload["unique_links"] == 3
    assert payload["unique_domains"] == 3
    assert payload["repeated_url_count"] == 1
    assert payload["weak_anchor_count"] == 2

    by_domain = {domain["domain"]: domain for domain in payload["domains"]}
    docs_link = by_domain["docs.example.com"]["links"][0]
    assert docs_link["count"] == 2
    assert docs_link["repeated"] is True
    assert docs_link["flags"] == ["repeated_url", "weak_anchor_text"]
    assert "the API migration guide" in docs_link["anchors"]
    assert "read more" in docs_link["anchors"]
    assert any("API migration guide" in context for context in docs_link["contexts"])

    partner_link = by_domain["partner.example"]["links"][0]
    assert partner_link["anchors"] == ["case study"]
    assert partner_link["flags"] == []


def test_report_loads_newsletter_send_by_issue_id_and_generated_content_by_id():
    conn = _conn()
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, html, metadata, created_at)
           VALUES (1, 'issue-1', 'Launch notes', 'draft', ?, ?, ?, ?)""",
        (
            "[Launch writeup](https://example.com/launch)",
            '<a href="https://partner.example/case">case study</a>',
            json.dumps({"preview": "Preview links to [here](https://example.com/preview)"}),
            "2026-05-02T10:00:00+00:00",
        ),
    )
    conn.execute(
        """INSERT INTO generated_content
           (id, content_type, title, content, created_at, curation_quality)
           VALUES (20, 'newsletter', 'Generated issue', ?, ?, 'ready')""",
        ("Generated [source](https://other.example/read)", "2026-05-01T10:00:00+00:00"),
    )
    conn.commit()

    report = build_newsletter_link_inventory_report(
        conn,
        newsletter_ids=("issue-1", "20"),
        now=NOW,
    )
    payload = json.loads(format_newsletter_link_inventory_json(report))

    assert payload["artifact_type"] == "newsletter_link_inventory"
    assert payload["totals"]["newsletter_count"] == 2
    assert payload["totals"]["total_links"] == 4
    assert [item["newsletter_id"] for item in payload["newsletters"]] == ["issue-1", "20"]
    send = payload["newsletters"][0]
    assert send["subject"] == "Launch notes"
    assert {domain["domain"] for domain in send["domains"]} == {
        "example.com",
        "partner.example",
    }


def test_recent_count_supports_no_link_newsletters_and_text_formatter():
    conn = _conn()
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, created_at)
           VALUES (1, 'old', 'Old', 'draft', '[Old](https://old.example)', ?)""",
        ("2026-04-01T10:00:00+00:00",),
    )
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, created_at)
           VALUES (2, 'new', 'No links', 'draft', 'No outbound links here.', ?)""",
        ("2026-05-02T10:00:00+00:00",),
    )
    conn.commit()

    report = build_newsletter_link_inventory_report(conn, recent_count=1, now=NOW)
    text = format_newsletter_link_inventory_text(report)

    assert report.totals["newsletter_count"] == 1
    assert report.newsletters[0].newsletter_id == "new"
    assert report.newsletters[0].total_links == 0
    assert "No links found." in text


def test_missing_schema_returns_empty_report_with_diagnostics():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_newsletter_link_inventory_report(conn, now=NOW)

    assert report.totals["newsletter_count"] == 0
    assert report.missing_tables == ("newsletter_sends", "generated_content")
    assert report.newsletters == ()


def test_cli_supports_json_text_db_and_validation(tmp_path, capsys, monkeypatch):
    conn = _conn()
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, created_at)
           VALUES (1, 'issue-1', 'CLI notes', 'draft', '[Post](https://example.com/post)', ?)""",
        ("2026-05-02T10:00:00+00:00",),
    )
    conn.commit()
    monkeypatch.setattr(
        export_newsletter_link_inventory_script,
        "script_context",
        lambda: _script_context(conn),
    )

    assert export_newsletter_link_inventory_script.main(["--newsletter-id", "issue-1"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["newsletters"][0]["newsletter_id"] == "issue-1"
    assert payload["newsletters"][0]["domains"][0]["domain"] == "example.com"

    assert export_newsletter_link_inventory_script.main(["--recent-count", "1", "--format", "text"]) == 0
    assert "Newsletter Link Inventory" in capsys.readouterr().out

    assert (
        export_newsletter_link_inventory_script.main(
            ["--newsletter-id", "issue-1", "--recent-count", "1"]
        )
        == 1
    )
    assert "cannot be combined" in capsys.readouterr().err
