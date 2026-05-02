"""Tests for newsletter image placement reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.newsletter_image_placement import (
    analyze_newsletter_image_placement,
    build_newsletter_image_placement_report,
    build_newsletter_image_placement_report_from_text,
    format_newsletter_image_placement_json,
    format_newsletter_image_placement_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_image_placement.py"
spec = importlib.util.spec_from_file_location("newsletter_image_placement_script", SCRIPT_PATH)
newsletter_image_placement_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_image_placement_script)


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
            updated_at TEXT,
            sent_at TEXT
        );
        """
    )
    return conn


def test_markdown_parsing_reports_sections_images_and_warnings():
    body = """![Hero](https://cdn.example.com/hero.png)

# Intro
Opening note before the image.
![Intro diagram](https://cdn.example.com/intro.png)

## Deep Dive
""" + " ".join(["workflow"] * 85) + """

## Gallery
![A](https://cdn.example.com/a.png)
![B](https://cdn.example.com/b.png)
![C](https://cdn.example.com/c.png)

## CTA
Reply with questions.
![Footer](https://cdn.example.com/footer.png)
"""
    issue = analyze_newsletter_image_placement(body, max_images_per_section=2)

    assert issue.total_images == 6
    assert [section.heading for section in issue.sections] == [
        "Intro",
        "Intro",
        "Deep Dive",
        "Gallery",
        "CTA",
    ]
    assert issue.sections[0].image_count == 1
    assert issue.sections[3].image_count == 3
    assert issue.warning_totals == {
        "clustered_section": 1,
        "image_free_long_section": 1,
        "leading_image": 1,
        "post_cta_image": 1,
    }
    assert "clustered_section:Gallery:3" in issue.warnings
    assert "image_free_long_section:Deep Dive:85" in issue.warnings
    assert issue.images[-1].is_after_cta is True


def test_html_parsing_uses_html_headings_for_section_association():
    issue = analyze_newsletter_image_placement(
        "<h2>Intro</h2><p>Opening note.</p><img src=\"https://cdn.example.com/intro.png\">"
        "<h2>Results</h2><p><img src=\"https://cdn.example.com/chart.png\"></p>",
        max_images_per_section=1,
    )

    assert [(image.image_type, image.section) for image in issue.images] == [
        ("html", "Intro"),
        ("html", "Results"),
    ]
    assert issue.sections[0].image_count == 1
    assert issue.sections[1].image_count == 1
    assert issue.warnings == ()


def test_structured_input_supports_section_dictionaries():
    issue = analyze_newsletter_image_placement(
        {
            "sections": [
                {
                    "title": "Intro",
                    "paragraphs": ["Opening context for the newsletter."],
                    "images": [{"src": "https://cdn.example.com/intro.png"}],
                },
                {
                    "title": "CTA",
                    "body": "Reply with questions.",
                    "images": ["https://cdn.example.com/footer.png"],
                },
            ]
        }
    )

    assert [section.heading for section in issue.sections] == ["Intro", "CTA"]
    assert [section.image_count for section in issue.sections] == [1, 1]
    assert issue.images[0].src == "https://cdn.example.com/intro.png"
    assert issue.warning_totals == {"post_cta_image": 1}


def test_database_report_loads_newsletter_sends_body_when_present():
    conn = _conn()
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, updated_at)
           VALUES (1, 'issue-body', 'Body issue', 'sent', ?, ?)""",
        (
            "## Intro\nOpening.\n\n## Gallery\n![A](https://cdn.example.com/a.png)\n"
            "![B](https://cdn.example.com/b.png)",
            "2026-05-02T10:00:00+00:00",
        ),
    )
    conn.commit()

    report = build_newsletter_image_placement_report(
        conn,
        days=30,
        limit=5,
        max_images_per_section=1,
        now=NOW,
    )
    payload = json.loads(format_newsletter_image_placement_json(report))

    assert payload["artifact_type"] == "newsletter_image_placement"
    assert payload["totals"]["issues_scanned"] == 1
    assert payload["issues"][0]["newsletter_id"] == "issue-body"
    assert payload["issues"][0]["sections"][1]["image_count"] == 2
    assert payload["issues"][0]["warning_totals"] == {"clustered_section": 1}


def test_text_and_json_formatters_include_warning_totals_and_section_counts():
    report = build_newsletter_image_placement_report_from_text(
        "![Hero](https://cdn.example.com/hero.png)\n\n## Intro\nOpening.\n",
        subject="Input issue",
        now=NOW,
    )
    payload = json.loads(format_newsletter_image_placement_json(report))
    text = format_newsletter_image_placement_text(report)

    assert payload["totals"]["warning_totals"] == {"leading_image": 1}
    assert payload["issues"][0]["sections"][0]["image_count"] == 1
    assert "Newsletter Image Placement" in text
    assert "warnings=leading_image:Intro" in text


def test_cli_supports_database_json_and_file_input(tmp_path, monkeypatch, capsys):
    db_conn = _conn()
    db_conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, updated_at)
           VALUES (1, 'issue-cli', 'CLI issue', 'draft', ?, ?)""",
        (
            "## Intro\nOpening.\n\n## Gallery\n![A](https://cdn.example.com/a.png)\n"
            "![B](https://cdn.example.com/b.png)",
            "2026-05-02T10:00:00+00:00",
        ),
    )
    db_conn.commit()
    monkeypatch.setattr(
        newsletter_image_placement_script,
        "script_context",
        lambda: _script_context(db_conn),
    )

    exit_code = newsletter_image_placement_script.main(
        ["--limit", "1", "--max-images-per-section", "1", "--format", "json"]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["issues"][0]["newsletter_id"] == "issue-cli"
    assert payload["issues"][0]["warning_totals"]["clustered_section"] == 1

    input_path = tmp_path / "issue.md"
    input_path.write_text("## CTA\nReply.\n![Footer](https://cdn.example.com/footer.png)\n")
    assert (
        newsletter_image_placement_script.main(
            ["--input", str(input_path), "--subject", "File issue"]
        )
        == 0
    )
    out = capsys.readouterr().out
    assert "File issue" in out
    assert "post_cta_image:CTA" in out
