"""Tests for newsletter image alt text coverage reports."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import csv
import importlib.util
import io
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from output.newsletter_image_alt_text_report import (
    build_newsletter_image_alt_text_report,
    build_newsletter_image_alt_text_report_for_text,
    classify_image_alt_text,
    extract_newsletter_image_alt_text_occurrences,
    format_newsletter_image_alt_text_csv,
    format_newsletter_image_alt_text_json,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "newsletter_image_alt_text_report.py"
spec = importlib.util.spec_from_file_location("newsletter_image_alt_text_report_script", SCRIPT_PATH)
newsletter_image_alt_text_report_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(newsletter_image_alt_text_report_script)


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


def test_extracts_markdown_and_html_images_with_section_context():
    images = extract_newsletter_image_alt_text_occurrences(
        """
# Launch
![Architecture diagram](https://cdn.example.com/diagram.png)

## Demo
![image](https://cdn.example.com/generic.png)
<h2>Results</h2><p><img src="https://cdn.example.com/chart.png" alt="Revenue chart after launch"></p>
<p><img src="https://cdn.example.com/missing.png"></p>
""",
        source="issue.md",
        title="Weekly launch notes",
    )

    assert [image.classification for image in images] == [
        "descriptive_alt",
        "generic_alt",
        "descriptive_alt",
        "missing_alt",
    ]
    assert images[0].image_type == "markdown"
    assert images[0].section == "Launch"
    assert images[1].section == "Demo"
    assert images[2].image_type == "html"
    assert images[2].section == "Results"
    assert images[3].alt_text is None
    assert images[3].title == "Weekly launch notes"


def test_classification_is_deterministic_for_missing_empty_generic_and_descriptive():
    assert classify_image_alt_text(None) == "missing_alt"
    assert classify_image_alt_text("   ") == "empty_alt"
    assert classify_image_alt_text("image") == "generic_alt"
    assert classify_image_alt_text("Screenshot") == "generic_alt"
    assert classify_image_alt_text("photo") == "generic_alt"
    assert classify_image_alt_text("Image #2") == "generic_alt"
    assert classify_image_alt_text("Pipeline error trend chart") == "descriptive_alt"


def test_text_report_defaults_to_actionable_rows_and_can_include_descriptive():
    report = build_newsletter_image_alt_text_report_for_text(
        "![Architecture diagram](https://cdn.example.com/a.png)\n![photo](https://cdn.example.com/b.png)",
        include_descriptive=False,
        now=NOW,
    )
    payload = json.loads(format_newsletter_image_alt_text_json(report))

    assert payload["totals"]["total_image_count"] == 2
    assert payload["totals"]["actionable_image_count"] == 1
    assert [image["classification"] for image in payload["images"]] == ["generic_alt"]

    full_report = build_newsletter_image_alt_text_report_for_text(
        "![Architecture diagram](https://cdn.example.com/a.png)\n![photo](https://cdn.example.com/b.png)",
        include_descriptive=True,
        now=NOW,
    )
    assert [image.classification for image in full_report.images] == [
        "descriptive_alt",
        "generic_alt",
    ]


def test_database_report_filters_by_date_and_reads_metadata_texts():
    conn = _conn()
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, metadata, updated_at)
           VALUES (1, 'issue-new', 'New issue', 'draft', ?, ?, ?)""",
        (
            "![image](https://cdn.example.com/generic.png)",
            json.dumps({"html": '<h2>Hero</h2><img src="https://cdn.example.com/hero.png" alt="">'}),
            "2026-05-02T10:00:00+00:00",
        ),
    )
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, updated_at)
           VALUES (2, 'issue-old', 'Old issue', 'draft', ?, ?)""",
        (
            '<img src="https://cdn.example.com/old.png">',
            "2026-04-01T10:00:00+00:00",
        ),
    )
    conn.execute(
        """INSERT INTO generated_content
           (id, content_type, title, content, created_at, curation_quality)
           VALUES (20, 'newsletter', 'Generated issue', ?, ?, 'ready')""",
        (
            '<h2>Preview</h2><img src="https://cdn.example.com/preview.png" alt="screenshot">',
            "2026-05-01T10:00:00+00:00",
        ),
    )
    conn.commit()

    report = build_newsletter_image_alt_text_report(conn, days=7, now=NOW)
    payload = json.loads(format_newsletter_image_alt_text_json(report))

    assert payload["totals"]["record_count"] == 2
    assert payload["totals"]["classification_totals"] == {
        "descriptive_alt": 0,
        "empty_alt": 1,
        "generic_alt": 2,
        "missing_alt": 0,
    }
    assert {image["classification"] for image in payload["images"]} == {
        "empty_alt",
        "generic_alt",
    }
    assert all("old.png" not in image["src"] for image in payload["images"])
    assert any(image["section"] == "Hero" for image in payload["images"])


def test_csv_formatter_outputs_flat_rows_with_header():
    report = build_newsletter_image_alt_text_report_for_text(
        '<h2>Intro</h2><img src="https://cdn.example.com/hero.png" alt="">',
        include_descriptive=False,
        now=NOW,
    )
    rows = list(csv.DictReader(io.StringIO(format_newsletter_image_alt_text_csv(report))))

    assert rows == [
        {
            "source": "text:text",
            "image_type": "html",
            "classification": "empty_alt",
            "src": "https://cdn.example.com/hero.png",
            "alt_text": "",
            "section": "Intro",
            "title": "",
            "line": "1",
            "column": "15",
        }
    ]


def test_cli_emits_json_csv_and_writes_output(tmp_path, capsys, monkeypatch):
    conn = _conn()
    conn.execute(
        """INSERT INTO newsletter_sends
           (id, issue_id, subject, status, body, updated_at)
           VALUES (1, 'issue-cli', 'CLI issue', 'draft', ?, ?)""",
        (
            "![image](https://cdn.example.com/generic.png)\n"
            "![Specific launch diagram](https://cdn.example.com/diagram.png)",
            "2026-05-02T10:00:00+00:00",
        ),
    )
    conn.commit()
    monkeypatch.setattr(
        newsletter_image_alt_text_report_script,
        "script_context",
        lambda: _script_context(conn),
    )

    assert newsletter_image_alt_text_report_script.main(["--days", "7"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["artifact_type"] == "newsletter_image_alt_text_report"
    assert [image["classification"] for image in payload["images"]] == ["generic_alt"]

    output = tmp_path / "alt-report.csv"
    assert (
        newsletter_image_alt_text_report_script.main(
            ["--days", "7", "--format", "csv", "--include-descriptive", "--output", str(output)]
        )
        == 0
    )
    assert capsys.readouterr().out == ""
    rows = list(csv.DictReader(io.StringIO(output.read_text(encoding="utf-8"))))
    assert [row["classification"] for row in rows] == ["generic_alt", "descriptive_alt"]
