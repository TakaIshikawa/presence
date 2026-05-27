from __future__ import annotations

import importlib.util
import json
import sqlite3
from pathlib import Path

from evaluation.blog_draft_table_accessibility_gaps import (
    build_blog_draft_table_accessibility_gaps_report,
    build_blog_draft_table_accessibility_gaps_report_from_db,
    format_blog_draft_table_accessibility_gaps_json,
    format_blog_draft_table_accessibility_gaps_text,
)

SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "blog_draft_table_accessibility_gaps.py"
spec = importlib.util.spec_from_file_location("blog_draft_table_accessibility_gaps_script", SCRIPT)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


def test_flags_markdown_and_html_table_gaps_sorted():
    rows = [
        {
            "draft_id": 2,
            "body": "Caption: ok\n| Name | Age |\n| --- | --- |\n| A | 1 | 2 |\n",
        },
        {
            "draft_id": 1,
            "body": "<table><tr><td>A</td><td>B</td></tr><tr><td>1</td></tr></table>",
        },
    ]
    report = build_blog_draft_table_accessibility_gaps_report(rows)
    assert [(f["draft_id"], f["table_index"]) for f in report["findings"]] == [(1, 1), (2, 1)]
    assert report["findings"][0]["issue_codes"] == [
        "missing_header_row",
        "missing_caption_or_summary",
        "inconsistent_column_counts",
    ]
    assert "inconsistent_column_counts" in report["findings"][1]["issue_codes"]
    assert "missing_caption_or_summary" not in report["findings"][1]["issue_codes"]


def test_flags_empty_headers_and_formats():
    body = "| Name |  |\n| --- | --- |\n| A | B |\n"
    report = build_blog_draft_table_accessibility_gaps_report([{"draft_id": "a", "body": body}])
    assert report["findings"][0]["issue_codes"] == ["empty_header_cells", "missing_caption_or_summary"]
    payload = json.loads(format_blog_draft_table_accessibility_gaps_json(report))
    text = format_blog_draft_table_accessibility_gaps_text(report)
    assert payload["artifact_type"] == "blog_draft_table_accessibility_gaps"
    assert "draft_id | table | severity" in text


def test_db_and_cli(tmp_path):
    db = tmp_path / "drafts.sqlite"
    conn = sqlite3.connect(db)
    conn.execute("CREATE TABLE blog_drafts (id INTEGER, markdown TEXT)")
    conn.execute("INSERT INTO blog_drafts VALUES (3, '<table summary=\"stats\"><tr><th></th></tr><tr><td>x</td></tr></table>')")
    conn.commit()
    report = build_blog_draft_table_accessibility_gaps_report_from_db(conn)
    assert report["findings"][0]["issue_codes"] == ["empty_header_cells"]
    assert script.main(["--db", str(db), "--format", "json"]) == 0
