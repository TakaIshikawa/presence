from __future__ import annotations

import sqlite3

from evaluation.blog_draft_code_block_language_gaps import (
    build_blog_draft_code_block_language_gaps_report,
    build_blog_draft_code_block_language_gaps_report_from_db,
    format_blog_draft_code_block_language_gaps_json,
    format_blog_draft_code_block_language_gaps_text,
)


def test_valid_blocks_have_no_findings():
    body = "```python\nprint('ok')\n```\n```json\n{\"ok\": true}\n```"
    report = build_blog_draft_code_block_language_gaps_report([{"draft_id": "d1", "body": body}])
    assert report["issues"] == []
    assert "blog_draft_code_block_language_gaps" in format_blog_draft_code_block_language_gaps_json(report)
    assert "No blog draft" in format_blog_draft_code_block_language_gaps_text(report)


def test_missing_language_includes_evidence_severity_and_recommendation():
    report = build_blog_draft_code_block_language_gaps_report([{"draft_id": "d1", "body": "```\nprint('x')\n```"}])
    issue = report["issues"][0]
    assert issue["issue_type"] == "missing_language"
    assert issue["severity"] == "medium"
    assert "print" in issue["evidence"]
    assert issue["recommendation"]


def test_unsupported_language_tag():
    report = build_blog_draft_code_block_language_gaps_report([{"draft_id": "d1", "body": "```wat\nx\n```"}], allowed_languages="python")
    issue = report["issues"][0]
    assert issue["issue_type"] == "unsupported_language"
    assert issue["language"] == "wat"


def test_simple_mismatch_heuristics():
    report = build_blog_draft_code_block_language_gaps_report([{"draft_id": "d1", "body": "```python\nconst x = 1;\nconsole.log(x)\n```"}])
    issue = report["issues"][0]
    assert issue["issue_type"] == "likely_language_mismatch"
    assert "javascript" in issue["evidence"]
    assert issue["severity"] == "low"


def test_db_reads_blog_drafts():
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE blog_drafts (id INTEGER, markdown TEXT)")
    conn.execute("INSERT INTO blog_drafts VALUES (1, ?)", ("```\nx\n```",))
    assert build_blog_draft_code_block_language_gaps_report_from_db(conn)["issues"][0]["block_index"] == 1
