"""Tests for blog draft CTA alignment reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from output.blog_cta_alignment import (
    BlogDraftCtaRecord,
    build_blog_cta_alignment_report,
    detect_cta_intent,
    detect_draft_theme,
    format_blog_cta_alignment_json,
)


NOW = datetime(2026, 5, 3, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "blog_cta_alignment.py"
spec = importlib.util.spec_from_file_location("blog_cta_alignment_script", SCRIPT_PATH)
blog_cta_alignment_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(blog_cta_alignment_script)


def test_aligned_project_draft_uses_try_project_cta_and_can_be_included():
    draft = BlogDraftCtaRecord(
        draft_id=1,
        title="A tiny API retry project",
        theme="project",
        content=(
            "# A tiny API retry project\n\n"
            "The implementation keeps queue recovery visible and easy to test.\n\n"
            "Try the project by cloning the repo and running the demo."
        ),
    )

    report = build_blog_cta_alignment_report(drafts=[draft], include_aligned=True, now=NOW)
    payload = json.loads(format_blog_cta_alignment_json(report))

    assert detect_draft_theme(draft) == "project"
    assert detect_cta_intent(draft.content) == "try_project"
    assert payload["artifact_type"] == "blog_cta_alignment"
    assert list(payload) == sorted(payload)
    assert payload["counts"]["aligned"] == 1
    assert payload["rows"][0]["alignment_status"] == "aligned"
    assert payload["rows"][0]["detected_theme"] == "project"
    assert payload["rows"][0]["cta_intent"] == "try_project"


def test_missing_cta_is_reported_with_draft_identity_and_reason():
    report = build_blog_cta_alignment_report(
        drafts=[
            {
                "draft_id": 2,
                "title": "Guide to source freshness",
                "category": "guide",
                "content": "# Guide to source freshness\n\nUse checklists to keep evidence current.",
            }
        ],
        now=NOW,
    )
    row = report.rows[0]

    assert report.counts["missing_cta"] == 1
    assert row.draft_id == 2
    assert row.title == "Guide to source freshness"
    assert row.detected_theme == "guide"
    assert row.cta_intent is None
    assert row.alignment_status == "missing_cta"
    assert "recognized CTA" in row.reason


def test_mismatched_cta_is_reported_when_theme_expects_a_different_action():
    report = build_blog_cta_alignment_report(
        drafts=[
            {
                "id": 3,
                "title": "Launch checklist guide",
                "theme": "guide",
                "content": (
                    "# Launch checklist guide\n\n"
                    "The checklist helps teams keep release notes grounded.\n\n"
                    "Reply with your favorite launch ritual."
                ),
            }
        ],
        now=NOW,
    )
    row = report.rows[0]

    assert row.draft_id == 3
    assert row.detected_theme == "guide"
    assert row.cta_intent == "reply"
    assert row.alignment_status == "mismatched_cta"
    assert "read_related_post" in row.reason
    assert report.blocking_issue_count == 1


def test_default_report_emits_only_missing_and_mismatched_rows():
    report = build_blog_cta_alignment_report(
        drafts=[
            {
                "draft_id": 1,
                "theme": "project",
                "content": "# Project\n\nTry it in the demo.",
            },
            {
                "draft_id": 2,
                "theme": "discussion",
                "content": "# Discussion\n\nThis changes the tradeoff.",
            },
            {
                "draft_id": 3,
                "theme": "newsletter",
                "content": "# Digest\n\nFollow me on Bluesky for more.",
            },
        ],
        now=NOW,
    )

    assert [row.draft_id for row in report.rows] == [2, 3]
    assert report.counts == {
        "drafts": 3,
        "aligned": 1,
        "missing_cta": 1,
        "mismatched_cta": 1,
        "unknown_theme": 0,
        "reported": 2,
    }


def test_sqlite_loader_uses_blog_variants_and_planned_topic_theme(tmp_path, capsys):
    db_path = tmp_path / "drafts.db"
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            content TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE content_variants (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            platform TEXT,
            variant_type TEXT,
            content TEXT,
            metadata TEXT
        )"""
    )
    conn.execute(
        """CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            content_id INTEGER,
            topic TEXT,
            angle TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO generated_content VALUES (?, ?, ?)",
        (10, "x_post", "Short social source."),
    )
    conn.execute(
        "INSERT INTO content_variants VALUES (?, ?, ?, ?, ?, ?)",
        (
            1,
            10,
            "blog",
            "post",
            "# Retry tool\n\nA longer project writeup.\n\nSubscribe for future posts.",
            "{}",
        ),
    )
    conn.execute(
        "INSERT INTO planned_topics VALUES (?, ?, ?, ?)",
        (1, 10, "project", "retry workflow"),
    )
    conn.commit()
    conn.close()

    exit_code = blog_cta_alignment_script.main(["--db", str(db_path)])
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 1
    assert payload["rows"] == [
        {
            "alignment_status": "mismatched_cta",
            "cta_intent": "subscribe",
            "detected_theme": "project",
            "draft_id": 10,
            "reason": "project drafts expect one of: try_project, follow",
            "title": "Retry tool",
        }
    ]


def test_missing_generated_content_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")

    report = build_blog_cta_alignment_report(conn, now=NOW)

    assert report.rows == ()
    assert report.missing_tables == ("generated_content",)
    assert report.counts["drafts"] == 0
