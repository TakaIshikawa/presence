"""Tests for generated content topic coverage reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

from evaluation.generated_content_topic_coverage import (
    build_generated_content_topic_coverage_report,
    format_generated_content_topic_coverage_json,
    format_generated_content_topic_coverage_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent
    / "scripts"
    / "generated_content_topic_coverage.py"
)
spec = importlib.util.spec_from_file_location("generated_content_topic_coverage_script", SCRIPT_PATH)
generated_content_topic_coverage_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(generated_content_topic_coverage_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(
    db,
    *,
    content_type: str = "x_post",
    created_days_ago: int = 1,
    published: bool = False,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=f"Generated {content_type} content",
        eval_score=8.0,
        eval_feedback="usable",
    )
    created_at = (NOW - timedelta(days=created_days_ago)).isoformat()
    published_at = (NOW - timedelta(hours=6)).isoformat() if published else None
    db.conn.execute(
        """UPDATE generated_content
           SET created_at = ?, published = ?, published_at = ?
           WHERE id = ?""",
        (created_at, int(published), published_at, content_id),
    )
    db.conn.commit()
    return content_id


def test_reports_missing_blank_and_low_confidence_topic_issues(db):
    missing_id = _content(db, content_type="x_post")
    blank_id = _content(db, content_type="blog_post", published=True)
    low_id = _content(db, content_type="x_thread")
    ok_id = _content(db, content_type="newsletter")
    db.insert_content_topics(blank_id, [("   ", "", 0.9)])
    db.insert_content_topics(low_id, [("testing", "", 0.49)])
    db.insert_content_topics(ok_id, [("architecture", "", 0.9)])

    report = build_generated_content_topic_coverage_report(
        db,
        days=7,
        min_confidence=0.5,
        now=NOW,
    )
    payload = json.loads(format_generated_content_topic_coverage_json(report))
    text = format_generated_content_topic_coverage_text(report)

    assert list(payload) == sorted(payload)
    assert payload["artifact_type"] == "generated_content_topic_coverage"
    assert payload["has_issues"] is True
    assert payload["totals"]["content_scanned"] == 4
    assert payload["totals"]["by_content_type"] == {
        "blog_post": 1,
        "newsletter": 1,
        "x_post": 1,
        "x_thread": 1,
    }
    assert payload["totals"]["by_published_state"] == {"published": 1, "unpublished": 3}
    assert payload["totals"]["by_issue_type"] == {
        "blank_topic": 1,
        "low_confidence": 1,
        "missing_topic": 1,
    }

    missing = _finding(payload, "missing_topic")
    assert missing["content_id"] == missing_id
    assert missing["topic_id"] is None
    assert missing["recommended_action"] == "insert_content_topic_assignment"

    blank = _finding(payload, "blank_topic")
    assert blank["content_id"] == blank_id
    assert blank["topic"] is None
    assert blank["published_state"] == "published"

    low = _finding(payload, "low_confidence")
    assert low["content_id"] == low_id
    assert low["topic"] == "testing"
    assert low["confidence"] == 0.49
    assert "Generated Content Topic Coverage" in text
    assert "missing_topic=1 blank_topic=1 low_confidence=1" in text


def test_content_type_and_published_filters_affect_totals_and_findings(db):
    published_missing = _content(db, content_type="x_post", published=True)
    unpublished_missing = _content(db, content_type="x_post", published=False)
    other_type = _content(db, content_type="blog_post", published=True)
    old_content = _content(db, content_type="x_post", created_days_ago=40, published=True)
    db.insert_content_topics(other_type, [("launch", "", 0.4)])
    db.insert_content_topics(old_content, [("archive", "", 0.1)])

    report = build_generated_content_topic_coverage_report(
        db,
        days=7,
        content_type="x_post",
        published_only=True,
        now=NOW,
    )
    payload = report.to_dict()

    assert payload["filters"]["content_type"] == "x_post"
    assert payload["filters"]["published_only"] is True
    assert payload["totals"]["content_scanned"] == 1
    assert payload["totals"]["by_content_type"] == {"x_post": 1}
    assert payload["totals"]["by_published_state"] == {"published": 1}
    assert [finding["content_id"] for finding in payload["findings"]] == [published_missing]
    assert unpublished_missing > 0


def test_missing_tables_return_deterministic_empty_or_missing_topic_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    report = build_generated_content_topic_coverage_report(conn, now=NOW)

    assert report.missing_tables == ("generated_content", "content_topics")
    assert report.totals["content_scanned"] == 0
    assert report.findings == ()

    conn.execute(
        """CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            created_at TEXT
        )"""
    )
    conn.execute(
        "INSERT INTO generated_content (id, content_type, created_at) VALUES (1, 'x_post', ?)",
        ((NOW - timedelta(days=1)).isoformat(),),
    )

    report = build_generated_content_topic_coverage_report(conn, days=7, now=NOW)
    text = format_generated_content_topic_coverage_text(report)

    assert report.missing_tables == ("content_topics",)
    assert report.totals["content_scanned"] == 1
    assert report.totals["by_issue_type"]["missing_topic"] == 1
    assert report.findings[0].content_id == 1
    assert "Missing tables: content_topics" in text


def test_cli_json_validation_filters_and_fail_on_issues(db, monkeypatch, capsys):
    content_id = _content(db, content_type="x_post", published=True)
    _content(db, content_type="blog_post", published=True)
    monkeypatch.setattr(
        generated_content_topic_coverage_script,
        "script_context",
        lambda: _script_context(db),
    )
    monkeypatch.setattr(
        generated_content_topic_coverage_script,
        "build_generated_content_topic_coverage_report",
        lambda db, **kwargs: build_generated_content_topic_coverage_report(
            db,
            now=NOW,
            **kwargs,
        ),
    )

    assert generated_content_topic_coverage_script.main(["--days", "0"]) == 2
    assert "value must be positive" in capsys.readouterr().err
    assert generated_content_topic_coverage_script.main(["--min-confidence", "1.2"]) == 2
    assert "confidence must be between 0 and 1" in capsys.readouterr().err

    exit_code = generated_content_topic_coverage_script.main(
        [
            "--days",
            "7",
            "--content-type",
            "x_post",
            "--published-only",
            "--format",
            "json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert payload["filters"]["content_type"] == "x_post"
    assert payload["filters"]["published_only"] is True
    assert payload["findings"][0]["content_id"] == content_id

    assert generated_content_topic_coverage_script.main(["--fail-on-issues"]) == 1
    assert "type=missing_topic" in capsys.readouterr().out


def _finding(payload: dict, finding_type: str) -> dict:
    matches = [
        finding
        for finding in payload["findings"]
        if finding["finding_type"] == finding_type
    ]
    assert len(matches) == 1
    return matches[0]
