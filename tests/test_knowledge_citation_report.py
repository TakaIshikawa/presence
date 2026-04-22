"""Tests for knowledge_citation_report.py."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge_citation_report import (
    build_report_payload,
    format_text_report,
    main,
)


def seed_citation_report(db) -> tuple[int, int, int]:
    content_id = db.insert_generated_content(
        content_type="x_post",
        source_commits=["sha-cite"],
        source_messages=["msg-cite"],
        content="Generated post using external knowledge.",
        eval_score=8.4,
        eval_feedback="clear",
    )
    missing_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, attribution_required, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
        (
            "curated_article",
            "article-missing",
            None,
            "Grace",
            "Source material without URL",
            "attribution_required",
            1,
        ),
    ).lastrowid
    traced_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, attribution_required, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
        (
            "curated_x",
            "tweet-traced",
            "https://example.test/tweet",
            "Lin",
            "Source material with URL",
            "open",
            0,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(missing_id, 0.91), (traced_id, 0.72)])
    return content_id, missing_id, traced_id


def test_build_report_payload_includes_coverage_and_rows(db):
    content_id, missing_id, traced_id = seed_citation_report(db)

    payload = build_report_payload(db, days=30, only_missing=False)

    assert payload["coverage"]["content_count"] == 1
    assert payload["coverage"]["knowledge_link_count"] == 2
    assert {row["knowledge_id"] for row in payload["rows"]} == {missing_id, traced_id}
    assert payload["rows"][0]["content_id"] == content_id


def test_format_text_report_marks_missing_link(db):
    seed_citation_report(db)
    payload = build_report_payload(db, days=30, only_missing=True)

    output = format_text_report(payload)

    assert "Knowledge Citation Report" in output
    assert "Filter: only missing traceable links" in output
    assert "[MISSING]" in output
    assert "source=curated_article" in output
    assert "license=attribution_required" in output
    assert "https://example.test/tweet" not in output


def test_main_json_output(db, capsys):
    _content_id, missing_id, _traced_id = seed_citation_report(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("knowledge_citation_report.script_context", fake_script_context):
        main(["--format", "json", "--only-missing"])

    payload = json.loads(capsys.readouterr().out)
    assert payload["only_missing"] is True
    assert [row["knowledge_id"] for row in payload["rows"]] == [missing_id]
    assert payload["coverage"]["missing_traceable_link_count"] == 1


def test_main_rejects_non_positive_days():
    with pytest.raises(SystemExit, match="--days must be at least 1"):
        main(["--days", "0"])
