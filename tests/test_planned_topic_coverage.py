"""Tests for planned topic knowledge coverage reporting."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

import pytest

from knowledge.planned_topic_coverage import build_planned_topic_coverage_report

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))
from planned_topic_coverage import format_json_report, main  # noqa: E402


def _add_knowledge(
    db,
    *,
    source_type: str = "curated_article",
    source_id: str,
    author: str,
    content: str,
    insight: str | None = None,
    approved: int = 1,
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            source_id,
            f"https://example.test/{source_id}",
            author,
            content,
            insight,
            approved,
        ),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_report_evaluates_only_planned_topics_by_default(db):
    planned_id = db.insert_planned_topic(
        "testing",
        angle="integration test coverage for API workflows",
    )
    db.insert_planned_topic(
        "testing",
        angle="already generated testing topic",
        status="generated",
    )
    knowledge_id = _add_knowledge(
        db,
        source_id="testing-1",
        author="Ada",
        content="Integration tests catch API workflow regressions before release.",
    )

    report = build_planned_topic_coverage_report(db, min_sources=1)

    assert report.planned_topic_count == 1
    assert [topic.planned_topic_id for topic in report.covered_topics] == [planned_id]
    topic = report.covered_topics[0]
    assert topic.status == "covered"
    assert topic.matched_knowledge_ids == [knowledge_id]
    assert topic.source_authors == ["Ada"]
    assert topic.source_types == ["curated_article"]


def test_report_supports_campaign_filtering(db):
    campaign_id = db.create_campaign(name="Launch", status="active")
    other_campaign_id = db.create_campaign(name="Later", status="planned")
    db.insert_planned_topic("testing", campaign_id=campaign_id)
    db.insert_planned_topic("workflow", campaign_id=other_campaign_id)
    _add_knowledge(
        db,
        source_id="testing-1",
        author="Ada",
        content="Testing fixtures improve integration test coverage.",
    )
    _add_knowledge(
        db,
        source_id="workflow-1",
        author="Lin",
        content="Workflow automation keeps handoffs visible.",
    )

    report = build_planned_topic_coverage_report(
        db,
        campaign_id=campaign_id,
        min_sources=1,
    )

    assert report.campaign_id == campaign_id
    assert report.planned_topic_count == 1
    assert report.covered_topics[0].campaign_id == campaign_id
    with pytest.raises(ValueError, match="Campaign 999 does not exist"):
        build_planned_topic_coverage_report(db, campaign_id=999)


def test_report_groups_covered_weak_and_missing_topics_with_suggestions(db):
    db.insert_planned_topic(
        "testing",
        angle="integration test fixtures for release validation",
    )
    db.insert_planned_topic(
        "workflow",
        angle="review automation handoffs",
    )
    db.insert_planned_topic(
        "performance",
        angle="cache latency profiling",
    )
    _add_knowledge(
        db,
        source_id="testing-1",
        author="Ada",
        content="Integration testing fixtures validate release workflows.",
    )
    _add_knowledge(
        db,
        source_type="own_post",
        source_id="testing-2",
        author="self",
        content="A second test coverage note about pytest fixture isolation.",
    )
    _add_knowledge(
        db,
        source_id="workflow-1",
        author="Lin",
        content="Review workflow automation reduces handoff mistakes.",
    )

    report = build_planned_topic_coverage_report(db, min_sources=2)

    assert [topic.topic for topic in report.covered_topics] == ["testing"]
    assert [topic.topic for topic in report.weakly_covered_topics] == ["workflow"]
    assert [topic.topic for topic in report.missing_topics] == ["performance"]

    weak = report.weakly_covered_topics[0]
    assert weak.status == "weak"
    assert weak.source_count == 1
    assert weak.suggested_search_terms == ["automation", "handoffs", "review", "workflow"]

    missing = report.missing_topics[0]
    assert missing.status == "missing"
    assert missing.matched_knowledge_ids == []
    assert missing.suggested_search_terms == ["cache", "latency", "performance", "profiling"]


def test_json_output_is_deterministic_for_stable_fixtures(db):
    db.insert_planned_topic(
        "ai-agents",
        angle="tool call validation and model handoff failures",
    )
    _add_knowledge(
        db,
        source_type="curated_x",
        source_id="agents-1",
        author="Zed",
        content="Agent tool call validation prevents model handoff failures.",
    )
    _add_knowledge(
        db,
        source_type="curated_article",
        source_id="agents-2",
        author="Ana",
        content="LLM agent workflows need prompt and tool diagnostics.",
    )

    first = format_json_report(build_planned_topic_coverage_report(db, min_sources=1))
    second = format_json_report(build_planned_topic_coverage_report(db, min_sources=1))

    assert first == second
    payload = json.loads(first)
    topic = payload["covered_topics"][0]
    assert topic["matched_knowledge_ids"] == [1, 2]
    assert topic["source_authors"] == ["Ana", "Zed"]
    assert payload["top_matching_sources"][0]["knowledge_id"] == 1


def test_cli_writes_json_output_file(db, tmp_path):
    db.insert_planned_topic("debugging", angle="trace regression diagnostics")
    _add_knowledge(
        db,
        source_id="debug-1",
        author="Ada",
        content="Debug traces make regression diagnosis easier.",
    )
    output_path = tmp_path / "coverage.json"

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("planned_topic_coverage.script_context", fake_script_context):
        main(["--min-sources", "1", "--json", "--output", str(output_path)])

    payload = json.loads(output_path.read_text())
    assert payload["planned_topic_count"] == 1
    assert payload["covered_topics"][0]["topic"] == "debugging"

