"""Tests for planned topic fulfillment link gap reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3
from types import SimpleNamespace

import pytest

from evaluation.planned_topic_fulfillment_link_gaps import (
    build_planned_topic_fulfillment_link_gaps_report,
    build_planned_topic_fulfillment_link_gaps_report_from_db,
    format_planned_topic_fulfillment_link_gaps_json,
    format_planned_topic_fulfillment_link_gaps_text,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "planned_topic_fulfillment_link_gaps.py"
spec = importlib.util.spec_from_file_location("planned_topic_fulfillment_link_gaps_script", SCRIPT_PATH)
script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        CREATE TABLE content_campaigns (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE generated_content (
            id INTEGER PRIMARY KEY,
            content_type TEXT,
            published INTEGER,
            status TEXT
        );
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            status TEXT,
            content_id INTEGER,
            source_material TEXT
        );
        """
    )
    return conn


def test_builder_flags_missing_content_id_missing_content_abandoned_and_type_mismatch():
    report = build_planned_topic_fulfillment_link_gaps_report(
        [
            {"planned_topic_id": 1, "campaign_id": 10, "topic_status": "generated", "content_id": None},
            {"planned_topic_id": 2, "campaign_id": 10, "topic_status": "generated", "content_id": 99, "resolved_content_id": None},
            {
                "planned_topic_id": 3,
                "campaign_id": 11,
                "topic_status": "generated",
                "content_id": 3,
                "resolved_content_id": 3,
                "content_status": "abandoned",
                "content_published": -1,
            },
            {
                "planned_topic_id": 4,
                "campaign_id": 11,
                "topic_status": "generated",
                "content_id": 4,
                "resolved_content_id": 4,
                "content_type": "x_post",
                "source_material": {"expected_content_type": "blog_post"},
            },
        ],
        now=NOW,
    )

    counts = report["summary"]["by_issue_type"]
    assert report["artifact_type"] == "planned_topic_fulfillment_link_gaps"
    assert counts["generated_missing_content_id"] == 1
    assert counts["missing_generated_content"] == 1
    assert counts["abandoned_generated_content"] == 1
    assert counts["content_type_mismatch"] == 1
    assert {"planned_topic_id", "campaign_id"}.issubset(report["findings"][0])


def test_campaign_status_filter_and_limit_are_applied():
    report = build_planned_topic_fulfillment_link_gaps_report(
        [
            {"planned_topic_id": 1, "campaign_id": 10, "topic_status": "generated", "content_id": None},
            {"planned_topic_id": 2, "campaign_id": 11, "topic_status": "planned", "content_id": None},
        ],
        campaign_id=10,
        status="generated",
        limit=1,
        now=NOW,
    )

    assert report["summary"]["planned_topic_count"] == 1
    assert report["summary"]["finding_count"] == 1
    assert report["findings"][0]["planned_topic_id"] == 1


def test_db_adapter_reads_joined_topics_and_handles_missing_tables():
    conn = _conn()
    conn.execute("INSERT INTO content_campaigns VALUES (10, 'Campaign')")
    conn.execute("INSERT INTO generated_content VALUES (1, 'x_post', 1, 'published')")
    conn.execute("INSERT INTO generated_content VALUES (2, 'blog_post', -1, 'abandoned')")
    conn.execute("INSERT INTO planned_topics VALUES (1, 10, 'generated', NULL, '{}')")
    conn.execute("INSERT INTO planned_topics VALUES (2, 10, 'generated', 99, '{}')")
    conn.execute("INSERT INTO planned_topics VALUES (3, 10, 'generated', 2, '{}')")
    conn.execute(
        "INSERT INTO planned_topics VALUES (4, 10, 'generated', 1, ?)",
        (json.dumps({"expected_content_type": "blog_post"}),),
    )

    report = build_planned_topic_fulfillment_link_gaps_report_from_db(conn, campaign_id=10, now=NOW)

    assert report["summary"]["planned_topic_count"] == 4
    assert report["summary"]["by_issue_type"]["generated_missing_content_id"] == 1
    assert report["summary"]["by_issue_type"]["missing_generated_content"] == 1
    assert report["summary"]["by_issue_type"]["abandoned_generated_content"] == 1
    assert report["summary"]["by_issue_type"]["content_type_mismatch"] == 1

    empty = build_planned_topic_fulfillment_link_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)
    assert empty["missing_tables"] == ["content_campaigns", "generated_content", "planned_topics"]
    assert empty["findings"] == []


def test_json_and_text_formatters_are_stable():
    report = build_planned_topic_fulfillment_link_gaps_report(
        [{"planned_topic_id": 1, "campaign_id": 10, "topic_status": "generated", "content_id": None}],
        now=NOW,
    )

    payload = json.loads(format_planned_topic_fulfillment_link_gaps_json(report))
    assert payload["artifact_type"] == "planned_topic_fulfillment_link_gaps"
    assert list(payload) == sorted(payload)
    text = format_planned_topic_fulfillment_link_gaps_text(report)
    assert "Planned Topic Fulfillment Link Gaps" in text
    assert "planned_topic_id | campaign_id | status" in text


def test_cli_supports_db_campaign_status_json_text_and_invalid_limit(tmp_path, monkeypatch, capsys):
    db_path = tmp_path / "topics.sqlite"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE content_campaigns (id INTEGER PRIMARY KEY, name TEXT);
        CREATE TABLE generated_content (id INTEGER PRIMARY KEY, content_type TEXT, published INTEGER, status TEXT);
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            status TEXT,
            content_id INTEGER,
            source_material TEXT
        );
        INSERT INTO content_campaigns VALUES (10, 'Campaign');
        INSERT INTO planned_topics VALUES (1, 10, 'generated', NULL, '{}');
        """
    )
    conn.close()

    assert script.main(["--db", str(db_path), "--campaign-id", "10", "--status", "generated", "--limit", "5", "--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["artifact_type"] == "planned_topic_fulfillment_link_gaps"
    assert script.main(["--db", str(db_path), "--format", "text"]) == 0
    assert "generated_missing_content_id" in capsys.readouterr().out

    monkeypatch.setattr(script, "script_context", lambda: _script_context(sqlite3.connect(":memory:")))
    assert script.main(["--format", "json"]) == 0
    assert json.loads(capsys.readouterr().out)["findings"] == []
    with pytest.raises(SystemExit):
        script.parse_args(["--limit", "0"])
