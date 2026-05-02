"""Tests for queued publish format readiness reporting."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from output.publish_readiness_report import (
    build_publish_format_readiness_report,
    format_publish_format_readiness_json,
)


NOW = datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = Path(__file__).resolve().parent.parent / "scripts" / "publish_readiness.py"
spec = importlib.util.spec_from_file_location("publish_readiness_script", SCRIPT_PATH)
publish_readiness_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(publish_readiness_script)


@contextmanager
def _script_context(db):
    yield SimpleNamespace(), db


def _content(db, content_type: str, content: str) -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="ok",
    )


def _queue(db, content_id: int, platform: str) -> int:
    return db.queue_for_publishing(
        content_id,
        "2026-05-01T13:00:00+00:00",
        platform=platform,
    )


def test_valid_examples_for_each_supported_destination_have_no_findings(db):
    examples = [
        ("x_post", "Short X post.", "x"),
        ("x_thread", "TWEET 1:\nFirst point\nTWEET 2:\nSecond point", "x"),
        (
            "newsletter",
            json.dumps({"subject": "Weekly notes", "body": "Useful links and context."}),
            "newsletter",
        ),
        ("blog_post", "# A practical launch note\n\nBody copy for the blog.", "blog"),
    ]
    for content_type, body, platform in examples:
        _queue(db, _content(db, content_type, body), platform)

    report = build_publish_format_readiness_report(db, now=NOW)
    payload = json.loads(format_publish_format_readiness_json(report))

    assert payload["artifact_type"] == "publish_format_readiness"
    assert payload["totals"]["items"] == 4
    assert payload["totals"]["findings"] == 0
    assert payload["totals"]["blocked"] == 0
    assert all(item["status"] == "ready" for item in payload["items"])


def test_invalid_x_post_reports_missing_body(db):
    content_id = _content(db, "x_post", " ")
    queue_id = _queue(db, content_id, "x")

    report = build_publish_format_readiness_report(db, now=NOW)
    finding = report.findings_by_destination["x_post"][0].to_dict()

    assert finding["item_id"] == content_id
    assert finding["queue_id"] == queue_id
    assert finding["destination"] == "x_post"
    assert finding["destination_id"] == "x"
    assert finding["severity"] == "blocked"
    assert finding["missing_fields"] == ["body"]
    assert finding["invalid_fields"] == []
    assert "post copy" in finding["fix_hint"]


def test_invalid_x_thread_reports_item_count_problem(db):
    content_id = _content(db, "x_thread", "TWEET 1:\nOnly one item")
    _queue(db, content_id, "x")

    report = build_publish_format_readiness_report(db, now=NOW)
    finding = report.findings_by_destination["x_thread"][0].to_dict()

    assert finding["item_id"] == content_id
    assert finding["destination"] == "x_thread"
    assert finding["missing_fields"] == []
    assert finding["invalid_fields"] == ["thread_items"]
    assert "at least two" in finding["fix_hint"]


def test_invalid_newsletter_reports_missing_subject_and_body(db):
    content_id = _content(db, "newsletter", json.dumps({"subject": "", "body": ""}))
    _queue(db, content_id, "newsletter")

    report = build_publish_format_readiness_report(db, now=NOW)
    finding = report.findings_by_destination["newsletter"][0].to_dict()

    assert finding["item_id"] == content_id
    assert finding["destination"] == "newsletter"
    assert finding["destination_id"] == "newsletter"
    assert finding["missing_fields"] == ["subject", "body"]
    assert finding["invalid_fields"] == []


def test_invalid_blog_reports_missing_title(db):
    content_id = _content(db, "blog_post", "Body without a markdown heading.")
    _queue(db, content_id, "blog")

    report = build_publish_format_readiness_report(db, now=NOW)
    finding = report.findings_by_destination["blog"][0].to_dict()

    assert finding["item_id"] == content_id
    assert finding["destination"] == "blog"
    assert finding["destination_id"] == "blog"
    assert finding["missing_fields"] == ["title"]
    assert finding["invalid_fields"] == []
    assert "blog title" in finding["fix_hint"]


def test_script_format_report_outputs_grouped_json_findings(db, capsys):
    content_id = _content(db, "x_thread", "TWEET 1:\nOnly one item")
    _queue(db, content_id, "x")

    with patch.object(
        publish_readiness_script,
        "script_context",
        lambda: _script_context(db),
    ):
        exit_code = publish_readiness_script.main(["--report", "format"])

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 1
    assert payload["artifact_type"] == "publish_format_readiness"
    assert payload["findings_by_destination"]["x_thread"][0]["item_id"] == content_id
    assert payload["totals"]["by_destination"]["x_thread"]["findings"] == 1
