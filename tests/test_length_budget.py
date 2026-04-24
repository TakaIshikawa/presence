"""Tests for platform copy length budget reports."""

import json
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from output.length_budget import (  # noqa: E402
    LengthBudgetRecordNotFound,
    build_length_budget_report,
    evaluate_copy_budget,
    format_length_budget_report,
)


def _insert_content(db, content="Short post", content_type="x_post"):
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )


def test_single_post_reports_platform_budgets(db):
    content_id = _insert_content(db, "A" * 281)

    report = build_length_budget_report(db, content_id=content_id)

    assert report["platforms"]["x"]["status"] == "overflow"
    assert report["platforms"]["x"]["count"] == 281
    assert report["platforms"]["x"]["limit"] == 280
    assert report["platforms"]["x"]["remaining"] == -1
    assert report["platforms"]["x"]["overflow"] == 1
    assert report["platforms"]["x"]["recommended_target"] == 252
    assert report["platforms"]["bluesky"]["status"] == "risk"
    assert report["platforms"]["bluesky"]["limit"] == 300
    assert report["platforms"]["linkedin"]["limit"] == 3000


def test_thread_content_reports_segments_and_overall(db):
    content = f"TWEET 1:\n{'A' * 260}\nTWEET 2:\n{'B' * 281}"
    content_id = _insert_content(db, content, content_type="x_thread")

    report = build_length_budget_report(db, content_id=content_id, platform="x")
    x_budget = report["platforms"]["x"]

    assert x_budget["status"] == "overflow"
    assert x_budget["count"] == 541
    assert x_budget["limit"] == 560
    assert x_budget["remaining"] == 19
    assert x_budget["overflow"] == 0
    assert x_budget["thread_segment_risk"] is True
    assert [segment["count"] for segment in x_budget["segments"]] == [260, 281]
    assert x_budget["segments"][1]["overflow"] == 1


def test_stored_content_variants_are_included(db):
    content_id = _insert_content(db, "Base post")
    variant_id = db.upsert_content_variant(
        content_id,
        platform="newsletter",
        variant_type="subject",
        content="N" * 91,
        metadata={"source": "test"},
    )

    report = build_length_budget_report(db, content_id=content_id, platform="newsletter")

    variants = report["platforms"]["newsletter"]["variants"]
    assert len(variants) == 1
    assert variants[0]["variant_id"] == variant_id
    assert variants[0]["variant_type"] == "subject"
    assert variants[0]["status"] == "overflow"
    assert variants[0]["limit"] == 90
    assert variants[0]["overflow"] == 1
    assert variants[0]["metadata"] == {"source": "test"}


def test_queue_row_can_be_inspected(db):
    content_id = _insert_content(db, "Queued copy")
    queue_id = db.queue_for_publishing(
        content_id,
        "2026-04-17T12:00:00+00:00",
        platform="all",
    )

    report = build_length_budget_report(db, queue_id=queue_id, platform="bluesky")

    assert report["content"]["id"] == content_id
    assert report["queue"]["queue_id"] == queue_id
    assert report["queue"]["queue_status"] == "queued"
    assert list(report["platforms"]) == ["bluesky"]


def test_platform_filter_supports_newsletter_and_blog_budgets(db):
    newsletter = evaluate_copy_budget("S" * 82, "newsletter")
    blog = evaluate_copy_budget("B" * 71, "blog_title")

    assert newsletter["status"] == "risk"
    assert newsletter["limit"] == 90
    assert newsletter["remaining"] == 8
    assert blog["platform"] == "blog"
    assert blog["limit"] == 70
    assert blog["status"] == "overflow"
    assert blog["overflow"] == 1


def test_length_budget_cli_outputs_json_and_filters_platform(db, capsys):
    content_id = _insert_content(db, "CLI post")

    import length_budget

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("length_budget.script_context", return_value=Context()):
        exit_code = length_budget.main(
            ["--content-id", str(content_id), "--platform", "x", "--json"]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    payload = json.loads(captured.out)
    assert list(payload["platforms"]) == ["x"]
    assert payload["platforms"]["x"]["count"] == len("CLI post")


def test_length_budget_cli_reports_missing_record(db, capsys):
    import length_budget

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("length_budget.script_context", return_value=Context()):
        exit_code = length_budget.main(["--content-id", "999"])

    captured = capsys.readouterr()
    assert exit_code == 1
    assert captured.out == ""
    assert "generated_content id 999 not found" in captured.err


def test_format_length_budget_report_includes_status_fields(db):
    content_id = _insert_content(db, "Readable")

    report = build_length_budget_report(db, content_id=content_id, platform="x")
    text = format_length_budget_report(report)

    assert f"Content {content_id} (x_post)" in text
    assert "X: ok 8/280 (remaining 272, overflow 0, target 252)" in text


def test_missing_queue_raises(db):
    with pytest.raises(LengthBudgetRecordNotFound, match="publish_queue id 999 not found"):
        build_length_budget_report(db, queue_id=999)
