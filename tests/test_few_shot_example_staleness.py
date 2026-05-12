"""Tests for few-shot example staleness reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import json

from synthesis.few_shot_example_staleness import (
    build_few_shot_example_staleness_report,
    format_few_shot_example_staleness_json,
    format_few_shot_example_staleness_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def test_flags_age_format_and_low_engagement():
    recent = [
        {
            "id": 10,
            "content_type": "x_post",
            "content_format": "question",
            "created_at": "2026-05-01T00:00:00+00:00",
        }
    ]
    examples = [
        {
            "id": "ex-1",
            "content_type": "x_post",
            "content_format": "shortform",
            "last_engagement_timestamp": "2025-12-01T00:00:00+00:00",
            "engagement_score": 0.2,
        }
    ]

    report = build_few_shot_example_staleness_report(recent, examples, now=NOW)

    assert len(report.rows) == 1
    row = report.rows[0]
    assert row.example_id == "ex-1"
    assert row.detected_format == "shortform"
    assert row.last_engagement_timestamp == "2025-12-01T00:00:00+00:00"
    assert row.staleness_reasons == (
        "format_mismatch",
        "low_recent_engagement",
        "old_engagement",
    )
    assert row.priority_score > 0


def test_missing_engagement_and_empty_outputs_are_stable():
    empty = build_few_shot_example_staleness_report([], [], now=NOW)
    missing = build_few_shot_example_staleness_report(
        [],
        [{"example_id": "ex-2", "content_type": "blog_post", "content": "Long note"}],
        now=NOW,
    )

    assert "No stale few-shot examples found." in format_few_shot_example_staleness_text(empty)
    assert missing.rows[0].staleness_reasons == ("missing_engagement",)
    payload = json.loads(format_few_shot_example_staleness_json(missing))
    assert payload["artifact_type"] == "few_shot_example_staleness"
    assert payload["totals"]["missing_engagement_count"] == 1


def test_mixed_format_inputs_sort_by_priority_then_identity():
    recent = [{"content_type": "x_thread", "content": "1\n2\n3", "created_at": NOW}]
    examples = [
        {
            "id": "b",
            "content_type": "x_thread",
            "content": "Still ok\nline\nline",
            "last_engagement_timestamp": NOW,
            "engagement_score": 5,
        },
        {
            "id": "a",
            "content_type": "x_thread",
            "content": "single post",
            "last_engagement_timestamp": NOW,
            "engagement_score": 0,
            "source_timestamp": "2025-01-01T00:00:00+00:00",
        },
    ]

    report = build_few_shot_example_staleness_report(recent, examples, now=NOW)

    assert [row.example_id for row in report.rows] == ["a"]
    assert report.rows[0].staleness_reasons == (
        "format_mismatch",
        "low_recent_engagement",
        "stale_source_evidence",
    )
