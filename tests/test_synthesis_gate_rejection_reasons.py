"""Tests for synthesis gate rejection reasons reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sqlite3

from synthesis.gate_rejection_reasons import (
    build_gate_rejection_reasons_report,
    format_gate_rejection_reasons_json,
    format_gate_rejection_reasons_text,
    normalize_rejection_reason,
)


NOW = datetime(2026, 5, 4, 12, 0, tzinfo=timezone.utc)
SCRIPT_PATH = (
    Path(__file__).resolve().parent.parent / "scripts" / "synthesis_gate_rejections.py"
)
spec = importlib.util.spec_from_file_location("synthesis_gate_rejections_script", SCRIPT_PATH)
synthesis_gate_rejections_script = importlib.util.module_from_spec(spec)
assert spec and spec.loader
spec.loader.exec_module(synthesis_gate_rejections_script)


def test_normalize_rejection_reasons():
    """Test that various rejection patterns map to stable normalized labels."""
    # Budget rejections
    assert normalize_rejection_reason("Budget exceeded for daily model usage") == "budget_exceeded"
    assert normalize_rejection_reason("Cost threshold reached") == "budget_exceeded"

    # Filter rejections
    assert normalize_rejection_reason("All candidates filtered (repetitive, stale, duplicate, or unsupported claims)") == "all_filtered"
    assert normalize_rejection_reason("Unsupported claims detected in final content") == "unsupported_claims"

    # Quality gate rejections
    assert normalize_rejection_reason("Below threshold score (6.5 < 7.0)") == "below_threshold"
    assert normalize_rejection_reason("Quality gate rejected candidate") == "quality_gate_rejected"

    # Persona rejections
    assert normalize_rejection_reason("Persona alignment check failed") == "persona_misalignment"
    assert normalize_rejection_reason("Voice mismatch detected") == "persona_misalignment"

    # Stale patterns
    assert normalize_rejection_reason("Stale pattern detected") == "stale_pattern"

    # Thread validation
    assert normalize_rejection_reason("Thread continuity validation failed") == "thread_validation_failed"

    # Other patterns
    assert normalize_rejection_reason("Content exceeds length constraint") == "length_constraint"
    assert normalize_rejection_reason("Format error in generated content") == "format_error"

    # Unknown
    assert normalize_rejection_reason("Something unexpected happened") == "other"
    assert normalize_rejection_reason(None) == "unknown"
    assert normalize_rejection_reason("") == "unknown"


def test_classify_rejections_into_reason_buckets():
    """Test that pipeline run rejections are classified into stable reason buckets."""
    rows = [
        {
            "id": 1,
            "batch_id": "batch_001",
            "content_type": "x_post",
            "rejection_reason": "Below threshold score (6.5 < 7.0)",
            "content_id": None,
            "outcome": "below_threshold",
            "filter_stats": None,
            "created_at": "2026-05-01T10:00:00+00:00",
        },
        {
            "id": 2,
            "batch_id": "batch_002",
            "content_type": "x_post",
            "rejection_reason": "All candidates filtered (repetitive, stale, duplicate, or unsupported claims)",
            "content_id": None,
            "outcome": "all_filtered",
            "filter_stats": '{"repetitive": 2, "stale": 1}',
            "created_at": "2026-05-01T10:05:00+00:00",
        },
        {
            "id": 3,
            "batch_id": "batch_003",
            "content_type": "x_thread",
            "rejection_reason": "Budget exceeded for daily model usage",
            "content_id": None,
            "outcome": "budget_gate",
            "filter_stats": None,
            "created_at": "2026-05-01T10:10:00+00:00",
        },
        {
            "id": 4,
            "batch_id": "batch_004",
            "content_type": "x_post",
            "rejection_reason": "Persona alignment check failed",
            "content_id": None,
            "outcome": "below_threshold",
            "filter_stats": None,
            "created_at": "2026-05-01T10:15:00+00:00",
        },
        {
            "id": 5,
            "batch_id": "batch_005",
            "content_type": "x_post",
            "rejection_reason": "Stale pattern detected",
            "content_id": None,
            "outcome": "all_filtered",
            "filter_stats": '{"stale": 1}',
            "created_at": "2026-05-01T10:20:00+00:00",
        },
        {
            "id": 6,
            "batch_id": "batch_006",
            "content_type": "x_post",
            "rejection_reason": "Quality gate rejected candidate",
            "content_id": None,
            "outcome": "below_threshold",
            "filter_stats": None,
            "created_at": "2026-05-01T10:25:00+00:00",
        },
        {
            "id": 7,
            "batch_id": "batch_007",
            "content_type": "x_post",
            "rejection_reason": None,
            "content_id": 100,
            "outcome": "published",
            "filter_stats": None,
            "created_at": "2026-05-01T10:30:00+00:00",
        },
    ]

    report = build_gate_rejection_reasons_report(rows, days=7, limit_examples=3, now=NOW)

    # Should only include rows with rejection_reason
    assert len(report.items) == 6

    # Items are sorted by (content_type, reason_label, created_at)
    # x_post items come before x_thread items
    # Within x_post, sorted alphabetically by reason_label:
    # all_filtered, below_threshold, persona_misalignment, quality_gate_rejected, stale_pattern
    x_post_items = [item for item in report.items if item.content_type == "x_post"]
    x_thread_items = [item for item in report.items if item.content_type == "x_thread"]

    assert len(x_post_items) == 5
    assert len(x_thread_items) == 1

    assert x_post_items[0].reason_label == "all_filtered"
    assert x_post_items[1].reason_label == "below_threshold"
    assert x_post_items[2].reason_label == "persona_misalignment"
    assert x_post_items[3].reason_label == "quality_gate_rejected"
    assert x_post_items[4].reason_label == "stale_pattern"
    assert x_thread_items[0].reason_label == "budget_exceeded"

    # Check totals
    assert report.totals["rejection_count"] == 6
    assert report.totals["content_type_count"] == 2  # x_post and x_thread
    assert report.totals["rows_scanned"] == 7

    # Check summaries
    assert len(report.summaries) == 6
    x_post_summaries = [s for s in report.summaries if s.content_type == "x_post"]
    assert len(x_post_summaries) == 5


def test_missing_rejection_reasons_are_skipped():
    """Test that rows without rejection_reason are ignored."""
    rows = [
        {
            "id": 1,
            "batch_id": "batch_001",
            "content_type": "x_post",
            "rejection_reason": None,
            "content_id": 100,
            "outcome": "published",
            "created_at": "2026-05-01T10:00:00+00:00",
        },
        {
            "id": 2,
            "batch_id": "batch_002",
            "content_type": "x_post",
            "rejection_reason": "",
            "content_id": None,
            "outcome": "dry_run",
            "created_at": "2026-05-01T10:05:00+00:00",
        },
    ]

    report = build_gate_rejection_reasons_report(rows, days=7, now=NOW)
    assert len(report.items) == 0
    assert report.totals["rejection_count"] == 0


def test_example_limiting():
    """Test that recent examples are properly limited per reason."""
    # Create 10 rows with varying rejection scores to get unique excerpts
    rows = [
        {
            "id": i,
            "batch_id": f"batch_{i:03d}",
            "content_type": "x_post",
            "rejection_reason": f"Below threshold score ({6.0 + i * 0.1:.1f} < 7.0)",
            "content_id": None,
            "outcome": "below_threshold",
            "created_at": f"2026-05-01T{10 + i:02d}:00:00+00:00",
        }
        for i in range(10)
    ]

    # Limit to 3 examples
    report = build_gate_rejection_reasons_report(rows, days=7, limit_examples=3, now=NOW)
    assert len(report.items) == 10
    assert len(report.summaries) == 1

    summary = report.summaries[0]
    assert summary.rejection_count == 10
    assert len(summary.recent_examples) == 3

    # Verify examples are from most recent runs (sorted descending by timestamp)
    # Most recent should have highest scores (9, 8, 7)
    for example in summary.recent_examples:
        assert "Below threshold score" in example


def test_json_output_format():
    """Test that JSON output is valid and contains expected structure."""
    rows = [
        {
            "id": 1,
            "batch_id": "batch_001",
            "content_type": "x_post",
            "rejection_reason": "Below threshold score (6.5 < 7.0)",
            "content_id": None,
            "outcome": "below_threshold",
            "filter_stats": None,
            "created_at": "2026-05-01T10:00:00+00:00",
        },
    ]

    report = build_gate_rejection_reasons_report(rows, days=7, limit_examples=5, now=NOW)
    json_output = format_gate_rejection_reasons_json(report)

    parsed = json.loads(json_output)
    assert parsed["artifact_type"] == "synthesis_gate_rejection_reasons"
    assert "filters" in parsed
    assert "generated_at" in parsed
    assert "items" in parsed
    assert "summaries" in parsed
    assert "totals" in parsed

    assert parsed["filters"]["days"] == 7
    assert parsed["filters"]["limit_examples"] == 5
    assert len(parsed["items"]) == 1
    assert len(parsed["summaries"]) == 1

    item = parsed["items"][0]
    assert item["run_id"] == 1
    assert item["batch_id"] == "batch_001"
    assert item["content_type"] == "x_post"
    assert item["reason_label"] == "below_threshold"

    summary = parsed["summaries"][0]
    assert summary["content_type"] == "x_post"
    assert summary["reason_label"] == "below_threshold"
    assert summary["rejection_count"] == 1
    assert isinstance(summary["recent_examples"], list)


def test_text_output_format():
    """Test that text output is readable and contains key information."""
    rows = [
        {
            "id": 1,
            "batch_id": "batch_001",
            "content_type": "x_post",
            "rejection_reason": "Below threshold score (6.5 < 7.0)",
            "content_id": None,
            "outcome": "below_threshold",
            "filter_stats": None,
            "created_at": "2026-05-01T10:00:00+00:00",
        },
    ]

    report = build_gate_rejection_reasons_report(rows, days=7, limit_examples=5, now=NOW)
    text_output = format_gate_rejection_reasons_text(report)

    assert "Synthesis Gate Rejection Reasons" in text_output
    assert "days=7" in text_output
    assert "limit_examples=5" in text_output
    assert "rejections=1" in text_output
    assert "content_type=x_post" in text_output
    assert "reason=below_threshold" in text_output
    assert "count=1" in text_output


def test_cli_script_integration():
    """Test that CLI script can be invoked and produces expected output."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE pipeline_runs (
            id INTEGER PRIMARY KEY,
            batch_id TEXT,
            content_type TEXT,
            rejection_reason TEXT,
            content_id INTEGER,
            outcome TEXT,
            filter_stats TEXT,
            created_at TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO pipeline_runs VALUES
        (1, 'batch_001', 'x_post', 'Below threshold score', NULL, 'below_threshold', NULL, '2026-05-01T10:00:00+00:00')
        """
    )
    conn.commit()

    report = build_gate_rejection_reasons_report(conn, days=7, limit_examples=5, now=NOW)
    assert len(report.items) == 1
    assert report.items[0].reason_label == "below_threshold"


def test_diverse_content_types():
    """Test grouping across different content types."""
    rows = [
        {
            "id": 1,
            "batch_id": "batch_001",
            "content_type": "x_post",
            "rejection_reason": "Below threshold score",
            "created_at": "2026-05-01T10:00:00+00:00",
        },
        {
            "id": 2,
            "batch_id": "batch_002",
            "content_type": "x_thread",
            "rejection_reason": "Below threshold score",
            "created_at": "2026-05-01T10:05:00+00:00",
        },
        {
            "id": 3,
            "batch_id": "batch_003",
            "content_type": "blog_post",
            "rejection_reason": "Stale pattern detected",
            "created_at": "2026-05-01T10:10:00+00:00",
        },
    ]

    report = build_gate_rejection_reasons_report(rows, days=7, limit_examples=5, now=NOW)
    assert len(report.items) == 3
    assert report.totals["content_type_count"] == 3

    # Should have 3 summaries: x_post+below_threshold, x_thread+below_threshold, blog_post+stale_pattern
    assert len(report.summaries) == 3

    content_types = {s.content_type for s in report.summaries}
    assert content_types == {"x_post", "x_thread", "blog_post"}


def test_empty_report():
    """Test behavior with no rejections."""
    rows = []
    report = build_gate_rejection_reasons_report(rows, days=7, limit_examples=5, now=NOW)

    assert len(report.items) == 0
    assert len(report.summaries) == 0
    assert report.totals["rejection_count"] == 0

    text_output = format_gate_rejection_reasons_text(report)
    assert "No gate rejections found" in text_output


def test_deduplication_of_examples():
    """Test that duplicate reason excerpts are deduplicated in examples."""
    rows = [
        {
            "id": 1,
            "batch_id": "batch_001",
            "content_type": "x_post",
            "rejection_reason": "Below threshold score (6.5 < 7.0)",
            "created_at": "2026-05-01T10:00:00+00:00",
        },
        {
            "id": 2,
            "batch_id": "batch_002",
            "content_type": "x_post",
            "rejection_reason": "Below threshold score (6.5 < 7.0)",
            "created_at": "2026-05-01T10:05:00+00:00",
        },
        {
            "id": 3,
            "batch_id": "batch_003",
            "content_type": "x_post",
            "rejection_reason": "Below threshold score (6.2 < 7.0)",
            "created_at": "2026-05-01T10:10:00+00:00",
        },
    ]

    report = build_gate_rejection_reasons_report(rows, days=7, limit_examples=5, now=NOW)
    assert len(report.items) == 3

    summary = report.summaries[0]
    assert summary.rejection_count == 3
    # Should have 2 unique examples (6.5 appears twice, 6.2 once)
    assert len(summary.recent_examples) == 2
