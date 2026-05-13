from __future__ import annotations

from datetime import datetime, timezone
import json
import sqlite3

from evaluation.pipeline_filter_dropoff import (
    build_pipeline_filter_dropoff_report,
    format_pipeline_filter_dropoff_json,
)


NOW = datetime(2026, 5, 13, 12, 0, tzinfo=timezone.utc)


def _run(db, *, content_type="x_post", outcome="all_filtered", reason="filtered", stats=None):
    db.conn.execute(
        """INSERT INTO pipeline_runs
           (batch_id, content_type, outcome, rejection_reason, filter_stats, created_at)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            f"batch-{content_type}-{outcome}-{stats}",
            content_type,
            outcome,
            reason,
            stats,
            NOW.isoformat(),
        ),
    )
    db.conn.commit()
    return db.conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def test_report_groups_dropoff_by_stage_content_type_and_outcome(db):
    first_id = _run(
        db,
        content_type="x_post",
        outcome="all_filtered",
        stats=json.dumps({"char_limit_rejected": 2, "semantic_dedup_rejected": 5}),
    )
    _run(
        db,
        content_type="blog_post",
        outcome="below_threshold",
        stats=json.dumps({"stages": {"persona_guard": {"filtered": 3}}}),
    )

    payload = json.loads(format_pipeline_filter_dropoff_json(build_pipeline_filter_dropoff_report(db, now=NOW)))

    assert payload["artifact_type"] == "pipeline_filter_dropoff"
    assert payload["totals"]["pipeline_runs"] == 2
    assert payload["totals"]["malformed_filter_stats"] == 0
    by_stage = {group["stage"]: group for group in payload["stage_groups"]}
    assert by_stage["semantic_dedup"]["total_filtered"] == 5
    assert by_stage["semantic_dedup"]["representative_pipeline_run_ids"] == [first_id]
    by_outcome = {group["outcome"]: group for group in payload["outcome_groups"]}
    assert by_outcome["all_filtered"]["content_types"] == {"x_post": 1}
    row = payload["rows"][0]
    assert set(row) >= {
        "pipeline_run_id",
        "content_type",
        "outcome",
        "rejection_reason",
        "dominant_filter_stage",
        "total_filtered",
    }


def test_malformed_filter_stats_are_counted_without_crashing(db):
    run_id = _run(db, stats="{bad json")
    report = build_pipeline_filter_dropoff_report(db, now=NOW)

    assert report["totals"]["malformed_filter_stats"] == 1
    assert report["rows"][0]["pipeline_run_id"] == run_id
    assert report["rows"][0]["dominant_filter_stage"] is None


def test_missing_pipeline_runs_table_returns_empty_report():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    report = build_pipeline_filter_dropoff_report(conn, now=NOW)

    assert report["missing_tables"] == ["pipeline_runs"]
    assert report["totals"]["pipeline_runs"] == 0
