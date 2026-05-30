from datetime import datetime, timezone
import json
import sqlite3

from evaluation.mastodon_engagement_snapshot_drift import build_mastodon_engagement_snapshot_drift_report, build_mastodon_engagement_snapshot_drift_report_from_db, format_mastodon_engagement_snapshot_drift_json, format_mastodon_engagement_snapshot_drift_text

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_mastodon_snapshot_drift_builder_db_and_formatters():
    rows = [
        {"row_id": 1, "content_id": 1, "fetched_at": "2026-05-01T00:00:00+00:00", "likes_count": 5, "raw_metrics": "{}"},
        {"row_id": 2, "content_id": 1, "fetched_at": "2026-05-01T00:00:00+00:00", "likes_count": 4, "raw_metrics": "bad"},
        {"row_id": 3, "content_id": 1, "fetched_at": "2026-05-04T00:00:00+00:00", "likes_count": 6, "raw_metrics": "{}"},
    ]
    report = build_mastodon_engagement_snapshot_drift_report(rows, max_gap_hours=24, now=NOW)
    assert report["artifact_type"] == "mastodon_engagement_snapshot_drift"
    assert {"count_drop", "duplicate_fetched_at", "invalid_raw_metrics_json", "stale_gap"} <= {i["reason"] for g in report["findings"] for i in g["items"]}
    assert json.loads(format_mastodon_engagement_snapshot_drift_json(report))["artifact_type"] == report["artifact_type"]
    assert "Mastodon Engagement Snapshot Drift" in format_mastodon_engagement_snapshot_drift_text(report)
    conn = sqlite3.connect(":memory:")
    assert build_mastodon_engagement_snapshot_drift_report_from_db(conn, now=NOW)["missing_tables"] == ["mastodon_engagement"]
