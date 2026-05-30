from datetime import datetime, timezone
import sqlite3

from evaluation.engagement_prediction_component_completeness import build_engagement_prediction_component_completeness_report, build_engagement_prediction_component_completeness_report_from_db

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_engagement_prediction_component_completeness():
    report = build_engagement_prediction_component_completeness_report([
        {"id": 1, "content_id": 1, "hook_score": None, "novelty_score": 1.2, "predicted_engagement_score": 0.8, "actual_engagement_score": 0.5, "prediction_error": 0.1, "published_at": "2026-04-20T00:00:00+00:00"},
    ], now=NOW)
    assert {"missing_component", "invalid_component_range", "missing_prompt_identity", "prediction_error_mismatch"} <= {f["reason"] for f in report["findings"]}
    assert build_engagement_prediction_component_completeness_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["engagement_predictions"]
