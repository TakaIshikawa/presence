from datetime import datetime, timezone
import sqlite3

from evaluation.publication_attempt_response_shape_catalog import build_publication_attempt_response_shape_catalog_report, build_publication_attempt_response_shape_catalog_report_from_db

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_publication_attempt_response_shape_catalog():
    report = build_publication_attempt_response_shape_catalog_report([
        {"id": 1, "platform": "x", "success": 1, "response_metadata": "{\"ok\": true}"},
        {"id": 2, "platform": "x", "success": 0, "response_metadata": "bad"},
        {"id": 3, "platform": "x", "success": 0, "response_metadata": "{\"a\":\"" + "x" * 20 + "\"}"},
    ], max_metadata_bytes=10, now=NOW)
    reasons = {i["reason"] for g in report["findings"] for i in g["items"]}
    assert {"malformed_json", "missing_success_response_key", "oversized_metadata", "rare_shape"} <= reasons
    assert report["shape_summaries"]
    assert build_publication_attempt_response_shape_catalog_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["publication_attempts"]
