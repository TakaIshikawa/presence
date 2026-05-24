from datetime import datetime, timezone
import sqlite3

from evaluation.content_feedback_revision_adoption import build_content_feedback_revision_adoption_report, build_content_feedback_revision_adoption_report_from_db, format_content_feedback_revision_adoption_text

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_content_feedback_revision_adoption():
    report = build_content_feedback_revision_adoption_report([
        {"id": 1, "content_id": 1, "type": "revise", "replacement_text": "new", "created_at": "2026-04-30T00:00:00+00:00", "variant_content": "use new"},
        {"id": 2, "content_id": 1, "type": "revise", "replacement_text": "other", "created_at": "2026-04-29T00:00:00+00:00"},
        {"id": 3, "content_id": 2, "type": "prefer", "replacement_text": "source", "source_content": "source"},
    ], now=NOW)
    assert {"adopted_in_variant", "unadopted_revision", "adopted_in_source_content", "conflicting_replacements"} <= {f["reason"] for f in report["findings"]}
    assert "Content Feedback Revision Adoption" in format_content_feedback_revision_adoption_text(report)
    assert build_content_feedback_revision_adoption_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["content_feedback"]
