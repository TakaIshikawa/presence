from datetime import datetime, timezone
import sqlite3

from evaluation.linkedin_engagement_identity_gaps import build_linkedin_engagement_identity_gaps_report, build_linkedin_engagement_identity_gaps_report_from_db, format_linkedin_engagement_identity_gaps_text

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_linkedin_identity_gaps():
    report = build_linkedin_engagement_identity_gaps_report([
        {"row_id": 1, "content_id": 1, "resolved_content_id": None},
        {"row_id": 2, "content_id": 2, "resolved_content_id": 2, "post_id": "p", "publication_post_id": "other"},
        {"row_id": 3, "content_id": 3, "resolved_content_id": 3, "post_id": "p"},
    ], now=NOW)
    assert report["artifact_type"] == "linkedin_engagement_identity_gaps"
    assert {"missing_content", "missing_platform_identity", "publication_identity_conflict", "duplicate_identity"} <= {f["reason"] for f in report["findings"]}
    assert "LinkedIn Engagement Identity Gaps" in format_linkedin_engagement_identity_gaps_text(report)
    assert build_linkedin_engagement_identity_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["generated_content", "linkedin_engagement"]
