from datetime import datetime, timezone
import sqlite3

from evaluation.newsletter_subject_candidate_rank_gaps import build_newsletter_subject_candidate_rank_gaps_report, build_newsletter_subject_candidate_rank_gaps_report_from_db

NOW = datetime(2026, 5, 1, tzinfo=timezone.utc)


def test_newsletter_subject_candidate_rank_gaps():
    report = build_newsletter_subject_candidate_rank_gaps_report([
        {"id": 1, "issue_id": 1, "rank": 1, "score": 1.0, "subject": "A"},
        {"id": 2, "issue_id": 1, "rank": 2, "score": 0.2, "selected": 1, "subject": "B", "send_subject": "C"},
        {"id": 4, "issue_id": 1, "rank": 2, "score": 0.1, "subject": "D"},
        {"id": 3, "issue_id": 2, "rank": 1, "score": 1.0},
    ], now=NOW)
    assert {"duplicate_rank", "selected_low_rank", "send_subject_mismatch", "missing_selected_candidate"} <= {f["reason"] for f in report["findings"]}
    assert build_newsletter_subject_candidate_rank_gaps_report_from_db(sqlite3.connect(":memory:"), now=NOW)["missing_tables"] == ["newsletter_subject_candidates"]
