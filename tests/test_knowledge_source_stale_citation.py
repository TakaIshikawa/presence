from __future__ import annotations

from datetime import datetime, timezone

from evaluation.knowledge_source_stale_citation import build_knowledge_source_stale_citation_report

NOW = datetime(2026, 6, 4, tzinfo=timezone.utc)


def test_stale_freshness_sensitive_citation_is_flagged():
    report = build_knowledge_source_stale_citation_report([
        {"content_id": "c1", "source_id": "s1", "source_date": "2025-01-01", "topic": "api"}
    ], max_allowed_age_days=180, now=NOW)
    row = report["findings"][0]
    assert row["content_id"] == "c1"
    assert row["age_days"] > 180
    assert row["max_allowed_age_days"] == 180


def test_evergreen_and_missing_dates_are_ignored():
    report = build_knowledge_source_stale_citation_report([
        {"content_id": "ever", "source_id": "s2", "source_date": "2020-01-01", "topic": "history"},
        {"content_id": "missing", "source_id": "s3", "topic": "api"},
    ], now=NOW)
    assert report["findings"] == []
