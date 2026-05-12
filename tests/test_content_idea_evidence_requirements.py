"""Tests for content idea evidence requirements reporting."""

from __future__ import annotations

from datetime import datetime, timezone
import json

from synthesis.content_idea_evidence_requirements import (
    build_content_idea_evidence_requirements_report,
    format_content_idea_evidence_requirements_json,
    format_content_idea_evidence_requirements_text,
)


NOW = datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def _idea(db, *, note="Idea", topic="ops", priority="normal", status="open", source=None, metadata=None):
    idea_id = db.add_content_idea(
        note,
        topic=topic,
        priority=priority,
        status=status,
        source=source,
        source_metadata=metadata,
    )
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ?, updated_at = ? WHERE id = ?",
        ("2026-05-01T00:00:00+00:00", "2026-05-01T00:00:00+00:00", idea_id),
    )
    db.conn.commit()
    return idea_id


def test_categorizes_ready_and_missing_source(db):
    ready_id = _idea(
        db,
        priority="high",
        source="manual",
        metadata={
            "commits": ["abc"],
            "sessions": ["sess"],
            "knowledge_sources": ["ks"],
            "last_activity_at": "2026-05-01T00:00:00+00:00",
        },
    )
    missing_id = _idea(db, topic="missing")

    report = build_content_idea_evidence_requirements_report(db, now=NOW)
    by_id = {row.idea_id: row for row in report.rows}

    assert by_id[ready_id].category == "ready"
    assert by_id[ready_id].evidence_counts["commits"] == 1
    assert by_id[missing_id].category == "needs_source"
    assert "source_evidence" in by_id[missing_id].missing_requirements


def test_freshness_window_controls_recent_activity(db):
    stale_id = _idea(
        db,
        source="github",
        metadata={"commits": ["old"], "last_activity_at": "2025-01-01T00:00:00+00:00"},
    )
    db.conn.execute(
        "UPDATE content_ideas SET created_at = ?, updated_at = ? WHERE id = ?",
        ("2025-01-01T00:00:00+00:00", "2025-01-01T00:00:00+00:00", stale_id),
    )
    db.conn.commit()

    report = build_content_idea_evidence_requirements_report(db, freshness_days=30, now=NOW)

    assert report.rows[0].category == "needs_recent_activity"
    assert report.rows[0].recommended_next_action.startswith("Link fresh")


def test_specific_claim_evidence_and_formats(db):
    _idea(
        db,
        note="Claim: this improves conversion by 20%",
        source="session",
        metadata={"sessions": ["s1"], "last_activity_at": "2026-05-01T00:00:00+00:00"},
    )
    _idea(db, status="dismissed")

    report = build_content_idea_evidence_requirements_report(db, now=NOW)
    payload = json.loads(format_content_idea_evidence_requirements_json(report))
    text = format_content_idea_evidence_requirements_text(report)

    assert payload["artifact_type"] == "content_idea_evidence_requirements"
    assert payload["rows"][0]["category"] == "needs_specific_claim_evidence"
    assert payload["rows"][0]["missing_requirements"] == ["specific_claim_evidence"]
    assert "Open ideas: 1" in text
