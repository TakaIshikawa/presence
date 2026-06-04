from __future__ import annotations

from evaluation.reply_draft_relationship_context_overuse import build_reply_draft_relationship_context_overuse_report


def test_no_context_and_single_context_use_are_ignored():
    report = build_reply_draft_relationship_context_overuse_report([
        {"draft_id": "none", "draft_text": "Thanks for the note.", "relationship_context_terms": ["school", "conference"]},
        {"draft_id": "one", "draft_text": "It was good seeing you at the conference.", "relationship_context_terms": ["conference"]},
    ])
    assert report["findings"] == []


def test_overuse_and_repeated_same_term_are_flagged():
    report = build_reply_draft_relationship_context_overuse_report([
        {"draft_id": "many", "draft_text": "After school and the conference, your hiring plan and school update stood out.", "relationship_context_terms": ["school", "conference", "hiring plan"]},
        {"draft_id": "repeat", "draft_text": "The meetup note and meetup follow-up both helped.", "relationship_context_terms": ["meetup"]},
    ])
    counts = {row["draft_id"]: row["context_reference_count"] for row in report["findings"]}
    assert counts["many"] == 4
    assert counts["repeat"] == 2
