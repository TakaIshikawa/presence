"""Tests for deterministic quote opportunity safety review."""

from __future__ import annotations

from engagement.quote_opportunities import QuoteOpportunity
from engagement.quote_safety import QuoteSafetyReviewer


def _opportunity(**overrides) -> QuoteOpportunity:
    values = {
        "knowledge_id": 1,
        "source_type": "curated_x",
        "source_id": "tweet-1",
        "source_url": "https://x.com/source/status/tweet-1",
        "author": "source",
        "content": "AI agents need testing harnesses for tool-call regressions.",
        "insight": "AI agents need testing harnesses.",
        "published_at": "2026-04-23T09:00:00+00:00",
        "campaign_id": None,
        "campaign_name": None,
        "topics": ["ai-agents", "testing"],
        "score": 0.75,
        "topical_relevance": 0.7,
        "freshness": 1.0,
        "source_quality": 0.8,
        "novelty": 0.8,
        "prior_performance": 0.7,
        "reasons": ["fresh source item"],
        "draft_text": "Quote-post angle: connect @source's point to ai-agents.",
        "already_enqueued": False,
    }
    values.update(overrides)
    return QuoteOpportunity(**values)


def _insert_knowledge(db, *, license_value: str = "attribution_required") -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_x",
            f"{license_value}-source",
            "https://x.com/source/status/1",
            "source",
            "AI agents need testing harnesses for tool-call regressions.",
            license_value,
            1,
        ),
    )
    db.conn.commit()
    return cursor.lastrowid


def test_quote_safety_passes_clean_attributed_relevant_draft(db):
    knowledge_id = _insert_knowledge(db)
    review = QuoteSafetyReviewer(db).review(_opportunity(knowledge_id=knowledge_id))

    assert review.score == 1.0
    assert review.blocking_flags == []
    assert review.blocked is False
    assert review.checks == {
        "attribution": True,
        "quote_length": True,
        "inflammatory_language": True,
        "relevance": True,
        "restricted_license": True,
        "platform_length": True,
    }


def test_quote_safety_flags_missing_attribution_and_length_violations():
    long_source = " ".join(["testing"] * 81)
    review = QuoteSafetyReviewer().review(
        _opportunity(
            author=None,
            source_url=None,
            content=long_source,
            draft_text="Quote-post angle: connect the point to testing.",
        )
    )

    assert "missing_attribution" in review.blocking_flags
    assert "excessive_quote_length" in review.blocking_flags
    assert review.score < 1.0


def test_quote_safety_flags_inflammatory_weak_relevance_and_platform_length():
    review = QuoteSafetyReviewer(platform_limit=40).review(
        _opportunity(
            content="Cache sizing changed after a release note.",
            draft_text="Quote-post angle: this garbage take is wrong and should be revisited with calmer evidence.",
            topics=["ai-agents"],
            topical_relevance=0.0,
        )
    )

    assert "inflammatory_language" in review.blocking_flags
    assert "weak_relevance" in review.blocking_flags
    assert "platform_length" in review.blocking_flags


def test_quote_safety_flags_restricted_license_reference(db):
    knowledge_id = _insert_knowledge(db, license_value="restricted")

    review = QuoteSafetyReviewer(db).review(_opportunity(knowledge_id=knowledge_id))

    assert "restricted_license" in review.blocking_flags
    assert review.checks["restricted_license"] is False
    assert review.to_dict()["blocked"] is True
