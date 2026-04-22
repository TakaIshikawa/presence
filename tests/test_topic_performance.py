import pytest

from evaluation.topic_performance import TopicPerformanceAnalyzer


def _published_topic_post(
    db,
    content: str,
    topic: str,
    engagement_score: float,
    auto_quality: str | None,
    content_type: str = "x_post",
):
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=["sha"],
        source_messages=["uuid"],
        content=content,
        eval_score=7.0,
        eval_feedback="ok",
    )
    db.conn.execute(
        """UPDATE generated_content
           SET published = 1,
               published_at = datetime('now', '-1 day'),
               auto_quality = ?
           WHERE id = ?""",
        (auto_quality, content_id),
    )
    db.conn.commit()
    db.insert_content_topics(content_id, [(topic, "", 0.9)])
    db.insert_engagement(
        content_id,
        f"tw-{content_id}",
        1,
        0,
        0,
        0,
        engagement_score,
    )
    return content_id


def test_get_topic_performance_uses_latest_engagement_by_topic(db):
    _published_topic_post(db, "testing post", "testing", 10.0, "resonated")
    low_id = _published_topic_post(db, "testing miss", "testing", 4.0, None)
    db.insert_engagement(low_id, "tw-latest", 0, 0, 0, 0, 0.0)
    _published_topic_post(db, "agents post", "ai-agents", 0.0, "low_resonance")

    analyzer = TopicPerformanceAnalyzer(db)
    results = analyzer.get_topic_performance(topics=["testing"], days=30)

    assert len(results) == 1
    assert results[0].topic == "testing"
    assert results[0].sample_count == 2
    assert results[0].avg_engagement == pytest.approx(5.0)
    assert results[0].resonated_count == 1
    assert results[0].low_resonance_count == 0


def test_build_evaluation_context_includes_matching_and_historical_notes(db):
    _published_topic_post(db, "testing post", "testing", 10.0, "resonated")
    _published_topic_post(db, "testing follow up", "testing", 8.0, "resonated")
    _published_topic_post(db, "agent post", "ai-agents", 0.0, "low_resonance")
    _published_topic_post(db, "agent follow up", "ai-agents", 0.0, "low_resonance")

    analyzer = TopicPerformanceAnalyzer(db)
    context = analyzer.build_evaluation_context(
        source_texts=["Added pytest coverage for retry behavior"],
        candidate_texts=["Testing caught the retry edge case before publish"],
        content_type="x_post",
    )

    assert "ENGAGEMENT HISTORY BY TOPIC" in context
    assert "Current/source topics detected: testing" in context
    assert "testing: n=2" in context
    assert "Historically resonant topics" in context
    assert "Historically low-resonance topics" in context
    assert "ai-agents: n=2" in context
    assert "not a hard rule" in context


def test_infer_topics_uses_optional_extractor_before_keyword_fallback(db):
    class StubExtractor:
        def extract_topics(self, content):
            return [("architecture", "boundaries", 0.9)]

    analyzer = TopicPerformanceAnalyzer(db, topic_extractor=StubExtractor())

    assert analyzer.infer_topics(["pytest debugging"]) == ["architecture"]


def test_invalid_platform_is_rejected(db):
    analyzer = TopicPerformanceAnalyzer(db)

    with pytest.raises(ValueError):
        analyzer.get_topic_performance(platform="mastodon")
