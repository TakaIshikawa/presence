"""Tests for quote-post opportunity recommendations."""

from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from engagement.quote_opportunities import QuoteOpportunityRecommender
from engagement.quote_safety import QuoteSafetyReviewer
from quote_opportunities import format_json_output, format_table_output, main, write_artifact


NOW = datetime(2026, 4, 23, 12, 0, tzinfo=timezone.utc)


def _add_knowledge(
    db,
    *,
    source_id: str,
    source_url: str,
    author: str,
    content: str,
    published_at: str = "2026-04-23T09:00:00+00:00",
    source_type: str = "curated_x",
    approved: int = 1,
    license_value: str = "attribution_required",
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight, approved, published_at, license)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            source_type,
            source_id,
            source_url,
            author,
            content,
            content,
            approved,
            published_at,
            license_value,
        ),
    )
    db.conn.commit()
    return cursor.lastrowid


def _add_published_post(
    db,
    *,
    content: str,
    topic: str,
    engagement_score: float = 10.0,
    published_at: str = "2026-04-20T09:00:00+00:00",
) -> int:
    content_id = db.insert_generated_content(
        "x_post",
        [],
        [],
        content,
        8.0,
        "ok",
    )
    db.conn.execute(
        "UPDATE generated_content SET published = 1, published_at = ?, auto_quality = 'resonated' WHERE id = ?",
        (published_at, content_id),
    )
    db.conn.commit()
    db.insert_content_topics(content_id, [(topic, "", 1.0)])
    db.insert_engagement(content_id, f"tweet-{content_id}", 10, 2, 1, 1, engagement_score)
    return content_id


def seed_quote_data(db) -> dict[str, int]:
    campaign_id = db.create_campaign(
        name="Agent Systems",
        goal="Explain AI agent testing and tool workflows",
        start_date="2026-04-01",
        end_date="2026-04-30",
        status="active",
    )
    other_campaign_id = db.create_campaign(
        name="Performance Month",
        goal="Cover cache and latency work",
        start_date="2026-04-01",
        end_date="2026-04-30",
        status="active",
    )
    db.insert_planned_topic(
        topic="ai-agents",
        angle="quote outside examples of eval loops",
        target_date="2026-04-24",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="testing",
        angle="cover test fixtures",
        target_date="2026-04-24",
        campaign_id=campaign_id,
    )
    db.insert_planned_topic(
        topic="performance",
        angle="cache latency",
        target_date="2026-04-24",
        campaign_id=other_campaign_id,
    )

    _add_published_post(
        db,
        content="AI agent evals need regression tests and tool-call traces.",
        topic="ai-agents",
        engagement_score=30.0,
    )
    source_knowledge_id = _add_knowledge(
        db,
        source_id="source-prior",
        source_url="https://x.com/high/status/source-prior",
        author="high",
        content="Agent evaluation loops improve when tool calls are checked with pytest fixtures.",
        published_at="2026-04-15T09:00:00+00:00",
    )
    db.insert_content_knowledge_links(_add_published_post(db, content="Source-backed agent post", topic="ai-agents"), [(source_knowledge_id, 0.9)])

    fresh_id = _add_knowledge(
        db,
        source_id="tweet-agent-fresh",
        source_url="https://x.com/high/status/tweet-agent-fresh",
        author="high",
        content="AI agents need testing harnesses that inspect every tool call before users see the result.",
    )
    perf_id = _add_knowledge(
        db,
        source_id="tweet-perf",
        source_url="https://x.com/perf/status/tweet-perf",
        author="perf",
        content="Cache latency dominates the slow path in developer tooling workflows.",
    )
    stale_id = _add_knowledge(
        db,
        source_id="tweet-stale",
        source_url="https://x.com/high/status/tweet-stale",
        author="high",
        content="AI agents need testing harnesses, fixtures, traces, and tool call checks.",
        published_at="2026-03-01T09:00:00+00:00",
    )
    used_id = _add_knowledge(
        db,
        source_id="tweet-used",
        source_url="https://x.com/used/status/tweet-used",
        author="used",
        content="Testing AI agents with fixtures makes regressions visible.",
    )
    db.insert_content_knowledge_links(
        _add_published_post(db, content="Already used source", topic="testing"),
        [(used_id, 0.8)],
    )

    return {
        "campaign_id": campaign_id,
        "other_campaign_id": other_campaign_id,
        "fresh_id": fresh_id,
        "perf_id": perf_id,
        "stale_id": stale_id,
        "used_id": used_id,
    }


def test_scores_campaign_relevant_fresh_quality_novel_sources(db):
    ids = seed_quote_data(db)

    opportunities = QuoteOpportunityRecommender(db).recommend(
        days=7,
        limit=5,
        campaign_id=ids["campaign_id"],
        min_score=0.0,
        now=NOW,
    )

    assert opportunities[0].knowledge_id == ids["fresh_id"]
    assert opportunities[0].topical_relevance > 0
    assert opportunities[0].freshness > 0.85
    assert opportunities[0].source_quality > 0.5
    assert opportunities[0].novelty > 0.5
    assert opportunities[0].prior_performance == 1.0
    assert "ai-agents" in opportunities[0].topics


def test_campaign_filter_changes_recommended_topic(db):
    ids = seed_quote_data(db)

    opportunities = QuoteOpportunityRecommender(db).recommend(
        days=7,
        limit=5,
        campaign_id=ids["other_campaign_id"],
        min_score=0.0,
        now=NOW,
    )

    assert opportunities[0].knowledge_id == ids["perf_id"]
    assert opportunities[0].campaign_id == ids["other_campaign_id"]
    assert "performance" in opportunities[0].topics


def test_filters_narrow_candidates_without_reordering(db):
    ids = seed_quote_data(db)
    article_id = _add_knowledge(
        db,
        source_id="article-testing",
        source_url="https://example.com/testing-article",
        author="researcher",
        content="Testing article with pytest fixtures and regression checks.",
        source_type="curated_article",
    )

    opportunities = QuoteOpportunityRecommender(db).recommend(
        days=60,
        limit=10,
        campaign_id=ids["campaign_id"],
        authors=["high"],
        topics=["ai-agents"],
        source_types=["curated_x"],
        min_score=0.0,
        now=NOW,
    )

    assert [item.knowledge_id for item in opportunities] == [ids["fresh_id"], ids["stale_id"]]
    assert all(item.author == "high" for item in opportunities)
    assert all("ai-agents" in item.topics for item in opportunities)
    assert all(item.source_type == "curated_x" for item in opportunities)

    article_opportunities = QuoteOpportunityRecommender(db).recommend(
        days=60,
        limit=10,
        campaign_id=ids["campaign_id"],
        authors=["researcher"],
        topics=["testing"],
        source_types=["curated_article"],
        min_score=0.0,
        now=NOW,
    )

    assert [item.knowledge_id for item in article_opportunities] == [article_id]
    assert article_opportunities[0].source_type == "curated_article"


def test_deduplicates_already_used_source_urls(db):
    ids = seed_quote_data(db)

    opportunities = QuoteOpportunityRecommender(db).recommend(
        days=7,
        limit=10,
        campaign_id=ids["campaign_id"],
        min_score=0.0,
        now=NOW,
    )

    assert ids["used_id"] not in {item.knowledge_id for item in opportunities}


def test_enqueue_writes_pending_quote_actions_and_dedupes(db):
    ids = seed_quote_data(db)
    recommender = QuoteOpportunityRecommender(db)
    opportunities = recommender.recommend(
        days=7,
        limit=2,
        campaign_id=ids["campaign_id"],
        min_score=0.0,
        now=NOW,
    )

    action_ids = recommender.enqueue(opportunities, limit=1)
    assert len(action_ids) == 1
    assert recommender.enqueue(opportunities, limit=1) == []

    pending = db.get_pending_proactive_actions()
    assert pending[0]["action_type"] == "quote_tweet"
    assert pending[0]["discovery_source"] == "quote_opportunities"
    metadata = json.loads(pending[0]["platform_metadata"])
    assert metadata["kind"] == "quote_opportunity"
    assert metadata["knowledge_id"] == opportunities[0].knowledge_id


def test_cli_formatters_emit_json_and_table(db):
    ids = seed_quote_data(db)
    opportunities = QuoteOpportunityRecommender(db).recommend(
        days=7,
        limit=1,
        campaign_id=ids["campaign_id"],
        min_score=0.0,
        now=NOW,
    )

    payload = json.loads(format_json_output(opportunities, [123]))
    assert payload["enqueued_ids"] == [123]
    assert payload["opportunities"][0]["knowledge_id"] == opportunities[0].knowledge_id
    assert "safety" not in payload["opportunities"][0]

    table = format_table_output(opportunities, [123])
    assert "Score" in table
    assert "Enqueued proactive quote actions: 123" in table
    assert "safety:" not in table


def test_cli_formatters_include_safety_only_when_requested(db):
    ids = seed_quote_data(db)
    opportunities = QuoteOpportunityRecommender(db).recommend(
        days=7,
        limit=1,
        campaign_id=ids["campaign_id"],
        min_score=0.0,
        now=NOW,
    )
    reviews = QuoteSafetyReviewer(db).review_many(opportunities)

    payload = json.loads(format_json_output(opportunities, safety_reviews=reviews))
    assert payload["opportunities"][0]["safety"]["score"] == 1.0
    assert payload["opportunities"][0]["safety"]["blocking_flags"] == []

    table = format_table_output(opportunities, safety_reviews=reviews)
    assert "safety: 1.00 flags=none" in table


def test_write_artifact_writes_markdown_review_payload(db, tmp_path):
    ids = seed_quote_data(db)
    opportunities = QuoteOpportunityRecommender(db).recommend(
        days=7,
        limit=1,
        campaign_id=ids["campaign_id"],
        min_score=0.0,
        now=NOW,
    )

    artifact_path = write_artifact(
        tmp_path / "quote_opportunities.md",
        opportunities,
        format="markdown",
        enqueued_ids=[123],
        filters={
            "days": 7,
            "limit": 1,
            "campaign_id": ids["campaign_id"],
            "authors": ["high"],
            "topics": ["ai-agents"],
            "platforms": ["x"],
            "min_score": 0.0,
        },
    )

    artifact = artifact_path.read_text(encoding="utf-8")
    assert artifact.startswith("# Quote Opportunity Review")
    assert "- Enqueued IDs: 123" in artifact
    assert '"enqueued_ids": [' in artifact
    assert opportunities[0].source_url in artifact


def test_cli_writes_markdown_artifact_with_filters(db, tmp_path, capsys):
    ids = seed_quote_data(db)
    artifact_path = tmp_path / "quote_opportunities.md"

    import quote_opportunities

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    with patch("quote_opportunities.script_context", return_value=Context()):
        exit_code = quote_opportunities.main(
            [
                "--days",
                "7",
                "--limit",
                "2",
                "--campaign-id",
                str(ids["campaign_id"]),
                "--author",
                "high",
                "--topic",
                "ai-agents",
                "--platform",
                "x",
                "--enqueue",
                "--out",
                str(artifact_path),
                "--format",
                "markdown",
            ]
        )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert f"Quote opportunity artifact: {artifact_path}" in captured.err
    artifact = artifact_path.read_text(encoding="utf-8")
    assert artifact.startswith("# Quote Opportunity Review")
    assert "Enqueued IDs:" in artifact
    assert "high" in artifact
    assert "ai-agents" in artifact


def test_main_json_output(db, capsys):
    ids = seed_quote_data(db)

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("quote_opportunities.script_context", fake_script_context):
        main(["--days", "7", "--campaign-id", str(ids["campaign_id"]), "--limit", "1", "--json"])

    payload = json.loads(capsys.readouterr().out)
    assert len(payload["opportunities"]) == 1
    assert payload["opportunities"][0]["knowledge_id"] == ids["fresh_id"]


def test_main_safety_report_filters_without_mutating_database(db, capsys):
    ids = seed_quote_data(db)
    unsafe_id = _add_knowledge(
        db,
        source_id="tweet-unsafe",
        source_url="https://x.com/high/status/tweet-unsafe",
        author="high",
        content="AI agents need testing harnesses, but this garbage take is a scam.",
        license_value="restricted",
    )
    before_actions = db.conn.execute("SELECT COUNT(*) FROM proactive_actions").fetchone()[0]

    @contextmanager
    def fake_script_context():
        yield None, db

    with patch("quote_opportunities.script_context", fake_script_context):
        exit_code = main(
            [
                "--days",
                "7",
                "--campaign-id",
                str(ids["campaign_id"]),
                "--limit",
                "10",
                "--json",
                "--safety-report",
                "--min-safety-score",
                "0.9",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert unsafe_id not in {item["knowledge_id"] for item in payload["opportunities"]}
    assert all("safety" in item for item in payload["opportunities"])
    after_actions = db.conn.execute("SELECT COUNT(*) FROM proactive_actions").fetchone()[0]
    assert after_actions == before_actions
