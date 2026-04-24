"""Tests for exporting blog seed briefs from resonated social content."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import patch

from synthesis.blog_seed_export import (
    BlogSeedExporter,
    export_to_dict,
    format_export_markdown,
    write_export,
)

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))


def _social_content(
    db,
    *,
    content_type: str = "x_post",
    content: str = "A small quality gate made the release process easier to trust.",
    url: str = "https://x.com/taka/status/100",
    tweet_id: str = "100",
    score: float | None = 12.0,
    auto_quality: str | None = None,
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="usable",
    )
    db.mark_published(content_id, url, tweet_id=tweet_id)
    if auto_quality:
        db.conn.execute(
            "UPDATE generated_content SET auto_quality = ? WHERE id = ?",
            (auto_quality, content_id),
        )
        db.conn.commit()
    if score is not None:
        db.insert_engagement(
            content_id=content_id,
            tweet_id=tweet_id,
            like_count=8,
            retweet_count=2,
            reply_count=1,
            quote_count=0,
            engagement_score=score,
        )
    return content_id


def _knowledge(db, *, url: str = "https://example.com/quality-gates") -> int:
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, insight)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "quality-gates",
            url,
            "Ada",
            "Quality gates improve release confidence.",
            "Reviewable gates make reliability work easier to explain.",
        ),
    ).lastrowid


def test_selects_resonated_or_engagement_qualified_social_content(db):
    low_id = _social_content(db, content="Low response.", score=2.0, tweet_id="low")
    high_id = _social_content(db, content="High response.", score=18.0, tweet_id="high")
    resonated_id = _social_content(
        db,
        content="Resonated without an engagement snapshot.",
        score=None,
        auto_quality="resonated",
        tweet_id="resonated",
    )
    blog_id = _social_content(
        db,
        content_type="blog_post",
        content="Not social.",
        score=99.0,
        tweet_id="blog",
    )

    sources = BlogSeedExporter(db).select_sources(min_engagement=10.0, limit=10)
    ids = {source.content_id for source in sources}

    assert high_id in ids
    assert resonated_id in ids
    assert low_id not in ids
    assert blog_id not in ids
    assert {source.content_type for source in sources} == {"x_post"}


def test_export_includes_source_urls_topics_knowledge_angle_and_outline(db):
    content_id = _social_content(
        db,
        content_type="x_thread",
        content=(
            "TWEET 1: A thread about turning operational reviews into small "
            "reliable habits.\nTWEET 2: The key is making context reviewable."
        ),
        score=21.0,
    )
    db.insert_content_topics(
        content_id,
        [("operations", "review loops", 0.94), ("testing", "quality gates", 0.72)],
    )
    knowledge_id = _knowledge(db)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.88)])

    export = BlogSeedExporter(db).build_export(min_engagement=10.0)
    payload = export_to_dict(export)
    seed = payload["seeds"][0]

    assert payload["artifact_type"] == "blog_seed_export"
    assert seed["source_content_ids"] == [content_id]
    assert "https://x.com/taka/status/100" in seed["source_urls"]
    assert "https://example.com/quality-gates" in seed["source_urls"]
    assert seed["topics"][0]["topic"] == "operations"
    assert seed["linked_knowledge"][0]["knowledge_id"] == knowledge_id
    assert seed["suggested_title"].startswith("Operations:")
    assert "confirmed resonance" not in seed["target_angle"]
    assert "21 engagement score" in seed["target_angle"]
    assert len(seed["outline"]) == 5
    assert "TWEET 1" not in seed["source_excerpt"]


def test_json_and_markdown_exports_contain_equivalent_seed_data(db, tmp_path):
    content_id = _social_content(
        db,
        content="Reviewable briefs beat jumping straight from a post to a draft.",
        score=16.0,
        auto_quality="resonated",
    )
    db.insert_content_topics(content_id, [("writing", "blog briefs", 0.91)])
    knowledge_id = _knowledge(db, url="https://example.com/blog-briefs")
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.77)])

    export = BlogSeedExporter(db).build_export(min_engagement=10.0)
    json_path = tmp_path / "seeds.json"
    md_path = tmp_path / "seeds.md"
    write_export(export, json_path, artifact_format="json")
    write_export(export, md_path, artifact_format="markdown")

    payload = json.loads(json_path.read_text())
    markdown = md_path.read_text()
    seed = payload["seeds"][0]

    assert seed["source_content_ids"] == [content_id]
    assert str(content_id) in markdown
    assert seed["suggested_title"] in markdown
    assert seed["target_angle"] in markdown
    assert seed["source_urls"][0] in markdown
    assert seed["topics"][0]["topic"] in markdown
    assert str(seed["linked_knowledge"][0]["knowledge_id"]) in markdown
    assert seed["linked_knowledge"][0]["source_url"] in markdown
    for item in seed["outline"]:
        assert item in markdown
    assert format_export_markdown(export).startswith("# Blog Seed Export")


def test_export_uses_latest_engagement_and_deduplicates_sources(db):
    content_id = _social_content(
        db,
        content="One source should only appear once even with several snapshots.",
        score=None,
        tweet_id="multi",
    )
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (content_id, "multi", 20, 2, 1, 0, 30.0, "2026-04-20T10:00:00+00:00"),
    )
    db.conn.execute(
        """INSERT INTO post_engagement
           (content_id, tweet_id, like_count, retweet_count, reply_count,
            quote_count, engagement_score, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (content_id, "multi", 1, 0, 0, 0, 3.0, "2026-04-21T10:00:00+00:00"),
    )
    db.conn.commit()

    assert BlogSeedExporter(db).select_sources(min_engagement=10.0) == []

    db.conn.execute(
        "UPDATE generated_content SET auto_quality = 'resonated' WHERE id = ?",
        (content_id,),
    )
    db.conn.commit()
    export = BlogSeedExporter(db).build_export(min_engagement=10.0)

    assert len(export.seeds) == 1
    assert export.seeds[0].source_content_ids == [content_id]
    assert export.seeds[0].engagement["score"] == 3.0


def test_export_blog_seeds_cli_writes_artifact(db, tmp_path, capsys):
    content_id = _social_content(db, score=14.0)

    import export_blog_seeds

    class Context:
        def __enter__(self):
            return None, db

        def __exit__(self, exc_type, exc, tb):
            return False

    out_path = tmp_path / "blog-seeds.json"
    with patch("export_blog_seeds.script_context", return_value=Context()):
        exit_code = export_blog_seeds.main(
            [
                "--output-path",
                str(out_path),
                "--format",
                "json",
                "--min-engagement",
                "10",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    artifact = json.loads(out_path.read_text())

    assert exit_code == 0
    assert "Blog seed export:" in captured.err
    assert payload["artifact_path"] == str(out_path)
    assert payload["seed_count"] == 1
    assert artifact["seeds"][0]["source_content_ids"] == [content_id]
