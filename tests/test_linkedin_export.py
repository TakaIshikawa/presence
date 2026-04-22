"""Tests for manual LinkedIn publishing artifacts."""

from output.linkedin_export import (
    LinkedInExportOptions,
    build_linkedin_export,
    build_linkedin_export_from_db,
    count_graphemes,
    format_linkedin_markdown,
)


def _insert_content(
    db,
    content: str,
    content_type: str = "x_post",
) -> int:
    content_id = db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )
    db.set_curation_quality(content_id, "good")
    return content_id


def test_single_post_expands_terse_x_language_for_linkedin():
    export = build_linkedin_export(
        {
            "id": 7,
            "content_type": "x_post",
            "content": "Devs ship faster w/ tighter feedback bc prod tells the truth.",
        }
    )

    assert "Developers ship faster with tighter feedback because production tells the truth." in export.text
    assert "w/" not in export.text
    assert " bc " not in export.text
    assert export.graphemes <= export.max_length


def test_thread_is_condensed_into_one_linkedin_post():
    export = build_linkedin_export(
        {
            "id": 8,
            "content_type": "x_thread",
            "content": (
                "TWEET 1:\nTweeting this on X w/ a small lesson.\n"
                "TWEET 2:\nDevs miss it bc the failure is quiet."
            ),
        }
    )

    assert "TWEET" not in export.text
    assert "post" in export.text.lower()
    assert " X " not in f" {export.text} "
    assert "with a small lesson" in export.text
    assert "developers miss it because" in export.text.lower()
    assert export.text.count("\n\n") >= 1


def test_source_attribution_preserves_content_and_knowledge_links(db):
    content_id = _insert_content(
        db,
        "The rollout notes are here: https://example.com/rollout.",
    )
    knowledge_id = db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, approved)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            "article-1",
            "https://source.example.com/post",
            "Jane Builder",
            "Useful rollout background",
            1,
        ),
    ).lastrowid
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    export = build_linkedin_export_from_db(db, content_id=content_id)

    assert "Sources:" in export.text
    assert "- Original link: https://example.com/rollout" in export.text
    assert "- Jane Builder: https://source.example.com/post" in export.text
    assert len(export.sources) == 2


def test_length_trimming_preserves_source_link():
    link = "https://example.com/details"
    export = build_linkedin_export(
        {
            "id": 9,
            "content_type": "x_post",
            "content": " ".join(["This sentence has useful context."] * 80),
        },
        sources=[{"source_url": link, "author": "Source"}],
        options=LinkedInExportOptions(max_length=220),
    )

    assert export.was_trimmed is True
    assert link in export.text
    assert export.text.endswith(link)
    assert count_graphemes(export.text) <= 220


def test_markdown_artifact_contains_post_and_metadata():
    export = build_linkedin_export(
        {
            "id": 10,
            "content_type": "x_post",
            "content": "Short post for manual publishing.",
        },
        queue={"queue_id": 3},
    )

    markdown = format_linkedin_markdown(export)

    assert markdown.startswith("# LinkedIn Draft")
    assert "- Content ID: 10" in markdown
    assert "- Queue ID: 3" in markdown
    assert "## Post\n\nShort post for manual publishing." in markdown
