"""Tests for attribution-required knowledge publication guard."""

from output.attribution_guard import check_publication_attribution_guard


def _insert_content(db, content="Source-backed post"):
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=8.0,
        eval_feedback="Good",
    )


def _insert_knowledge(
    db,
    license_value="attribution_required",
    source_url="https://source.example/post",
    author="Source Author",
):
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            f"{license_value}-source",
            source_url,
            author,
            "Useful source context",
            license_value,
            1,
        ),
    ).lastrowid


def test_attribution_guard_passes_without_attribution_required_links(db):
    content_id = _insert_content(db)
    knowledge_id = _insert_knowledge(db, license_value="open")
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    result = check_publication_attribution_guard(db, content_id, "No citation needed")

    assert result.status == "passed"
    assert result.action == "pass"
    assert result.passed is True
    assert result.blocked is False
    assert result.required_sources == []
    assert result.missing_sources == []


def test_attribution_guard_blocks_missing_visible_attribution(db):
    content_id = _insert_content(db)
    knowledge_id = _insert_knowledge(db)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    result = check_publication_attribution_guard(db, content_id, "Inspired post")

    assert result.status == "blocked"
    assert result.action == "block"
    assert result.passed is False
    assert result.blocked is True
    assert result.as_dict()["missing_sources"] == [
        {
            "knowledge_id": knowledge_id,
            "source_url": "https://source.example/post",
            "author": "Source Author",
            "license": "attribution_required",
        }
    ]


def test_attribution_guard_accepts_visible_source_url(db):
    content_id = _insert_content(db)
    knowledge_id = _insert_knowledge(db)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    result = check_publication_attribution_guard(
        db,
        content_id,
        "Inspired post\nSource: https://source.example/post",
    )

    assert result.status == "passed"
    assert result.missing_sources == []


def test_attribution_guard_accepts_author_attribution_note(db):
    content_id = _insert_content(db)
    knowledge_id = _insert_knowledge(db)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    result = check_publication_attribution_guard(
        db,
        content_id,
        "Inspired post. Via Source Author.",
    )

    assert result.status == "passed"
    assert result.missing_sources == []
