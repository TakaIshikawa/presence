"""Tests for restricted knowledge publication guard."""

from types import SimpleNamespace

from output.license_guard import (
    check_publication_license_guard,
    restricted_prompt_behavior_from_config,
)


def _insert_content(db):
    return db.insert_generated_content(
        content_type="x_post",
        source_commits=[],
        source_messages=[],
        content="Source-backed post",
        eval_score=8.0,
        eval_feedback="Good",
    )


def _insert_knowledge(db, license_value="restricted", source_url="https://source.example/post"):
    return db.conn.execute(
        """INSERT INTO knowledge
           (source_type, source_id, source_url, author, content, license, approved)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            "curated_article",
            f"{license_value}-source",
            source_url,
            "Source Author",
            "Useful source context",
            license_value,
            1,
        ),
    ).lastrowid


def test_license_guard_passes_without_restricted_links(db):
    content_id = _insert_content(db)
    knowledge_id = _insert_knowledge(db, license_value="open")
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    result = check_publication_license_guard(db, content_id)

    assert result.status == "passed"
    assert result.action == "pass"
    assert result.passed is True
    assert result.blocked is False
    assert result.restricted_sources == []


def test_license_guard_blocks_restricted_links_in_strict_mode(db):
    content_id = _insert_content(db)
    knowledge_id = _insert_knowledge(db)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    result = check_publication_license_guard(db, content_id)

    assert result.status == "blocked"
    assert result.action == "block"
    assert result.passed is False
    assert result.blocked is True
    assert result.as_dict()["restricted_sources"] == [
        {
            "knowledge_id": knowledge_id,
            "source_url": "https://source.example/post",
            "license": "restricted",
        }
    ]


def test_license_guard_warns_with_override_or_permissive_config(db):
    content_id = _insert_content(db)
    knowledge_id = _insert_knowledge(db)
    db.insert_content_knowledge_links(content_id, [(knowledge_id, 0.9)])

    override_result = check_publication_license_guard(
        db,
        content_id,
        allow_restricted=True,
    )
    permissive_result = check_publication_license_guard(
        db,
        content_id,
        restricted_prompt_behavior="permissive",
    )

    assert override_result.status == "warning"
    assert override_result.passed is True
    assert override_result.override is True
    assert permissive_result.status == "warning"
    assert permissive_result.passed is True


def test_restricted_prompt_behavior_from_config_normalizes_values():
    permissive = SimpleNamespace(
        curated_sources=SimpleNamespace(restricted_prompt_behavior="permissive")
    )
    invalid = SimpleNamespace(
        curated_sources=SimpleNamespace(restricted_prompt_behavior="unexpected")
    )

    assert restricted_prompt_behavior_from_config(permissive) == "permissive"
    assert restricted_prompt_behavior_from_config(invalid) == "strict"
    assert restricted_prompt_behavior_from_config(None) == "strict"
