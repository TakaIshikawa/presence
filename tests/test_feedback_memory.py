"""Tests for synthesis feedback memory."""

from synthesis.feedback_memory import FeedbackMemory, build_feedback_memory_context


def _content(db, content: str, content_type: str = "x_post") -> int:
    return db.insert_generated_content(
        content_type=content_type,
        source_commits=[],
        source_messages=[],
        content=content,
        eval_score=5.0,
        eval_feedback="",
    )


def test_empty_feedback_returns_empty_constraints(db):
    memory = FeedbackMemory(db=db)

    assert memory.build_prompt_constraints("x_post") == ""


def test_reject_notes_become_avoid_constraints_without_draft_leakage(db):
    content_id = _content(
        db,
        "Today's breakthrough: I added auth retries and fixed the API handler.",
    )
    db.add_content_feedback(
        content_id,
        "reject",
        "Too changelog-like and too grandiose.",
    )

    constraints = FeedbackMemory(db=db).build_prompt_constraints("x_post")

    assert "Too changelog-like and too grandiose." in constraints
    assert "Today's breakthrough" not in constraints
    assert "Do not quote or imitate rejected drafts" in constraints


def test_revise_replacement_becomes_preference_not_full_replacement(db):
    content_id = _content(db, "Generic thought leadership about agents.")
    db.add_content_feedback(
        content_id,
        "revise",
        "Make it more concrete.",
        "I found the bug only after writing the failure case first.",
    )

    constraints = FeedbackMemory(db=db).build_prompt_constraints("x_post")

    assert "Make it more concrete." in constraints
    assert "Prefer concrete first-person builder observations" in constraints
    assert "I found the bug" not in constraints


def test_lookback_days_excludes_old_feedback(db):
    content_id = _content(db, "Old rejected draft")
    feedback_id = db.add_content_feedback(content_id, "reject", "Old note")
    db.conn.execute(
        "UPDATE content_feedback SET created_at = datetime('now', '-90 days') WHERE id = ?",
        (feedback_id,),
    )
    db.conn.commit()

    constraints = FeedbackMemory(db=db, lookback_days=30).build_prompt_constraints("x_post")

    assert constraints == ""


def test_classifies_obvious_patterns_when_notes_are_empty(db):
    content_id = _content(
        db,
        "Today's breakthrough: implemented queue retries and fixed stale jobs.",
    )
    db.add_content_feedback(content_id, "reject")

    constraints = FeedbackMemory(db=db).build_prompt_constraints("x_post")

    assert "Avoid stale announcement hooks" in constraints
    assert "Avoid changelog-style summaries" in constraints
    assert "Today's breakthrough" not in constraints


def test_falls_back_to_cross_type_feedback_when_type_has_no_rows(db):
    content_id = _content(db, "Rejected post", content_type="x_post")
    db.add_content_feedback(content_id, "reject", "Avoid vague hooks.")

    constraints = FeedbackMemory(db=db).build_prompt_constraints("blog_post")

    assert "Avoid vague hooks." in constraints


def test_feedback_memory_context_returns_empty_without_feedback(db):
    context = build_feedback_memory_context(db, content_type="x_post")

    assert context == ""


def test_feedback_memory_context_groups_by_type_and_includes_replacements(db):
    first = _content(db, "Rejected post", content_type="x_post")
    second = _content(db, "Needs revision", content_type="x_post")
    third = _content(db, "Preferred post", content_type="x_post")
    db.add_content_feedback(first, "reject", "Too vague.")
    db.add_content_feedback(
        second,
        "revise",
        "Needs a concrete example.",
        "Show the deployment failure and the fix.",
    )
    db.add_content_feedback(third, "prefer", "Strong operator detail.")

    context = build_feedback_memory_context(db, content_type="x_post", char_budget=1000)

    assert "reject:" in context
    assert "revise:" in context
    assert "prefer:" in context
    assert "Too vague." in context
    assert "Needs a concrete example." in context
    assert "Replacement: Show the deployment failure and the fix." in context
    assert "Strong operator detail." in context


def test_feedback_memory_context_includes_tags_when_rows_provide_them():
    class StubDB:
        def get_recent_content_feedback(self, **kwargs):
            return [
                {
                    "feedback_type": "reject",
                    "notes": "Too generic.",
                    "replacement_text": "",
                    "content_type": "x_post",
                    "tags": ["voice", "specificity"],
                },
                {
                    "feedback_type": "prefer",
                    "notes": "Useful concrete detail.",
                    "replacement_text": "",
                    "content_type": "blog_post",
                    "tag": "evidence",
                },
            ]

    context = build_feedback_memory_context(StubDB(), char_budget=500)

    assert "reject [voice, specificity]:" in context
    assert "prefer [evidence]:" in context


def test_feedback_memory_context_respects_character_budget(db):
    content_id = _content(db, "Rejected post", content_type="x_post")
    db.add_content_feedback(content_id, "reject", "Avoid long generic claims.")

    context = build_feedback_memory_context(db, content_type="x_post", char_budget=32)

    assert len(context) <= 32
    assert context == "Recent content feedback memory:"
