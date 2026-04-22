"""Tests for deterministic persona drift guard."""

from synthesis.persona_guard import PersonaGuard, PersonaGuardConfig


RECENT_POSTS = [
    {
        "content": (
            "I traced the queue worker timeout in worker.py today. "
            "The useful part was making the retry path explicit."
        )
    },
    {
        "content": (
            "I kept the pipeline guard small: a plain test, a fixture, "
            "and the exact error from the log."
        )
    },
    {
        "content": (
            "Debugging the CLI is easier when the config failure names "
            "the file and the branch that produced it."
        )
    },
]


def test_persona_guard_passes_grounded_author_like_content():
    guard = PersonaGuard()

    result = guard.check(
        "I traced the queue worker timeout in worker.py and kept the retry path explicit.",
        RECENT_POSTS,
    )

    assert result.passed is True
    assert result.status == "passed"
    assert result.metrics["phrase_overlap"] >= 0.08
    assert result.metrics["grounding_score"] == 1.0


def test_persona_guard_fails_generic_salesy_content():
    guard = PersonaGuard()

    result = guard.check(
        "Unlock scalable innovation and revolutionary momentum with best-in-class systems.",
        RECENT_POSTS,
    )

    assert result.passed is False
    assert result.status == "failed"
    assert result.metrics["banned_marker_count"] >= 1
    assert any("banned tone markers" in reason for reason in result.reasons)
    assert any("grounding score" in reason for reason in result.reasons)


def test_persona_guard_disabled_skips_check():
    guard = PersonaGuard(PersonaGuardConfig(enabled=False))

    result = guard.check(
        "Unlock revolutionary momentum with best-in-class systems.",
        RECENT_POSTS,
    )

    assert result.passed is True
    assert result.checked is False
    assert result.status == "disabled"
