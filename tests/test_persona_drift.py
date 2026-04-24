"""Tests for preview-time persona drift detection."""

from synthesis.persona_drift import detect_persona_drift


RECENT_ACCEPTED_POSTS = [
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


def test_persona_drift_flags_hype_heavy_generic_content():
    result = detect_persona_drift(
        (
            "Thrilled to announce a revolutionary framework to unlock scalable "
            "innovation and transform the future of high-performing teams."
        ),
        RECENT_ACCEPTED_POSTS,
    )

    assert result.level == "high"
    assert result.score >= 0.6
    assert "hype-heavy tone" in result.reasons
    assert "generic abstract language" in result.reasons


def test_persona_drift_flags_unusual_self_promotion():
    result = detect_persona_drift(
        (
            "I built my framework to scale your strategy. Follow me and subscribe "
            "if you want the only proven path."
        ),
        RECENT_ACCEPTED_POSTS,
    )

    assert result.level in {"medium", "high"}
    assert "self-promotional language" in result.reasons
    assert "unusually absolute certainty" in result.reasons


def test_persona_drift_treats_specific_measured_technical_content_as_low():
    result = detect_persona_drift(
        (
            "I think the retry path in worker.py is clearer now: the test names "
            "the timeout and the log keeps the branch visible."
        ),
        RECENT_ACCEPTED_POSTS,
    )

    assert result.level == "low"
    assert result.score < 0.3
    assert "technical specifics match normal voice" in result.reasons
    assert "measured uncertainty lowers drift risk" in result.reasons
