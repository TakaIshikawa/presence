"""Tests for generated visual post publishing metadata."""

from synthesis.image_generator import build_alt_text


def test_visual_post_alt_text_is_human_readable():
    alt_text = build_alt_text(
        style="comparison",
        title="Auth refactor",
        before="five helpers and scattered checks",
        after="one validation path",
    )

    assert alt_text == (
        'Comparison graphic titled "Auth refactor". '
        "Before: five helpers and scattered checks. After: one validation path."
    )
    assert "COMPARISON |" not in alt_text
    assert len(alt_text) < 300
