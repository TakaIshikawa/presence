"""Tests for deterministic visual alt-text validation."""

from synthesis.alt_text_guard import validate_alt_text


def test_non_visual_content_does_not_require_alt_text():
    result = validate_alt_text(None, content_type="x_post")

    assert result.passed is True
    assert result.required is False
    assert result.status == "not_required"


def test_visual_content_requires_alt_text():
    result = validate_alt_text(
        "",
        image_path="/tmp/presence-images/launch.png",
        content_type="x_visual",
    )

    assert result.passed is False
    assert [issue.code for issue in result.issues] == ["missing_alt_text"]


def test_rejects_short_generic_alt_text():
    result = validate_alt_text(
        "A screenshot",
        image_path="/tmp/presence-images/dashboard.png",
    )

    assert result.passed is False
    assert "alt_text_too_short" in {issue.code for issue in result.issues}


def test_rejects_excessive_length_and_file_name_leakage():
    result = validate_alt_text(
        "launch-card-123.png " + ("detail " * 220),
        image_path="/tmp/presence-images/launch-card-123.png",
    )

    issue_codes = {issue.code for issue in result.issues}
    assert "alt_text_too_long" in issue_codes
    assert "file_name_leakage" in issue_codes


def test_rejects_prompt_keyword_mismatch_when_prompt_available():
    result = validate_alt_text(
        "A concise diagram of a database migration plan.",
        image_prompt="Launch metrics dashboard with conversion trend annotations",
        image_path="/tmp/presence-images/launch.png",
    )

    assert "image_prompt_mismatch" in {issue.code for issue in result.issues}


def test_accepts_descriptive_alt_text_with_prompt_overlap():
    result = validate_alt_text(
        "Launch metrics dashboard with conversion trend annotations and status labels.",
        image_prompt="Launch metrics dashboard with conversion trend annotations",
        image_path="/tmp/presence-images/launch.png",
    )

    assert result.passed is True
    assert result.issues == ()
