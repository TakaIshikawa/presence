"""Tests for generated blog draft frontmatter validation."""

from output.blog_frontmatter_validator import validate_blog_draft_frontmatter
from scripts.validate_blog_drafts import validate_draft_directory


VALID_DRAFT = """---
title: "Draft Title"
date: "2026-04-25"
description: "A concise draft summary."
source_content_ids: [123, 456]
---

Draft body.
"""


def test_valid_frontmatter_passes():
    result = validate_blog_draft_frontmatter(VALID_DRAFT, path="drafts/post.md")

    assert result.ok is True
    assert result.frontmatter["title"] == "Draft Title"
    assert result.frontmatter["source_content_ids"] == [123, 456]
    assert result.errors == []


def test_missing_frontmatter_returns_structured_error():
    result = validate_blog_draft_frontmatter("Draft body.")

    assert result.ok is False
    assert result.errors[0].code == "missing_frontmatter"
    assert result.errors[0].level == "error"


def test_missing_required_fields_are_reported_by_field():
    result = validate_blog_draft_frontmatter("---\ntitle: \"Only Title\"\n---\nBody.")

    fields = {issue.field for issue in result.errors}
    assert result.ok is False
    assert {"date", "description", "source_content_ids"}.issubset(fields)


def test_invalid_iso_date_is_rejected():
    draft = VALID_DRAFT.replace('"2026-04-25"', '"April 25, 2026"')

    result = validate_blog_draft_frontmatter(draft)

    assert result.ok is False
    assert any(issue.code == "invalid_date" for issue in result.errors)


def test_source_content_ids_must_be_positive_integer_list():
    draft = VALID_DRAFT.replace("[123, 456]", "[123, \"bad\", 0]")

    result = validate_blog_draft_frontmatter(draft)

    assert result.ok is False
    assert any(
        issue.code == "invalid_source_content_ids"
        and issue.field == "source_content_ids"
        for issue in result.errors
    )


def test_empty_body_emits_warning_only():
    draft = """---
title: "Draft Title"
date: "2026-04-25"
description: "A concise draft summary."
source_content_ids: [123]
---
"""

    result = validate_blog_draft_frontmatter(draft)

    assert result.ok is True
    assert result.warnings[0].code == "empty_body"


def test_validate_draft_directory_returns_json_ready_report(tmp_path):
    draft_dir = tmp_path / "drafts"
    draft_dir.mkdir()
    (draft_dir / "valid.md").write_text(VALID_DRAFT)
    (draft_dir / "invalid.md").write_text("Draft body.")

    report = validate_draft_directory(draft_dir)

    assert report["ok"] is False
    assert report["draft_count"] == 2
    assert report["error_count"] == 1
    assert report["results"][0]["path"].endswith("invalid.md")
    assert report["results"][0]["errors"][0]["code"] == "missing_frontmatter"
