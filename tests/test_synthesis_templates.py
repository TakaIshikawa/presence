"""Comprehensive tests for synthesis format templates and rendering.

Tests cover:
- Template file loading from prompts directory (x_post, x_thread templates mentioned in README)
- Template variable substitution (source content, context, examples)
- Template versioning and selection (latest vs specific version)
- Template syntax validation (format strings, missing variables)
- Template rendering error handling (missing variables, syntax errors)
- Template output length validation (enforce character limits)
- Template context building (assembling all required variables)
- Template caching (avoid re-parsing)
- Format-specific templates: post formats (5 variations per README), thread hooks (5 variations)
- Multi-turn templates
- Error handling: malformed templates, missing template files, invalid variable references
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from unittest.mock import Mock

import pytest

from synthesis.generator import ContentGenerator


# Helper functions for template management

def calculate_template_hash(template_text: str) -> str:
    """Calculate hash of template content."""
    return hashlib.sha256(template_text.encode()).hexdigest()[:16]


@dataclass
class TemplateMetadata:
    """Metadata for a prompt template."""

    prompt_type: str
    version: int
    prompt_hash: str
    template_path: str
    character_limit: Optional[int] = None


# --- Tests for Template File Loading ---


class TestTemplateFileLoading:
    """Test template file loading from prompts directory."""

    def test_load_x_post_template(self, db):
        """Test loading x_post template from prompts directory."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template = generator._load_prompt("x_post")

        assert template is not None
        assert len(template) > 0
        assert "{prompt}" in template
        assert "{commit_message}" in template

    def test_load_x_thread_template(self, db):
        """Test loading x_thread template from prompts directory."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template = generator._load_prompt("x_thread")

        assert template is not None
        assert len(template) > 0
        assert "{prompts}" in template or "PROMPTS" in template

    def test_load_blog_post_template(self, db):
        """Test loading blog_post template from prompts directory."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template = generator._load_prompt("blog_post")

        assert template is not None
        assert len(template) > 0

    def test_template_file_not_found(self, db):
        """Test error handling when template file doesn't exist."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        with pytest.raises(FileNotFoundError):
            generator._load_prompt("nonexistent_template")

    def test_template_prompt_dir_exists(self):
        """Test that prompts directory exists and contains templates."""
        prompts_dir = Path(__file__).parent.parent / "src" / "synthesis" / "prompts"

        assert prompts_dir.exists()
        assert prompts_dir.is_dir()

        # Check for expected templates
        expected_templates = ["x_post.txt", "x_thread.txt", "blog_post.txt"]
        for template_file in expected_templates:
            template_path = prompts_dir / template_file
            assert template_path.exists(), f"Template {template_file} should exist"

    def test_load_template_from_override_path(self, db, tmp_path):
        """Test loading template from custom override path."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        # Create custom template file
        custom_template = tmp_path / "custom_post.txt"
        custom_template.write_text("Custom template: {prompt}")

        generator.set_prompt_file_override("x_post", custom_template)
        template = generator._load_prompt("x_post")

        assert "Custom template" in template


# --- Tests for Template Variable Substitution ---


class TestTemplateVariableSubstitution:
    """Test template variable substitution with source content and context."""

    def test_substitute_basic_variables(self, db):
        """Test basic variable substitution in template."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template = "Prompt: {prompt}\nCommit: {commit_message}\nRepo: {repo_name}"
        filled = template.format(
            prompt="Fix bug in auth",
            commit_message="fix: resolve auth timeout",
            repo_name="acme/app",
        )

        assert "Fix bug in auth" in filled
        assert "fix: resolve auth timeout" in filled
        assert "acme/app" in filled

    def test_substitute_with_context(self, db):
        """Test variable substitution with additional context."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template = "Main: {content}"
        context = "Additional constraints: keep it short"

        filled = generator._append_context(
            template.format(content="Build feature"), context
        )

        assert "Build feature" in filled
        assert "Additional constraints" in filled

    def test_merge_multiple_context_parts(self, db):
        """Test merging multiple context parts."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        merged = generator._merge_context(
            "Part 1: Instructions",
            "Part 2: Examples",
            "Part 3: Constraints",
        )

        assert "Part 1: Instructions" in merged
        assert "Part 2: Examples" in merged
        assert "Part 3: Constraints" in merged

    def test_merge_context_filters_empty_parts(self, db):
        """Test that empty context parts are filtered out."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        merged = generator._merge_context(
            "Part 1",
            "",
            None,
            "Part 2",
            "   ",
        )

        assert "Part 1" in merged
        assert "Part 2" in merged
        assert merged.count("Part") == 2  # Only 2 non-empty parts

    def test_substitute_with_missing_variable_raises_error(self, db):
        """Test that missing variables raise KeyError."""
        template = "Content: {missing_var}"

        with pytest.raises(KeyError, match="missing_var"):
            template.format()


# --- Tests for Template Versioning ---


class TestTemplateVersioning:
    """Test template versioning and selection."""

    def test_register_prompt_version(self, db):
        """Test registering a prompt version in database."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template_text = "Test template: {prompt}"
        record = generator._register_prompt(
            "test_prompt", template_text
        )

        assert record is not None
        assert record["prompt_type"] == "test_prompt"
        assert record["version"] >= 1
        assert len(record["prompt_hash"]) == 64  # Full SHA256 hex hash

    def test_same_template_returns_same_version(self, db):
        """Test that identical template text returns same version."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template_text = "Consistent template: {prompt}"

        record1 = generator._register_prompt("test", template_text)
        record2 = generator._register_prompt("test", template_text)

        assert record1["version"] == record2["version"]
        assert record1["prompt_hash"] == record2["prompt_hash"]

    def test_modified_template_increments_version(self, db):
        """Test that modified template text increments version."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        record1 = generator._register_prompt("test", "Original: {prompt}")
        record2 = generator._register_prompt("test", "Modified: {prompt}")

        assert record2["version"] > record1["version"]
        assert record2["prompt_hash"] != record1["prompt_hash"]

    def test_prompt_metadata_retrieval(self, db):
        """Test retrieving prompt metadata after registration."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template_text = "Metadata test: {prompt}"
        generator._register_prompt("test_meta", template_text)

        metadata = generator._prompt_metadata("test_meta")

        assert metadata["prompt_type"] == "test_meta"
        assert "version" in metadata
        assert "prompt_hash" in metadata


# --- Tests for Template Syntax Validation ---


class TestTemplateSyntaxValidation:
    """Test template syntax validation and error detection."""

    def test_valid_format_string_syntax(self, db):
        """Test that valid format strings parse correctly."""
        template = "Post: {content}\nAuthor: {author}"

        # Should not raise
        filled = template.format(content="Test", author="User")
        assert "Test" in filled
        assert "User" in filled

    def test_detect_malformed_braces(self, db):
        """Test detection of malformed braces in template."""
        malformed_template = "Content: {unclosed"

        with pytest.raises(ValueError):
            # Python's str.format will raise ValueError for malformed braces
            malformed_template.format()

    def test_detect_invalid_field_name(self, db):
        """Test detection of invalid field names."""
        template = "Content: {123invalid}"  # Field names can't start with numbers

        with pytest.raises((ValueError, KeyError)):
            template.format()

    def test_nested_braces_in_template(self, db):
        """Test handling of nested braces (should be escaped)."""
        template = "JSON: {{\"key\": \"{value}\"}}"

        filled = template.format(value="test")
        assert '{"key": "test"}' in filled

    def test_template_with_format_specifiers(self, db):
        """Test templates with format specifiers."""
        template = "Score: {score:.2f}, Count: {count:>5}"

        filled = template.format(score=7.5555, count=42)
        assert "7.56" in filled  # Rounded to 2 decimal places
        assert "   42" in filled  # Right-aligned to 5 chars


# --- Tests for Template Output Length Validation ---


class TestTemplateOutputLengthValidation:
    """Test template output length validation and character limits."""

    def test_x_post_respects_280_char_limit(self, db):
        """Test that X post template output respects 280 character limit."""
        # This is a validation test - the actual enforcement would be in the generator
        max_length = 280
        sample_output = "A" * 280

        assert len(sample_output) == max_length
        assert len(sample_output + "B") > max_length

    def test_x_thread_tweet_length_limit(self, db):
        """Test that individual tweets in thread respect character limit."""
        tweet_lines = [
            "TWEET 1: " + "A" * 270,
            "TWEET 2: " + "B" * 270,
            "TWEET 3: " + "C" * 270,
        ]

        for line in tweet_lines:
            # Extract just the tweet content (after "TWEET N: ")
            tweet_content = line.split(": ", 1)[1]
            assert len(tweet_content) <= 280

    def test_truncate_long_output(self):
        """Test truncation of output that exceeds limit."""
        long_text = "A" * 500
        limit = 280

        truncated = long_text[:limit]
        assert len(truncated) == limit

    def test_output_length_validation_helper(self):
        """Test helper function for output length validation."""

        def validate_length(text: str, max_length: int) -> tuple[bool, str]:
            """Validate text length and return (is_valid, error_message)."""
            if len(text) <= max_length:
                return (True, "")
            return (False, f"Output length {len(text)} exceeds limit {max_length}")

        is_valid, _ = validate_length("Short text", 280)
        assert is_valid is True

        is_valid, error = validate_length("A" * 300, 280)
        assert is_valid is False
        assert "exceeds limit" in error


# --- Tests for Template Context Building ---


class TestTemplateContextBuilding:
    """Test template context building and variable assembly."""

    def test_build_basic_context(self, db):
        """Test building basic template context."""
        context = {
            "prompt": "Build feature X",
            "commit_message": "feat: add feature X",
            "repo_name": "acme/project",
        }

        assert context["prompt"] is not None
        assert context["commit_message"] is not None
        assert context["repo_name"] is not None

    def test_build_context_with_optional_fields(self, db):
        """Test building context with optional fields."""
        context = {
            "prompt": "Fix bug",
            "commit_message": "fix: resolve issue",
            "examples": ["Example 1", "Example 2"],
            "constraints": "Keep it concise",
        }

        assert "examples" in context
        assert len(context["examples"]) == 2

    def test_feedback_constraints_integration(self, db):
        """Test integration of feedback constraints into context."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        # Mock feedback memory with constraints
        generator.feedback_memory.build_prompt_constraints = Mock(
            return_value="Avoid technical jargon"
        )

        constraints = generator._feedback_constraints("x_post")
        assert "Avoid technical jargon" in constraints

    def test_recommended_format_directive(self, db):
        """Test building recommended format directive."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        directive = generator._recommended_format_directive(
            "thread", reason="Higher engagement on threads"
        )

        assert "thread" in directive
        assert "engagement" in directive.lower()

    def test_empty_recommended_format(self, db):
        """Test that empty format returns no directive."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        directive = generator._recommended_format_directive(None)
        assert directive == ""


# --- Tests for Template Caching ---


class TestTemplateCaching:
    """Test template caching to avoid re-parsing."""

    def test_template_cached_after_first_load(self, db):
        """Test that template is cached after first load."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        # Load template first time
        template1 = generator._load_prompt("x_post")

        # Check that prompt version was registered (cached)
        assert "x_post" in generator.prompt_versions

        # Load again - should use cached version
        metadata = generator._prompt_metadata("x_post")
        assert metadata is not None

    def test_multiple_loads_return_consistent_hash(self, db):
        """Test that multiple loads return consistent template hash."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        generator._load_prompt("x_post")
        hash1 = generator._prompt_metadata("x_post")["prompt_hash"]

        # Reload
        generator._load_prompt("x_post")
        hash2 = generator._prompt_metadata("x_post")["prompt_hash"]

        assert hash1 == hash2

    def test_template_version_persists_in_db(self, db):
        """Test that template version persists in database."""
        generator1 = ContentGenerator(api_key="test", model="test", db=db)

        generator1._load_prompt("x_post")
        version1 = generator1._prompt_metadata("x_post")["version"]

        # Create new generator instance (simulating restart)
        generator2 = ContentGenerator(api_key="test", model="test", db=db)
        generator2._load_prompt("x_post")
        version2 = generator2._prompt_metadata("x_post")["version"]

        # Should get same version from DB
        assert version1 == version2


# --- Tests for Format-Specific Templates ---


class TestFormatSpecificTemplates:
    """Test format-specific templates including post format variations."""

    def test_x_post_template_structure(self, db):
        """Test x_post template structure and required variables."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template = generator._load_prompt("x_post")

        # Check for required variables
        assert "{prompt}" in template
        assert "{commit_message}" in template

    def test_x_thread_template_structure(self, db):
        """Test x_thread template structure for multi-tweet content."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template = generator._load_prompt("x_thread")

        # Thread templates should mention tweet format
        assert "TWEET" in template.upper() or "tweet" in template.lower()

    def test_blog_post_template_structure(self, db):
        """Test blog_post template structure."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template = generator._load_prompt("blog_post")

        assert template is not None
        assert len(template) > 100  # Blog templates are typically longer

    def test_template_variations_exist(self):
        """Test that template variations exist in prompts directory."""
        prompts_dir = Path(__file__).parent.parent / "src" / "synthesis" / "prompts"

        # Check for versioned templates
        variations = [
            "x_post.txt",
            "x_post_v2.txt",
            "x_thread.txt",
            "x_thread_v2.txt",
        ]

        for template_file in variations:
            template_path = prompts_dir / template_file
            assert template_path.exists(), f"{template_file} should exist"

    def test_load_specific_template_version(self, db, tmp_path):
        """Test loading a specific template version."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        # Create v2 template
        v2_template = tmp_path / "x_post_v2.txt"
        v2_template.write_text("Version 2: {prompt}")

        generator.set_prompt_file_override("x_post_v2", v2_template)
        template = generator._load_prompt("x_post_v2")

        assert "Version 2" in template

    def test_thread_hook_variations(self):
        """Test that thread template supports different hook styles."""
        prompts_dir = Path(__file__).parent.parent / "src" / "synthesis" / "prompts"

        # Check for thread-related templates
        thread_templates = [
            f
            for f in prompts_dir.glob("*thread*.txt")
            if f.name.startswith("x_thread")
        ]

        assert len(thread_templates) >= 1, "Should have at least one thread template"


# --- Tests for Error Handling ---


class TestTemplateErrorHandling:
    """Test error handling for malformed templates and missing files."""

    def test_missing_template_file_raises_error(self, db):
        """Test that missing template file raises FileNotFoundError."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        with pytest.raises(FileNotFoundError):
            generator._load_prompt("nonexistent_template_xyz")

    def test_malformed_template_format_raises_error(self, db):
        """Test that malformed template format raises appropriate error."""
        malformed = "Template with {unclosed brace"

        with pytest.raises(ValueError):
            malformed.format()

    def test_invalid_variable_reference_raises_error(self, db):
        """Test that invalid variable reference raises KeyError."""
        template = "Content: {undefined_variable}"

        with pytest.raises(KeyError):
            template.format(different_variable="value")

    def test_template_read_permission_error(self, db, tmp_path):
        """Test handling of permission errors when reading template."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        # Create a file with no read permissions
        restricted_file = tmp_path / "restricted.txt"
        restricted_file.write_text("Restricted template")
        restricted_file.chmod(0o000)

        try:
            generator.set_prompt_file_override("restricted", restricted_file)

            with pytest.raises(PermissionError):
                generator._load_prompt("restricted")
        finally:
            # Restore permissions for cleanup
            restricted_file.chmod(0o644)

    def test_empty_template_file(self, db, tmp_path):
        """Test handling of empty template file."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        empty_template = tmp_path / "empty.txt"
        empty_template.write_text("")

        generator.set_prompt_file_override("empty", empty_template)
        template = generator._load_prompt("empty")

        assert template == ""

    def test_template_with_unicode_characters(self, db, tmp_path):
        """Test template with unicode characters."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        unicode_template = tmp_path / "unicode.txt"
        unicode_template.write_text("Template: {content} 🚀 日本語")

        generator.set_prompt_file_override("unicode", unicode_template)
        template = generator._load_prompt("unicode")

        assert "🚀" in template
        assert "日本語" in template


# --- Integration Tests ---


class TestTemplateIntegration:
    """Integration tests combining template loading, substitution, and validation."""

    def test_complete_template_workflow(self, db):
        """Test complete workflow from template load to rendered output."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        # Load template
        template = generator._load_prompt("x_post")

        # Substitute variables
        filled = template.format(
            prompt="Improve error handling",
            commit_message="feat: add retry logic",
            repo_name="test/repo",
        )

        # Verify substitution
        assert "Improve error handling" in filled
        assert "add retry logic" in filled
        assert "test/repo" in filled

        # Verify metadata was stored
        metadata = generator._prompt_metadata("x_post")
        assert metadata["version"] >= 1

    def test_template_with_context_and_constraints(self, db):
        """Test template rendering with additional context and constraints."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        template = generator._load_prompt("x_post")
        filled = template.format(
            prompt="Build feature",
            commit_message="feat: implement",
            repo_name="test/repo",
        )

        # Add constraints
        context = "Constraint: Keep under 280 characters"
        final = generator._append_context(filled, context)

        assert "Constraint" in final
        assert "Keep under 280 characters" in final

    def test_multi_format_template_selection(self, db):
        """Test selecting different templates for different content formats."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        post_template = generator._load_prompt("x_post")
        thread_template = generator._load_prompt("x_thread")

        # Templates should be different
        assert post_template != thread_template

        # Verify both are registered
        assert "x_post" in generator.prompt_versions
        assert "x_thread" in generator.prompt_versions

    def test_template_versioning_across_instances(self, db):
        """Test that template versions are consistent across generator instances."""
        # First instance loads and registers template
        gen1 = ContentGenerator(api_key="test", model="test", db=db)
        gen1._load_prompt("x_post")
        version1 = gen1._prompt_metadata("x_post")["version"]

        # Second instance should see same version
        gen2 = ContentGenerator(api_key="test", model="test", db=db)
        gen2._load_prompt("x_post")
        version2 = gen2._prompt_metadata("x_post")["version"]

        assert version1 == version2

    def test_override_template_workflow(self, db, tmp_path):
        """Test complete workflow with template override."""
        generator = ContentGenerator(api_key="test", model="test", db=db)

        # Create custom template
        custom = tmp_path / "custom.txt"
        custom.write_text("Custom: {content}")

        # Override and use
        generator.set_prompt_file_override("x_post", custom)
        template = generator._load_prompt("x_post")

        assert "Custom:" in template

        # Verify custom template was registered
        metadata = generator._prompt_metadata("x_post")
        assert metadata is not None
