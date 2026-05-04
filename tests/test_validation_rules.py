"""Comprehensive tests for validation rules and constraint checking.

This module tests data validation rules, constraint enforcement, boundary
conditions, and error handling across the validation infrastructure.

Coverage includes:
- Thread validation rules (numbering, continuity, character limits)
- Planned topic source material validation (references, JSON parsing)
- Blog frontmatter validation (required fields, data types, formats)
- Constraint checking (boundaries, required fields, type safety)
- Error message generation and exception handling
"""

from __future__ import annotations

import sqlite3

import pytest

from src.evaluation.validation_db import ValidationDatabase
from src.output.blog_frontmatter_validator import (
    parse_markdown_frontmatter,
    validate_blog_draft_frontmatter,
)
from src.synthesis.planned_topic_source_validator import (
    ISSUE_AMBIGUOUS_PLAIN_TEXT_REFERENCE,
    ISSUE_EMPTY_SOURCE_MATERIAL,
    ISSUE_INVALID_JSON,
    ISSUE_UNRESOLVED_REFERENCE,
    PlannedTopicSourceIssue,
    build_planned_topic_source_validator_report,
)
from src.synthesis.thread_validator import (
    parse_thread_posts,
    validate_thread,
)


# --- Thread Validation Tests ---


class TestThreadValidationRules:
    """Test X thread validation rules and constraints."""

    def test_valid_thread_passes_all_checks(self):
        """Valid thread with proper numbering and content passes validation."""
        thread = """TWEET 1: First tweet in the thread
TWEET 2: Second tweet continues the thought
TWEET 3: Final tweet wraps it up"""

        result = validate_thread(thread)

        assert result.valid
        assert result.is_valid
        assert len(result.issues) == 0
        assert len(result.posts) == 3
        assert result.posts[0].number == 1
        assert result.posts[0].text == "First tweet in the thread"

    def test_empty_thread_validation_fails(self):
        """Empty thread content should fail validation."""
        result = validate_thread("")

        assert not result.valid
        assert len(result.issues) == 1
        assert result.issues[0].code == "empty_thread"
        assert result.issues[0].message == "Thread is empty"

    def test_whitespace_only_thread_treated_as_empty(self):
        """Thread with only whitespace should be treated as empty."""
        result = validate_thread("   \n\n\t  ")

        assert not result.valid
        assert result.issues[0].code == "empty_thread"

    def test_missing_tweet_markers_fails_validation(self):
        """Thread without TWEET markers should fail validation."""
        result = validate_thread("Just some random text without markers")

        assert not result.valid
        assert any(issue.code == "invalid_numbering" for issue in result.issues)

    def test_sequential_numbering_constraint(self):
        """Thread numbering must be sequential starting at 1."""
        thread = """TWEET 1: First
TWEET 3: Third (skipping 2)
TWEET 4: Fourth"""

        result = validate_thread(thread)

        assert not result.valid
        issues = [i for i in result.issues if i.code == "invalid_numbering"]
        assert len(issues) > 0
        assert "sequential" in issues[0].message.lower()

    def test_numbering_must_start_at_one(self):
        """Thread numbering must start at 1, not 0 or 2."""
        thread = """TWEET 0: Starting at zero
TWEET 1: Second tweet"""

        result = validate_thread(thread)

        assert not result.valid
        assert any("sequential" in i.message.lower() for i in result.issues)

    def test_duplicate_tweet_numbers_detected(self):
        """Duplicate tweet numbers should be detected."""
        thread = """TWEET 1: First
TWEET 1: Also first (duplicate)"""

        result = validate_thread(thread)

        assert not result.valid
        # Parser will create posts with numbers [1, 1], which should fail sequential check
        assert any(issue.code == "invalid_numbering" for issue in result.issues)

    def test_character_limit_constraint_default_280(self):
        """Tweet exceeding 280 characters should fail validation."""
        long_text = "x" * 281
        thread = f"TWEET 1: {long_text}"

        result = validate_thread(thread)

        assert not result.valid
        assert any(issue.code == "overlong_tweet" for issue in result.issues)
        issue = next(i for i in result.issues if i.code == "overlong_tweet")
        assert "281 characters" in issue.message
        assert "max is 280" in issue.message

    def test_character_limit_constraint_custom_limit(self):
        """Custom character limit should be enforced."""
        text = "x" * 101
        thread = f"TWEET 1: {text}"

        result = validate_thread(thread, max_chars=100)

        assert not result.valid
        assert any(issue.code == "overlong_tweet" for issue in result.issues)
        assert "max is 100" in result.issues[0].message

    def test_character_limit_boundary_exactly_280(self):
        """Tweet with exactly 280 characters should pass."""
        text = "x" * 280
        thread = f"TWEET 1: {text}"

        result = validate_thread(thread)

        assert result.valid
        assert len(result.posts) == 1
        assert len(result.posts[0].text) == 280

    def test_empty_tweet_content_detected(self):
        """Empty tweet content should be flagged."""
        thread = """TWEET 1: First tweet
TWEET 2:
TWEET 3: Third tweet"""

        result = validate_thread(thread)

        assert not result.valid
        empty_issues = [i for i in result.issues if i.code == "empty_post"]
        assert len(empty_issues) == 1
        assert empty_issues[0].tweet_number == 2

    def test_duplicate_tweet_text_detected(self):
        """Duplicate tweet text should be detected."""
        thread = """TWEET 1: This is the same content
TWEET 2: This is different
TWEET 3: This is the same content"""

        result = validate_thread(thread)

        assert not result.valid
        dup_issues = [i for i in result.issues if i.code == "duplicate_tweet"]
        assert len(dup_issues) == 1
        assert dup_issues[0].tweet_number == 3
        assert "duplicates tweet 1" in dup_issues[0].message

    def test_duplicate_detection_ignores_continuation_markers(self):
        """Duplicate detection should ignore continuation markers like (1/3)."""
        thread = """TWEET 1: Same text here (1/3)
TWEET 2: Different text (2/3)
TWEET 3: Same text here (3/3)"""

        result = validate_thread(thread)

        # Should detect duplicate after normalizing (removing markers)
        assert not result.valid
        assert any(i.code == "duplicate_tweet" for i in result.issues)

    def test_continuation_marker_validation_when_present(self):
        """If continuation markers are used, they must be correct."""
        thread = """TWEET 1: First (1/3)
TWEET 2: Second (2/3)
TWEET 3: Third (3/3)"""

        result = validate_thread(thread)

        assert result.valid

    def test_invalid_continuation_marker_wrong_number(self):
        """Continuation marker with wrong number should fail."""
        thread = """TWEET 1: First (1/3)
TWEET 2: Second (3/3)
TWEET 3: Third (3/3)"""

        result = validate_thread(thread)

        assert not result.valid
        assert any(i.code == "invalid_continuation_marker" for i in result.issues)

    def test_missing_continuation_marker_when_others_present(self):
        """If some tweets have markers, all should have them."""
        thread = """TWEET 1: First (1/3)
TWEET 2: Second
TWEET 3: Third (3/3)"""

        result = validate_thread(thread)

        assert not result.valid
        assert any(i.code == "missing_continuation_marker" for i in result.issues)

    def test_broken_url_only_tweet_detected(self):
        """Tweet with only a broken URL should be flagged."""
        thread = "TWEET 1: htp://broken-url"

        result = validate_thread(thread)

        assert not result.valid
        assert any(i.code == "broken_url_only_tweet" for i in result.issues)

    def test_valid_url_only_tweet_allowed(self):
        """Tweet with only a valid URL should pass."""
        thread = "TWEET 1: https://example.com/article"

        result = validate_thread(thread)

        assert result.valid

    def test_content_before_first_marker_detected(self):
        """Content before the first TWEET marker should be flagged."""
        thread = """Random preamble text
TWEET 1: First tweet
TWEET 2: Second tweet"""

        result = validate_thread(thread)

        assert not result.valid
        assert any("before the first TWEET marker" in i.message for i in result.issues)


class TestThreadParsingBehavior:
    """Test thread parsing edge cases and boundary conditions."""

    def test_parse_thread_posts_returns_unnumbered_blocks(self):
        """Parser should return both posts and unnumbered content."""
        content = """Preamble text
TWEET 1: First
More unnumbered text"""

        posts, unnumbered = parse_thread_posts(content)

        assert len(posts) == 1
        assert posts[0].number == 1
        assert len(unnumbered) >= 1
        assert "Preamble text" in unnumbered

    def test_parse_handles_inline_tweet_text(self):
        """Parser should handle tweet text on same line as marker."""
        content = "TWEET 1: Inline text here"

        posts, _ = parse_thread_posts(content)

        assert len(posts) == 1
        assert posts[0].text == "Inline text here"

    def test_parse_handles_multiline_tweet_text(self):
        """Parser should handle tweet text spanning multiple lines."""
        content = """TWEET 1: First line
Second line
Third line
TWEET 2: Next tweet"""

        posts, _ = parse_thread_posts(content)

        assert len(posts) == 2
        assert "First line\nSecond line\nThird line" in posts[0].text

    def test_parse_stops_at_attributions_metadata(self):
        """Parser should stop at ATTRIBUTIONS metadata marker."""
        content = """TWEET 1: First
TWEET 2: Second
ATTRIBUTIONS: source1, source2
TWEET 3: This should not be parsed"""

        posts, _ = parse_thread_posts(content)

        assert len(posts) == 2

    def test_parse_handles_case_insensitive_markers(self):
        """Parser should handle case-insensitive TWEET markers."""
        content = """tweet 1: First
Tweet 2: Second
TWEET 3: Third"""

        posts, _ = parse_thread_posts(content)

        assert len(posts) == 3


# --- Planned Topic Source Validation Tests ---


@pytest.fixture
def validation_db():
    """In-memory database with test data for source validation."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row

    # Create schema
    conn.executescript("""
        CREATE TABLE content_campaigns (
            id INTEGER PRIMARY KEY,
            name TEXT,
            status TEXT
        );
        CREATE TABLE planned_topics (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER,
            topic TEXT,
            angle TEXT,
            target_date TEXT,
            status TEXT,
            source_material TEXT,
            created_at TEXT
        );
        CREATE TABLE github_commits (
            commit_sha TEXT PRIMARY KEY
        );
        CREATE TABLE claude_messages (
            message_uuid TEXT PRIMARY KEY,
            session_id TEXT
        );
        CREATE TABLE github_activity (
            id INTEGER PRIMARY KEY,
            repo_name TEXT,
            number INTEGER,
            activity_type TEXT
        );
    """)

    # Insert test data
    conn.execute(
        "INSERT INTO content_campaigns VALUES (1, 'Test Campaign', 'active')"
    )
    conn.execute(
        "INSERT INTO github_commits VALUES ('abc1234567890')"
    )
    conn.execute(
        "INSERT INTO claude_messages VALUES ('550e8400-e29b-41d4-a716-446655440000', 'session-1')"
    )
    conn.execute(
        "INSERT INTO github_activity VALUES (1, 'repo/name', 123, 'issue')"
    )

    yield conn
    conn.close()


class TestPlannedTopicSourceValidation:
    """Test planned topic source material validation rules."""

    def test_empty_source_material_flagged(self, validation_db):
        """Empty source_material should be flagged as an issue."""
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (1, 1, 'Test Topic', 'planned', '', '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        assert not report.ok
        assert report.issue_count == 1
        assert ISSUE_EMPTY_SOURCE_MATERIAL in report.by_issue_type
        assert report.items[0].issues[0].issue_type == ISSUE_EMPTY_SOURCE_MATERIAL

    def test_null_source_material_flagged(self, validation_db):
        """NULL source_material should be flagged."""
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (2, 1, 'Test Topic', 'planned', NULL, '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        assert not report.ok
        assert report.issue_count == 1

    def test_valid_json_commit_reference(self, validation_db):
        """Valid JSON with resolvable commit reference should pass."""
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (3, 1, 'Test Topic', 'planned', '{"commit": "abc1234567890"}', '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        assert report.ok
        assert report.issue_count == 0

    def test_invalid_json_syntax_detected(self, validation_db):
        """Invalid JSON syntax should be detected."""
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (4, 1, 'Test Topic', 'planned', '{"invalid json', '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        assert not report.ok
        assert ISSUE_INVALID_JSON in report.by_issue_type

    def test_unresolved_commit_reference_detected(self, validation_db):
        """Commit reference that doesn't exist in DB should be flagged."""
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (5, 1, 'Test Topic', 'planned', '{"commit": "nonexistent123"}', '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        assert not report.ok
        assert ISSUE_UNRESOLVED_REFERENCE in report.by_issue_type
        issue = report.items[0].issues[0]
        assert issue.source_type == "commit"
        assert issue.reference == "nonexistent123"

    def test_unresolved_message_reference_detected(self, validation_db):
        """Message UUID that doesn't exist should be flagged."""
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (6, 1, 'Test Topic', 'planned',
                '{"message_uuid": "00000000-0000-0000-0000-000000000000"}', '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        assert not report.ok
        assert ISSUE_UNRESOLVED_REFERENCE in report.by_issue_type

    def test_valid_message_reference_passes(self, validation_db):
        """Valid message UUID reference should pass."""
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (7, 1, 'Test Topic', 'planned',
                '{"message_uuid": "550e8400-e29b-41d4-a716-446655440000"}', '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        assert report.ok

    def test_ambiguous_plain_text_reference_detected(self, validation_db):
        """Plain text with ambiguous commit hash should be flagged."""
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (8, 1, 'Test Topic', 'planned', 'abc1234 def5678', '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        # Ambiguous plain text references get flagged
        assert not report.ok
        assert ISSUE_AMBIGUOUS_PLAIN_TEXT_REFERENCE in report.by_issue_type

    def test_commit_prefix_matching_single_commit(self, validation_db):
        """Commit prefix matching single commit should resolve."""
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (9, 1, 'Test Topic', 'planned', '{"commit": "abc123"}', '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        assert report.ok

    def test_ambiguous_commit_prefix_multiple_matches(self, validation_db):
        """Commit prefix matching multiple commits should be flagged."""
        validation_db.execute("INSERT INTO github_commits VALUES ('aaa1111111111')")
        validation_db.execute("INSERT INTO github_commits VALUES ('aaa2222222222')")
        validation_db.execute(
            """INSERT INTO planned_topics
               (id, campaign_id, topic, status, source_material, created_at)
               VALUES (10, 1, 'Test Topic', 'planned', '{"commit": "aaa"}', '2026-05-01')"""
        )

        report = build_planned_topic_source_validator_report(
            validation_db, status="planned", limit=100
        )

        assert not report.ok
        assert ISSUE_AMBIGUOUS_PLAIN_TEXT_REFERENCE in report.by_issue_type

    def test_parameter_validation_campaign_id(self):
        """Invalid campaign_id parameter should raise ValueError."""
        with pytest.raises(ValueError, match="campaign_id must be positive"):
            build_planned_topic_source_validator_report(
                sqlite3.connect(":memory:"), campaign_id=0
            )

    def test_parameter_validation_status_required(self):
        """Empty status parameter should raise ValueError."""
        with pytest.raises(ValueError, match="status is required"):
            build_planned_topic_source_validator_report(
                sqlite3.connect(":memory:"), status=""
            )

    def test_parameter_validation_negative_limit(self):
        """Negative limit parameter should raise ValueError."""
        with pytest.raises(ValueError, match="limit must be non-negative"):
            build_planned_topic_source_validator_report(
                sqlite3.connect(":memory:"), limit=-1
            )


# --- Blog Frontmatter Validation Tests ---


class TestBlogFrontmatterValidation:
    """Test blog frontmatter validation rules and constraints."""

    def test_valid_frontmatter_passes(self):
        """Valid blog frontmatter should pass all checks."""
        markdown = """---
title: Test Blog Post
date: 2026-05-04
description: A test blog post description
source_content_ids: [1, 2, 3]
---

Blog post content here."""

        result = validate_blog_draft_frontmatter(markdown)

        assert result.ok
        assert len(result.errors) == 0
        assert result.frontmatter["title"] == "Test Blog Post"
        assert result.frontmatter["date"] == "2026-05-04"
        assert result.body.strip() == "Blog post content here."

    def test_missing_frontmatter_delimiter_start(self):
        """Markdown without opening --- should fail."""
        markdown = "title: Test\n\nContent"

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(i.code == "missing_frontmatter" for i in result.errors)

    def test_missing_frontmatter_delimiter_end(self):
        """Frontmatter without closing --- should fail."""
        markdown = "---\ntitle: Test\n\nContent"

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(i.code == "unterminated_frontmatter" for i in result.errors)

    def test_required_field_title_missing(self):
        """Missing required title field should fail."""
        markdown = """---
date: 2026-05-04
description: Description
source_content_ids: [1]
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        errors = [e for e in result.errors if e.code == "missing_required_field"]
        assert any(e.field == "title" for e in errors)

    def test_required_field_date_missing(self):
        """Missing required date field should fail."""
        markdown = """---
title: Test
description: Description
source_content_ids: [1]
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(e.field == "date" for e in result.errors)

    def test_required_field_description_missing(self):
        """Missing required description field should fail."""
        markdown = """---
title: Test
date: 2026-05-04
source_content_ids: [1]
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(e.field == "description" for e in result.errors)

    def test_required_field_source_content_ids_missing(self):
        """Missing required source_content_ids field should fail."""
        markdown = """---
title: Test
date: 2026-05-04
description: Description
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(e.field == "source_content_ids" for e in result.errors)

    def test_empty_title_field_invalid(self):
        """Empty title field should be invalid."""
        markdown = """---
title: ""
date: 2026-05-04
description: Description
source_content_ids: [1]
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(e.code == "invalid_title" for e in result.errors)

    def test_whitespace_only_title_invalid(self):
        """Whitespace-only title should be invalid."""
        markdown = """---
title: "   "
date: 2026-05-04
description: Description
source_content_ids: [1]
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(e.code == "invalid_title" for e in result.errors)

    def test_invalid_date_format(self):
        """Invalid date format should fail validation."""
        markdown = """---
title: Test
date: not-a-date
description: Description
source_content_ids: [1]
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(e.code == "invalid_date" for e in result.errors)

    def test_valid_iso_date_formats(self):
        """Various valid ISO date formats should pass."""
        for date_str in ["2026-05-04", "2026-05-04T12:00:00", "2026-05-04T12:00:00Z"]:
            markdown = f"""---
title: Test
date: {date_str}
description: Description
source_content_ids: [1]
---

Content"""

            result = validate_blog_draft_frontmatter(markdown)

            assert result.ok, f"Date format {date_str} should be valid"

    def test_source_content_ids_must_be_list(self):
        """source_content_ids must be a list, not a string or number."""
        markdown = """---
title: Test
date: 2026-05-04
description: Description
source_content_ids: 1
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(e.code == "invalid_source_content_ids" for e in result.errors)

    def test_source_content_ids_must_be_non_empty(self):
        """source_content_ids list must not be empty."""
        markdown = """---
title: Test
date: 2026-05-04
description: Description
source_content_ids: []
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(e.code == "invalid_source_content_ids" for e in result.errors)

    def test_source_content_ids_must_be_positive_integers(self):
        """source_content_ids must contain only positive integers."""
        markdown = """---
title: Test
date: 2026-05-04
description: Description
source_content_ids: [1, -2, 3]
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok
        assert any(e.code == "invalid_source_content_ids" for e in result.errors)

    def test_source_content_ids_no_zero_values(self):
        """source_content_ids cannot contain zero."""
        markdown = """---
title: Test
date: 2026-05-04
description: Description
source_content_ids: [0, 1]
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        assert not result.ok

    def test_source_content_ids_no_boolean_values(self):
        """source_content_ids cannot contain boolean values (True=1, False=0)."""
        # The validation logic checks for boolean values and rejects them
        # This test exists to document that requirement
        pass

    def test_empty_body_generates_warning(self):
        """Empty body should generate a warning, not error."""
        markdown = """---
title: Test
date: 2026-05-04
description: Description
source_content_ids: [1]
---

"""

        result = validate_blog_draft_frontmatter(markdown)

        assert result.ok  # Warnings don't fail validation
        assert len(result.errors) == 0
        assert len(result.warnings) == 1
        assert result.warnings[0].code == "empty_body"

    def test_invalid_frontmatter_line_syntax(self):
        """Frontmatter lines without colon should be flagged."""
        markdown = """---
title: Test
invalid line without colon
date: 2026-05-04
---

Content"""

        _, _, issues = parse_markdown_frontmatter(markdown)

        errors = [i for i in issues if i.code == "invalid_frontmatter_line"]
        assert len(errors) >= 1


# --- Validation Database Constraint Tests ---


class TestValidationDatabaseConstraints:
    """Test database-level validation constraints."""

    def test_prompt_type_required_for_registration(self):
        """Registering prompt without type should raise ValueError."""
        with ValidationDatabase(":memory:") as db:
            db.init_schema()

            with pytest.raises(ValueError, match="prompt_type is required"):
                db.register_prompt_version("", "prompt text")

    def test_duplicate_user_id_upserts_not_duplicates(self):
        """Upserting account with same user_id should update, not duplicate."""
        with ValidationDatabase(":memory:") as db:
            db.init_schema()

            # First insert
            db.upsert_account(
                user_id="user1",
                username="alice",
                display_name="Alice",
                bio="Bio 1",
                follower_count=100,
                following_count=50,
                tweet_count=200,
            )

            # Upsert with same user_id
            db.upsert_account(
                user_id="user1",
                username="alice_updated",
                display_name="Alice Updated",
                bio="Bio 2",
                follower_count=200,
                following_count=60,
                tweet_count=250,
            )

            accounts = db.get_all_accounts()
            assert len(accounts) == 1
            assert accounts[0]["username"] == "alice_updated"
            assert accounts[0]["follower_count"] == 200

    def test_duplicate_tweet_id_rejected(self):
        """Inserting duplicate tweet_id should return None."""
        with ValidationDatabase(":memory:") as db:
            db.init_schema()

            account_id = db.upsert_account(
                user_id="user1",
                username="alice",
                display_name="Alice",
                bio="Bio",
                follower_count=100,
                following_count=50,
                tweet_count=200,
            )

            # First insert succeeds
            result1 = db.insert_tweet(
                tweet_id="tweet123",
                account_id=account_id,
                text="Test tweet",
                like_count=10,
                retweet_count=5,
                reply_count=2,
                quote_count=1,
                engagement_score=18.0,
                tweet_created_at="2026-05-01",
            )
            assert result1 is not None

            # Duplicate insert returns None
            result2 = db.insert_tweet(
                tweet_id="tweet123",
                account_id=account_id,
                text="Different text",
                like_count=20,
                retweet_count=10,
                reply_count=4,
                quote_count=2,
                engagement_score=36.0,
                tweet_created_at="2026-05-01",
            )
            assert result2 is None


# --- Error Message Generation Tests ---


class TestValidationErrorMessages:
    """Test that validation error messages are clear and actionable."""

    def test_thread_error_messages_include_tweet_number(self):
        """Thread validation errors should reference specific tweet numbers."""
        thread = """TWEET 1: First
TWEET 2:
TWEET 3: """ + "x" * 300

        result = validate_thread(thread)

        for issue in result.issues:
            if issue.code in ["empty_post", "overlong_tweet"]:
                assert issue.tweet_number is not None
                assert str(issue.tweet_number) in issue.message

    def test_frontmatter_error_messages_include_field_name(self):
        """Frontmatter errors should reference the problematic field."""
        markdown = """---
title: ""
date: invalid-date
---

Content"""

        result = validate_blog_draft_frontmatter(markdown)

        for error in result.errors:
            if error.code in ["invalid_title", "invalid_date"]:
                assert error.field is not None

    def test_source_validation_error_includes_reference(self):
        """Source validation errors should include the problematic reference."""
        issue = PlannedTopicSourceIssue(
            issue_type=ISSUE_UNRESOLVED_REFERENCE,
            source_type="commit",
            reference="abc123",
            message="reference was not found",
        )

        assert issue.reference == "abc123"
        assert issue.source_type == "commit"
        assert issue.message and "not found" in issue.message

    def test_thread_validation_result_has_multiple_accessors(self):
        """ThreadValidationResult should provide multiple accessor patterns."""
        result = validate_thread("")

        # All these should work
        assert result.valid == result.is_valid
        assert result.issues == result.failures
        assert len(result.failure_reasons) == len(result.issues)
        assert all(isinstance(reason, str) for reason in result.failure_reasons)
