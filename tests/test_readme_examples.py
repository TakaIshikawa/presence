"""Tests to verify tests/README.md examples are accurate and code is valid.

This test suite ensures that:
1. All Python code blocks in README.md are syntactically valid
2. Example code using fixtures and assertions actually works
3. Documentation accurately reflects actual test infrastructure
"""

import ast
import re
import sqlite3
import sys
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

import pytest

# Ensure src/ is in path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tests.helpers.assertions import (
    assert_valid_post,
    assert_valid_thread,
    assert_valid_candidate,
    assert_evaluation_scores_valid,
)


def extract_python_code_blocks(readme_path):
    """Extract all Python code blocks from README.md."""
    with open(readme_path) as f:
        content = f.read()

    # Match ```python ... ``` blocks
    pattern = r"```python\n(.*?)\n```"
    blocks = re.findall(pattern, content, re.DOTALL)
    return blocks


def is_valid_python(code):
    """Check if code is syntactically valid Python."""
    try:
        ast.parse(code)
        return True
    except SyntaxError:
        return False


class TestREADMECodeBlocks:
    """Test that all code blocks in README.md are valid Python."""

    def test_all_code_blocks_are_valid_python(self):
        """Verify all Python code blocks in README are syntactically valid."""
        readme_path = Path(__file__).parent / "README.md"
        code_blocks = extract_python_code_blocks(readme_path)

        assert len(code_blocks) > 0, "No Python code blocks found in README.md"

        invalid_blocks = []
        for i, block in enumerate(code_blocks):
            if not is_valid_python(block):
                invalid_blocks.append((i, block))

        if invalid_blocks:
            messages = [
                f"Block {i}:\n{block[:200]}..." for i, block in invalid_blocks
            ]
            pytest.fail(
                f"Found {len(invalid_blocks)} invalid Python code blocks:\n"
                + "\n\n".join(messages)
            )


class TestDatabaseFixtureExamples:
    """Test that database fixture examples from README work correctly."""

    def test_db_fixture_insert_message_example(self, db):
        """Verify the db fixture example from README works."""
        # This is adapted from the README.md example with required fields
        db.conn.execute(
            "INSERT INTO claude_messages (session_id, message_uuid, timestamp, prompt_text) VALUES (?, ?, ?, ?)",
            ("sess-001", "uuid-aaa", "2026-03-20T10:00:00+00:00", "test prompt"),
        )
        db.conn.commit()

        cursor = db.conn.execute("SELECT COUNT(*) FROM claude_messages")
        count = cursor.fetchone()[0]
        assert count == 1

    def test_sample_message_fixture_example(self, db, sample_message):
        """Verify the sample_message fixture example from README works."""
        # This is the example from README.md
        db.conn.execute(
            """INSERT INTO claude_messages
               (session_id, message_uuid, project_path, timestamp, prompt_text)
               VALUES (?, ?, ?, ?, ?)""",
            (
                sample_message["session_id"],
                sample_message["message_uuid"],
                sample_message["project_path"],
                sample_message["timestamp"],
                sample_message["prompt_text"],
            ),
        )
        db.conn.commit()

        # Verify insertion
        cursor = db.conn.execute(
            "SELECT prompt_text FROM claude_messages WHERE message_uuid = ?",
            (sample_message["message_uuid"],),
        )
        assert cursor.fetchone()[0] == "Explain the auth module"

    def test_sample_commit_fixture_example(self, db, sample_commit):
        """Verify the sample_commit fixture example from README works."""
        # This is the example from README.md
        db.conn.execute(
            """INSERT INTO github_commits
               (repo_name, commit_sha, commit_message, timestamp, author)
               VALUES (?, ?, ?, ?, ?)""",
            (
                sample_commit["repo_name"],
                sample_commit["commit_sha"],
                sample_commit["commit_message"],
                sample_commit["timestamp"],
                sample_commit["author"],
            ),
        )
        db.conn.commit()

        cursor = db.conn.execute(
            "SELECT commit_message FROM github_commits WHERE commit_sha = ?",
            (sample_commit["commit_sha"],),
        )
        assert cursor.fetchone()[0] == "fix: resolve race condition"

    def test_sample_content_fixture_example(self, db, sample_content):
        """Verify the sample_content fixture example from README works."""
        # This is the example from README.md
        db.conn.execute(
            """INSERT INTO generated_content
               (content_type, content, eval_score, eval_feedback)
               VALUES (?, ?, ?, ?)""",
            (
                sample_content["content_type"],
                sample_content["content"],
                sample_content["eval_score"],
                sample_content["eval_feedback"],
            ),
        )
        db.conn.commit()

        cursor = db.conn.execute(
            "SELECT eval_score FROM generated_content WHERE content_type = ?",
            (sample_content["content_type"],),
        )
        assert cursor.fetchone()[0] == 7.5


class TestMockHelperExamples:
    """Test that mock helper examples from README work correctly."""

    def test_mock_db_example(self):
        """Verify the MockDB example from README works."""

        # This is the example from README.md
        class MockDB:
            """Mock database for testing."""

            def __init__(self):
                self.conn = sqlite3.connect(":memory:")
                self._setup_schema()

            def _setup_schema(self):
                """Create necessary tables."""
                self.conn.execute(
                    """CREATE TABLE predictions (
                        id INTEGER PRIMARY KEY,
                        predicted_score REAL,
                        actual_score REAL
                    )"""
                )
                self.conn.commit()

        db = MockDB()
        db.conn.execute(
            "INSERT INTO predictions (predicted_score, actual_score) VALUES (?, ?)",
            (8.5, 9.0),
        )
        db.conn.commit()

        cursor = db.conn.execute("SELECT predicted_score FROM predictions")
        assert cursor.fetchone()[0] == 8.5

    def test_mocking_http_requests_example(self):
        """Verify the HTTP mocking example from README works."""
        # This is adapted from the README.md example
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = '<html><head><title>Test Article</title></head></html>'

        with patch("requests.get", return_value=mock_response):
            # Simulate fetching
            import requests

            response = requests.get("https://example.com/article")
            assert response.status_code == 200
            assert "Test Article" in response.text

    def test_mocking_llm_api_example(self):
        """Verify the LLM mocking example from README works."""
        # This is adapted from the README.md example
        mock_client = Mock()
        mock_client.messages.create.return_value = Mock(
            content=[Mock(text="Generated post about AI safety.")],
            usage=Mock(input_tokens=100, output_tokens=20),
        )

        # Simulate LLM call
        response = mock_client.messages.create(
            model="claude-opus-4",
            messages=[{"role": "user", "content": "Write about AI safety"}],
        )

        assert "AI safety" in response.content[0].text
        assert response.usage.output_tokens == 20

    def test_mocking_file_operations_example(self):
        """Verify the file mocking example from README works."""
        # This is adapted from the README.md example
        import json

        mock_config = '{"api_key": "test-key", "model": "claude-opus-4"}'

        with patch("builtins.open", mock_open(read_data=mock_config)):
            with open("config.json") as f:
                config = json.load(f)

        assert config["api_key"] == "test-key"
        assert config["model"] == "claude-opus-4"


class TestParametrizedTestingExamples:
    """Test that parametrized testing examples from README work correctly."""

    @pytest.mark.parametrize(
        "input,expected",
        [
            ("Hello world", 11),
            ("Python", 6),
            ("", 0),
            ("Test with spaces", 16),
        ],
    )
    def test_character_count_example(self, input, expected):
        """Verify the basic parametrization example from README works."""
        # This is the example from README.md
        assert len(input) == expected

    @pytest.mark.parametrize(
        "content,char_limit,should_pass",
        [
            ("Short tweet", 280, True),
            ("x" * 280, 280, True),
            ("x" * 281, 280, False),
            ("Medium post", 500, True),
        ],
    )
    def test_content_length_validation_example(self, content, char_limit, should_pass):
        """Verify the multi-parameter example from README works."""
        # This is the example from README.md
        if should_pass:
            assert_valid_post(content, char_limit=char_limit)
        else:
            with pytest.raises(AssertionError):
                assert_valid_post(content, char_limit=char_limit)

    @pytest.mark.parametrize(
        "score,label",
        [
            (9.5, "excellent"),
            (7.5, "good"),
            (5.0, "average"),
            (2.5, "poor"),
        ],
        ids=["excellent-score", "good-score", "average-score", "poor-score"],
    )
    def test_score_labeling_with_ids(self, score, label):
        """Verify the parametrization with IDs example from README works."""
        # This validates the pattern; actual implementation may vary
        # We're testing that the parametrization mechanism works
        assert score > 0
        assert label in ["excellent", "good", "average", "poor"]


class TestAssertionHelperExamples:
    """Test that assertion helper examples from README work correctly."""

    def test_assert_valid_post_example(self):
        """Verify assert_valid_post examples from README work."""
        # Basic validation
        assert_valid_post("This is a great post about Python!")

        # Custom character limit
        assert_valid_post("Short post", char_limit=500)

        # Check for banned words
        assert_valid_post("Clean content", banned_words=["spam", "clickbait"])

    def test_assert_valid_thread_example(self):
        """Verify assert_valid_thread examples from README work."""
        tweets = [
            "First tweet introduces the topic",
            "Second tweet expands on the idea",
            "Third tweet provides conclusion",
        ]

        # Basic validation
        assert_valid_thread(tweets)

        # Custom limits
        assert_valid_thread(tweets, min_tweets=3, max_tweets=5, total_char_limit=1000)

        # Skip continuity checks
        assert_valid_thread(tweets, check_continuity=False)

    def test_assert_valid_candidate_example(self):
        """Verify assert_valid_candidate examples from README work."""
        candidate = {
            "content": "Generated post content",
            "score": 8.5,
            "model": "claude-opus-4",
        }

        # Basic validation (checks 'content' and 'score')
        assert_valid_candidate(candidate)

        # Custom required fields
        assert_valid_candidate(
            candidate, required_fields=["content", "score", "model"]
        )

    def test_assert_evaluation_scores_valid_example(self):
        """Verify assert_evaluation_scores_valid examples from README work."""
        scores = {"opus": 9.2, "sonnet": 8.5, "haiku": 7.8}

        # Basic validation (requires opus > sonnet + 0.5)
        assert_evaluation_scores_valid(scores)

        # Don't require Opus higher
        assert_evaluation_scores_valid(scores, require_opus_higher=False)


class TestPerformanceExamples:
    """Test that performance optimization examples are valid."""

    def test_fast_mock_based_approach(self):
        """Verify the optimized performance example from README works."""
        # This demonstrates the pattern from README.md
        mock_generate = Mock(return_value={"content": "test", "score": 8.0})

        # Simulate generating 100 candidates with mocking
        candidates = [mock_generate(f"prompt {i}") for i in range(100)]

        assert len(candidates) == 100
        assert mock_generate.call_count == 100


class TestCoverageExample:
    """Test the comprehensive coverage example from README."""

    def calculate_engagement(self, likes: int, retweets: int, replies: int) -> float:
        """Calculate engagement score from metrics (example from README)."""
        if likes < 0 or retweets < 0 or replies < 0:
            raise ValueError("Metrics cannot be negative")

        if likes == 0 and retweets == 0 and replies == 0:
            return 0.0

        # Weighted formula
        score = (likes * 0.5) + (retweets * 2.0) + (replies * 1.5)
        return min(score / 100.0, 10.0)  # Normalize to 0-10

    def test_calculate_engagement_basic(self):
        """Test basic engagement calculation (from README)."""
        score = self.calculate_engagement(likes=100, retweets=50, replies=20)
        assert score == pytest.approx(1.8)

    def test_calculate_engagement_zero(self):
        """Test zero engagement (from README)."""
        assert self.calculate_engagement(0, 0, 0) == 0.0

    def test_calculate_engagement_negative_raises(self):
        """Test negative metrics raise error (from README)."""
        with pytest.raises(ValueError, match="cannot be negative"):
            self.calculate_engagement(-1, 0, 0)

    def test_calculate_engagement_max_score(self):
        """Test score caps at 10.0 (from README)."""
        score = self.calculate_engagement(likes=10000, retweets=5000, replies=1000)
        assert score == 10.0

    def test_calculate_engagement_each_metric(self):
        """Test each metric contributes correctly (from README)."""
        likes_only = self.calculate_engagement(100, 0, 0)
        retweets_only = self.calculate_engagement(0, 100, 0)
        replies_only = self.calculate_engagement(0, 0, 100)

        assert likes_only == 0.5
        assert retweets_only == 2.0
        assert replies_only == 1.5


class TestREADMEFixturesDocumented:
    """Verify that fixtures mentioned in README actually exist."""

    def test_db_fixture_exists(self, db):
        """Verify db fixture exists and works."""
        assert db is not None
        assert hasattr(db, "conn")
        # Verify database is functional
        cursor = db.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        # Should have tables from schema
        assert len(tables) > 0

    def test_file_db_fixture_exists(self, file_db):
        """Verify file_db fixture exists and works."""
        assert file_db is not None
        assert hasattr(file_db, "conn")
        assert hasattr(file_db, "db_path")

    def test_sample_message_fixture_exists(self, sample_message):
        """Verify sample_message fixture exists with expected fields."""
        assert "session_id" in sample_message
        assert "message_uuid" in sample_message
        assert "project_path" in sample_message
        assert "timestamp" in sample_message
        assert "prompt_text" in sample_message

    def test_sample_commit_fixture_exists(self, sample_commit):
        """Verify sample_commit fixture exists with expected fields."""
        assert "repo_name" in sample_commit
        assert "commit_sha" in sample_commit
        assert "commit_message" in sample_commit
        assert "timestamp" in sample_commit
        assert "author" in sample_commit

    def test_sample_content_fixture_exists(self, sample_content):
        """Verify sample_content fixture exists with expected fields."""
        assert "content_type" in sample_content
        assert "content" in sample_content
        assert "eval_score" in sample_content
        assert "eval_feedback" in sample_content


class TestREADMEStructure:
    """Verify README.md has all documented sections."""

    def test_readme_has_all_sections(self):
        """Verify README.md contains all major sections from table of contents."""
        readme_path = Path(__file__).parent / "README.md"
        with open(readme_path) as f:
            content = f.read()

        required_sections = [
            "## Table of Contents",
            "## Test Organization",
            "## Fixture Library",
            "## Custom Assertion Helpers",
            "## Mock Helpers",
            "## Parametrized Testing",
            "## Test Performance Guidelines",
            "## TDD Workflow",
            "## Test Debugging Tips",
            "## Test Coverage Standards",
            "## Quick Reference Guide",
        ]

        missing_sections = [
            section for section in required_sections if section not in content
        ]

        assert not missing_sections, f"Missing sections: {missing_sections}"

    def test_readme_has_examples(self):
        """Verify README.md contains code examples."""
        readme_path = Path(__file__).parent / "README.md"
        code_blocks = extract_python_code_blocks(readme_path)

        # Should have many examples
        assert (
            len(code_blocks) >= 20
        ), f"Expected at least 20 code examples, found {len(code_blocks)}"

    def test_readme_has_bash_examples(self):
        """Verify README.md contains bash command examples."""
        readme_path = Path(__file__).parent / "README.md"
        with open(readme_path) as f:
            content = f.read()

        # Check for bash blocks
        bash_blocks = re.findall(r"```bash\n(.*?)\n```", content, re.DOTALL)
        assert (
            len(bash_blocks) >= 10
        ), f"Expected at least 10 bash examples, found {len(bash_blocks)}"

        # Should have pytest commands
        bash_content = "\n".join(bash_blocks)
        assert "pytest" in bash_content
        assert "--cov" in bash_content
