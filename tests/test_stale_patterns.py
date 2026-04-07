"""Unit tests for stale rhetorical pattern detection."""

import re

import pytest

from synthesis.stale_patterns import STALE_PATTERNS, has_stale_pattern


# --- Test has_stale_pattern() returns True for each pattern ---


class TestStalePatternMatching:
    """Test that has_stale_pattern() correctly identifies stale patterns."""

    @pytest.mark.parametrize(
        "text,pattern_description",
        [
            # Pattern: r"(?i)^AI\s"
            ("AI is transforming everything", "starts with 'AI '"),
            ("AI agents are the future", "starts with 'AI ' (uppercase)"),
            ("ai systems need better design", "starts with 'ai ' (lowercase)"),
            # Pattern: r"(?i)isn.t about .{5,40}[—\-].{0,5}it.s about"
            (
                "Development isn't about writing code—it's about solving problems",
                "'isn't about X—it's about Y'",
            ),
            (
                "Success isn't about the tools - it's about the mindset",
                "'isn't about X - it's about Y' (hyphen)",
            ),
            (
                "Leadership isn't about authority—it's about trust",
                "'isn't about X—it's about Y' (em dash)",
            ),
            # Pattern: r"(?i)\bbreakthrough\b"
            ("This is a major breakthrough in AI", "contains 'breakthrough'"),
            ("Breakthrough discovery today", "contains 'Breakthrough' (capitalized)"),
            ("Just had a breakthrough moment", "contains 'breakthrough' (mid-sentence)"),
            # Pattern: r"(?i)perfect (prompts?|memory|agents?|handoffs?|context)"
            ("Writing perfect prompts is impossible", "perfect prompts"),
            ("Striving for perfect memory", "perfect memory"),
            ("Building perfect agents", "perfect agents"),
            ("Perfect agent design", "perfect agent"),
            ("Achieving perfect handoffs", "perfect handoffs"),
            ("Perfect handoff between systems", "perfect handoff"),
            ("Maintaining perfect context", "perfect context"),
            # Pattern: r"\d+ commits? across \d+"
            ("5 commits across 3 repositories", "N commits across M"),
            ("1 commit across 2 projects", "1 commit across N"),
            ("42 commits across 7 files", "commits across pattern"),
            # Pattern: r"(?i)^(TWEET 1:\s*\n)?Today.s (insight|breakthrough|lesson)"
            ("Today's insight on debugging", "Today's insight"),
            ("Today's breakthrough in performance", "Today's breakthrough"),
            ("Today's lesson about testing", "Today's lesson"),
            ("TWEET 1:\nToday's insight is critical", "TWEET 1 + Today's insight"),
            ("TWEET 1:  \nToday's lesson matters", "TWEET 1 + Today's lesson (whitespace before newline)"),
            # Pattern: r"(?i)^(unpopular opinion|controversial take)\s*[:\-–—]"
            ("Unpopular opinion: TDD is overrated", "Unpopular opinion:"),
            ("Controversial take - frameworks are crutches", "Controversial take -"),
            ("unpopular opinion — types slow you down", "unpopular opinion — (lowercase)"),
            ("Controversial take: microservices are overhyped", "Controversial take:"),
            # Pattern: r"(?i)\bnobody (is )?(talk(s|ing) about|mentions?)"
            ("nobody talks about the real problem", "nobody talks about"),
            ("Nobody is talking about performance", "Nobody is talking about"),
            ("nobody mentions the tradeoffs", "nobody mentions"),
            ("Nobody is mentioning this issue", "Nobody is mentioning"),
            ("Why nobody talks about this", "nobody talks about (mid-sentence)"),
            # Pattern: r"(?i)^the (secret|trick) to\b"
            ("The secret to better code", "The secret to"),
            ("The trick to debugging faster", "The trick to"),
            ("the secret to productivity", "the secret to (lowercase)"),
            # Pattern: r"(?i)^stop \w[\w ]{0,30}\.\s*start \w"
            ("Stop writing tests. Start shipping code", "Stop X. Start Y"),
            ("Stop overthinking. Start building", "Stop X. Start Y"),
            ("STOP procrastinating. START executing", "Stop X. Start Y (uppercase)"),
            # Pattern: r"(?i)\w[\w ]{0,30} (is|are) dead\.\s*long live\b"
            ("REST is dead. Long live GraphQL", "X is dead. Long live Y"),
            ("Monoliths are dead. Long live microservices", "X are dead. Long live Y"),
            ("Waterfall is dead. Long live agile", "X is dead. Long live Y"),
            # Pattern: r"(?i)^I spent \d+\s*(hours?|days?|weeks?|months?)"
            ("I spent 3 hours debugging this", "I spent N hours"),
            ("I spent 2 days refactoring", "I spent N days"),
            ("I spent 4 weeks on this feature", "I spent N weeks"),
            ("I spent 6 months building this", "I spent N months"),
            ("I spent 1 hour learning regex", "I spent 1 hour"),
            # Pattern: r"(?i)^most (people|developers?|devs|engineers?) don.t\b"
            ("Most developers don't understand async", "Most developers don't"),
            ("Most people don't know about this", "Most people don't"),
            ("most devs don't care about performance", "most devs don't (lowercase)"),
            ("Most engineers don't test enough", "Most engineers don't"),
            ("Most developer don't read docs", "Most developer don't (singular)"),
            # Pattern: r"(?i)^everyone (says|preaches|thinks|knows|believes)\b"
            ("Everyone says TypeScript is better", "Everyone says"),
            ("Everyone preaches clean code", "Everyone preaches"),
            ("everyone thinks testing is important", "everyone thinks (lowercase)"),
            ("Everyone knows DRY is essential", "Everyone knows"),
            ("Everyone believes in agile", "Everyone believes"),
        ],
    )
    def test_detects_stale_pattern(self, text, pattern_description):
        """Test that has_stale_pattern() returns True for texts matching stale patterns."""
        assert has_stale_pattern(text), f"Failed to detect: {pattern_description}"


# --- Test has_stale_pattern() returns False for clean text ---


class TestCleanTextPassing:
    """Test that has_stale_pattern() returns False for clean, non-stale text."""

    @pytest.mark.parametrize(
        "text",
        [
            # Normal technical observations
            "Spent the afternoon refactoring the auth module",
            "Debugging async race conditions in production",
            "The new caching layer improved response time by 40%",
            "Refactored error handling to use Result types",
            "Learned about distributed tracing today",
            # Text that looks superficially similar but doesn't match
            "AIs can help with code review",  # No space after 'AI'
            "The AIspace is evolving rapidly",  # 'AI' not at start
            "Writing good prompts takes practice",  # Not 'perfect prompts'
            "Today I learned about testing",  # Not "Today's insight/lesson/breakthrough"
            "Some developers prefer dynamic typing",  # Not "Most developers don't"
            "Many people find this useful",  # Not "Everyone says/thinks"
            "Research breakthroughs in NLP",  # Contains 'breakthrough' but as part of compound
            # Edge cases
            "",  # Empty string
            "Hi",  # Very short text
            "Code review feedback: looks good",  # Contains colon but not stale pattern
            "10 changes in 5 files",  # Numbers but not 'commits across' pattern
            "It's about quality",  # 'it's about' but no 'isn't about' before it
        ],
    )
    def test_clean_text_passes(self, text):
        """Test that has_stale_pattern() returns False for clean text."""
        assert not has_stale_pattern(text), f"Incorrectly flagged as stale: {text!r}"


# --- Test STALE_PATTERNS structure ---


class TestStalePatternsStructure:
    """Test that STALE_PATTERNS is properly structured."""

    def test_is_nonempty_list(self):
        """Verify STALE_PATTERNS is a non-empty list."""
        assert isinstance(STALE_PATTERNS, list)
        assert len(STALE_PATTERNS) > 0

    def test_all_entries_are_compiled_regexes(self):
        """Verify all entries in STALE_PATTERNS are compiled regex Pattern objects."""
        for pattern in STALE_PATTERNS:
            assert isinstance(pattern, re.Pattern), f"Expected re.Pattern, got {type(pattern)}"

    def test_expected_pattern_count(self):
        """Verify we have the expected number of patterns."""
        # As of this test file creation, there are 14 patterns
        # Update this if patterns are added/removed
        assert len(STALE_PATTERNS) == 14
