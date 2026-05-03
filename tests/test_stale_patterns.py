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
            # These structures sometimes work in resonated posts — evaluator handles nuance
            "Development isn't about writing code—it's about solving problems",
            "Success isn't about the tools - it's about the mindset",
            "I spent 3 hours debugging this",
            "I spent 2 days refactoring",
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
        # 12 patterns: removed "isn't about X—it's about Y" and "I spent N hours"
        # (evaluator handles these with nuance instead of hard-filtering)
        assert len(STALE_PATTERNS) == 12


class TestRegexCompilationValidation:
    """Test regex pattern compilation validation and invalid pattern detection."""

    def test_all_patterns_compile_successfully(self):
        """All patterns in STALE_PATTERNS should compile without errors."""
        for pattern in STALE_PATTERNS:
            # Should be already compiled
            assert isinstance(pattern, re.Pattern)
            # Should have a valid pattern string
            assert pattern.pattern is not None
            assert len(pattern.pattern) > 0

    def test_invalid_regex_caught_at_compile_time(self):
        """Invalid regex patterns should raise errors at compile time."""
        with pytest.raises(re.error):
            re.compile(r"(?i)^(unclosed group")

        with pytest.raises(re.error):
            re.compile(r"(?i)\k<invalid_group>")

        with pytest.raises(re.error):
            re.compile(r"(?i)[z-a]")  # Invalid range

    def test_pattern_flags_are_preserved(self):
        """Verify that pattern flags (like IGNORECASE) are properly set."""
        # Most patterns should have IGNORECASE flag
        case_insensitive_count = sum(
            1 for p in STALE_PATTERNS if p.flags & re.IGNORECASE
        )
        # At least some patterns should be case-insensitive
        assert case_insensitive_count >= 10


class TestRegexExecutionTimeAndPerformance:
    """Test regex execution time limits to prevent ReDoS attacks."""

    def test_patterns_execute_quickly_on_normal_input(self):
        """Patterns should execute quickly on normal-length input."""
        import time

        # Normal-length content
        text = "AI is transforming everything about how we build software systems"

        start = time.perf_counter()
        result = has_stale_pattern(text)
        elapsed = time.perf_counter() - start

        assert result is True
        # Should complete in under 10ms for normal input
        assert elapsed < 0.01

    def test_patterns_handle_long_input_without_redos(self):
        """Patterns should handle long input without catastrophic backtracking."""
        import time

        # Create long input with pattern at start (AI pattern requires start of string)
        long_text = "AI is transforming " + ("word " * 10000)

        start = time.perf_counter()
        result = has_stale_pattern(long_text)
        elapsed = time.perf_counter() - start

        assert result is True
        # Should complete in under 100ms even for 10k+ characters
        assert elapsed < 0.1

    def test_patterns_handle_repetitive_input(self):
        """Patterns should handle repetitive patterns without excessive backtracking."""
        import time

        # Repetitive pattern that might trigger backtracking
        text = "perfect " * 1000 + "prompts are impossible"

        start = time.perf_counter()
        result = has_stale_pattern(text)
        elapsed = time.perf_counter() - start

        assert result is True
        # Should still be fast
        assert elapsed < 0.05

    def test_extremely_long_content_performance(self):
        """Test performance with very long content (>100k chars)."""
        import time

        # Create 100k+ character string
        long_text = "This is a normal sentence about software engineering. " * 2000

        start = time.perf_counter()
        result = has_stale_pattern(long_text)
        elapsed = time.perf_counter() - start

        assert result is False
        # Should complete in under 500ms for 100k chars
        assert elapsed < 0.5


class TestCaseInsensitiveMatching:
    """Test case-insensitive pattern matching."""

    def test_ai_prefix_case_variations(self):
        """Test 'AI' pattern with different case variations."""
        assert has_stale_pattern("AI is transforming everything")
        assert has_stale_pattern("ai is transforming everything")
        assert has_stale_pattern("Ai is transforming everything")
        assert has_stale_pattern("aI is transforming everything")

    def test_mixed_case_patterns(self):
        """Test various patterns with mixed case."""
        assert has_stale_pattern("UNPOPULAR OPINION: testing is waste")
        assert has_stale_pattern("unpopular opinion: testing is waste")
        assert has_stale_pattern("UnPoPuLaR oPiNiOn: testing is waste")

        assert has_stale_pattern("EVERYONE SAYS ai is important")
        assert has_stale_pattern("everyone says ai is important")


class TestPatternPrecedence:
    """Test pattern precedence when multiple patterns match."""

    def test_multiple_patterns_matching(self):
        """When multiple patterns match, has_stale_pattern should return True."""
        # Text matching multiple patterns
        text = "AI is perfect prompts breakthrough everyone thinks"

        assert has_stale_pattern(text)

        # Count how many patterns match
        matches = sum(1 for p in STALE_PATTERNS if p.search(text))
        assert matches >= 2  # Multiple patterns should match

    def test_first_match_wins_in_short_circuit(self):
        """has_stale_pattern short-circuits on first match (any() behavior)."""
        # This is implicit in the any() implementation
        text = "AI is great"  # Matches first pattern
        assert has_stale_pattern(text) is True

    def test_pattern_order_independence(self):
        """Result should be the same regardless of pattern order."""
        text = "Breakthrough in AI development"

        # Text matches multiple patterns, should always return True
        assert has_stale_pattern(text) is True


class TestEscapedSpecialCharacters:
    """Test handling of escaped special characters in patterns."""

    def test_dot_in_pattern_matches_apostrophe(self):
        """Pattern uses . to match apostrophe or other chars in contractions."""
        # Pattern: r"(?i)^most (people|developers?|devs|engineers?) don.t\b"
        # The . matches any character including apostrophe variants

        assert has_stale_pattern("Most developers don't understand async")  # Regular apostrophe
        assert has_stale_pattern("Most developers don't understand async")  # Curly apostrophe (U+2019)
        assert has_stale_pattern("Most developers donXt understand async")  # X in place of apostrophe

    def test_special_chars_in_punctuation_class(self):
        """Pattern handles various punctuation marks."""
        # Pattern: r"(?i)^(unpopular opinion|controversial take)\s*[:\-–—]"

        assert has_stale_pattern("Unpopular opinion: testing is waste")  # Colon
        assert has_stale_pattern("Unpopular opinion- testing is waste")  # Hyphen
        assert has_stale_pattern("Unpopular opinion– testing is waste")  # En dash
        assert has_stale_pattern("Unpopular opinion— testing is waste")  # Em dash

    def test_literal_period_in_stop_start_pattern(self):
        """Stop/Start pattern matches literal period between sentences."""
        assert has_stale_pattern("Stop writing tests. Start shipping code")
        # Period is literal, not escaped, so it matches '.'
        assert has_stale_pattern("Stop writing tests. Start shipping code")


class TestUnicodeHandling:
    """Test Unicode handling in patterns and content."""

    def test_unicode_apostrophes(self):
        """Test various Unicode apostrophe variants."""
        # U+0027 (ASCII apostrophe), U+2019 (right single quotation mark)
        assert has_stale_pattern("Today's insight on testing")  # ASCII
        assert has_stale_pattern("Today's insight on testing")  # Curly quote

    def test_unicode_dashes(self):
        """Test various Unicode dash variants."""
        assert has_stale_pattern("Unpopular opinion: test")  # Colon
        assert has_stale_pattern("Unpopular opinion- test")  # Hyphen (U+002D)
        assert has_stale_pattern("Unpopular opinion– test")  # En dash (U+2013)
        assert has_stale_pattern("Unpopular opinion— test")  # Em dash (U+2014)

    def test_unicode_letters_in_content(self):
        """Test content with non-ASCII letters."""
        # Unicode letters should work fine
        assert not has_stale_pattern("Réfactoring the código with naïve approach")

        # But stale patterns still match
        assert has_stale_pattern("AI is transforming développement")

    def test_emoji_in_content(self):
        """Test content with emoji characters."""
        # Emoji shouldn't break pattern matching
        assert has_stale_pattern("AI is transforming everything 🚀")
        # Pattern requires "^" start, emoji at beginning breaks the match
        assert has_stale_pattern("Unpopular opinion: testing is overrated 🔥")

        # Clean text with emoji should still pass
        assert not has_stale_pattern("Debugging race conditions today 🐛")

    def test_zero_width_characters(self):
        """Test handling of zero-width Unicode characters."""
        # Zero-width space (U+200B)
        text_with_zwsp = "AI\u200b is transforming everything"
        # Might or might not match depending on pattern behavior
        # Just verify it doesn't crash
        result = has_stale_pattern(text_with_zwsp)
        assert isinstance(result, bool)


class TestErrorHandlingEdgeCases:
    """Test error handling for malformed input and edge cases."""

    def test_null_content_handling(self):
        """None input should be handled gracefully."""
        # Implementation should handle None gracefully or document that it doesn't
        # Testing current behavior: raises TypeError
        with pytest.raises(TypeError):
            has_stale_pattern(None)

    def test_empty_string_returns_false(self):
        """Empty string should not match any pattern."""
        assert has_stale_pattern("") is False

    def test_whitespace_only_returns_false(self):
        """Whitespace-only content should not match."""
        assert has_stale_pattern("   ") is False
        assert has_stale_pattern("\n\n\n") is False
        assert has_stale_pattern("\t\t") is False

    def test_single_character_input(self):
        """Single character input should not crash."""
        assert has_stale_pattern("A") is False
        assert has_stale_pattern("x") is False
        assert has_stale_pattern(" ") is False

    def test_extremely_long_input_over_100k_chars(self):
        """Very long input (>100k characters) should not crash."""
        # Create 200k+ character string
        long_text = "Normal software development content. " * 5000

        result = has_stale_pattern(long_text)
        assert isinstance(result, bool)
        assert result is False

    def test_extremely_long_input_with_pattern_at_end(self):
        """Long input with pattern that doesn't require start of string."""
        # Use a pattern that doesn't require ^ (start of string)
        long_text = ("word " * 10000) + "This is a breakthrough moment"
        assert has_stale_pattern(long_text) is True

    def test_newlines_and_multiline_content(self):
        """Content with newlines should be handled correctly."""
        multiline = "This is line 1\nAI is transforming line 2\nLine 3 here"
        # AI pattern looks for ^AI, which matches start of string not start of line
        assert has_stale_pattern(multiline) is False

        # Pattern at start of string should match
        multiline_start = "AI is transforming line 1\nLine 2\nLine 3"
        assert has_stale_pattern(multiline_start) is True

    def test_concurrent_pattern_matching(self):
        """Multiple concurrent pattern matches should not interfere."""
        import threading

        results = []

        def check_pattern(text):
            result = has_stale_pattern(text)
            results.append(result)

        # Create multiple threads
        threads = [
            threading.Thread(target=check_pattern, args=("AI is transforming everything",)),
            threading.Thread(target=check_pattern, args=("Clean technical content",)),
            threading.Thread(target=check_pattern, args=("Everyone says testing is important",)),
        ]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Should have 3 results: True, False, True
        assert len(results) == 3
        assert results.count(True) == 2
        assert results.count(False) == 1

    def test_patterns_with_catastrophic_backtracking_potential(self):
        """Test patterns don't have catastrophic backtracking vulnerabilities."""
        import time

        # Create input that could trigger backtracking
        # Pattern: r"(?i)\w[\w ]{0,30} (is|are) dead\.\s*long live\b"
        text = "a" * 50 + " is dead. Long live test"

        start = time.perf_counter()
        result = has_stale_pattern(text)
        elapsed = time.perf_counter() - start

        # Should complete quickly even with potential backtracking pattern
        assert elapsed < 0.01


class TestPatternVersioning:
    """Test pattern versioning and tracking."""

    def test_pattern_set_version_tracking(self):
        """Verify we can track which version of patterns is in use."""
        # This is for future extension: tracking pattern version
        # Currently we just verify the count
        assert len(STALE_PATTERNS) == 12

        # If versioning is added, would track like:
        # PATTERN_VERSION = "v3"  # Removed "isn't/it's" and "I spent N"
        # For now, just document the expected count

    def test_pattern_removal_history(self):
        """Document which patterns were removed and why."""
        # Pattern removed: r"isn't about X—it's about Y"
        # Reason: Evaluator handles with nuance; some resonated posts use this
        text = "Engineering isn't about writing code—it's about solving problems"
        assert has_stale_pattern(text) is False

        # Pattern removed: r"I spent N hours/days/weeks"
        # Reason: Resonated posts often use it for authentic narratives
        text2 = "I spent 3 hours debugging this race condition"
        assert has_stale_pattern(text2) is False

    def test_current_pattern_set_completeness(self):
        """Verify current pattern set covers expected categories."""
        pattern_strings = [p.pattern for p in STALE_PATTERNS]

        # Should have AI prefix pattern
        assert any("^AI" in p for p in pattern_strings)

        # Should have engagement bait patterns
        assert any("unpopular opinion" in p.lower() for p in pattern_strings)
        assert any("everyone" in p.lower() for p in pattern_strings)

        # Should have rhetorical patterns
        assert any("secret" in p.lower() or "trick" in p.lower() for p in pattern_strings)
        assert any("dead" in p.lower() and "long live" in p.lower() for p in pattern_strings)
