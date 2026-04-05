"""Tests for stale rhetorical pattern detection in few-shot filtering."""

import pytest

from synthesis.few_shot import _has_stale_pattern
from synthesis.pipeline import SynthesisPipeline


# ---------------------------------------------------------------------------
# Helper: run the same text against both the few_shot module-level patterns
# and the pipeline class-level patterns, verifying they agree.
# ---------------------------------------------------------------------------


def _both_detect(text: str) -> bool:
    """Return True only if BOTH pattern lists flag the text as stale."""
    few_shot_hit = _has_stale_pattern(text)
    pipeline_hit = any(p.search(text) for p in SynthesisPipeline.STALE_PATTERNS)
    assert few_shot_hit == pipeline_hit, (
        f"Pattern lists diverge on: {text!r} "
        f"(few_shot={few_shot_hit}, pipeline={pipeline_hit})"
    )
    return few_shot_hit


# ===========================================================================
# 1. Unpopular opinion / Controversial take
# ===========================================================================


class TestUnpopularOpinionPattern:
    @pytest.mark.parametrize("text", [
        "Unpopular opinion: most AI wrappers are fine",
        "unpopular opinion - LLMs peaked last year",
        "Unpopular opinion — nobody cares about your framework",
        "UNPOPULAR OPINION: hot takes only",
        "Controversial take: TypeScript is overrated",
        "controversial take - testing is a waste of time",
        "Controversial take — microservices hurt more than they help",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "I have an unpopular opinion about error handling",
        "That's a controversial take on caching strategies",
        "My opinion is unpopular among backend devs",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 2. Nobody talks about / Nobody is talking about
# ===========================================================================


class TestNobodyTalksAboutPattern:
    @pytest.mark.parametrize("text", [
        "Nobody talks about the cost of context switching",
        "Nobody is talking about how fragile CI pipelines are",
        "nobody mentions the memory overhead",
        "Nobody talks about error budgets enough",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "I noticed nobody at the standup raised the latency issue",
        "The talk about distributed systems was great",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 3. The secret to / The trick to
# ===========================================================================


class TestSecretTrickPattern:
    @pytest.mark.parametrize("text", [
        "The secret to good prompts is specificity",
        "The trick to fast deploys is caching layers",
        "THE SECRET TO reliable agents is structured output",
        "The trick to debugging race conditions",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "I learned the secret to this module's behavior by reading the source",
        "There's a neat trick to rebase without conflicts",
        "Discovered the secret behind the flaky test",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 4. Stop doing X. Start doing Y.
# ===========================================================================


class TestStopStartPattern:
    @pytest.mark.parametrize("text", [
        "Stop writing unit tests. Start writing integration tests.",
        "Stop using REST. Start using GraphQL.",
        "Stop chasing metrics. Start shipping value.",
        "stop refactoring everything. start shipping.",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "We had to stop the deploy and start the rollback",
        "I decided to stop and rethink the architecture",
        "The service will start after the migration completes",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 5. X is dead. Long live Y.
# ===========================================================================


class TestIsDeadLongLivePattern:
    @pytest.mark.parametrize("text", [
        "REST is dead. Long live GraphQL.",
        "Monoliths are dead. Long live microservices.",
        "jQuery is dead. Long live vanilla JS.",
        "OOP is dead. Long live functional programming.",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "The process is dead after an OOM kill",
        "Long live the king of merge conflicts",
        "That branch is dead code we should remove",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 6. I spent X hours/days/weeks (effort-brag framing)
# ===========================================================================


class TestEffortBragPattern:
    @pytest.mark.parametrize("text", [
        "I spent 10 hours debugging a single test",
        "I spent 3 days rewriting the auth module",
        "I spent 2 weeks building an agent framework",
        "I spent 6 months on this side project",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "The team spent 3 days on the migration",
        "After I spent the afternoon pairing, we found the bug",
        "We spent 2 hours in a design review",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# 7. Most people don't / Most developers don't
# ===========================================================================


class TestMostPeopleDontPattern:
    @pytest.mark.parametrize("text", [
        "Most people don't understand event loops",
        "Most developers don't test their error paths",
        "Most devs don't read the docs",
        "Most engineers don't profile before optimizing",
        "most people don't realize how slow DNS can be",
    ])
    def test_matches(self, text):
        assert _both_detect(text)

    @pytest.mark.parametrize("text", [
        "I think most people would agree this API is clunky",
        "Most of the developers on our team prefer Rust",
        "Unlike most people, I enjoy writing Makefiles",
    ])
    def test_no_false_positive(self, text):
        assert not _both_detect(text)


# ===========================================================================
# Existing patterns still work (regression)
# ===========================================================================


class TestExistingPatternsRegression:
    @pytest.mark.parametrize("text", [
        "AI is changing everything",
        "Coding isn't about syntax—it's about thinking",
        "This is a major breakthrough for LLMs",
        "perfect prompts are a myth",
        "42 commits across 8 repos",
        "Today's insight on agent design",
    ])
    def test_existing_patterns_still_match(self, text):
        assert _both_detect(text)
