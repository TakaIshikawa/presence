"""Tests for deterministic generated-claim support checks."""

from synthesis.claim_checker import ClaimChecker


def test_supported_metric_when_value_and_context_appear_in_sources():
    checker = ClaimChecker()

    result = checker.check(
        "The backoff change cut retry errors by 42%.",
        source_commits=[
            "fix: change backoff and cut retry errors by 42% in the polling worker"
        ],
    )

    assert result.supported is True
    assert len(result.claims) == 1
    assert result.claims[0].kind == "metric"


def test_invented_metric_is_unsupported():
    checker = ClaimChecker()

    result = checker.check(
        "The backoff change cut retry errors by 87%.",
        source_commits=["fix: change backoff for retry errors"],
    )

    assert result.supported is False
    assert result.unsupported_claims[0].kind == "metric"
    assert "metric value not found" in result.unsupported_claims[0].reason


def test_factual_claim_supported_by_linked_knowledge():
    checker = ClaimChecker()

    result = checker.check(
        "Redis added vector indexing for search workloads.",
        linked_knowledge=[
            "Redis added vector indexing for search workloads in the latest release."
        ],
    )

    assert result.supported is True
    assert result.claims[0].kind == "factual"


def test_factual_claim_without_source_terms_is_unsupported():
    checker = ClaimChecker()

    result = checker.check(
        "Postgres removed JSONB indexing.",
        linked_knowledge=["SQLite added JSON functions for application data."],
    )

    assert result.supported is False
    assert result.unsupported_claims[0].kind == "factual"
