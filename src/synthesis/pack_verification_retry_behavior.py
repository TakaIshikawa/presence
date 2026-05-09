"""Pack verification retry behavior analyzer.

Analyzes verification retry patterns in execution packs. Tracks how agents handle
verification failures, retry strategies, retry success rates, and excessive retry
detection to identify inefficient verification loops.

Verification retry metrics:
- Total verification attempts: All verification command executions
- First-attempt success rate: Verifications passing on first try
- Retry attempts: Verifications requiring multiple attempts
- Retry success rate: Percentage of retries that eventually succeed
- Excessive retries: Verifications with >3 retry attempts
- Average retries per verification: Mean retry count
- Retry resolution time: Time spent on retry cycles

Quality indicators:
- High first-attempt success (>80%): Most verifications pass initially
- High retry success rate (>70%): Retries effectively fix issues
- Low excessive retries (<10%): Few verification loops
- Low average retries (<1.5): Efficient verification process
- Short retry resolution time (<60s): Quick issue resolution
- Decreasing retry trend: Fewer retries needed over time
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_verification_retry_behavior(records: object) -> dict[str, Any]:
    """Analyze verification retry patterns and efficiency in execution packs.

    Tracks retry behavior and identifies inefficient verification loops.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_verification_attempts: Total verification executions
            - first_attempt_successes: Verifications passing on first try
            - first_attempt_failures: Verifications failing on first try
            - total_retries: Number of retry attempts
            - successful_retries: Retries that eventually passed
            - failed_retries: Retries that continued to fail
            - excessive_retry_count: Verifications with >3 retries
            - avg_retries_per_verification: Average retry count
            - total_retry_time_seconds: Time spent on retries
            - pack_title: Optional pack title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_verification_attempts: Average total verification attempts
            - avg_first_attempt_success_rate: Average % passing on first try
            - avg_retry_count: Average number of retries per pack
            - avg_retry_success_rate: Average % successful retries
            - avg_retries_per_verification: Average retries per verification
            - avg_excessive_retry_rate: Average % excessive retry cases
            - avg_retry_time_seconds: Average time spent on retries
            - high_first_attempt_packs: Count with >85% first-attempt success
            - low_first_attempt_packs: Count with <60% first-attempt success
            - packs_with_excessive_retries: Count with excessive retries
            - efficient_retry_packs: Count with <1.5 avg retries

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    verification_attempts: list[int | float] = []
    first_attempt_success_rates: list[float] = []
    retry_counts: list[int | float] = []
    retry_success_rates: list[float] = []
    retries_per_verification: list[float] = []
    excessive_retry_rates: list[float] = []
    retry_times: list[float] = []

    high_first_attempt_packs = 0  # >85% first-attempt success
    low_first_attempt_packs = 0   # <60% first-attempt success
    packs_with_excessive_retries = 0
    efficient_retry_packs = 0  # <1.5 avg retries

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        total_attempts = _extract_number(record.get("total_verification_attempts"))
        first_successes = _extract_number(record.get("first_attempt_successes"))
        first_failures = _extract_number(record.get("first_attempt_failures"))
        total_retries = _extract_number(record.get("total_retries"))
        successful_retries = _extract_number(record.get("successful_retries"))
        failed_retries = _extract_number(record.get("failed_retries"))
        excessive_retries = _extract_number(record.get("excessive_retry_count"))
        avg_retries = _extract_number(record.get("avg_retries_per_verification"))
        retry_time = _extract_number(record.get("total_retry_time_seconds"))

        # Track total verification attempts
        if total_attempts is not None:
            verification_attempts.append(total_attempts)

        # Calculate first-attempt success rate
        if first_successes is not None and first_failures is not None:
            total_first_attempts = first_successes + first_failures
            if total_first_attempts > 0:
                first_success_rate = _percentage(first_successes, total_first_attempts)
                first_attempt_success_rates.append(first_success_rate)

                if first_success_rate > 85.0:
                    high_first_attempt_packs += 1
                elif first_success_rate < 60.0:
                    low_first_attempt_packs += 1

        # Track retry count
        if total_retries is not None:
            retry_counts.append(total_retries)

        # Calculate retry success rate
        if successful_retries is not None and failed_retries is not None:
            total_retry_attempts = successful_retries + failed_retries
            if total_retry_attempts > 0:
                retry_success_rates.append(_percentage(successful_retries, total_retry_attempts))

        # Track retries per verification
        if avg_retries is not None:
            retries_per_verification.append(avg_retries)

            if avg_retries < 1.5:
                efficient_retry_packs += 1

        # Calculate excessive retry rate
        if excessive_retries is not None and total_attempts is not None and total_attempts > 0:
            excessive_retry_rates.append(_percentage(excessive_retries, total_attempts))

            if excessive_retries > 0:
                packs_with_excessive_retries += 1

        # Track retry time
        if retry_time is not None:
            retry_times.append(retry_time)

    # Calculate aggregate metrics
    avg_attempts = _average(verification_attempts)
    avg_first_success = _average(first_attempt_success_rates)
    avg_retries = _average(retry_counts)
    avg_retry_success = _average(retry_success_rates)
    avg_retries_per_ver = _average(retries_per_verification)
    avg_excessive = _average(excessive_retry_rates)
    avg_retry_time = _average(retry_times)

    return {
        "total_packs": total_packs,
        "avg_verification_attempts": avg_attempts,
        "avg_first_attempt_success_rate": avg_first_success,
        "avg_retry_count": avg_retries,
        "avg_retry_success_rate": avg_retry_success,
        "avg_retries_per_verification": avg_retries_per_ver,
        "avg_excessive_retry_rate": avg_excessive,
        "avg_retry_time_seconds": avg_retry_time,
        "high_first_attempt_packs": high_first_attempt_packs,
        "low_first_attempt_packs": low_first_attempt_packs,
        "packs_with_excessive_retries": packs_with_excessive_retries,
        "efficient_retry_packs": efficient_retry_packs,
    }


def _extract_number(value: object) -> int | float | None:
    """Extract numeric value (int or float) if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator."""
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    """Calculate average of numeric values."""
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
