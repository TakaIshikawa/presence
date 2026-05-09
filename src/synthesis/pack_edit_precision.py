"""Pack Edit tool precision analyzer.

Analyzes Edit tool usage precision and effectiveness in execution packs. Measures
old_string uniqueness, edit size distribution, use of replace_all vs targeted edits,
and correlation between edit precision and verification outcomes.

Edit precision metrics:
- Edit success rate: % of Edit calls that succeed
- Old_string uniqueness failures: Failed edits due to non-unique matches
- Average edit size: Mean size of old_string in edits
- Replace_all ratio: % of edits using replace_all vs targeted
- Edit-induced error rate: % of edits that trigger subsequent errors
- Verification correlation: Relationship between edit precision and verify outcomes

Precision patterns:
- High success rate (>95%): Clean, precise edits
- Low uniqueness failures (<5%): Good old_string selection
- Small average edit size (<100 chars): Focused, minimal changes
- Low replace_all ratio (<10%): Targeted edits preferred
- Low error induction (<15%): Edits don't introduce new issues
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_edit_precision(records: object) -> dict[str, Any]:
    """Analyze Edit tool precision and effectiveness in execution packs.

    Evaluates edit quality through success rates, uniqueness checking,
    and correlation with subsequent errors.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - edit_events: List of edit event dicts with:
                - tool_name: "Edit" or "Write"
                - file_path: File being edited
                - old_string_length: Length of old_string parameter
                - new_string_length: Length of new_string parameter
                - replace_all: Boolean indicating replace_all usage
                - outcome: "success", "uniqueness_failure", "error"
                - subsequent_error: Boolean if error occurred after edit
                - verification_passed: Boolean if verification passed after edit

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_total_edits: Average Edit calls per pack
            - avg_edit_success_rate: % of successful edits
            - avg_uniqueness_failure_rate: % of non-unique old_string failures
            - avg_edit_size: Average old_string length
            - avg_replace_all_ratio: % of edits using replace_all
            - avg_edit_error_correlation: % of edits inducing errors
            - avg_verification_pass_rate: % of edits passing verification
            - high_precision_packs: Count with >95% success rate
            - low_precision_packs: Count with <80% success rate
            - edit_size_distribution: Breakdown by edit size categories

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    if not records:
        return _empty_result()

    total_packs = 0
    total_edits_list: list[int] = []
    success_rates: list[float] = []
    uniqueness_failure_rates: list[float] = []
    edit_sizes: list[int] = []
    replace_all_ratios: list[float] = []
    error_correlations: list[float] = []
    verification_pass_rates: list[float] = []

    high_precision_packs = 0  # >95% success
    low_precision_packs = 0   # <80% success

    # Edit size categories
    small_edits = 0  # <50 chars
    medium_edits = 0  # 50-200 chars
    large_edits = 0  # >200 chars

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1

        edit_events = record.get("edit_events")
        if not isinstance(edit_events, list):
            continue

        pack_total_edits = 0
        pack_successful_edits = 0
        pack_uniqueness_failures = 0
        pack_replace_all_count = 0
        pack_error_induced_count = 0
        pack_verification_pass_count = 0

        for event in edit_events:
            if not isinstance(event, Mapping):
                continue

            tool_name = _string(event.get("tool_name"))
            if tool_name not in ["Edit", "Write"]:
                continue

            pack_total_edits += 1

            # Track outcome
            outcome = _string(event.get("outcome"))
            if outcome == "success":
                pack_successful_edits += 1
            elif outcome == "uniqueness_failure":
                pack_uniqueness_failures += 1

            # Track edit size
            old_string_len = _int(event.get("old_string_length"))
            if old_string_len is not None:
                edit_sizes.append(old_string_len)

                # Categorize by size
                if old_string_len < 50:
                    small_edits += 1
                elif old_string_len <= 200:
                    medium_edits += 1
                else:
                    large_edits += 1

            # Track replace_all usage
            replace_all = event.get("replace_all")
            if replace_all is True:
                pack_replace_all_count += 1

            # Track error induction
            subsequent_error = event.get("subsequent_error")
            if subsequent_error is True:
                pack_error_induced_count += 1

            # Track verification outcome
            verification_passed = event.get("verification_passed")
            if verification_passed is True:
                pack_verification_pass_count += 1

        if pack_total_edits > 0:
            total_edits_list.append(pack_total_edits)

            # Calculate pack-level rates
            success_rate = _percentage(pack_successful_edits, pack_total_edits)
            success_rates.append(success_rate)

            uniqueness_rate = _percentage(pack_uniqueness_failures, pack_total_edits)
            uniqueness_failure_rates.append(uniqueness_rate)

            replace_all_ratio = _percentage(pack_replace_all_count, pack_total_edits)
            replace_all_ratios.append(replace_all_ratio)

            error_correlation = _percentage(pack_error_induced_count, pack_total_edits)
            error_correlations.append(error_correlation)

            verification_rate = _percentage(pack_verification_pass_count, pack_total_edits)
            verification_pass_rates.append(verification_rate)

            # Classify pack
            if success_rate > 95:
                high_precision_packs += 1
            elif success_rate < 80:
                low_precision_packs += 1

    # Calculate aggregate metrics
    avg_total_edits = _average([float(e) for e in total_edits_list])
    avg_success_rate = _average(success_rates)
    avg_uniqueness_failure = _average(uniqueness_failure_rates)
    avg_edit_size = _average([float(s) for s in edit_sizes])
    avg_replace_all_ratio = _average(replace_all_ratios)
    avg_error_correlation = _average(error_correlations)
    avg_verification_pass = _average(verification_pass_rates)

    # Format size distribution
    total_edits_with_size = small_edits + medium_edits + large_edits
    size_distribution = [
        {
            "category": "small (<50 chars)",
            "count": small_edits,
            "percentage": _percentage(small_edits, total_edits_with_size),
        },
        {
            "category": "medium (50-200 chars)",
            "count": medium_edits,
            "percentage": _percentage(medium_edits, total_edits_with_size),
        },
        {
            "category": "large (>200 chars)",
            "count": large_edits,
            "percentage": _percentage(large_edits, total_edits_with_size),
        },
    ]

    return {
        "total_packs": total_packs,
        "avg_total_edits": avg_total_edits,
        "avg_edit_success_rate": avg_success_rate,
        "avg_uniqueness_failure_rate": avg_uniqueness_failure,
        "avg_edit_size": avg_edit_size,
        "avg_replace_all_ratio": avg_replace_all_ratio,
        "avg_edit_error_correlation": avg_error_correlation,
        "avg_verification_pass_rate": avg_verification_pass,
        "high_precision_packs": high_precision_packs,
        "low_precision_packs": low_precision_packs,
        "edit_size_distribution": size_distribution,
    }


def _empty_result() -> dict[str, Any]:
    """Return empty result structure."""
    return {
        "total_packs": 0,
        "avg_total_edits": 0.0,
        "avg_edit_success_rate": 0.0,
        "avg_uniqueness_failure_rate": 0.0,
        "avg_edit_size": 0.0,
        "avg_replace_all_ratio": 0.0,
        "avg_edit_error_correlation": 0.0,
        "avg_verification_pass_rate": 0.0,
        "high_precision_packs": 0,
        "low_precision_packs": 0,
        "edit_size_distribution": [],
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace.

    Args:
        value: Value to convert

    Returns:
        String value
    """
    return value.strip() if isinstance(value, str) else ""


def _int(value: object) -> int | None:
    """Convert value to int.

    Args:
        value: Value to convert

    Returns:
        Int value, or None if invalid
    """
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            pass
    return None


def _percentage(numerator: int | float, denominator: int | float) -> float:
    """Calculate percentage, handling zero denominator.

    Args:
        numerator: Numerator value
        denominator: Denominator value

    Returns:
        Percentage value (0.0-100.0)
    """
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[float]) -> float:
    """Calculate average of numeric values.

    Args:
        values: List of numeric values

    Returns:
        Average value
    """
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)
