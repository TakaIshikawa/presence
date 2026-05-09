"""Pack deduplication detection accuracy analyzer for dedup effectiveness.

Analyzes the effectiveness of 3-layer deduplication (opening-clause similarity,
stale patterns, semantic embeddings) across execution packs. Measures dedup
detection rate, false positive rate (incorrectly flagged as duplicates),
semantic embedding hit rate vs regex pattern hits, and correlation between
dedup detection and final content quality scores.

Deduplication metrics:
- Dedup detection rate: Percentage of actual duplicates detected
- False positive rate: Incorrectly flagged duplicates
- Semantic embedding hit rate: Embeddings vs regex patterns
- Multi-layer detection: Items caught by multiple layers
- Quality correlation: Dedup detection vs content quality

Optimization indicators:
- High detection rate: Effective duplicate identification
- Low false positive rate: Accurate duplicate detection
- Balanced layer usage: All dedup layers contributing
- Positive quality correlation: Dedup improves content quality
"""

from __future__ import annotations

from typing import Any, Mapping


def analyze_pack_dedup_detection_accuracy(records: object) -> dict[str, Any]:
    """Analyze deduplication detection effectiveness in execution packs.

    Tracks dedup detection rate, measures false positives, calculates layer
    effectiveness, and identifies correlation with content quality.

    Args:
        records: List of pack dictionaries with keys:
            - pack_id: Execution pack identifier
            - total_items: Total items analyzed for duplicates
            - actual_duplicates: Number of actual duplicate items
            - detected_duplicates: Number of duplicates detected
            - false_positives: Items incorrectly flagged as duplicates
            - opening_clause_hits: Detections via opening-clause similarity
            - stale_pattern_hits: Detections via stale pattern regex
            - semantic_embedding_hits: Detections via semantic embeddings
            - multi_layer_hits: Items detected by multiple layers
            - content_quality_score: Optional quality score (0-100)
            - task_title: Optional task title

    Returns:
        Dict with:
            - total_packs: Total number of packs analyzed
            - avg_detection_rate: Average duplicate detection rate
            - avg_false_positive_rate: Average false positive rate
            - avg_semantic_vs_regex_ratio: Semantic embeddings vs regex ratio
            - avg_multi_layer_detection_rate: Multi-layer detection percentage
            - high_accuracy_packs: Packs with >90% detection and <10% FP
            - low_accuracy_packs: Packs with <50% detection or >20% FP
            - opening_clause_total: Total opening-clause detections
            - stale_pattern_total: Total stale pattern detections
            - semantic_embedding_total: Total semantic embedding detections
            - quality_correlation: Dedup detection vs quality correlation

    Raises:
        ValueError: If records is not a list
    """
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    total_packs = 0
    detection_rates: list[float] = []
    false_positive_rates: list[float] = []
    semantic_vs_regex_ratios: list[float] = []
    multi_layer_rates: list[float] = []

    high_accuracy_packs = 0  # >90% detection, <10% FP
    low_accuracy_packs = 0   # <50% detection or >20% FP

    # Layer totals
    opening_clause_total = 0
    stale_pattern_total = 0
    semantic_embedding_total = 0

    # For correlation analysis
    detection_percentages: list[float] = []
    quality_scores: list[float] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_id = _string(record.get("pack_id")) or f"pack_{index}"
        total_items = _extract_int(record.get("total_items"))
        actual_duplicates = _extract_int(record.get("actual_duplicates"))
        detected_duplicates = _extract_int(record.get("detected_duplicates"))
        false_positives = _extract_int(record.get("false_positives"))
        opening_clause_hits = _extract_int(record.get("opening_clause_hits"))
        stale_pattern_hits = _extract_int(record.get("stale_pattern_hits"))
        semantic_embedding_hits = _extract_int(record.get("semantic_embedding_hits"))
        multi_layer_hits = _extract_int(record.get("multi_layer_hits"))
        content_quality_score = _extract_float(record.get("content_quality_score"))

        total_packs += 1

        # Calculate detection rate
        if actual_duplicates is not None and actual_duplicates > 0:
            if detected_duplicates is not None:
                detection_rate = _percentage(detected_duplicates, actual_duplicates)
                detection_rates.append(detection_rate)
                detection_percentages.append(detection_rate)

        # Calculate false positive rate
        if total_items is not None and total_items > 0:
            if false_positives is not None:
                fp_rate = _percentage(false_positives, total_items)
                false_positive_rates.append(fp_rate)

        # Calculate semantic vs regex ratio
        regex_hits = 0
        if opening_clause_hits is not None:
            regex_hits += opening_clause_hits
            opening_clause_total += opening_clause_hits
        if stale_pattern_hits is not None:
            regex_hits += stale_pattern_hits
            stale_pattern_total += stale_pattern_hits

        if semantic_embedding_hits is not None:
            semantic_embedding_total += semantic_embedding_hits

        total_detection_hits = regex_hits + (semantic_embedding_hits or 0)
        if total_detection_hits > 0 and semantic_embedding_hits is not None:
            semantic_ratio = _percentage(semantic_embedding_hits, total_detection_hits)
            semantic_vs_regex_ratios.append(semantic_ratio)

        # Calculate multi-layer detection rate
        if detected_duplicates is not None and detected_duplicates > 0:
            if multi_layer_hits is not None:
                multi_layer_rate = _percentage(multi_layer_hits, detected_duplicates)
                multi_layer_rates.append(multi_layer_rate)

        # Track quality for correlation
        if content_quality_score is not None:
            quality_scores.append(content_quality_score)

        # Classify accuracy
        if detected_duplicates is not None and actual_duplicates is not None and actual_duplicates > 0:
            detection_pct = (detected_duplicates / actual_duplicates) * 100.0
            fp_pct = 0.0
            if total_items is not None and total_items > 0 and false_positives is not None:
                fp_pct = (false_positives / total_items) * 100.0

            if detection_pct > 90.0 and fp_pct < 10.0:
                high_accuracy_packs += 1
            elif detection_pct < 50.0 or fp_pct > 20.0:
                low_accuracy_packs += 1

    # Calculate metrics
    avg_detection_rate = _average(detection_rates)
    avg_fp_rate = _average(false_positive_rates)
    avg_semantic_vs_regex = _average(semantic_vs_regex_ratios)
    avg_multi_layer_rate = _average(multi_layer_rates)

    # Calculate quality correlation
    correlation = _calculate_correlation(detection_percentages, quality_scores)

    return {
        "total_packs": total_packs,
        "avg_detection_rate": avg_detection_rate,
        "avg_false_positive_rate": avg_fp_rate,
        "avg_semantic_vs_regex_ratio": avg_semantic_vs_regex,
        "avg_multi_layer_detection_rate": avg_multi_layer_rate,
        "high_accuracy_packs": high_accuracy_packs,
        "low_accuracy_packs": low_accuracy_packs,
        "opening_clause_total": opening_clause_total,
        "stale_pattern_total": stale_pattern_total,
        "semantic_embedding_total": semantic_embedding_total,
        "quality_correlation": correlation,
    }


def _string(value: object) -> str:
    """Convert value to string, stripping whitespace."""
    return value.strip() if isinstance(value, str) else ""


def _extract_int(value: object) -> int | None:
    """Extract integer from value if available."""
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _extract_float(value: object) -> float | None:
    """Extract float from value if available."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
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


def _calculate_correlation(x_values: list[float], y_values: list[float]) -> float:
    """Calculate Pearson correlation coefficient.

    Returns correlation between -1.0 (negative) and 1.0 (positive).
    Returns 0.0 if insufficient data or no variance.
    """
    if not x_values or not y_values or len(x_values) != len(y_values):
        return 0.0

    n = len(x_values)
    if n < 2:
        return 0.0

    # Calculate means
    mean_x = sum(x_values) / n
    mean_y = sum(y_values) / n

    # Calculate covariance and standard deviations
    covariance = sum((x - mean_x) * (y - mean_y) for x, y in zip(x_values, y_values))
    std_x = (sum((x - mean_x) ** 2 for x in x_values)) ** 0.5
    std_y = (sum((y - mean_y) ** 2 for y in y_values)) ** 0.5

    # Avoid division by zero
    if std_x == 0 or std_y == 0:
        return 0.0

    correlation = covariance / (std_x * std_y)
    return round(correlation, 3)
