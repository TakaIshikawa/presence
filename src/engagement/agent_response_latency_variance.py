"""Agent response latency variance analyzer for engagement reports."""

from __future__ import annotations

from typing import Any, Mapping


def analyze_agent_response_latency_variance(records: object) -> dict[str, Any]:
    """Detect sessions with high variance in agent response times."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of response time dictionaries")

    if not records:
        return {
            "total_responses": 0,
            "min_latency": 0.0,
            "max_latency": 0.0,
            "mean_latency": 0.0,
            "variance_ratio": 0.0,
            "degradation_detected": False,
            "examples": [],
        }

    latencies: list[float] = []
    turn_indices: list[int] = []

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        latency = _latency(record)
        if latency is not None and latency >= 0:
            latencies.append(latency)
            turn_indices.append(_turn_index(record, index))

    if not latencies:
        return {
            "total_responses": len(records),
            "min_latency": 0.0,
            "max_latency": 0.0,
            "mean_latency": 0.0,
            "variance_ratio": 0.0,
            "degradation_detected": False,
            "examples": [],
        }

    min_latency = min(latencies)
    max_latency = max(latencies)
    mean_latency = sum(latencies) / len(latencies)
    variance_ratio = max_latency / min_latency if min_latency > 0 else 0.0

    # Check for degradation pattern (early fast, later slow)
    degradation_detected = False
    examples: list[dict[str, Any]] = []

    if len(latencies) >= 4:
        # Split into early and late responses
        mid = len(latencies) // 2
        early_latencies = latencies[:mid]
        late_latencies = latencies[mid:]

        early_mean = sum(early_latencies) / len(early_latencies)
        late_mean = sum(late_latencies) / len(late_latencies)

        # Degradation if late responses are significantly slower (>2x)
        if late_mean > early_mean * 2:
            degradation_detected = True
            _append_example(
                examples,
                "degradation",
                f"early mean {early_mean:.1f}s, late mean {late_mean:.1f}s ({late_mean/early_mean:.1f}x slower)"
            )

    # Flag high variance (>3x between fastest and slowest)
    if variance_ratio > 3.0:
        _append_example(
            examples,
            "high_variance",
            f"fastest {min_latency:.1f}s, slowest {max_latency:.1f}s ({variance_ratio:.1f}x variance)"
        )

    # Add examples of slowest responses
    if latencies:
        # Find slowest response
        slowest_idx = latencies.index(max_latency)
        _append_example(
            examples,
            "slowest_response",
            f"turn {turn_indices[slowest_idx]}: {max_latency:.1f}s"
        )

    return {
        "total_responses": len(latencies),
        "min_latency": round(min_latency, 2),
        "max_latency": round(max_latency, 2),
        "mean_latency": round(mean_latency, 2),
        "variance_ratio": round(variance_ratio, 2),
        "degradation_detected": degradation_detected,
        "examples": examples[:5],
    }


def _latency(record: Mapping[str, Any]) -> float | None:
    """Extract latency from record."""
    for key in ("latency", "response_time", "duration", "latency_seconds"):
        value = record.get(key)
        if isinstance(value, bool):
            continue
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                continue
    return None


def _turn_index(record: Mapping[str, Any], fallback: int) -> int:
    """Extract turn index from record."""
    value = record.get("turn_index") or record.get("turnIndex")
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return fallback


def _append_example(
    examples: list[dict[str, Any]],
    reason: str,
    details: str
) -> None:
    """Add example if under limit."""
    if len(examples) < 5:
        examples.append({
            "reason": reason,
            "details": details,
        })
