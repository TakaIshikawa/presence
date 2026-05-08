"""Agent idle gap detection for timestamped event streams."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any


def analyze_agent_idle_gaps(events: object, threshold_seconds: float = 300.0) -> dict[str, Any]:
    """Compute idle gaps between agent events and flag long intervals."""
    if events is None:
        events = []
    if not isinstance(events, list):
        raise ValueError("events must be a list of dictionaries")
    if threshold_seconds < 0:
        raise ValueError("threshold_seconds must be non-negative")

    normalized = []
    invalid_events = 0
    for index, event in enumerate(events):
        timestamp = _parse_timestamp(event.get("timestamp") if isinstance(event, dict) else None)
        if timestamp is None:
            invalid_events += 1
            continue
        normalized.append(
            {
                "index": index,
                "timestamp": timestamp,
                "label": str(event.get("label", event.get("event", ""))) if isinstance(event, dict) else "",
            }
        )

    normalized.sort(key=lambda item: item["timestamp"])
    gaps: list[dict[str, Any]] = []
    label_counts: Counter[str] = Counter()
    durations: list[float] = []
    for previous, current in zip(normalized, normalized[1:]):
        duration = (current["timestamp"] - previous["timestamp"]).total_seconds()
        durations.append(duration)
        if duration > threshold_seconds:
            label_counts[_gap_label(previous, current)] += 1
            gaps.append(
                {
                    "start_event": _event_summary(previous),
                    "end_event": _event_summary(current),
                    "duration_seconds": round(duration, 3),
                    "duration_minutes": round(duration / 60.0, 3),
                }
            )

    return {
        "event_count": len(normalized),
        "invalid_event_count": invalid_events,
        "gap_count": len(durations),
        "threshold_seconds": threshold_seconds,
        "max_gap_seconds": round(max(durations), 3) if durations else 0.0,
        "average_gap_seconds": round(sum(durations) / len(durations), 3) if durations else 0.0,
        "flagged_gap_count": len(gaps),
        "flagged_gap_label_counts": dict(sorted(label_counts.items())),
        "flagged_intervals": gaps,
    }


def _parse_timestamp(value: object) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _event_summary(event: dict[str, Any]) -> dict[str, Any]:
    return {
        "index": event["index"],
        "timestamp": event["timestamp"].isoformat(),
        "label": event["label"],
    }


def _gap_label(start_event: dict[str, Any], end_event: dict[str, Any]) -> str:
    return f"{start_event['label']} -> {end_event['label']}"
