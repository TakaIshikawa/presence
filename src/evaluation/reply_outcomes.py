"""Reply outcome aggregation helpers."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from statistics import median
from typing import Any, Iterable

REVIEW_EVENT_TYPES = {"approved", "edited", "rejected", "expired"}


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse ISO or SQLite timestamps, returning UTC-aware datetimes."""
    if not value:
        return None
    normalized = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            parsed = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def build_reply_outcome_report(
    rows: Iterable[dict[str, Any]],
    events: Iterable[dict[str, Any]] | None = None,
    *,
    days: int | None = None,
    platform: str | None = None,
    intent: str | None = None,
) -> dict[str, Any]:
    """Aggregate reply_queue rows into stable outcome conversion groups."""
    event_index = _index_events(events or [])
    items = [_reply_item(dict(row), event_index.get(int(row["id"]), [])) for row in rows]
    return {
        "filters": {
            "days": days,
            "platform": platform,
            "intent": intent,
        },
        "overall": _summarize_group("overall", items),
        "by_platform": _group_items(items, "platform"),
        "by_intent": _group_items(items, "intent"),
        "by_priority": _group_items(items, "priority"),
        "by_status": _group_items(items, "status"),
    }


def format_reply_outcome_json(report: dict[str, Any]) -> str:
    """Serialize a report with stable key ordering."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_outcome_text(
    report: dict[str, Any],
    *,
    low_posted_rate: float = 0.5,
    high_dismissal_rate: float = 0.3,
) -> str:
    """Format a compact operator-facing report."""
    overall = report["overall"]
    lines = [
        "",
        "=" * 96,
        "Reply Outcome Report",
        "=" * 96,
        "",
    ]
    filters = _format_filters(report["filters"])
    if filters:
        lines.append(f"Filters: {filters}")
    lines.append(
        "Total: {total}  pending={pending} approved={approved} posted={posted} dismissed={dismissed}".format(
            **overall["counts"]
        )
    )
    lines.append(
        "Rates: posted={posted_rate:.1%} approved={approved_rate:.1%} dismissed={dismissed_rate:.1%} "
        "avg_quality={avg_quality_score}".format(
            **overall["conversion_rates"],
            avg_quality_score=_format_number(overall["avg_quality_score"]),
        )
    )
    lines.append(
        "Timing medians: review={review}h post={post}h".format(
            review=_format_number(overall["timing"]["median_time_to_review_hours"]),
            post=_format_number(overall["timing"]["median_time_to_post_hours"]),
        )
    )
    lines.append("")

    _append_group_table(lines, "By intent", report["by_intent"], annotate=True,
                        low_posted_rate=low_posted_rate, high_dismissal_rate=high_dismissal_rate)
    lines.append("")
    _append_group_table(lines, "By platform", report["by_platform"])
    lines.append("")
    _append_group_table(lines, "By priority", report["by_priority"])
    lines.append("")
    _append_group_table(lines, "By status", report["by_status"])
    return "\n".join(lines)


def _index_events(events: Iterable[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    indexed: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        reply_id = event.get("reply_queue_id")
        if reply_id is None:
            continue
        indexed[int(reply_id)].append(dict(event))
    for reply_events in indexed.values():
        reply_events.sort(
            key=lambda event: (
                parse_timestamp(event.get("created_at")) or datetime.max.replace(tzinfo=timezone.utc),
                int(event.get("id") or 0),
            )
        )
    return indexed


def _reply_item(row: dict[str, Any], events: list[dict[str, Any]]) -> dict[str, Any]:
    detected_at = parse_timestamp(row.get("detected_at"))
    reviewed_at = _event_timestamp(events, review=True) or parse_timestamp(row.get("reviewed_at"))
    posted_at = _event_timestamp(events, posted=True) or parse_timestamp(row.get("posted_at"))
    return {
        "id": row["id"],
        "platform": _value(row.get("platform"), "x"),
        "intent": _value(row.get("intent"), "other"),
        "priority": _value(row.get("priority"), "normal"),
        "status": _value(row.get("status"), "unknown"),
        "quality_score": row.get("quality_score"),
        "time_to_review_hours": _elapsed_hours(detected_at, reviewed_at),
        "time_to_post_hours": _elapsed_hours(detected_at, posted_at),
    }


def _event_timestamp(
    events: list[dict[str, Any]],
    *,
    review: bool = False,
    posted: bool = False,
) -> datetime | None:
    for event in events:
        event_type = event.get("event_type")
        new_status = event.get("new_status")
        if posted and event_type == "posted":
            return parse_timestamp(event.get("created_at"))
        if review and (event_type in REVIEW_EVENT_TYPES or new_status in {"approved", "dismissed"}):
            return parse_timestamp(event.get("created_at"))
    return None


def _elapsed_hours(start: datetime | None, end: datetime | None) -> float | None:
    if not start or not end:
        return None
    seconds = (end - start).total_seconds()
    if seconds < 0:
        return None
    return seconds / 3600


def _group_items(items: list[dict[str, Any]], field: str) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in items:
        grouped[item[field]].append(item)
    return [_summarize_group(key, grouped[key]) for key in sorted(grouped)]


def _summarize_group(name: str, items: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(items)
    status_counts = {
        "pending": 0,
        "approved": 0,
        "posted": 0,
        "dismissed": 0,
    }
    other_statuses: dict[str, int] = {}
    for item in items:
        status = item["status"]
        if status in status_counts:
            status_counts[status] += 1
        else:
            other_statuses[status] = other_statuses.get(status, 0) + 1

    counts = {
        "total": total,
        **status_counts,
        "other": sum(other_statuses.values()),
    }
    return {
        "group": name,
        "counts": counts,
        "status_counts": {**status_counts, **dict(sorted(other_statuses.items()))},
        "conversion_rates": {
            "pending_rate": _rate(counts["pending"], total),
            "approved_rate": _rate(counts["approved"], total),
            "posted_rate": _rate(counts["posted"], total),
            "dismissed_rate": _rate(counts["dismissed"], total),
            "reviewed_rate": _rate(counts["approved"] + counts["posted"] + counts["dismissed"], total),
        },
        "avg_quality_score": _average(item.get("quality_score") for item in items),
        "timing": {
            "median_time_to_review_hours": _median(item.get("time_to_review_hours") for item in items),
            "median_time_to_post_hours": _median(item.get("time_to_post_hours") for item in items),
        },
    }


def _rate(numerator: int, denominator: int) -> float:
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 4)


def _average(values: Iterable[Any]) -> float | None:
    numeric = [float(value) for value in values if value is not None]
    if not numeric:
        return None
    return round(sum(numeric) / len(numeric), 2)


def _median(values: Iterable[Any]) -> float | None:
    numeric = [float(value) for value in values if value is not None]
    if not numeric:
        return None
    return round(float(median(numeric)), 2)


def _value(value: Any, default: str) -> str:
    return str(value or default)


def _format_filters(filters: dict[str, Any]) -> str:
    parts = []
    for key in ("days", "platform", "intent"):
        if filters.get(key) is not None:
            parts.append(f"{key}={filters[key]}")
    return ", ".join(parts)


def _append_group_table(
    lines: list[str],
    title: str,
    groups: list[dict[str, Any]],
    *,
    annotate: bool = False,
    low_posted_rate: float = 0.5,
    high_dismissal_rate: float = 0.3,
) -> None:
    lines.append(title)
    if not groups:
        lines.append("  No rows matched.")
        return
    lines.append(
        f"  {'Group':<18} {'Total':>5} {'Pend':>5} {'Appr':>5} {'Post':>5} {'Dismiss':>7} "
        f"{'Post%':>7} {'Dismiss%':>8} {'Quality':>7} {'Review h':>8} {'Post h':>7}  Flags"
    )
    lines.append("  " + "-" * 94)
    for group in groups:
        counts = group["counts"]
        rates = group["conversion_rates"]
        timing = group["timing"]
        flags = []
        if annotate and counts["total"] > 0:
            if rates["posted_rate"] < low_posted_rate:
                flags.append("LOW_POSTED")
            if rates["dismissed_rate"] > high_dismissal_rate:
                flags.append("HIGH_DISMISSAL")
        lines.append(
            f"  {group['group'][:18]:<18} {counts['total']:>5} {counts['pending']:>5} "
            f"{counts['approved']:>5} {counts['posted']:>5} {counts['dismissed']:>7} "
            f"{rates['posted_rate']:>6.1%} {rates['dismissed_rate']:>7.1%} "
            f"{_format_number(group['avg_quality_score']):>7} "
            f"{_format_number(timing['median_time_to_review_hours']):>8} "
            f"{_format_number(timing['median_time_to_post_hours']):>7}  {', '.join(flags)}"
        )


def _format_number(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}"
