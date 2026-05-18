"""Reconstruct publication attempt sequences and flag risky retry patterns."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
from typing import Any


DEFAULT_MAX_RETRY_GAP_HOURS = 24.0
DEFAULT_LIMIT = 50


def build_publication_attempt_sequence_report(
    rows: list[dict[str, Any]],
    *,
    max_retry_gap_hours: float = DEFAULT_MAX_RETRY_GAP_HOURS,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if max_retry_gap_hours < 0:
        raise ValueError("max_retry_gap_hours must be non-negative")
    if limit <= 0:
        raise ValueError("limit must be positive")

    generated_at = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    sequences = []
    for content_id, attempts in _group_attempts(rows).items():
        ordered = sorted(attempts, key=lambda item: (item["attempted_at_sort"], item["attempt_id"]))
        sequence = _sequence(content_id, ordered, max_retry_gap_hours=max_retry_gap_hours)
        sequences.append(sequence)
    sequences.sort(key=lambda sequence: (-len(sequence["anomalies"]), sequence["content_id"]))
    anomalies = [anomaly for sequence in sequences for anomaly in sequence["anomalies"]]
    anomaly_counts = _count(anomaly["type"] for anomaly in anomalies)
    return {
        "artifact_type": "publication_attempt_sequence",
        "generated_at": generated_at.isoformat(),
        "filters": {"max_retry_gap_hours": max_retry_gap_hours, "limit": limit},
        "summary": {
            "content_items": len(sequences),
            "attempts": sum(len(sequence["attempts"]) for sequence in sequences),
            "anomalies": len(anomalies),
            "anomaly_counts": dict(sorted(anomaly_counts.items())),
        },
        "sequences": sequences[:limit],
        "anomalies": sorted(anomalies, key=lambda item: (item["content_id"], item["attempt_index"], item["type"]))[:limit],
    }


def format_publication_attempt_sequence_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_attempt_sequence_text(report: dict[str, Any]) -> str:
    summary = report["summary"]
    lines = [
        "Publication Attempt Sequence",
        f"Generated: {report['generated_at']}",
        f"Totals: content_items={summary['content_items']} attempts={summary['attempts']} anomalies={summary['anomalies']}",
    ]
    if report["anomalies"]:
        lines.extend(["", "Anomalies:"])
        for anomaly in report["anomalies"]:
            lines.append(
                f"  - content_id={anomaly['content_id']} type={anomaly['type']} "
                f"attempt={anomaly['attempt_id']} detail={anomaly['detail']}"
            )
    return "\n".join(lines)


def _group_attempts(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for index, row in enumerate(rows):
        content_id = _text(_first(row, "content_id", "post_id", "item_id", "candidate_id")) or "unknown"
        attempted_at = _parse_dt(_first(row, "attempted_at", "created_at", "published_at", "updated_at"))
        grouped[content_id].append(
            {
                "attempt_id": _text(_first(row, "attempt_id", "id")) or f"{content_id}:{index}",
                "content_id": content_id,
                "attempted_at": attempted_at.isoformat() if attempted_at else None,
                "attempted_at_sort": attempted_at or datetime.min.replace(tzinfo=timezone.utc),
                "channel": _text(_first(row, "channel", "platform", "target_channel")) or "unknown",
                "expected_channel": _text(_first(row, "expected_channel", "planned_channel", "scheduled_channel")),
                "status": _status(row),
                "error": _text(_first(row, "error", "error_code", "error_signature", "failure_reason")),
            }
        )
    return grouped


def _sequence(content_id: str, attempts: list[dict[str, Any]], *, max_retry_gap_hours: float) -> dict[str, Any]:
    anomalies = []
    seen_success = False
    previous = None
    previous_error = None
    expected_channels = {attempt["expected_channel"] for attempt in attempts if attempt["expected_channel"]}
    observed_channels = {attempt["channel"] for attempt in attempts if attempt["channel"] != "unknown"}
    channel_mismatch = bool(expected_channels and not observed_channels.issubset(expected_channels)) or len(observed_channels) > 1

    public_attempts = []
    for index, attempt in enumerate(attempts):
        if attempt["status"] == "success":
            seen_success = True
        elif seen_success:
            anomalies.append(_anomaly(content_id, attempt, index, "retry_after_success", "attempt occurred after a successful publish"))

        if attempt["status"] == "failed" and attempt["error"] and attempt["error"] == previous_error:
            anomalies.append(_anomaly(content_id, attempt, index, "repeated_same_error", f"same failure repeated: {attempt['error']}"))
        previous_error = attempt["error"] if attempt["status"] == "failed" else None

        if previous and attempt["attempted_at_sort"] and previous["attempted_at_sort"]:
            gap = (attempt["attempted_at_sort"] - previous["attempted_at_sort"]).total_seconds() / 3600
            if gap > max_retry_gap_hours:
                anomalies.append(_anomaly(content_id, attempt, index, "excessive_retry_gap", f"gap_hours={round(gap, 4)}"))

        if channel_mismatch:
            expected = ",".join(sorted(expected_channels)) or "consistent channel"
            observed = ",".join(sorted(observed_channels)) or "unknown"
            anomalies.append(_anomaly(content_id, attempt, index, "channel_mismatch", f"expected={expected} observed={observed}"))
            channel_mismatch = False

        public_attempts.append({key: value for key, value in attempt.items() if key != "attempted_at_sort"})
        previous = attempt

    return {"content_id": content_id, "attempts": public_attempts, "anomalies": anomalies}


def _anomaly(content_id: str, attempt: dict[str, Any], index: int, anomaly_type: str, detail: str) -> dict[str, Any]:
    return {
        "content_id": content_id,
        "attempt_id": attempt["attempt_id"],
        "attempt_index": index,
        "type": anomaly_type,
        "detail": detail,
        "attempted_at": attempt["attempted_at"],
    }


def _status(row: dict[str, Any]) -> str:
    raw = _text(_first(row, "status", "outcome", "result")).lower()
    if raw in {"success", "succeeded", "published", "sent", "ok"}:
        return "success"
    if raw in {"failure", "failed", "error", "rejected"}:
        return "failed"
    if _text(_first(row, "error", "error_code", "error_signature", "failure_reason")):
        return "failed"
    return raw or "unknown"


def _parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _count(values: Any) -> dict[str, int]:
    counts: dict[str, int] = defaultdict(int)
    for value in values:
        counts[value] += 1
    return counts


def _first(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = row.get(key)
        if value not in (None, ""):
            return value
    return None


def _text(value: Any) -> str:
    return str(value).strip() if value is not None else ""
