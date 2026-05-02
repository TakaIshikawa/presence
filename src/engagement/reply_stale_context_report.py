"""Report pending reply drafts with stale relationship context timestamps."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping


DEFAULT_MAX_AGE_DAYS = 14

STATUS_FRESH = "fresh"
STATUS_STALE = "stale"
STATUS_MISSING = "missing"
STATUS_MALFORMED = "malformed"

ACTION_REVIEW = "review"
ACTION_REFRESH = "refresh_context"
ACTION_REPAIR = "repair_context_timestamp"

CONTEXT_TIMESTAMP_COLUMNS = (
    "relationship_context_updated_at",
    "context_updated_at",
)


def build_reply_stale_context_report(
    rows: Iterable[Mapping[str, Any]],
    *,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return a stable JSON-serializable report for reply draft context freshness."""

    if max_age_days <= 0:
        raise ValueError("max_age_days must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    findings = [
        inspect_reply_stale_context(row, max_age_days=max_age_days, now=generated_at)
        for row in rows
    ]
    findings.sort(key=_finding_sort_key)

    return {
        "artifact_type": "reply_stale_context_report",
        "generated_at": generated_at.isoformat(),
        "filters": {
            "max_age_days": max_age_days,
            "status": "pending",
        },
        "counts": {
            "rows_scanned": len(findings),
            STATUS_FRESH: sum(1 for item in findings if item["context_status"] == STATUS_FRESH),
            STATUS_STALE: sum(1 for item in findings if item["context_status"] == STATUS_STALE),
            STATUS_MISSING: sum(1 for item in findings if item["context_status"] == STATUS_MISSING),
            STATUS_MALFORMED: sum(
                1 for item in findings if item["context_status"] == STATUS_MALFORMED
            ),
        },
        "findings": findings,
    }


def inspect_reply_stale_context(
    row: Mapping[str, Any],
    *,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Classify one reply draft row by relationship context timestamp freshness."""

    if max_age_days <= 0:
        raise ValueError("max_age_days must be positive")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    row_dict = _row_dict(row)
    timestamp_column, timestamp_value = _timestamp_value(row_dict)
    parsed = _parse_datetime(timestamp_value)

    if timestamp_value in (None, ""):
        status = STATUS_MISSING
        age_days = None
        severity = "high"
        action = ACTION_REFRESH
    elif parsed is None:
        status = STATUS_MALFORMED
        age_days = None
        severity = "high"
        action = ACTION_REPAIR
    else:
        raw_age_days = max((generated_at - parsed).total_seconds() / 86400, 0.0)
        age_days = round(raw_age_days, 2)
        if raw_age_days > max_age_days:
            status = STATUS_STALE
            severity = "medium"
            action = ACTION_REFRESH
        else:
            status = STATUS_FRESH
            severity = "info"
            action = ACTION_REVIEW

    return {
        "draft_id": _int_or_none(_first_value(row_dict, "id", "draft_id", "reply_queue_id")),
        "mention_id": _string_or_none(
            _first_value(
                row_dict,
                "mention_id",
                "inbound_tweet_id",
                "inbound_id",
                "inbound_cid",
            )
        ),
        "platform": _string_or_none(row_dict.get("platform")) or "x",
        "context_timestamp_field": timestamp_column,
        "context_timestamp": _string_or_none(timestamp_value),
        "context_status": status,
        "age_days": age_days,
        "severity": severity,
        "recommended_action": action,
    }


def format_reply_stale_context_json(report: dict[str, Any]) -> str:
    """Render a reply stale context report as deterministic JSON."""

    return json.dumps(report, indent=2, sort_keys=True)


def _timestamp_value(row: dict[str, Any]) -> tuple[str | None, Any]:
    for column in CONTEXT_TIMESTAMP_COLUMNS:
        if column in row and row.get(column) not in (None, ""):
            return column, row.get(column)
    for column in CONTEXT_TIMESTAMP_COLUMNS:
        if column in row:
            return column, row.get(column)
    return None, None


def _parse_datetime(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    text = str(value).strip()
    if not text:
        return None
    for parser in (
        lambda candidate: datetime.fromisoformat(candidate.replace("Z", "+00:00")),
        lambda candidate: datetime.strptime(candidate, "%Y-%m-%d %H:%M:%S"),
    ):
        try:
            return _as_utc(parser(text))
        except ValueError:
            continue
    return None


def _row_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    if hasattr(row, "keys"):
        return {str(key): row[key] for key in row.keys()}
    return dict(row)


def _first_value(row: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in row and row[key] not in (None, ""):
            return row[key]
    return None


def _string_or_none(value: Any) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _finding_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    severity_rank = {"high": 0, "medium": 1, "info": 2}
    status_rank = {
        STATUS_MALFORMED: 0,
        STATUS_MISSING: 1,
        STATUS_STALE: 2,
        STATUS_FRESH: 3,
    }
    return (
        severity_rank.get(item["severity"], 9),
        status_rank.get(item["context_status"], 9),
        -(item["age_days"] or 0),
        item["platform"],
        item["draft_id"] or 0,
    )


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
