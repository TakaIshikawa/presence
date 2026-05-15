"""Summarize recurring publication failures involving media assets."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any


DEFAULT_LIMIT = 50
_URL_RE = re.compile(r"\b(?:https?://|www\.)\S+", re.I)
_HEX_RE = re.compile(r"\b[0-9a-f]{8,}\b", re.I)
_NUM_RE = re.compile(r"\b\d+\b")
_SPACE_RE = re.compile(r"\s+")


def build_publication_media_failure_patterns_report(
    attempt_rows: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return grouped media failure patterns from publication attempt rows."""
    if limit <= 0:
        raise ValueError("limit must be positive")
    generated_at = _utc(now or datetime.now(timezone.utc))
    groups: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    failure_rows = [row for row in attempt_rows if _is_media_failure(row)]
    retry_successes = _retry_success_index(attempt_rows)
    for row in failure_rows:
        platform = _text(row.get("platform") or "unknown").lower()
        media_type = _text(row.get("media_type") or row.get("asset_type") or "unknown").lower()
        signature = normalize_media_error_signature(row.get("error") or row.get("error_message"))
        retry_outcome = _retry_outcome(row, retry_successes)
        key = (platform, media_type, signature, retry_outcome)
        group = groups.setdefault(
            key,
            {
                "platform": platform,
                "media_type": media_type,
                "error_signature": signature,
                "retry_outcome": retry_outcome,
                "failure_count": 0,
                "retry_count_total": 0,
                "retry_success_count": 0,
                "affected_content_ids": set(),
                "attempt_ids": set(),
                "sample_errors": [],
            },
        )
        group["failure_count"] += 1
        group["retry_count_total"] += _int(row.get("retry_count"))
        if retry_outcome == "succeeded_after_retry":
            group["retry_success_count"] += 1
        content_id = _text(row.get("content_id") or row.get("post_id"))
        attempt_id = _text(row.get("attempt_id") or row.get("id"))
        if content_id:
            group["affected_content_ids"].add(content_id)
        if attempt_id:
            group["attempt_ids"].add(attempt_id)
        if len(group["sample_errors"]) < 3:
            group["sample_errors"].append(_text(row.get("error") or row.get("error_message")))

    patterns = [_finalize(group) for group in groups.values()]
    patterns.sort(key=_sort_key)
    ranked = patterns[:limit]
    totals = {
        "failure_count": len(failure_rows),
        "pattern_count": len(patterns),
        "ranked_count": len(ranked),
        "critical": sum(1 for item in patterns if item["severity"] == "critical"),
        "high": sum(1 for item in patterns if item["severity"] == "high"),
        "medium": sum(1 for item in patterns if item["severity"] == "medium"),
        "low": sum(1 for item in patterns if item["severity"] == "low"),
    }
    return {
        "artifact_type": "publication_media_failure_patterns",
        "generated_at": generated_at.isoformat(),
        "filters": {"limit": limit},
        "totals": totals,
        "patterns": ranked,
        "empty_state": {
            "is_empty": not patterns,
            "message": "No failed media publication attempts found." if not patterns else None,
        },
    }


def build_publication_media_failure_patterns_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    return build_publication_media_failure_patterns_report(_load_attempts(conn, schema), **kwargs)


def normalize_media_error_signature(value: Any) -> str:
    text = _text(value).lower()
    text = _URL_RE.sub("<url>", text)
    text = _HEX_RE.sub("<token>", text)
    text = _NUM_RE.sub("<num>", text)
    text = _SPACE_RE.sub(" ", text).strip(" .")
    return text[:160] or "unknown_error"


def format_publication_media_failure_patterns_json(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_media_failure_patterns_text(report: dict[str, Any]) -> str:
    lines = [
        "Publication Media Failure Patterns",
        f"Generated: {report['generated_at']}",
        (
            "Totals: "
            f"failures={report['totals']['failure_count']} patterns={report['totals']['pattern_count']} "
            f"critical={report['totals']['critical']} high={report['totals']['high']}"
        ),
    ]
    if not report["patterns"]:
        lines.extend(["", report["empty_state"]["message"]])
        return "\n".join(lines)
    lines.extend(["", "Patterns:", "severity  count platform media  retry_rate outcome  signature"])
    for item in report["patterns"]:
        lines.append(
            f"{item['severity']:<9} {item['failure_count']:<5} {item['platform'][:8]:<8} "
            f"{item['media_type'][:6]:<6} {item['retry_success_rate']:<10.2%} "
            f"{item['retry_outcome'][:8]:<8} {item['error_signature'][:70]}"
        )
    return "\n".join(lines)


def _load_attempts(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    for table in ("publication_attempts", "content_publication_attempts"):
        columns = schema.get(table)
        if not columns:
            continue
        selected = [
            "id",
            "content_id" if "content_id" in columns else "NULL AS content_id",
            "platform" if "platform" in columns else "NULL AS platform",
            "media_type" if "media_type" in columns else "asset_type" if "asset_type" in columns else "NULL AS media_type",
            "error" if "error" in columns else "error_message" if "error_message" in columns else "NULL AS error",
            "status" if "status" in columns else "NULL AS status",
            "retry_count" if "retry_count" in columns else "0 AS retry_count",
            "parent_attempt_id" if "parent_attempt_id" in columns else "NULL AS parent_attempt_id",
        ]
        return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table}").fetchall()]
    return []


def _is_media_failure(row: dict[str, Any]) -> bool:
    status = _text(row.get("status")).lower()
    error = _text(row.get("error") or row.get("error_message"))
    media_type = _text(row.get("media_type") or row.get("asset_type"))
    return bool(error and media_type and status not in {"success", "succeeded", "published"})


def _retry_success_index(rows: list[dict[str, Any]]) -> set[str]:
    successes = set()
    for row in rows:
        parent = _text(row.get("parent_attempt_id") or row.get("retry_of_attempt_id"))
        status = _text(row.get("status")).lower()
        if parent and status in {"success", "succeeded", "published"}:
            successes.add(parent)
    return successes


def _retry_outcome(row: dict[str, Any], retry_successes: set[str]) -> str:
    attempt_id = _text(row.get("attempt_id") or row.get("id"))
    if attempt_id and attempt_id in retry_successes:
        return "succeeded_after_retry"
    if _int(row.get("retry_count")) > 0:
        return "failed_after_retry"
    return "not_retried"


def _finalize(group: dict[str, Any]) -> dict[str, Any]:
    retryable = group["retry_success_count"] + (1 if group["retry_outcome"] == "succeeded_after_retry" else 0)
    retry_success_rate = group["retry_success_count"] / group["failure_count"] if group["failure_count"] else 0.0
    severity = _severity(group["failure_count"], retry_success_rate, group["retry_outcome"])
    return {
        "platform": group["platform"],
        "media_type": group["media_type"],
        "error_signature": group["error_signature"],
        "retry_outcome": group["retry_outcome"],
        "failure_count": group["failure_count"],
        "affected_content_ids": sorted(group["affected_content_ids"]),
        "attempt_ids": sorted(group["attempt_ids"]),
        "retry_count_total": group["retry_count_total"],
        "retry_success_count": group["retry_success_count"],
        "retry_success_rate": round(retry_success_rate, 4),
        "severity": severity,
        "sample_errors": group["sample_errors"],
        "recommended_action": _recommendation(severity),
    }


def _severity(count: int, retry_success_rate: float, outcome: str) -> str:
    if count >= 5 and retry_success_rate == 0:
        return "critical"
    if count >= 3 or outcome == "failed_after_retry":
        return "high"
    if count >= 2:
        return "medium"
    return "low"


def _recommendation(severity: str) -> str:
    return {
        "critical": "block publish until media asset handling is fixed",
        "high": "preflight matching media assets before retrying",
        "medium": "review asset dimensions, size, and platform constraints",
        "low": "track recurrence",
    }[severity]


def _sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    rank = {"critical": 3, "high": 2, "medium": 1, "low": 0}
    return (-rank[item["severity"]], -item["failure_count"], item["platform"])


def _int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _text(value: Any) -> str:
    return "" if value is None else str(value)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {row["name"]: {col["name"] for col in conn.execute(f"PRAGMA table_info({row['name']})")} for row in rows}
