"""Actionable recovery planning for synthesis pipeline rejections."""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from evaluation.pipeline_rejections import (
    iter_filter_rejections,
    normalize_rejection_reason,
    parse_filter_stats,
)


DEFAULT_DAYS = 14
DEFAULT_LIMIT = 10
REPRESENTATIVE_LIMIT = 3

RECOMMENDATIONS = {
    "hook": "adjust hook",
    "below_threshold": "adjust hook",
    "missing_evidence": "add evidence",
    "claim_check": "add evidence",
    "thread_length": "shorten thread",
    "char_limit": "shorten thread",
    "repetition": "retire pattern",
    "stale_pattern": "retire pattern",
    "topic_saturation": "retire pattern",
    "semantic_dedup": "retire pattern",
    "duplicate": "retire pattern",
    "manual_reject": "rewrite with reviewer notes",
    "manual_revise": "rewrite with reviewer notes",
    "persona_guard": "revise voice",
    "budget": "inspect pipeline configuration",
    "rate_limited": "retry after rate limit window",
    "publish_failed": "inspect publishing failure",
    "unknown": "inspect rejection details",
}


def build_pipeline_rejection_recovery_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    stage: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build grouped recovery recommendations for recent rejected work."""
    if days <= 0:
        raise ValueError("days must be positive")
    if limit <= 0:
        raise ValueError("limit must be positive")

    conn = _connection(db_or_conn)
    now = _aware(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    schema = _schema(conn)

    events: list[dict[str, Any]] = []
    parse_warnings: list[dict[str, Any]] = []
    events.extend(_pipeline_events(conn, schema, cutoff, now, parse_warnings))
    events.extend(_feedback_events(conn, schema, cutoff, now))
    events.extend(_generated_content_events(conn, schema, cutoff, now))
    if stage:
        events = [event for event in events if event["stage"] == stage]

    groups = _group_events(events, limit)
    return {
        "generated_at": now.isoformat(),
        "window_days": days,
        "stage": stage or "all",
        "limit": limit,
        "totals": {
            "events": len(events),
            "groups": len(groups),
            "candidate_content": len(
                {
                    content_id
                    for event in events
                    for content_id in event["candidate_content_ids"]
                }
            ),
            "by_stage": dict(sorted(Counter(event["stage"] for event in events).items())),
        },
        "groups": groups,
        "parse_warnings": parse_warnings,
        "empty_state": {
            "is_empty": not events,
            "schema_present": "pipeline_runs" in schema,
            "message": "No recent pipeline rejections found." if not events else None,
        },
    }


def format_pipeline_rejection_recovery_json(report: dict[str, Any]) -> str:
    """Render a recovery report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_pipeline_rejection_recovery_text(report: dict[str, Any]) -> str:
    """Render a stable human-readable recovery report."""
    lines = [
        "Pipeline rejection recovery report",
        f"Generated: {report['generated_at']}",
        f"Window: {report['window_days']} days",
        f"Stage: {report['stage']}",
        f"Total rejection signals: {report['totals']['events']}",
        "",
    ]

    if report["empty_state"]["is_empty"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)

    lines.append("Signals by stage:")
    for stage, count in report["totals"]["by_stage"].items():
        lines.append(f"- {stage}: {count}")

    lines.extend(["", "Recovery queue:"])
    for group in report["groups"]:
        lines.append(
            "- "
            f"{group['stage']} / {group['reason']}: "
            f"{group['count']} -> {group['recommendation']}"
        )
        if group["candidate_content_ids"]:
            ids = ", ".join(str(item) for item in group["candidate_content_ids"])
            lines.append(f"  candidates: {ids}")
        if group["representative_run_ids"]:
            run_ids = ", ".join(str(item) for item in group["representative_run_ids"])
            lines.append(f"  runs: {run_ids}")
        for example in group["representative_examples"]:
            labels = []
            if example.get("content_id") is not None:
                labels.append(f"content={example['content_id']}")
            if example.get("run_id") is not None:
                labels.append(f"run={example['run_id']}")
            if example.get("batch_id"):
                labels.append(f"batch={example['batch_id']}")
            label = ", ".join(labels) if labels else "example"
            lines.append(f"  example: {label} | {example['summary']}")

    if report["parse_warnings"]:
        lines.extend(["", "Parse warnings:"])
        for warning in report["parse_warnings"]:
            run_label = warning.get("batch_id") or warning.get("run_id") or "unknown"
            lines.append(f"- {run_label}: {warning['message']}")

    return "\n".join(lines)


def _pipeline_events(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
    parse_warnings: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if "pipeline_runs" not in schema:
        return []

    generated_columns = schema.get("generated_content", set())
    join_clause = (
        "LEFT JOIN generated_content gc ON gc.id = pr.content_id"
        if generated_columns
        else ""
    )
    content_expr = _column_expr(generated_columns, "content", "gc")
    published_expr = _column_expr(generated_columns, "published", "gc")
    content_type_expr = _column_expr(generated_columns, "content_type", "gc")

    rows = conn.execute(
        f"""SELECT pr.id AS run_id,
                  pr.batch_id,
                  pr.content_type AS run_content_type,
                  pr.outcome,
                  pr.published AS run_published,
                  pr.final_score,
                  pr.rejection_reason,
                  pr.filter_stats,
                  pr.content_id,
                  pr.created_at,
                  {content_expr} AS content,
                  {published_expr} AS content_published,
                  {content_type_expr} AS content_type
           FROM pipeline_runs pr
           {join_clause}
           ORDER BY pr.created_at ASC, pr.id ASC"""
    ).fetchall()

    events: list[dict[str, Any]] = []
    for row in rows:
        row_dict = dict(row)
        created_at = _parse_timestamp(row_dict.get("created_at")) or now
        if created_at < cutoff or created_at > now or not _is_rejected_run(row_dict):
            continue

        reason = normalize_rejection_reason(
            row_dict.get("rejection_reason"),
            outcome=row_dict.get("outcome"),
            final_score=row_dict.get("final_score"),
        )
        events.append(_event(row_dict, _stage_for_reason(reason), _recovery_reason(reason)))

        warning_objects: list[Any] = []
        parsed_filter_stats = parse_filter_stats(row_dict, warning_objects)
        for warning in warning_objects:
            parse_warnings.append(
                {
                    "run_id": warning.run_id,
                    "batch_id": warning.batch_id,
                    "content_type": warning.content_type,
                    "message": warning.message,
                }
            )
        if parsed_filter_stats:
            for category, _key, count in iter_filter_rejections(parsed_filter_stats):
                for _ in range(count):
                    events.append(
                        _event(
                            row_dict,
                            "filter",
                            _recovery_reason(category),
                            raw_reason=category,
                        )
                    )
    return events


def _feedback_events(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    if "content_feedback" not in schema:
        return []

    content_columns = schema.get("generated_content", set())
    join_clause = (
        "LEFT JOIN generated_content gc ON gc.id = cf.content_id"
        if content_columns
        else ""
    )
    content_expr = _column_expr(content_columns, "content", "gc")
    published_expr = _column_expr(content_columns, "published", "gc")
    content_type_expr = _column_expr(content_columns, "content_type", "gc")

    rows = conn.execute(
        f"""SELECT cf.id AS feedback_id,
                  cf.content_id,
                  cf.feedback_type,
                  cf.notes,
                  cf.replacement_text,
                  cf.created_at,
                  {content_expr} AS content,
                  {published_expr} AS content_published,
                  {content_type_expr} AS content_type
           FROM content_feedback cf
           {join_clause}
           WHERE cf.feedback_type IN ('reject', 'revise')
           ORDER BY cf.created_at ASC, cf.id ASC"""
    ).fetchall()

    events = []
    for row in rows:
        row_dict = dict(row)
        created_at = _parse_timestamp(row_dict.get("created_at")) or now
        if created_at < cutoff or created_at > now:
            continue
        reason = "manual_revise" if row_dict["feedback_type"] == "revise" else "manual_reject"
        note_reason = _recovery_reason(row_dict.get("notes") or row_dict.get("replacement_text"))
        if note_reason != "unknown":
            reason = note_reason
        events.append(_event(row_dict, "manual_feedback", reason))
    return events


def _generated_content_events(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    cutoff: datetime,
    now: datetime,
) -> list[dict[str, Any]]:
    columns = schema.get("generated_content")
    if not columns:
        return []

    selected = [
        "id AS content_id",
        f"{_column_expr(columns, 'content_type')} AS content_type",
        f"{_column_expr(columns, 'content')} AS content",
        f"{_column_expr(columns, 'published', fallback='0')} AS published",
        f"{_column_expr(columns, 'curation_quality')} AS curation_quality",
        f"{_column_expr(columns, 'auto_quality')} AS auto_quality",
        f"{_column_expr(columns, 'eval_feedback')} AS eval_feedback",
        f"{_column_expr(columns, 'created_at')} AS created_at",
    ]
    rows = conn.execute(
        f"""SELECT {', '.join(selected)}
           FROM generated_content
           ORDER BY created_at ASC, id ASC"""
    ).fetchall()

    events = []
    for row in rows:
        row_dict = dict(row)
        created_at = _parse_timestamp(row_dict.get("created_at")) or now
        if created_at < cutoff or created_at > now:
            continue
        reason = None
        if row_dict.get("curation_quality") == "too_specific":
            reason = "missing_evidence"
        elif row_dict.get("auto_quality") == "low_resonance":
            reason = "hook"
        elif int(row_dict.get("published") or 0) == -1:
            reason = "abandoned"
        if reason:
            events.append(_event(row_dict, "content_state", reason))
    return events


def _group_events(events: list[dict[str, Any]], limit: int) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for event in events:
        key = (event["stage"], event["reason"])
        group = groups.setdefault(
            key,
            {
                "stage": event["stage"],
                "reason": event["reason"],
                "count": 0,
                "recommendation": _recommendation(event["reason"]),
                "candidate_content_ids": [],
                "representative_run_ids": [],
                "representative_batch_ids": [],
                "representative_examples": [],
            },
        )
        group["count"] += 1
        _append_unique(group["candidate_content_ids"], event["candidate_content_ids"])
        _append_unique(group["representative_run_ids"], [event["run_id"]])
        _append_unique(group["representative_batch_ids"], [event["batch_id"]])
        if len(group["representative_examples"]) < REPRESENTATIVE_LIMIT:
            group["representative_examples"].append(event["example"])

    return sorted(
        groups.values(),
        key=lambda group: (-group["count"], group["stage"], group["reason"]),
    )[:limit]


def _event(
    row: dict[str, Any],
    stage: str,
    reason: str,
    *,
    raw_reason: str | None = None,
) -> dict[str, Any]:
    content_id = row.get("content_id")
    candidate_ids = []
    if content_id is not None and int(row.get("content_published") or row.get("published") or 0) <= 0:
        candidate_ids.append(int(content_id))
    summary = (
        _snippet(row.get("content"))
        or _snippet(row.get("notes"))
        or _snippet(row.get("rejection_reason"))
        or _snippet(raw_reason)
        or "No representative text recorded."
    )
    return {
        "stage": stage,
        "reason": reason,
        "run_id": row.get("run_id"),
        "batch_id": row.get("batch_id"),
        "candidate_content_ids": candidate_ids,
        "example": {
            "run_id": row.get("run_id"),
            "batch_id": row.get("batch_id"),
            "content_id": int(content_id) if content_id is not None else None,
            "content_type": row.get("content_type") or row.get("run_content_type") or "unknown",
            "summary": summary,
        },
    }


def _recovery_reason(raw: str | None) -> str:
    text = (raw or "").strip().lower()
    if not text:
        return "unknown"
    if "claim" in text or "evidence" in text or "unsupported" in text or "citation" in text:
        return "claim_check" if "claim" in text or "unsupported" in text else "missing_evidence"
    if "thread_validation" in text or "char_limit" in text or "too long" in text or "length" in text:
        return "thread_length" if "thread" in text or "too long" in text else "char_limit"
    if "repetition" in text or "repetitive" in text:
        return "repetition"
    if "stale_pattern" in text or "stale pattern" in text:
        return "stale_pattern"
    if "topic_saturation" in text or "saturated" in text:
        return "topic_saturation"
    if "semantic_dedup" in text or "duplicate" in text:
        return "semantic_dedup" if "semantic" in text else "duplicate"
    if "persona" in text or "voice" in text or "promotional" in text:
        return "persona_guard"
    if "below_threshold" in text or "below threshold" in text or "score" in text or "hook" in text:
        return "below_threshold" if "threshold" in text or "score" in text else "hook"
    if "budget" in text:
        return "budget"
    if "rate_limited" in text or "rate limited" in text:
        return "rate_limited"
    if "publish_failed" in text or "post failed" in text:
        return "publish_failed"
    if text in {"manual_reject", "manual_revise", "abandoned", "unknown"}:
        return text
    return "unknown"


def _stage_for_reason(reason: str) -> str:
    if reason.startswith("filter."):
        return "filter"
    if reason in {"below_threshold"}:
        return "evaluation"
    if reason in {"publish_failed", "rate_limited"}:
        return "publication"
    if reason in {"budget", "dry_run", "all_filtered"}:
        return "pipeline"
    return "pipeline"


def _recommendation(reason: str) -> str:
    return RECOMMENDATIONS.get(reason, RECOMMENDATIONS["unknown"])


def _is_rejected_run(row: dict[str, Any]) -> bool:
    outcome = (row.get("outcome") or "").strip().lower()
    published = row.get("run_published", row.get("published"))
    return bool(
        row.get("rejection_reason")
        or outcome in {"below_threshold", "all_filtered", "dry_run"}
        or (published is not None and int(published) == 0 and outcome != "published")
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
    ).fetchall()
    schema = {}
    for row in rows:
        name = row[0]
        columns = conn.execute(f"PRAGMA table_info({name})").fetchall()
        schema[name] = {column[1] for column in columns}
    return schema


def _column_expr(
    columns: set[str],
    column: str,
    alias: str | None = None,
    fallback: str = "NULL",
) -> str:
    if column not in columns:
        return fallback
    prefix = f"{alias}." if alias else ""
    return f"{prefix}{column}"


def _append_unique(target: list[Any], values: list[Any]) -> None:
    for value in values:
        if value is None or value in target:
            continue
        target.append(value)


def _snippet(value: Any, limit: int = 120) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text:
        return None
    return text if len(text) <= limit else text[: limit - 1].rstrip() + "..."


def _parse_timestamp(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
