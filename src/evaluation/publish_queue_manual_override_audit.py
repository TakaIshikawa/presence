"""Audit publish queue manual overrides for missing context and conflicts."""
from __future__ import annotations
from datetime import timedelta
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "publish_queue_manual_override_audit"
DEFAULT_MAX_AGE_HOURS = 72
DEFAULT_STATUS = "queued,scheduled,pending"
DEFAULT_LIMIT = 100
DEFAULT_LOOKBACK_DAYS = 30


def build_publish_queue_manual_override_audit_report(
    rows: list[dict[str, Any]],
    *,
    max_age_hours: int = DEFAULT_MAX_AGE_HOURS,
    status: str | None = DEFAULT_STATUS,
    lookback_days: int | None = DEFAULT_LOOKBACK_DAYS,
    limit: int = DEFAULT_LIMIT,
    now=None,
    missing_tables=None,
    missing_columns=None,
):
    positive("max_age_hours", max_age_hours)
    if lookback_days is not None:
        positive("lookback_days", lookback_days)
    positive("limit", limit)
    gen = now_value(now)
    cutoff = gen - timedelta(days=lookback_days) if lookback_days is not None else None
    wanted = {lower(part) for part in clean(status).split(",") if lower(part)}
    findings = []

    for row in rows:
        row_status = lower(row.get("status"), "queued")
        if wanted and row_status not in wanted:
            continue

        meta = _meta(row)
        when = dt(
            row.get("override_at")
            or row.get("manual_approved_at")
            or meta.get("override_at")
            or row.get("updated_at")
            or row.get("created_at")
        )
        if cutoff and when and when < cutoff:
            continue

        override_type = clean(
            row.get("override_type")
            or row.get("manual_override_type")
            or meta.get("override_type")
            or meta.get("manual_override")
        )
        override = _truthy(
            row.get("manual_override")
            or row.get("manual_approved")
            or meta.get("manual_override")
            or meta.get("manual_approved")
        )
        actor = clean(
            row.get("override_actor")
            or row.get("manual_actor")
            or row.get("manual_override_actor")
            or meta.get("override_actor")
            or meta.get("actor")
        )
        reason = clean(
            row.get("override_reason")
            or row.get("manual_reason")
            or row.get("manual_override_reason")
            or meta.get("override_reason")
            or meta.get("reason")
        )
        outcome = clean(
            row.get("publication_status")
            or row.get("published_at")
            or meta.get("publication_status")
            or meta.get("outcome")
        )
        if not (override or override_type or actor or reason):
            continue

        age = round((gen - (when or gen)).total_seconds() / 3600, 2)
        checks = [
            ("missing_actor", not actor),
            ("missing_reason", not reason),
            ("missing_publication_outcome", not outcome),
            ("stale_override", age > max_age_hours),
            ("quality_gate_conflict", _failed_gate(row, meta)),
        ]
        for issue, include in checks:
            if include:
                findings.append(
                    {
                        "queue_id": row.get("queue_id") or row.get("id"),
                        "content_id": clean(row.get("content_id")) or None,
                        "override_type": override_type or "manual",
                        "override_actor": actor or None,
                        "override_reason": reason or None,
                        "actor": actor or None,
                        "reason": reason or None,
                        "publication_status": outcome or None,
                        "age_hours": age,
                        "gap_reason": issue,
                        "issue_type": issue,
                    }
                )

    findings.sort(key=lambda item: (_sid(item["queue_id"]), item["issue_type"]))
    shown = findings[:limit]
    schema_gap = bool(missing_tables or missing_columns)
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {
            "max_age_hours": max_age_hours,
            "status": status,
            "lookback_days": lookback_days,
            "limit": limit,
        },
        "summary": {
            "queue_count": len(rows),
            "row_count": len(rows),
            "finding_count": len(findings),
            "shown_count": len(shown),
        },
        "findings": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v},
        "empty_state": empty_state(
            findings,
            "No publish queue manual override gaps found.",
            schema_gap=schema_gap,
        ),
    }


def build_publish_queue_manual_override_audit_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn)
    s = schema(conn)
    mt = []
    mc = {}
    rows = []
    if "publish_queue" not in s:
        mt.append("publish_queue")
    else:
        cols = s["publish_queue"]
        if "id" not in cols:
            mc["publish_queue"] = ["id"]
        else:
            rows = load_table(
                conn,
                "publish_queue",
                cols,
                {
                    "queue_id": ("id", "queue_id"),
                    "content_id": ("content_id",),
                    "status": ("status", "state"),
                    "manual_override": ("manual_override", "manual_approved"),
                    "override_type": ("override_type", "manual_override_type"),
                    "override_actor": ("override_actor", "manual_actor", "manual_override_actor", "approved_by"),
                    "override_reason": (
                        "override_reason",
                        "manual_reason",
                        "manual_override_reason",
                        "approval_reason",
                    ),
                    "publication_status": ("publication_status", "status"),
                    "published_at": ("published_at",),
                    "override_at": ("override_at", "manual_approved_at"),
                    "updated_at": ("updated_at",),
                    "created_at": ("created_at",),
                    "quality_status": ("quality_status", "quality_gate_status"),
                    "metadata": ("metadata", "quality_metadata"),
                },
            )
    return build_publish_queue_manual_override_audit_report(rows, missing_tables=mt, missing_columns=mc, **kw)


def _meta(row):
    try:
        value = clean(row.get("metadata"))
        parsed = json.loads(value) if value else {}
        return parsed if isinstance(parsed, dict) else {}
    except json.JSONDecodeError:
        return {}


def _truthy(value):
    return lower(value) in {"1", "true", "yes", "y", "approved", "override", "manual", "status"} or value is True


def _failed_gate(row, meta):
    return lower(row.get("quality_status") or meta.get("quality_status") or meta.get("quality_gate_status")) in {
        "failed",
        "fail",
        "blocked",
        "rejected",
    } or bool(meta.get("failed_quality_gates"))


def format_publish_queue_manual_override_audit_json(report):
    return json_dumps(report)


def format_publish_queue_manual_override_audit_text(report):
    lines = [
        "Publish Queue Manual Override Audit",
        f"Generated: {report['generated_at']}",
        f"Totals: rows={report['summary']['row_count']} findings={report['summary']['finding_count']} shown={report['summary']['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["findings"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    for item in report["findings"]:
        lines.append(
            f"- queue={item['queue_id']} type={item['override_type']} issue={item['issue_type']} "
            f"actor={item['actor'] or '-'} age={item['age_hours']}"
        )
    return "\n".join(lines)


def _sid(value):
    try:
        return (0, int(value))
    except (TypeError, ValueError):
        return (1, clean(value))
