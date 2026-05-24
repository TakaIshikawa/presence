"""Identify bounced newsletter subscribers with recovery signals."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
from typing import Any

from ._report_utils import clean, connection, dt, expr, iso, json_dumps, lower, now_iso, positive, schema


ARTIFACT_TYPE = "newsletter_bounce_recovery_candidates"
DEFAULT_LIMIT = 50
BOUNCE_TYPES = {"bounce", "bounced", "hard_bounce", "soft_bounce", "complaint"}
ENGAGEMENT_TYPES = {"open", "opened", "click", "clicked"}
RECOVERABLE_STATUSES = {"active", "confirmed", "subscribed", "resubscribed"}


def build_newsletter_bounce_recovery_candidates_report(
    subscribers: list[dict[str, Any]],
    events: list[dict[str, Any]],
    *,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    positive("limit", limit)
    by_subscriber = {str(row.get("subscriber_id")): row for row in subscribers if row.get("subscriber_id") is not None}
    state: dict[str, dict[str, Any]] = {
        key: {"subscriber": row, "last_bounce": None, "bounce_type": None, "last_engagement": None, "signal": None}
        for key, row in by_subscriber.items()
    }
    for event in events:
        sid = str(event.get("subscriber_id"))
        if sid not in state:
            continue
        event_type = lower(event.get("event_type"))
        when = dt(event.get("event_at"))
        if when is None:
            continue
        if event_type in BOUNCE_TYPES or "bounce" in event_type:
            if state[sid]["last_bounce"] is None or when > state[sid]["last_bounce"]:
                state[sid]["last_bounce"] = when
                state[sid]["bounce_type"] = clean(event.get("bounce_type")) or event_type
        elif event_type in ENGAGEMENT_TYPES:
            if state[sid]["last_engagement"] is None or when > state[sid]["last_engagement"]:
                state[sid]["last_engagement"] = when
                state[sid]["signal"] = event_type

    findings: list[dict[str, Any]] = []
    for sid, item in state.items():
        sub = item["subscriber"]
        bounced = item["last_bounce"]
        if bounced is None:
            continue
        status_changed_at = dt(sub.get("status_changed_at") or sub.get("updated_at"))
        status = lower(sub.get("status"))
        engagement = item["last_engagement"]
        signal_at = engagement
        signal = item["signal"]
        if status in RECOVERABLE_STATUSES and status_changed_at and status_changed_at > bounced:
            if signal_at is None or status_changed_at > signal_at:
                signal_at = status_changed_at
                signal = f"status:{status}"
        if signal_at is None or signal_at <= bounced:
            continue
        email = clean(sub.get("email")) or None
        findings.append(
            {
                "subscriber_id": sid,
                "email": email,
                "domain": (email.split("@", 1)[1].lower() if email and "@" in email else clean(sub.get("domain")) or None),
                "bounce_type": item["bounce_type"] or "bounce",
                "last_bounced_at": bounced.isoformat(),
                "last_engaged_at": signal_at.isoformat(),
                "recovery_signal": signal,
                "recommended_action": "verify engagement and remove from suppression" if signal and signal.startswith("status:") else "send reactivation confirmation",
            }
        )
    findings.sort(key=lambda row: (row["last_engaged_at"], row["subscriber_id"]), reverse=True)
    shown = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": now_iso(now),
        "thresholds": {"limit": limit},
        "summary": {
            "subscriber_count": len(subscribers),
            "event_count": len(events),
            "candidate_count": len(findings),
            "shown_count": len(shown),
            "by_bounce_type": dict(sorted(Counter(row["bounce_type"] for row in findings).items())),
        },
        "candidates": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
    }


def build_newsletter_bounce_recovery_candidates_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    db_schema = schema(conn)
    missing_tables = [t for t in ("newsletter_subscribers",) if t not in db_schema]
    event_tables = [t for t in ("newsletter_events", "newsletter_provider_events") if t in db_schema]
    if not event_tables:
        missing_tables.append("newsletter_events|newsletter_provider_events")
    missing_columns: dict[str, list[str]] = {}
    subscribers = _load_subscribers(conn, db_schema, missing_columns) if "newsletter_subscribers" in db_schema else []
    events: list[dict[str, Any]] = []
    for table in event_tables:
        events.extend(_load_events(conn, table, db_schema[table], missing_columns))
    return build_newsletter_bounce_recovery_candidates_report(subscribers, events, missing_tables=missing_tables, missing_columns=missing_columns, **kwargs)


def format_newsletter_bounce_recovery_candidates_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_bounce_recovery_candidates_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Bounce Recovery Candidates",
        f"Generated: {report['generated_at']}",
        f"Totals: subscribers={report['summary']['subscriber_count']} candidates={report['summary']['candidate_count']} shown={report['summary']['shown_count']}",
    ]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + "; ".join(f"{t}({', '.join(c)})" for t, c in report["missing_columns"].items()))
    if not report["candidates"]:
        lines.append("No recoverable bounced subscribers found.")
        return "\n".join(lines)
    lines.extend(["", "subscriber_id | email | domain | bounce_type | last_bounced_at | last_engaged_at | recommended_action"])
    for row in report["candidates"]:
        lines.append(f"{row['subscriber_id']} | {row['email'] or '-'} | {row['domain'] or '-'} | {row['bounce_type']} | {row['last_bounced_at']} | {row['last_engaged_at']} | {row['recommended_action']}")
    return "\n".join(lines)


def _load_subscribers(conn: Any, db_schema: dict[str, set[str]], missing: dict[str, list[str]]) -> list[dict[str, Any]]:
    cols = db_schema["newsletter_subscribers"]
    required = {"id"}
    if not required <= cols:
        missing["newsletter_subscribers"] = sorted(required - cols)
        return []
    selected = [
        "id AS subscriber_id",
        expr(cols, "email", default="NULL", out="email"),
        expr(cols, "domain", default="NULL", out="domain"),
        expr(cols, "status", default="'unknown'", out="status"),
        expr(cols, "status_changed_at", "updated_at", default="NULL", out="status_changed_at"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM newsletter_subscribers ORDER BY id")]


def _load_events(conn: Any, table: str, cols: set[str], missing: dict[str, list[str]]) -> list[dict[str, Any]]:
    sid = next((c for c in ("subscriber_id", "newsletter_subscriber_id") if c in cols), None)
    event_type = next((c for c in ("event_type", "type", "name") if c in cols), None)
    event_at = next((c for c in ("event_at", "occurred_at", "created_at", "timestamp") if c in cols), None)
    if not sid or not event_type or not event_at:
        missing[table] = sorted({"subscriber_id", "event_type", "event_at"} - cols)
        return []
    selected = [
        f"{sid} AS subscriber_id",
        f"{event_type} AS event_type",
        f"{event_at} AS event_at",
        expr(cols, "bounce_type", "bounce_class", default="NULL", out="bounce_type"),
    ]
    return [dict(row) for row in conn.execute(f"SELECT {', '.join(selected)} FROM {table} ORDER BY {event_at}, rowid")]

