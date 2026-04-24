"""Webhook delivery helpers for operational alert summaries."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any, Callable

import requests


LEVEL_RANKS = {"ok": 0, "warning": 1, "alert": 2}


def build_webhook_payload(summary: dict, source: str, min_level: str = "alert") -> dict:
    """Build a compact webhook payload for unhealthy checks at or above min_level."""
    min_rank = level_rank(min_level)
    generated_at = summary.get("generatedAt") or summary.get("generated_at")
    alerts = []
    for check_id, check in summary.get("checks", {}).items():
        level = check_level(check)
        if level_rank(level) < min_rank:
            continue
        alerts.append(
            {
                "id": check_id,
                "level": level,
                "summary": check_summary(check),
                "fingerprint": alert_fingerprint(source, check_id, check),
            }
        )

    warnings = _payload_warnings(summary, alerts)
    return {
        "source": source,
        "status": summary.get("status", "ok"),
        "generatedAt": generated_at,
        "generated_at": generated_at,
        "warning_count": len(warnings),
        "warnings": warnings,
        "alerts": alerts,
    }


def deliver_webhook_alerts(
    conn: sqlite3.Connection,
    summary: dict,
    *,
    webhook_url: str,
    webhook_enabled: bool,
    webhook_min_level: str,
    source: str,
    http_timeout: int = 30,
    dry_run: bool = False,
    http_post: Callable[..., Any] | None = None,
) -> dict:
    """Deliver newly triggered unhealthy checks to a webhook without raising HTTP errors."""
    if not webhook_enabled or not webhook_url:
        return {"status": "disabled", "sent": False, "payload": None, "dryRun": dry_run}

    if level_rank(summary.get("status", "ok")) < level_rank(webhook_min_level):
        return {
            "status": "below_min_level",
            "sent": False,
            "payload": None,
            "dryRun": dry_run,
        }

    payload = build_webhook_payload(
        summary,
        source=source,
        min_level=webhook_min_level,
    )
    if dry_run:
        return {"status": "dry_run", "sent": False, "payload": payload, "dryRun": True}

    new_alerts = []
    for alert in payload["alerts"]:
        check_key = metadata_key(source, alert["id"])
        previous = get_metadata(conn, check_key)
        if previous != alert["fingerprint"]:
            new_alerts.append(alert)

    clear_resolved_fingerprints(
        conn,
        source=source,
        active_check_ids=[alert["id"] for alert in payload["alerts"]],
    )

    if not new_alerts:
        return {"status": "deduped", "sent": False, "payload": None, "dryRun": dry_run}

    payload = {
        **payload,
        "alerts": new_alerts,
        "warning_count": len(_payload_warnings(summary, new_alerts)),
        "warnings": _payload_warnings(summary, new_alerts),
    }
    post = http_post or requests.post
    try:
        response = post(webhook_url, json=payload, timeout=http_timeout)
        response.raise_for_status()
    except Exception as exc:
        return {
            "status": "failed",
            "sent": False,
            "payload": payload,
            "dryRun": False,
            "error": str(exc),
        }

    for alert in new_alerts:
        set_metadata(conn, metadata_key(source, alert["id"]), alert["fingerprint"])
    conn.commit()
    return {"status": "sent", "sent": True, "payload": payload, "dryRun": False}


def alert_fingerprint(source: str, check_id: str, check: dict) -> str:
    """Return a stable fingerprint for one unhealthy check."""
    body = json.dumps(
        {
            "source": source,
            "check_id": check_id,
            "level": check_level(check),
            "summary": check_summary(check),
            "value": check.get("value"),
            "threshold": check.get("threshold"),
            "warnings": check.get("warnings", []),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def check_level(check: dict) -> str:
    status = check.get("status", "ok")
    if status == "alert":
        return "alert"
    if status == "warning":
        return "warning"
    return "ok"


def check_summary(check: dict) -> str:
    if check.get("summary"):
        return str(check["summary"])
    warnings = check.get("warnings") or []
    if warnings:
        return "; ".join(str(warning) for warning in warnings)
    return "Operational check is unhealthy"


def level_rank(level: str) -> int:
    return LEVEL_RANKS.get(str(level).lower(), 2)


def ensure_metadata_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS operations_alert_metadata (
           key TEXT PRIMARY KEY,
           value TEXT NOT NULL,
           updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )"""
    )


def get_metadata(conn: sqlite3.Connection, key: str) -> str | None:
    ensure_metadata_table(conn)
    row = _one(
        conn,
        "SELECT value FROM operations_alert_metadata WHERE key = ?",
        (key,),
    )
    return row["value"] if row else None


def set_metadata(conn: sqlite3.Connection, key: str, value: str) -> None:
    ensure_metadata_table(conn)
    conn.execute(
        """INSERT INTO operations_alert_metadata (key, value, updated_at)
           VALUES (?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(key) DO UPDATE SET
             value = excluded.value,
             updated_at = excluded.updated_at""",
        (key, value),
    )


def clear_resolved_fingerprints(
    conn: sqlite3.Connection,
    *,
    source: str,
    active_check_ids: list[str],
) -> None:
    ensure_metadata_table(conn)
    prefix = f"operations_alert:{source}:"
    rows = _all(
        conn,
        "SELECT key FROM operations_alert_metadata WHERE key LIKE ?",
        (f"{prefix}%",),
    )
    active_keys = {metadata_key(source, check_id) for check_id in active_check_ids}
    for row in rows:
        if row["key"] not in active_keys:
            conn.execute(
                "DELETE FROM operations_alert_metadata WHERE key = ?",
                (row["key"],),
            )
    conn.commit()


def metadata_key(source: str, check_id: str) -> str:
    return f"operations_alert:{source}:{check_id}"


def _payload_warnings(summary: dict, alerts: list[dict]) -> list[str]:
    if summary.get("warnings"):
        allowed_ids = {alert["id"] for alert in alerts}
        warnings = []
        for check_id, check in summary.get("checks", {}).items():
            if check_id not in allowed_ids:
                continue
            warnings.extend(str(warning) for warning in check.get("warnings", []))
        return warnings or [str(warning) for warning in summary["warnings"]]
    return [str(alert["summary"]) for alert in alerts]


def _one(conn: sqlite3.Connection, query: str, params: tuple = ()) -> sqlite3.Row | None:
    previous = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(query, params).fetchone()
    finally:
        conn.row_factory = previous


def _all(conn: sqlite3.Connection, query: str, params: tuple = ()) -> list[sqlite3.Row]:
    previous = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(query, params).fetchall()
    finally:
        conn.row_factory = previous
