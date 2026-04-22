#!/usr/bin/env python3
"""
Update operations.yaml with current run state from the database.
This ensures the tact maintainer has accurate data for anomaly detection.
"""

import argparse
import hashlib
import json
import logging
import sqlite3
import sys
import yaml
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import script_context

logger = logging.getLogger(__name__)

OPERATION_QUERIES = {
    "run-poll": {
        "query": "SELECT last_poll_time FROM poll_state WHERE id = 1",
        "summary": "Automated poll via launchd (state synced from database)",
    },
    "run-daily": {
        "query": "SELECT created_at FROM pipeline_runs WHERE content_type = 'x_thread' ORDER BY created_at DESC LIMIT 1",
        "summary": "Daily digest via launchd (state synced from database)",
    },
    "run-weekly": {
        "query": "SELECT created_at FROM pipeline_runs WHERE content_type = 'blog_post' ORDER BY created_at DESC LIMIT 1",
        "summary": "Weekly blog via launchd (state synced from database)",
    },
}


@dataclass
class OperationsAlertThresholds:
    max_consecutive_publish_failures: int = 3
    max_ingestion_age_minutes: int = 60
    max_queue_backlog_items: int = 10
    evaluation_window_hours: int = 24
    min_evaluation_runs: int = 3
    min_evaluation_pass_rate: float = 0.5


@dataclass
class OperationsWebhookConfig:
    webhook_url: str = ""
    webhook_enabled: bool = False
    webhook_min_level: str = "alert"


def alert_thresholds_from_config(config: Any) -> OperationsAlertThresholds:
    """Build alert thresholds from config.operations.alerts."""
    source = getattr(getattr(config, "operations", None), "alerts", None)
    if source is None:
        return OperationsAlertThresholds()
    return OperationsAlertThresholds(
        max_consecutive_publish_failures=source.max_consecutive_publish_failures,
        max_ingestion_age_minutes=source.max_ingestion_age_minutes,
        max_queue_backlog_items=source.max_queue_backlog_items,
        evaluation_window_hours=source.evaluation_window_hours,
        min_evaluation_runs=source.min_evaluation_runs,
        min_evaluation_pass_rate=source.min_evaluation_pass_rate,
    )


def webhook_config_from_config(config: Any) -> OperationsWebhookConfig:
    """Build webhook delivery config from config.operations.alerts."""
    source = getattr(getattr(config, "operations", None), "alerts", None)
    if source is None:
        return OperationsWebhookConfig()
    return OperationsWebhookConfig(
        webhook_url=getattr(source, "webhook_url", ""),
        webhook_enabled=getattr(source, "webhook_enabled", False),
        webhook_min_level=getattr(source, "webhook_min_level", "alert"),
    )


def sync_operation(cursor, ops_data, operation_id):
    """Sync a single operation's run entry from database to operations.yaml."""
    spec = OPERATION_QUERIES[operation_id]
    cursor.execute(spec["query"])
    result = cursor.fetchone()

    if not result:
        return False

    timestamp = result[0]

    # Replace existing entries for this operation
    ops_data['runs'] = [r for r in ops_data['runs'] if r.get('operationId') != operation_id]

    ops_data['runs'].append({
        'runId': f'run-{int(datetime.now().timestamp() * 1000)}',
        'operationId': operation_id,
        'sessionId': 'launchd-automated',
        'startedAt': timestamp,
        'completedAt': timestamp,
        'status': 'completed',
        'summary': spec["summary"],
        'costUsd': 0.0,
        'agent': {'agentType': 'launchd', 'model': 'n/a'},
    })

    logger.info(f"Synced {operation_id}: {timestamp}")
    return True


def compute_alert_statuses(
    conn: sqlite3.Connection,
    thresholds: OperationsAlertThresholds | None = None,
    now: datetime | None = None,
) -> dict:
    """Compute operational alert statuses for operations.yaml."""
    thresholds = thresholds or OperationsAlertThresholds()
    now = _aware(now or datetime.now(timezone.utc))

    checks = {
        "consecutive_publish_failures": _consecutive_publish_failures(conn, thresholds),
        "stale_ingestion": _stale_ingestion(conn, thresholds, now),
        "queue_backlog": _queue_backlog(conn, thresholds),
        "evaluation_pass_rate": _evaluation_pass_rate(conn, thresholds, now),
    }
    triggered = [
        alert_id
        for alert_id, check in checks.items()
        if check["status"] == "alert"
    ]
    return {
        "status": "alert" if triggered else "ok",
        "generatedAt": now.isoformat(),
        "thresholds": asdict(thresholds),
        "checks": checks,
        "triggered": triggered,
    }


def update_operations_yaml(db_path, operations=None, alert_thresholds=None, now=None):
    """Update operations.yaml with current state from database."""
    project_root = Path(__file__).parent.parent
    ops_path = project_root / ".tact" / "config" / "operations.yaml"

    if operations is None:
        operations = list(OPERATION_QUERIES.keys())

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    with open(ops_path, 'r') as f:
        ops_data = yaml.safe_load(f)

    if 'runs' not in ops_data:
        ops_data['runs'] = []

    synced = 0
    for op_id in operations:
        if sync_operation(cursor, ops_data, op_id):
            synced += 1

    ops_data["alerts"] = compute_alert_statuses(conn, alert_thresholds, now)

    conn.close()

    with open(ops_path, 'w') as f:
        yaml.dump(ops_data, f, default_flow_style=False, sort_keys=False)

    return synced > 0 or bool(ops_data["alerts"])


def build_webhook_payload(summary: dict, source: str, min_level: str = "alert") -> dict:
    """Build a compact webhook payload for unhealthy checks at or above min_level."""
    min_rank = _level_rank(min_level)
    generated_at = summary.get("generatedAt") or summary.get("generated_at")
    alerts = []
    for check_id, check in summary.get("checks", {}).items():
        level = _check_level(check)
        if _level_rank(level) < min_rank:
            continue
        alerts.append(
            {
                "id": check_id,
                "level": level,
                "summary": _check_summary(check),
                "fingerprint": alert_fingerprint(source, check_id, check),
            }
        )
    return {
        "source": source,
        "status": summary.get("status", "ok"),
        "generatedAt": generated_at,
        "alerts": alerts,
    }


def deliver_operations_alerts(
    conn: sqlite3.Connection,
    summary: dict,
    webhook_config: OperationsWebhookConfig,
    *,
    source: str,
    http_timeout: int = 30,
    dry_run: bool = False,
) -> dict:
    """Deliver newly triggered unhealthy checks to the configured webhook."""
    if not webhook_config.webhook_enabled or not webhook_config.webhook_url:
        return {"status": "disabled", "sent": False, "payload": None, "dryRun": dry_run}

    payload = build_webhook_payload(
        summary,
        source=source,
        min_level=webhook_config.webhook_min_level,
    )
    if dry_run:
        return {"status": "dry_run", "sent": False, "payload": payload, "dryRun": True}

    new_alerts = []
    for alert in payload["alerts"]:
        check_key = _metadata_key(source, alert["id"])
        previous = _get_metadata(conn, check_key)
        if previous != alert["fingerprint"]:
            new_alerts.append(alert)

    _clear_resolved_fingerprints(
        conn,
        source=source,
        active_check_ids=[alert["id"] for alert in payload["alerts"]],
    )

    if not new_alerts:
        return {"status": "deduped", "sent": False, "payload": None, "dryRun": dry_run}

    payload = {**payload, "alerts": new_alerts}
    response = requests.post(webhook_config.webhook_url, json=payload, timeout=http_timeout)
    response.raise_for_status()
    for alert in new_alerts:
        _set_metadata(conn, _metadata_key(source, alert["id"]), alert["fingerprint"])
    conn.commit()
    return {"status": "sent", "sent": True, "payload": payload, "dryRun": False}


def alert_fingerprint(source: str, check_id: str, check: dict) -> str:
    """Return a stable fingerprint for one unhealthy check."""
    body = json.dumps(
        {
            "source": source,
            "check_id": check_id,
            "level": _check_level(check),
            "summary": _check_summary(check),
            "value": check.get("value"),
            "threshold": check.get("threshold"),
            "warnings": check.get("warnings", []),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def _consecutive_publish_failures(
    conn: sqlite3.Connection,
    thresholds: OperationsAlertThresholds,
) -> dict:
    if _has_table(conn, "content_publications"):
        rows = _all(
            conn,
            """SELECT status
               FROM content_publications
               WHERE status IN ('published', 'failed')
               ORDER BY COALESCE(last_error_at, published_at, updated_at) DESC, id DESC""",
        )
    elif _has_table(conn, "publish_queue"):
        rows = _all(
            conn,
            """SELECT status
               FROM publish_queue
               WHERE status IN ('published', 'failed')
               ORDER BY COALESCE(published_at, created_at) DESC, id DESC""",
        )
    else:
        rows = []
    failures = 0
    for row in rows:
        if row["status"] != "failed":
            break
        failures += 1

    triggered = failures > thresholds.max_consecutive_publish_failures
    return {
        "status": "alert" if triggered else "ok",
        "value": failures,
        "threshold": thresholds.max_consecutive_publish_failures,
        "summary": (
            f"{failures} consecutive publish failures "
            f"> {thresholds.max_consecutive_publish_failures}"
            if triggered
            else "Consecutive publish failures within threshold"
        ),
    }


def _stale_ingestion(
    conn: sqlite3.Connection,
    thresholds: OperationsAlertThresholds,
    now: datetime,
) -> dict:
    timestamps = []
    for table, column in (
        ("poll_state", "last_poll_time"),
        ("github_commits", "timestamp"),
        ("claude_messages", "timestamp"),
        ("knowledge", "created_at"),
    ):
        if _has_table(conn, table):
            row = _one(conn, f"SELECT MAX({column}) AS timestamp FROM {table}")
            if row and row["timestamp"]:
                timestamps.append(row["timestamp"])

    latest = max((_parse_datetime(ts) for ts in timestamps), default=None)
    if latest is None:
        return {
            "status": "alert",
            "latestAt": None,
            "ageMinutes": None,
            "threshold": thresholds.max_ingestion_age_minutes,
            "summary": "No ingestion state found",
        }

    age_minutes = _age(now, latest).total_seconds() / 60
    triggered = age_minutes > thresholds.max_ingestion_age_minutes
    return {
        "status": "alert" if triggered else "ok",
        "latestAt": latest.isoformat(),
        "ageMinutes": round(age_minutes, 2),
        "threshold": thresholds.max_ingestion_age_minutes,
        "summary": (
            f"Ingestion is stale: {age_minutes:.1f}m "
            f"> {thresholds.max_ingestion_age_minutes}m"
            if triggered
            else "Ingestion freshness within threshold"
        ),
    }


def _queue_backlog(
    conn: sqlite3.Connection,
    thresholds: OperationsAlertThresholds,
) -> dict:
    if not _has_table(conn, "publish_queue"):
        backlog_count = 0
    else:
        row = _one(
            conn,
            """SELECT COUNT(*) AS count
               FROM publish_queue
               WHERE status IN ('queued', 'failed')""",
        )
        backlog_count = row["count"] or 0

    triggered = backlog_count > thresholds.max_queue_backlog_items
    return {
        "status": "alert" if triggered else "ok",
        "value": backlog_count,
        "threshold": thresholds.max_queue_backlog_items,
        "summary": (
            f"Publish queue backlog has {backlog_count} items "
            f"> {thresholds.max_queue_backlog_items}"
            if triggered
            else "Publish queue backlog within threshold"
        ),
    }


def _evaluation_pass_rate(
    conn: sqlite3.Connection,
    thresholds: OperationsAlertThresholds,
    now: datetime,
) -> dict:
    if not _has_table(conn, "pipeline_runs"):
        total = 0
        passed = 0
        pass_rate = None
        triggered = False
        return {
            "status": "ok",
            "passed": passed,
            "total": total,
            "passRate": pass_rate,
            "threshold": thresholds.min_evaluation_pass_rate,
            "windowHours": thresholds.evaluation_window_hours,
            "minimumRuns": thresholds.min_evaluation_runs,
            "summary": "Evaluation pass rate within threshold",
        }

    since = (now - timedelta(hours=thresholds.evaluation_window_hours)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    if _has_column(conn, "pipeline_runs", "outcome") and _has_column(
        conn, "pipeline_runs", "published"
    ):
        pass_expr = "outcome = 'published' OR published = 1"
        filter_expr = "AND outcome != 'dry_run'"
    elif _has_column(conn, "pipeline_runs", "outcome"):
        pass_expr = "outcome = 'published'"
        filter_expr = "AND outcome != 'dry_run'"
    elif _has_column(conn, "pipeline_runs", "published"):
        pass_expr = "published = 1"
        filter_expr = ""
    else:
        pass_expr = "0"
        filter_expr = ""
    row = _one(
        conn,
        f"""SELECT COUNT(*) AS total,
                  SUM(CASE WHEN {pass_expr} THEN 1 ELSE 0 END) AS passed
           FROM pipeline_runs
           WHERE created_at >= ?
             {filter_expr}""",
        (since,),
    )
    total = row["total"] or 0
    passed = row["passed"] or 0
    pass_rate = passed / total if total else None
    triggered = (
        total >= thresholds.min_evaluation_runs
        and pass_rate is not None
        and pass_rate < thresholds.min_evaluation_pass_rate
    )
    return {
        "status": "alert" if triggered else "ok",
        "passed": passed,
        "total": total,
        "passRate": round(pass_rate, 4) if pass_rate is not None else None,
        "threshold": thresholds.min_evaluation_pass_rate,
        "windowHours": thresholds.evaluation_window_hours,
        "minimumRuns": thresholds.min_evaluation_runs,
        "summary": (
            f"Evaluation pass rate is low: {pass_rate * 100:.1f}% "
            f"< {thresholds.min_evaluation_pass_rate * 100:.1f}%"
            if triggered
            else "Evaluation pass rate within threshold"
        ),
    }


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


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    row = _one(
        conn,
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    )
    return row is not None


def _has_column(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = _all(conn, f"PRAGMA table_info({table})")
    return any(row["name"] == column for row in rows)


def _parse_datetime(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return _aware(parsed)


def _aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _age(now: datetime, timestamp: datetime) -> timedelta:
    if timestamp > now:
        return timedelta(0)
    return now - timestamp


def _check_level(check: dict) -> str:
    status = check.get("status", "ok")
    if status == "alert":
        return "alert"
    if status == "warning":
        return "warning"
    return "ok"


def _check_summary(check: dict) -> str:
    if check.get("summary"):
        return str(check["summary"])
    warnings = check.get("warnings") or []
    if warnings:
        return "; ".join(str(warning) for warning in warnings)
    return "Operational check is unhealthy"


def _level_rank(level: str) -> int:
    return {"ok": 0, "warning": 1, "alert": 2}.get(str(level).lower(), 2)


def _ensure_metadata_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """CREATE TABLE IF NOT EXISTS operations_alert_metadata (
           key TEXT PRIMARY KEY,
           value TEXT NOT NULL,
           updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )"""
    )


def _get_metadata(conn: sqlite3.Connection, key: str) -> str | None:
    _ensure_metadata_table(conn)
    row = _one(
        conn,
        "SELECT value FROM operations_alert_metadata WHERE key = ?",
        (key,),
    )
    return row["value"] if row else None


def _set_metadata(conn: sqlite3.Connection, key: str, value: str) -> None:
    _ensure_metadata_table(conn)
    conn.execute(
        """INSERT INTO operations_alert_metadata (key, value, updated_at)
           VALUES (?, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(key) DO UPDATE SET
             value = excluded.value,
             updated_at = excluded.updated_at""",
        (key, value),
    )


def _clear_resolved_fingerprints(
    conn: sqlite3.Connection,
    *,
    source: str,
    active_check_ids: list[str],
) -> None:
    _ensure_metadata_table(conn)
    prefix = f"operations_alert:{source}:"
    rows = _all(
        conn,
        "SELECT key FROM operations_alert_metadata WHERE key LIKE ?",
        (f"{prefix}%",),
    )
    active_keys = {_metadata_key(source, check_id) for check_id in active_check_ids}
    for row in rows:
        if row["key"] not in active_keys:
            conn.execute(
                "DELETE FROM operations_alert_metadata WHERE key = ?",
                (row["key"],),
            )
    conn.commit()


def _metadata_key(source: str, check_id: str) -> str:
    return f"operations_alert:{source}:{check_id}"


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--operation', choices=list(OPERATION_QUERIES.keys()),
                        help='Sync a specific operation (default: all)')
    parser.add_argument(
        "--webhook-dry-run",
        action="store_true",
        help="Build and log the webhook payload without posting or updating dedupe state",
    )
    args = parser.parse_args()

    operations = [args.operation] if args.operation else None

    with script_context() as (config, db):
        success = update_operations_yaml(
            config.paths.database,
            operations,
            alert_thresholds_from_config(config),
        )
        summary = compute_alert_statuses(
            db.conn,
            alert_thresholds_from_config(config),
        )
        result = deliver_operations_alerts(
            db.conn,
            summary,
            webhook_config_from_config(config),
            source="update_operations_state",
            http_timeout=config.timeouts.http_seconds,
            dry_run=args.webhook_dry_run,
        )
        if args.webhook_dry_run:
            print(json.dumps(result["payload"] or {}, indent=2))

    exit(0 if success else 1)
