#!/usr/bin/env python3
"""
Update operations.yaml with current run state from the database.
This ensures the tact maintainer has accurate data for anomaly detection.
"""

import argparse
import logging
import sqlite3
import sys
import yaml
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

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


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--operation', choices=list(OPERATION_QUERIES.keys()),
                        help='Sync a specific operation (default: all)')
    args = parser.parse_args()

    operations = [args.operation] if args.operation else None

    with script_context() as (config, db):
        success = update_operations_yaml(
            config.paths.database,
            operations,
            alert_thresholds_from_config(config),
        )

    exit(0 if success else 1)
