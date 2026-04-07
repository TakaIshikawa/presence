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
from datetime import datetime
from pathlib import Path

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


def update_operations_yaml(db_path, operations=None):
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

    conn.close()

    if synced > 0:
        with open(ops_path, 'w') as f:
            yaml.dump(ops_data, f, default_flow_style=False, sort_keys=False)

    return synced > 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    parser = argparse.ArgumentParser()
    parser.add_argument('--operation', choices=list(OPERATION_QUERIES.keys()),
                        help='Sync a specific operation (default: all)')
    args = parser.parse_args()

    operations = [args.operation] if args.operation else None

    with script_context() as (config, db):
        success = update_operations_yaml(config.paths.database, operations)

    exit(0 if success else 1)
