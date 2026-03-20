#!/usr/bin/env python3
"""
Update operations.yaml with current run state from the database.
This ensures the tact maintainer has accurate data for anomaly detection.
"""

import sqlite3
import yaml
from datetime import datetime
from pathlib import Path

def update_operations_yaml():
    """Update operations.yaml with current poll state from database."""

    # Paths
    project_root = Path(__file__).parent.parent
    db_path = project_root / "presence.db"
    ops_path = project_root / ".tact" / "config" / "operations.yaml"

    # Read current poll state from database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT last_poll_time FROM poll_state WHERE id = 1")
    result = cursor.fetchone()
    conn.close()

    if not result:
        print("ERROR: No poll state found in database")
        return False

    last_poll_time = result[0]

    # Read operations.yaml
    with open(ops_path, 'r') as f:
        ops_data = yaml.safe_load(f)

    # Update or add a run entry for run-poll operation
    if 'runs' not in ops_data:
        ops_data['runs'] = []

    # Filter out old run-poll entries and add new one
    ops_data['runs'] = [r for r in ops_data['runs'] if r.get('operationId') != 'run-poll']

    # Add current run
    new_run = {
        'runId': f'run-{int(datetime.now().timestamp() * 1000)}',
        'operationId': 'run-poll',
        'sessionId': 'launchd-automated',
        'startedAt': last_poll_time,
        'completedAt': last_poll_time,
        'status': 'completed',
        'summary': 'Automated poll via launchd (state synced from database)',
        'costUsd': 0.0,
        'agent': {
            'agentType': 'launchd',
            'model': 'n/a'
        }
    }

    ops_data['runs'].append(new_run)

    # Write back to operations.yaml
    with open(ops_path, 'w') as f:
        yaml.dump(ops_data, f, default_flow_style=False, sort_keys=False)

    print(f"Updated operations.yaml with poll state: {last_poll_time}")
    return True

if __name__ == '__main__':
    success = update_operations_yaml()
    exit(0 if success else 1)
