#!/bin/bash
# Sync poll state from database to operations.yaml
# This ensures the tact maintainer has accurate data for anomaly detection

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB_PATH="$PROJECT_ROOT/presence.db"
OPS_PATH="$PROJECT_ROOT/.tact/config/operations.yaml"

# Get last poll time from database
LAST_POLL=$(sqlite3 "$DB_PATH" "SELECT last_poll_time FROM poll_state WHERE id = 1;" 2>/dev/null || echo "")

if [ -z "$LAST_POLL" ]; then
    # Database not initialized yet or empty, skip update
    exit 0
fi

# Create temporary file with updated operations.yaml
TMP_FILE=$(mktemp)

# Use sed to update the startedAt and completedAt fields in the last run-poll entry
# This is a simple approach - we update the most recent run-poll entry
awk -v timestamp="$LAST_POLL" '
BEGIN { in_poll_run=0; last_poll_line=0 }

# Track if we are in a run-poll entry
/operationId: run-poll/ {
    in_poll_run=1
    last_poll_line=NR
}

# Update timestamps if we are in the most recent poll run
in_poll_run && /startedAt:/ {
    print "    startedAt: " timestamp
    next
}

in_poll_run && /completedAt:/ {
    print "    completedAt: " timestamp
    in_poll_run=0
    next
}

# Reset when we hit a new run entry
/^  - runId:/ && in_poll_run && NR > last_poll_line + 1 {
    in_poll_run=0
}

# Print all other lines unchanged
{ print }
' "$OPS_PATH" > "$TMP_FILE"

# Replace the original file
mv "$TMP_FILE" "$OPS_PATH"

# Silent success
exit 0
