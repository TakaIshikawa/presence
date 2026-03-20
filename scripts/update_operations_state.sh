#!/bin/bash
# Update operations.yaml with current run state from the database.
# This ensures the tact maintainer has accurate data for anomaly detection.

set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DB_PATH="$PROJECT_ROOT/presence.db"
OPS_PATH="$PROJECT_ROOT/.tact/config/operations.yaml"

# Get last poll time from database
LAST_POLL=$(sqlite3 "$DB_PATH" "SELECT last_poll_time FROM poll_state WHERE id = 1;" 2>/dev/null)

if [ -z "$LAST_POLL" ]; then
    echo "ERROR: Could not read poll_state from database"
    exit 1
fi

# Generate a run ID based on current timestamp
RUN_ID="run-$(date +%s)000"

# Create a temporary file with the new run entry
TMP_RUN=$(mktemp)
cat > "$TMP_RUN" << EOF
  - runId: $RUN_ID
    operationId: run-poll
    sessionId: launchd-automated
    startedAt: $LAST_POLL
    completedAt: $LAST_POLL
    status: completed
    summary: 'Automated poll via launchd (state synced from database)'
    costUsd: 0.0
    agent:
      agentType: launchd
      model: n/a
EOF

# Read the operations.yaml file and update it
# First, remove any existing run-poll entries from the runs section
# Then append the new entry

# Create a backup
cp "$OPS_PATH" "$OPS_PATH.bak"

# Use awk to process the file
awk -v new_run_file="$TMP_RUN" '
BEGIN { in_runs=0; in_run_poll=0; skip_run=0; runs_found=0 }

# Detect the runs section
/^runs:/ {
    in_runs=1
    print
    next
}

# If we are in runs section
in_runs {
    # Detect start of a run entry
    if (/^  - runId:/) {
        # Check if this is a run-poll entry
        current_run = $0
        getline
        if (/operationId: run-poll/) {
            # Skip this entire run entry
            skip_run = 1
            while (getline && /^    /) {
                # Skip all indented lines of this run
            }
            # The current line is the start of next entry or end of runs
            # Check if we should exit runs section
            if (!/^  / && !/^$/) {
                # We have left the runs section, append our new run
                if (!runs_found) {
                    while ((getline line < new_run_file) > 0) {
                        print line
                    }
                    close(new_run_file)
                    runs_found = 1
                }
                in_runs = 0
                print
            } else if (/^  - runId:/) {
                # This is another run entry, print the line we read
                print $0
            }
            next
        } else {
            # Not a run-poll entry, print both lines
            print current_run
            print
            next
        }
    }

    # Check if we are leaving the runs section
    if (!/^  / && !/^$/ && !/^runs:/) {
        # Append our new run before leaving
        if (!runs_found) {
            while ((getline line < new_run_file) > 0) {
                print line
            }
            close(new_run_file)
            runs_found = 1
        }
        in_runs = 0
    }
}

# Print all other lines
{ print }

END {
    # If we never found a runs section or reached EOF while in runs, append the new run
    if (in_runs && !runs_found) {
        while ((getline line < new_run_file) > 0) {
            print line
        }
        close(new_run_file)
    }
}
' "$OPS_PATH.bak" > "$OPS_PATH"

rm -f "$TMP_RUN" "$OPS_PATH.bak"

echo "Updated operations.yaml with poll state: $LAST_POLL"
