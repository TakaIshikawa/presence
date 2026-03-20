#!/bin/bash
# Health check script for poll operation
# Returns the last poll time from the database

DB_PATH="${DB_PATH:-./presence.db}"

# Query the last poll time from the database
LAST_POLL=$(sqlite3 "$DB_PATH" "SELECT last_poll_time FROM poll_state WHERE id = 1;" 2>/dev/null)

if [ -z "$LAST_POLL" ]; then
    echo "ERROR: Could not read poll_state from database"
    exit 1
fi

# Output the last poll time for monitoring
echo "last_poll_time=$LAST_POLL"
exit 0
