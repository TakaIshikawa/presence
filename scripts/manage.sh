#!/bin/bash
# Presence automation manager

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"

AGENTS=(
    "com.presence.poll"
    "com.presence.daily"
    "com.presence.weekly"
)

usage() {
    echo "Usage: $0 {start|stop|restart|status|logs|run|retry}"
    echo ""
    echo "Commands:"
    echo "  start   - Load and start all automation jobs"
    echo "  stop    - Unload and stop all automation jobs"
    echo "  restart - Stop then start all jobs"
    echo "  status  - Show status of all jobs"
    echo "  logs    - Tail all log files"
    echo "  run     - Run a specific job now (poll|daily|weekly)"
    echo "  retry   - Retry posting unpublished content"
    exit 1
}

start() {
    echo "Starting Presence automation..."
    mkdir -p "$PROJECT_DIR/logs"

    for agent in "${AGENTS[@]}"; do
        plist="$LAUNCH_AGENTS_DIR/$agent.plist"
        if [ -f "$plist" ]; then
            launchctl load "$plist" 2>/dev/null
            echo "  ✓ Loaded $agent"
        else
            echo "  ✗ Missing $plist"
        fi
    done

    echo ""
    echo "Automation started. Check status with: $0 status"
}

stop() {
    echo "Stopping Presence automation..."

    for agent in "${AGENTS[@]}"; do
        plist="$LAUNCH_AGENTS_DIR/$agent.plist"
        if [ -f "$plist" ]; then
            launchctl unload "$plist" 2>/dev/null
            echo "  ✓ Unloaded $agent"
        fi
    done

    echo "Automation stopped."
}

status() {
    echo "Presence automation status:"
    echo ""

    for agent in "${AGENTS[@]}"; do
        result=$(launchctl list | grep "$agent" 2>/dev/null)
        if [ -n "$result" ]; then
            pid=$(echo "$result" | awk '{print $1}')
            status_code=$(echo "$result" | awk '{print $2}')
            if [ "$pid" = "-" ]; then
                echo "  ✓ $agent: loaded (idle, last exit: $status_code)"
            else
                echo "  ● $agent: running (PID: $pid)"
            fi
        else
            echo "  ○ $agent: not loaded"
        fi
    done

    echo ""
    echo "Log files:"
    for log in "$PROJECT_DIR/logs"/*.log; do
        if [ -f "$log" ]; then
            lines=$(wc -l < "$log" | tr -d ' ')
            echo "  $(basename "$log"): $lines lines"
        fi
    done
}

logs() {
    echo "Tailing logs (Ctrl+C to stop)..."
    tail -f "$PROJECT_DIR/logs"/*.log
}

run_job() {
    case "$1" in
        poll)
            echo "Running poll_commits.py..."
            cd "$PROJECT_DIR" && /opt/anaconda3/bin/python scripts/poll_commits.py
            ;;
        daily)
            echo "Running daily_digest.py..."
            cd "$PROJECT_DIR" && /opt/anaconda3/bin/python scripts/daily_digest.py
            ;;
        weekly)
            echo "Running weekly_digest.py..."
            cd "$PROJECT_DIR" && /opt/anaconda3/bin/python scripts/weekly_digest.py
            ;;
        *)
            echo "Usage: $0 run {poll|daily|weekly}"
            exit 1
            ;;
    esac
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        sleep 1
        start
        ;;
    status)
        status
        ;;
    logs)
        logs
        ;;
    run)
        run_job "$2"
        ;;
    retry)
        echo "Retrying unpublished content..."
        cd "$PROJECT_DIR" && /opt/anaconda3/bin/python scripts/retry_unpublished.py
        ;;
    *)
        usage
        ;;
esac
