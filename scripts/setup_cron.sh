#!/bin/bash
# Setup cron jobs for Presence automation

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PYTHON=$(which python3)

# Create log directory
mkdir -p "$PROJECT_DIR/logs"

# Create crontab entries
CRON_POLL="*/10 * * * * cd $PROJECT_DIR && $PYTHON scripts/poll_commits.py >> logs/poll.log 2>&1"
CRON_DAILY="59 23 * * * cd $PROJECT_DIR && $PYTHON scripts/daily_digest.py >> logs/daily.log 2>&1"
CRON_WEEKLY="0 12 * * 0 cd $PROJECT_DIR && $PYTHON scripts/weekly_digest.py >> logs/weekly.log 2>&1"

echo "Add these lines to your crontab (run 'crontab -e'):"
echo ""
echo "# Presence - Personal Branding Automation"
echo "$CRON_POLL"
echo "$CRON_DAILY"
echo "$CRON_WEEKLY"
echo ""
echo "Or run this to install automatically:"
echo "(crontab -l 2>/dev/null; echo '# Presence - Personal Branding Automation'; echo '$CRON_POLL'; echo '$CRON_DAILY'; echo '$CRON_WEEKLY') | crontab -"
