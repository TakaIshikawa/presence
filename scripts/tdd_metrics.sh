#!/usr/bin/env bash
# TDD metrics reporting script

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
METRICS_FILE="$PROJECT_ROOT/.tdd_metrics.json"

# Colors for output
RED='\033[0:31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}=== TDD Metrics Report ===${NC}\n"

# Check if metrics file exists
if [ ! -f "$METRICS_FILE" ]; then
    echo -e "${YELLOW}No TDD metrics found.${NC}"
    echo "Start tracking with: python scripts/tdd_workflow.py init <feature_name>"
    exit 0
fi

# Extract metrics using Python
python3 <<EOF
import json
from pathlib import Path

metrics_file = Path("$METRICS_FILE")
with open(metrics_file) as f:
    metrics = json.load(f)

print("📊 Overall TDD Metrics\n")
print(f"  Test-First Percentage: {metrics.get('test_first_percentage', 0):.1f}%")
print(f"  Avg Red-Green-Refactor Cycle Time: {metrics.get('avg_red_green_cycle_time', 0):.1f}s")
print(f"  Coverage Increase per Commit: {metrics.get('coverage_increase_per_commit', 0):.2f}%")
print(f"  Total Cycles Completed: {metrics.get('total_cycles', 0)}")
print(f"  Code-First Violations: {metrics.get('code_first_violations', 0)}")

# Per-developer compliance
compliance = metrics.get('tdd_compliance_by_developer', {})
if compliance:
    print("\n👥 TDD Compliance by Developer\n")
    for dev, score in sorted(compliance.items(), key=lambda x: x[1], reverse=True):
        bar = '█' * int(score / 10)
        print(f"  {dev:20s} {score:5.1f}% {bar}")
else:
    print("\n(No per-developer metrics yet)")
EOF

# Git-based analysis for test-first commits
echo -e "\n${BLUE}📈 Recent Commit Analysis${NC}\n"

# Count commits with test files
TOTAL_COMMITS=$(git log --oneline --since="1 month ago" | wc -l | tr -d ' ')
TEST_COMMITS=$(git log --oneline --since="1 month ago" --name-only | grep -E "test_.*\.py" | wc -l | tr -d ' ')

if [ "$TOTAL_COMMITS" -gt 0 ]; then
    TEST_COMMIT_PCT=$((100 * TEST_COMMITS / TOTAL_COMMITS))
    echo "  Commits with tests: $TEST_COMMITS / $TOTAL_COMMITS ($TEST_COMMIT_PCT%)"
else
    echo "  No commits in last month"
fi

# Check for test coverage trends
if command -v pytest &> /dev/null; then
    echo -e "\n${BLUE}📊 Test Coverage${NC}\n"

    # Run pytest with coverage (suppress output)
    COVERAGE_OUTPUT=$(python -m pytest --cov=src --cov-report=term-missing --tb=no -q 2>&1 | tail -5 || true)

    if echo "$COVERAGE_OUTPUT" | grep -q "TOTAL"; then
        echo "$COVERAGE_OUTPUT" | grep "TOTAL"
    else
        echo "  (Coverage data not available)"
    fi
fi

# TDD quality indicators
echo -e "\n${BLUE}✨ TDD Quality Indicators${NC}\n"

# Count test files vs source files
TEST_FILE_COUNT=$(find tests -name "test_*.py" 2>/dev/null | wc -l | tr -d ' ')
SRC_FILE_COUNT=$(find src -name "*.py" 2>/dev/null | wc -l | tr -d ' ')

if [ "$SRC_FILE_COUNT" -gt 0 ]; then
    TEST_RATIO=$((100 * TEST_FILE_COUNT / SRC_FILE_COUNT))
    echo "  Test/Source Ratio: $TEST_FILE_COUNT tests / $SRC_FILE_COUNT modules ($TEST_RATIO%)"
else
    echo "  Test/Source Ratio: N/A"
fi

# Analyze test assertions
if [ -d "tests" ]; then
    ASSERTION_COUNT=$(grep -r "assert " tests/ --include="*.py" | wc -l | tr -d ' ')
    echo "  Total Assertions: $ASSERTION_COUNT"

    # Custom assertions usage
    CUSTOM_ASSERT_COUNT=$(grep -r "from tests.helpers.assertions import\|assert_valid_" tests/ --include="*.py" | wc -l | tr -d ' ')
    if [ "$CUSTOM_ASSERT_COUNT" -gt 0 ]; then
        echo "  Custom Assertion Usage: $CUSTOM_ASSERT_COUNT occurrences"
    fi
fi

echo -e "\n${GREEN}Report generated at $(date)${NC}"
