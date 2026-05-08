"""Execution pack verification timeout consistency analyzer."""

from __future__ import annotations

import re
from collections import defaultdict
from statistics import mean, stdev
from typing import Any, Mapping


MIN_REASONABLE_TIMEOUT = 10
MAX_REASONABLE_TIMEOUT = 600
HIGH_VARIANCE_THRESHOLD = 0.5


def analyze_pack_verification_timeout_consistency(records: object) -> dict[str, Any]:
    """Detect inconsistent timeout handling across verification commands in packs."""
    if records is None:
        records = []
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")

    packs: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for index, record in enumerate(records):
        if not isinstance(record, Mapping):
            continue

        pack_key = _pack_key(record)
        verification_command = _verification_command(record)

        if not verification_command:
            continue

        timeout = _parse_timeout(verification_command)
        task_id = _task_id(record, index)

        packs[pack_key].append({
            "task_id": task_id,
            "command": verification_command,
            "timeout": timeout,
        })

    inconsistent_packs: list[dict[str, Any]] = []
    extreme_timeouts: list[dict[str, Any]] = []
    missing_timeouts: list[dict[str, Any]] = []

    for pack_key, commands in sorted(packs.items()):
        # Skip packs with single command
        if len(commands) <= 1:
            continue

        timeouts = [cmd["timeout"] for cmd in commands if cmd["timeout"] is not None]
        missing_count = sum(1 for cmd in commands if cmd["timeout"] is None)

        # Check for missing timeouts
        if missing_count > 0:
            missing_timeouts.append({
                "pack": pack_key,
                "total_commands": len(commands),
                "missing_count": missing_count,
                "commands": [
                    {"task_id": cmd["task_id"], "command": cmd["command"]}
                    for cmd in commands
                    if cmd["timeout"] is None
                ],
            })

        # Check for extreme timeouts
        for cmd in commands:
            if cmd["timeout"] is not None:
                if cmd["timeout"] < MIN_REASONABLE_TIMEOUT:
                    extreme_timeouts.append({
                        "pack": pack_key,
                        "task_id": cmd["task_id"],
                        "command": cmd["command"],
                        "timeout": cmd["timeout"],
                        "issue": "too_short",
                    })
                elif cmd["timeout"] > MAX_REASONABLE_TIMEOUT:
                    extreme_timeouts.append({
                        "pack": pack_key,
                        "task_id": cmd["task_id"],
                        "command": cmd["command"],
                        "timeout": cmd["timeout"],
                        "issue": "too_long",
                    })

        # Check for high variance
        if len(timeouts) >= 2:
            avg_timeout = mean(timeouts)
            if avg_timeout > 0:
                std_dev = stdev(timeouts) if len(timeouts) > 1 else 0
                coefficient_of_variation = std_dev / avg_timeout

                if coefficient_of_variation > HIGH_VARIANCE_THRESHOLD:
                    recommended = round(mean(timeouts), 0)
                    inconsistent_packs.append({
                        "pack": pack_key,
                        "command_count": len(commands),
                        "timeout_variance": round(coefficient_of_variation, 3),
                        "timeout_configs": [
                            {"task_id": cmd["task_id"], "timeout": cmd["timeout"]}
                            for cmd in commands
                        ],
                        "recommended_timeout": int(recommended),
                        "inconsistent_timeouts": True,
                    })

    total_commands = sum(len(cmds) for cmds in packs.values())
    total_missing = sum(item["missing_count"] for item in missing_timeouts)
    inconsistent_count = len(inconsistent_packs)

    return {
        "total_packs": len(packs),
        "total_commands": total_commands,
        "inconsistent_pack_count": inconsistent_count,
        "missing_timeout_count": total_missing,
        "extreme_timeout_count": len(extreme_timeouts),
        "inconsistent_packs": inconsistent_packs,
        "missing_timeouts": missing_timeouts,
        "extreme_timeouts": extreme_timeouts,
    }


def _pack_key(record: Mapping[str, Any]) -> str:
    """Extract pack key from record."""
    value = record.get("executionPack", record.get("execution_pack"))
    if isinstance(value, Mapping):
        value = value.get("key") or value.get("id")
    return str(value) if value else "unknown"


def _verification_command(record: Mapping[str, Any]) -> str:
    """Extract verification command from record."""
    cmd = record.get("verificationCommand", record.get("verification_command", ""))
    return cmd.strip() if isinstance(cmd, str) else ""


def _task_id(record: Mapping[str, Any], index: int) -> str:
    """Extract task ID from record."""
    return str(record.get("title") or record.get("task_id") or record.get("id") or index)


def _parse_timeout(command: str) -> int | None:
    """Parse timeout value from verification command."""
    if not command:
        return None

    # Common patterns:
    # pytest --timeout=30
    # pytest --timeout 30
    # npm test -- --timeout=5000 (milliseconds)
    # timeout 30 pytest
    # timeout 30s pytest

    # Pattern 1: --timeout=VALUE or --timeout VALUE
    match = re.search(r'--timeout[=\s]+(\d+)', command)
    if match:
        timeout = int(match.group(1))
        # Check if it's in milliseconds (npm style, typically >1000)
        if timeout > 1000 and "npm" in command.lower():
            return timeout // 1000
        return timeout

    # Pattern 2: timeout DURATION command (GNU timeout style)
    match = re.search(r'\btimeout\s+(\d+)([smh]?)\b', command)
    if match:
        value = int(match.group(1))
        unit = match.group(2) or "s"
        if unit == "s":
            return value
        elif unit == "m":
            return value * 60
        elif unit == "h":
            return value * 3600

    # Pattern 3: -t VALUE or -timeout VALUE
    match = re.search(r'\b-t(?:imeout)?\s+(\d+)', command)
    if match:
        return int(match.group(1))

    return None
