#!/usr/bin/env python3
"""Audit local configuration drift against the reference config."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from config_drift import audit_config_drift  # noqa: E402


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Reference config path (default: config.yaml)",
    )
    parser.add_argument(
        "--local-config",
        default="config.local.yaml",
        help="Local config path (default: config.local.yaml)",
    )
    parser.add_argument("--json", action="store_true", help="Print stable JSON output")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit nonzero when blocking drift findings exist",
    )
    return parser.parse_args(argv)


def _print_text(result: dict) -> None:
    print(
        "Config drift audit: "
        f"{result['status']} "
        f"({result['blocking_count']} blocking finding"
        f"{'' if result['blocking_count'] == 1 else 's'})"
    )
    counts = result["counts"]
    print(
        "Findings: "
        f"missing={counts.get('missing', 0)}, "
        f"extra={counts.get('extra', 0)}, "
        f"unresolved-env={counts.get('unresolved-env', 0)}, "
        f"empty-value={counts.get('empty-value', 0)}, "
        f"type-mismatch={counts.get('type-mismatch', 0)}"
    )
    if not result["findings"]:
        return

    print("")
    for finding in result["findings"]:
        detail = ""
        if finding.get("expected_type") or finding.get("actual_type"):
            detail = (
                f" ({finding.get('expected_type', 'unknown')} -> "
                f"{finding.get('actual_type', 'unknown')})"
            )
        redacted = " [redacted]" if finding.get("redacted") else ""
        print(
            f"- {finding['severity']}: {finding['kind']} "
            f"{finding['path']}{detail}{redacted}"
        )
        print(f"  {finding['message']}")


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        result = audit_config_drift(args.config, args.local_config)
    except (OSError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1

    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        _print_text(result)

    if args.strict and result["blocking_count"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
