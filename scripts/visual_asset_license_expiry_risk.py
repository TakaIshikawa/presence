#!/usr/bin/env python3
"""Report visual asset license expiry risk."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.visual_asset_license_expiry_risk import build_visual_asset_license_expiry_risk_report_from_db, format_visual_asset_license_expiry_risk_json, format_visual_asset_license_expiry_risk_text  # noqa: E402
from runner import script_context  # noqa: E402


def _dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db")
    parser.add_argument("--no-expiry-policy", choices=("warn", "ignore"), default="warn")
    parser.add_argument("--format", choices=("json", "text"), default="json")
    parser.add_argument("--now", type=_dt)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        kwargs = {"no_expiry_policy": args.no_expiry_policy, "now": args.now}
        if args.db:
            with sqlite3.connect(args.db) as conn:
                conn.row_factory = sqlite3.Row
                report = build_visual_asset_license_expiry_risk_report_from_db(conn, **kwargs)
        else:
            with script_context() as (_config, db):
                report = build_visual_asset_license_expiry_risk_report_from_db(db, **kwargs)
    except (SystemExit, OSError, sqlite3.Error, ValueError) as exc:
        if isinstance(exc, SystemExit):
            return int(exc.code or 0)
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(format_visual_asset_license_expiry_risk_text(report) if args.format == "text" else format_visual_asset_license_expiry_risk_json(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
