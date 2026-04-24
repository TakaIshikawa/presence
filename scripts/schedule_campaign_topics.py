#!/usr/bin/env python3
"""Schedule active campaign topics into planned_topics."""

from __future__ import annotations

import argparse
import json
import sys
from contextlib import contextmanager
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from runner import SCHEMA_PATH, script_context
from storage.db import Database
from synthesis.campaign_scheduler import CampaignScheduleReport, schedule_campaign_topics


def format_report_table(report: CampaignScheduleReport) -> str:
    lines = [
        (
            f"created={len(report.created)} proposed={len(report.proposed)} "
            f"skipped={len(report.skipped)}"
        ),
        f"{'Status':8s}  {'ID':>4s}  {'Campaign':24s}  {'Date':10s}  Topic / reason",
        f"{'-' * 8:8s}  {'-' * 4:>4s}  {'-' * 24:24s}  {'-' * 10:10s}  {'-' * 44}",
    ]
    if not report.items:
        lines.append("none      ----  ------------------------  ----------  no campaign topics scheduled")
        return "\n".join(lines)

    for item in report.items:
        record_id = str(item.record_id) if item.record_id is not None else "-"
        detail = item.topic
        if item.reason and item.status == "skipped":
            detail = f"{detail} ({item.reason})"
        lines.append(
            f"{item.status:8s}  "
            f"{record_id:>4s}  "
            f"{_shorten(item.campaign_name, 24):24s}  "
            f"{item.target_date:10s}  "
            f"{detail}"
        )
    return "\n".join(lines)


def format_report_json(report: CampaignScheduleReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def _shorten(value: str, width: int) -> str:
    text = " ".join(str(value).split())
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)].rstrip() + "..."


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", help="SQLite database path. Defaults to configured database.")
    parser.add_argument("--campaign-id", type=int, help="Schedule one campaign by ID")
    parser.add_argument("--days", type=int, default=14, help="Lookahead window in days (default: 14)")
    parser.add_argument("--dry-run", action="store_true", help="Report without writing planned topics")
    parser.add_argument("--output", "-o", help="Write report to a file instead of stdout")
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a table")
    return parser.parse_args(argv)


@contextmanager
def _db_context(path: str | None):
    if path is None:
        with script_context() as (_config, db):
            yield db
        return

    db = Database(path)
    db.connect()
    db.init_schema(SCHEMA_PATH)
    try:
        yield db
    finally:
        db.close()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    with _db_context(args.db) as db:
        report = schedule_campaign_topics(
            db,
            campaign_id=args.campaign_id,
            days=args.days,
            dry_run=args.dry_run,
        )

    rendered = format_report_json(report) if args.json else format_report_table(report)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


if __name__ == "__main__":
    main()
