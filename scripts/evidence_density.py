#!/usr/bin/env python3
"""Score generated content for concrete supporting evidence."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Sequence

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from storage.db import Database  # noqa: E402
from synthesis.evidence_density import EvidenceDensityReport, score_evidence_density  # noqa: E402


def build_report_for_content_id(db: Database, content_id: int) -> EvidenceDensityReport:
    row = db.get_generated_content(content_id)
    if not row:
        raise SystemExit(f"generated_content id {content_id} not found")
    return score_evidence_density(
        row["content"],
        content_id=content_id,
        source_commits=row.get("source_commits") or [],
        source_messages=row.get("source_messages") or [],
        source_activity_ids=row.get("source_activity_ids") or [],
    )


def read_text(args: argparse.Namespace) -> str:
    if args.text:
        if isinstance(args.text, list):
            return " ".join(args.text).strip()
        return args.text.strip()
    if not sys.stdin.isatty():
        return sys.stdin.read().strip()
    raise SystemExit("--text, --content-id, or stdin is required")


def format_text_report(report: EvidenceDensityReport) -> str:
    lines = [
        f"Evidence density: {report.score}/100 ({report.status})",
    ]
    if report.content_id is not None:
        lines.append(f"Content ID: {report.content_id}")

    lines.extend(["", "Positive signals:"])
    if report.positive_signals:
        for signal in report.positive_signals:
            lines.append(
                f"  - {signal.name}: {signal.count} "
                f"(+{signal.weight}; examples: {signal.examples})"
            )
    else:
        lines.append("  - none")

    lines.extend(["", "Negative signals:"])
    if report.negative_signals:
        for signal in report.negative_signals:
            lines.append(
                f"  - {signal.name}: {signal.count} "
                f"(-{signal.weight}; examples: {signal.examples})"
            )
    else:
        lines.append("  - none")

    lines.extend(["", "Recommendations:"])
    if report.recommendations:
        lines.extend(f"  - {recommendation}" for recommendation in report.recommendations)
    else:
        lines.append("  - none")
    return "\n".join(lines)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score generated content for concrete supporting evidence."
    )
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--content-id", type=int, help="Evaluate a generated_content row")
    source.add_argument("--text", nargs="*", help="Evaluate raw draft text")
    parser.add_argument("--db", default="./presence.db", help="SQLite database path")
    parser.add_argument("--min-score", type=int, help="Fail when score is below this threshold")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.content_id is not None:
        db = Database(args.db)
        db.connect()
        try:
            report = build_report_for_content_id(db, args.content_id)
        finally:
            db.close()
    else:
        report = score_evidence_density(read_text(args))

    if args.json:
        print(json.dumps(asdict(report), indent=2, sort_keys=True))
    else:
        print(format_text_report(report))

    if args.min_score is not None and report.score < args.min_score:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
