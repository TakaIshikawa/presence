#!/usr/bin/env python3
"""Score source-to-post citation coverage for generated content."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from knowledge.citation_coverage import CitationCoverageScorer
from runner import script_context


def _shorten(value: Any, width: int = 96) -> str:
    text = str(value or "").replace("\n", " ").strip()
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."


def _load_recent_content(db: Any, days: int) -> list[dict[str, Any]]:
    cursor = db.conn.execute(
        """SELECT * FROM generated_content
           WHERE created_at >= datetime('now', ?)
           ORDER BY created_at DESC, id DESC""",
        (f"-{days} days",),
    )
    rows = []
    for row in cursor.fetchall():
        content = dict(row)
        content["source_commits"] = db._parse_json_list(content.get("source_commits"))
        content["source_messages"] = db._parse_json_list(content.get("source_messages"))
        if "source_activity_ids" in content:
            content["source_activity_ids"] = db._parse_json_list(content.get("source_activity_ids"))
        rows.append(content)
    return rows


def build_coverage_payload(
    db: Any,
    *,
    content_id: int | None = None,
    days: int = 30,
    min_score: float | None = None,
) -> dict[str, Any]:
    """Build a read-only citation coverage payload."""
    if content_id is not None:
        content = db.get_generated_content(content_id)
        if content is None:
            raise ValueError(f"Content ID {content_id} not found")
        contents = [content]
    else:
        contents = _load_recent_content(db, days)

    scorer = CitationCoverageScorer()
    items = []
    for content in contents:
        provenance = db.get_content_provenance(content["id"])
        scored = scorer.score_content(content, provenance).to_dict()
        scored["generated_at"] = content.get("created_at")
        scored["content"] = content.get("content")
        scored["below_min_score"] = min_score is not None and scored["score"] < min_score
        items.append(scored)

    if min_score is not None and content_id is None:
        items = [item for item in items if item["below_min_score"]]

    return {
        "content_id": content_id,
        "days": days,
        "min_score": min_score,
        "count": len(items),
        "items": items,
    }


def format_json_report(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


def format_text_report(payload: dict[str, Any]) -> str:
    lines = [
        f"Citation Coverage Report (last {payload['days']} days)",
        f"Items: {payload['count']}",
    ]
    if payload["content_id"] is not None:
        lines.append(f"Content filter: #{payload['content_id']}")
    if payload["min_score"] is not None:
        lines.append(f"Minimum score: {payload['min_score']:.2f}")

    if not payload["items"]:
        lines.append("\nNo generated content matched the coverage filters.")
        return "\n".join(lines)

    for item in payload["items"]:
        marker = " BELOW_MIN" if item.get("below_min_score") else ""
        lines.append("")
        lines.append(
            f"Content #{item['content_id']} [{item['content_type']}] "
            f"score={item['score']:.3f} status={item['status']}{marker}"
        )
        lines.append(f"  {_shorten(item.get('content'), 140)}")
        lines.append(
            "  Claims: "
            f"{item['claim_count']} total, {item['covered_count']} covered, "
            f"{item['thin_count']} thin, {item['missing_count']} missing"
        )
        if item["missing_traceable_link_count"]:
            lines.append(f"  Missing traceable links: {item['missing_traceable_link_count']}")
        for reason in item.get("reasons") or []:
            lines.append(f"  reason: {reason}")
        for claim in item.get("claims") or []:
            evidence = ",".join(claim.get("evidence_types") or []) or "-"
            lines.append(
                f"  - [{claim['status']}] {claim['kind']} "
                f"evidence={evidence}: {_shorten(claim['text'], 110)}"
            )
            if claim["status"] != "covered":
                lines.append(f"    reason: {claim['reason']}")

    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--content-id", type=int, help="Score one generated_content.id")
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of generated-content days to inspect when --content-id is omitted (default: 30)",
    )
    parser.add_argument("--json", action="store_true", help="Output machine-readable JSON")
    parser.add_argument(
        "--min-score",
        type=float,
        help="Mark items below this score; for multi-item reports, show only below-threshold items",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    if args.days < 1:
        raise SystemExit("--days must be at least 1")
    if args.min_score is not None and not 0 <= args.min_score <= 1:
        raise SystemExit("--min-score must be between 0 and 1")

    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (_config, db):
        try:
            payload = build_coverage_payload(
                db,
                content_id=args.content_id,
                days=args.days,
                min_score=args.min_score,
            )
        except ValueError as exc:
            raise SystemExit(str(exc)) from exc

    if args.json:
        print(format_json_report(payload))
    else:
        print(format_text_report(payload))


if __name__ == "__main__":
    main()

