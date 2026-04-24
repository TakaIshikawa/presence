#!/usr/bin/env python3
"""Find curated source items worth quote-posting."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from engagement.quote_opportunities import (  # noqa: E402
    QuoteOpportunity,
    QuoteOpportunityRecommender,
    opportunities_to_dict,
)
from engagement.quote_safety import QuoteSafetyReview, QuoteSafetyReviewer  # noqa: E402
from runner import script_context  # noqa: E402


SOURCE_TYPE_ALIASES = {
    "x": "curated_x",
    "curated_x": "curated_x",
    "article": "curated_article",
    "curated_article": "curated_article",
    "newsletter": "curated_newsletter",
    "curated_newsletter": "curated_newsletter",
}


def _shorten(text: str | None, width: int = 72) -> str:
    value = " ".join((text or "").split())
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _split_values(values: list[str] | None) -> list[str]:
    if not values:
        return []
    parts: list[str] = []
    for value in values:
        for item in str(value).split(","):
            item = item.strip()
            if item:
                parts.append(item)
    return parts


def _normalize_values(values: list[str] | None, *, lowercase: bool = False) -> list[str]:
    normalized = []
    for value in _split_values(values):
        item = value.lower() if lowercase else value
        if item not in normalized:
            normalized.append(item)
    return normalized


def _normalize_source_types(platforms: list[str] | None) -> list[str]:
    source_types: list[str] = []
    for value in _split_values(platforms):
        key = value.lower()
        if key not in SOURCE_TYPE_ALIASES:
            raise ValueError(
                f"Unknown platform '{value}'. Expected one of: x, article, newsletter"
            )
        source_type = SOURCE_TYPE_ALIASES[key]
        if source_type not in source_types:
            source_types.append(source_type)
    return source_types


def build_review_payload(
    opportunities: list[QuoteOpportunity],
    enqueued_ids: list[int] | None = None,
    *,
    filters: dict[str, object] | None = None,
    generated_at: datetime | None = None,
    safety_reviews: dict[int, QuoteSafetyReview] | None = None,
) -> dict[str, object]:
    opportunity_payload = opportunities_to_dict(opportunities)
    if safety_reviews is not None:
        for item in opportunity_payload:
            review = safety_reviews.get(item["knowledge_id"])
            item["safety"] = review.to_dict() if review else None
    payload = {
        "generated_at": (generated_at or datetime.now(timezone.utc)).isoformat(),
        "filters": filters or {},
        "opportunities": opportunity_payload,
        "enqueued_ids": enqueued_ids or [],
    }
    return payload


def format_json_output(
    opportunities: list[QuoteOpportunity],
    enqueued_ids: list[int] | None = None,
    *,
    filters: dict[str, object] | None = None,
    generated_at: datetime | None = None,
    safety_reviews: dict[int, QuoteSafetyReview] | None = None,
) -> str:
    payload = build_review_payload(
        opportunities,
        enqueued_ids,
        filters=filters,
        generated_at=generated_at,
        safety_reviews=safety_reviews,
    )
    return json.dumps(payload, indent=2, sort_keys=True)


def _format_filters_line(filters: dict[str, object]) -> str:
    parts: list[str] = []
    if filters.get("days") is not None:
        parts.append(f"days={filters['days']}")
    if filters.get("limit") is not None:
        parts.append(f"limit={filters['limit']}")
    if filters.get("min_score") is not None:
        parts.append(f"min_score={filters['min_score']}")
    if filters.get("campaign_id") is not None:
        parts.append(f"campaign_id={filters['campaign_id']}")
    for key in ("authors", "topics", "platforms"):
        values = filters.get(key) or []
        if values:
            parts.append(f"{key}={', '.join(str(value) for value in values)}")
    return ", ".join(parts) if parts else "none"


def format_markdown_output(
    opportunities: list[QuoteOpportunity],
    enqueued_ids: list[int] | None = None,
    *,
    filters: dict[str, object] | None = None,
    generated_at: datetime | None = None,
    safety_reviews: dict[int, QuoteSafetyReview] | None = None,
) -> str:
    payload = build_review_payload(
        opportunities,
        enqueued_ids,
        filters=filters,
        generated_at=generated_at,
        safety_reviews=safety_reviews,
    )
    lines = [
        "# Quote Opportunity Review",
        "",
        f"- Generated at: {payload['generated_at']}",
        f"- Filters: {_format_filters_line(payload['filters'])}",
        f"- Enqueued IDs: {', '.join(str(item) for item in payload['enqueued_ids']) or 'none'}",
        "",
        "## Opportunities",
    ]
    if opportunities:
        lines.extend(
            [
                "| Score | Fresh | Novel | Author | Source Type | Topics | Knowledge ID | Source |",
                "| --- | --- | --- | --- | --- | --- | --- | --- |",
            ]
        )
        for item in opportunities:
            safety = safety_reviews.get(item.knowledge_id) if safety_reviews is not None else None
            lines.append(
                "| {score:.2f} | {freshness:.2f} | {novelty:.2f} | {author} | {source_type} | {topics} | {knowledge_id} | {source} |".format(
                    score=item.score,
                    freshness=item.freshness,
                    novelty=item.novelty,
                    author=_shorten(item.author, 18) or "-",
                    source_type=_shorten(item.source_type, 16),
                    topics=_shorten(", ".join(item.topics), 28),
                    knowledge_id=item.knowledge_id,
                    source=_shorten(item.source_url or item.source_id or str(item.knowledge_id), 48),
                )
            )
            if item.reasons:
                lines.append(f"  - Reasons: {_shorten('; '.join(item.reasons), 120)}")
            if safety is not None:
                lines.append(
                    f"  - Safety: {safety.score:.2f}; flags: {', '.join(safety.blocking_flags) or 'none'}; "
                    f"reasons: {_shorten('; '.join(safety.reasons), 120)}"
                )
    else:
        lines.append("No opportunities matched the selected filters.")
    lines.extend(
        [
            "",
            "## Payload",
            "",
            "```json",
            json.dumps(payload, indent=2, sort_keys=True),
            "```",
        ]
    )
    return "\n".join(lines)


def format_json_artifact(
    opportunities: list[QuoteOpportunity],
    enqueued_ids: list[int] | None = None,
    *,
    filters: dict[str, object] | None = None,
    generated_at: datetime | None = None,
    safety_reviews: dict[int, QuoteSafetyReview] | None = None,
) -> str:
    return format_json_output(
        opportunities,
        enqueued_ids,
        filters=filters,
        generated_at=generated_at,
        safety_reviews=safety_reviews,
    )


def write_artifact(
    path: str | Path,
    opportunities: list[QuoteOpportunity],
    *,
    format: str,
    enqueued_ids: list[int] | None = None,
    filters: dict[str, object] | None = None,
    generated_at: datetime | None = None,
    safety_reviews: dict[int, QuoteSafetyReview] | None = None,
) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    if format == "json":
        body = format_json_artifact(
            opportunities,
            enqueued_ids,
            filters=filters,
            generated_at=generated_at,
            safety_reviews=safety_reviews,
        )
        target.write_text(body + "\n", encoding="utf-8")
    elif format == "markdown":
        body = format_markdown_output(
            opportunities,
            enqueued_ids,
            filters=filters,
            generated_at=generated_at,
            safety_reviews=safety_reviews,
        )
        target.write_text(body, encoding="utf-8")
    else:
        raise ValueError("format must be 'json' or 'markdown'")
    return target


def format_table_output(
    opportunities: list[QuoteOpportunity],
    enqueued_ids: list[int] | None = None,
    *,
    safety_reviews: dict[int, QuoteSafetyReview] | None = None,
) -> str:
    lines = [
        f"{'Score':>5s}  {'Fresh':>5s}  {'Novel':>5s}  {'Author':18s}  {'Topics':24s}  Source",
        f"{'-' * 5:>5s}  {'-' * 5:>5s}  {'-' * 5:>5s}  {'-' * 18:18s}  {'-' * 24:24s}  {'-' * 48}",
    ]
    for item in opportunities:
        lines.append(
            f"{item.score:5.2f}  "
            f"{item.freshness:5.2f}  "
            f"{item.novelty:5.2f}  "
            f"{_shorten(item.author, 18):18s}  "
            f"{_shorten(', '.join(item.topics), 24):24s}  "
            f"{_shorten(item.source_url or item.source_id or str(item.knowledge_id), 72)}"
        )
        reason = "; ".join(item.reasons)
        if reason:
            lines.append(f"       reason: {_shorten(reason, 110)}")
        safety = safety_reviews.get(item.knowledge_id) if safety_reviews is not None else None
        if safety is not None:
            lines.append(
                f"       safety: {safety.score:.2f} flags={', '.join(safety.blocking_flags) or 'none'} "
                f"reason={_shorten('; '.join(safety.reasons), 100)}"
            )
    if enqueued_ids:
        lines.append("")
        lines.append(f"Enqueued proactive quote actions: {', '.join(str(item) for item in enqueued_ids)}")
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days", type=int, default=7, help="Recent source lookback in days (default: 7)")
    parser.add_argument("--limit", type=int, default=10, help="Maximum opportunities to show")
    parser.add_argument("--campaign-id", type=int, help="Only align to this active campaign")
    parser.add_argument(
        "--author",
        action="append",
        help="Only include sources from these authors; can be repeated or comma-separated",
    )
    parser.add_argument(
        "--topic",
        action="append",
        help="Only include sources matching these topics; can be repeated or comma-separated",
    )
    parser.add_argument(
        "--platform",
        action="append",
        help="Only include curated source platforms: x, article, newsletter; can be repeated or comma-separated",
    )
    parser.add_argument("--min-score", type=float, default=0.35, help="Minimum total score")
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        help="Write or print a review artifact in JSON or Markdown format",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Write the selected opportunities to this artifact path",
    )
    parser.add_argument("--enqueue", action="store_true", help="Create pending quote_tweet review actions")
    parser.add_argument(
        "--enqueue-limit",
        type=int,
        help="Maximum number of displayed opportunities to enqueue",
    )
    parser.add_argument("--json", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--safety-report",
        action="store_true",
        help="Include deterministic quote safety findings in the output",
    )
    parser.add_argument(
        "--min-safety-score",
        type=float,
        help="Only include opportunities with at least this safety score",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    authors = _normalize_values(args.author, lowercase=True)
    topics = _normalize_values(args.topic, lowercase=True)
    source_types = _normalize_source_types(args.platform)
    filters = {
        "days": args.days,
        "limit": args.limit,
        "campaign_id": args.campaign_id,
        "authors": authors,
        "topics": topics,
        "platforms": args.platform and _split_values(args.platform) or [],
        "min_score": args.min_score,
    }
    if args.safety_report:
        filters["safety_report"] = True
    if args.min_safety_score is not None:
        filters["min_safety_score"] = args.min_safety_score

    with script_context() as (_config, db):
        recommender = QuoteOpportunityRecommender(db)
        opportunities = recommender.recommend(
            days=args.days,
            limit=args.limit,
            campaign_id=args.campaign_id,
            min_score=args.min_score,
            authors=authors or None,
            topics=topics or None,
            source_types=source_types or None,
        )
        safety_reviews = None
        if args.safety_report or args.min_safety_score is not None:
            safety_reviews = QuoteSafetyReviewer(db).review_many(opportunities)
            if args.min_safety_score is not None:
                opportunities = [
                    opportunity
                    for opportunity in opportunities
                    if safety_reviews[opportunity.knowledge_id].score >= args.min_safety_score
                ]
        enqueued_ids = recommender.enqueue(
            opportunities,
            limit=args.enqueue_limit,
        ) if args.enqueue else []

    output_format = "json" if args.json else args.format
    if args.out:
        artifact_format = output_format or "markdown"
        write_artifact(
            args.out,
            opportunities,
            format=artifact_format,
            enqueued_ids=enqueued_ids,
            filters=filters,
            safety_reviews=safety_reviews,
        )
        print(f"Quote opportunity artifact: {args.out}", file=sys.stderr)
    elif output_format == "json":
        print(
            format_json_output(
                opportunities,
                enqueued_ids,
                filters=filters,
                safety_reviews=safety_reviews,
            )
        )
    elif output_format == "markdown":
        print(
            format_markdown_output(
                opportunities,
                enqueued_ids,
                filters=filters,
                safety_reviews=safety_reviews,
            )
        )
    else:
        print(format_table_output(opportunities, enqueued_ids, safety_reviews=safety_reviews))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
