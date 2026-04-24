#!/usr/bin/env python3
"""Fetch engagement metrics for already-published Bluesky posts."""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Callable

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from evaluation.engagement_scorer import compute_engagement_score
from output.api_rate_guard import should_skip_optional_api_call
from output.bluesky_client import BlueskyClient
from runner import script_context

logger = logging.getLogger(__name__)


METRIC_FIELDS = ("like_count", "repost_count", "reply_count", "quote_count")


def _has_bluesky_credentials(config) -> bool:
    bluesky = getattr(config, "bluesky", None)
    return bool(
        bluesky
        and getattr(bluesky, "enabled", False) is True
        and getattr(bluesky, "handle", None)
        and getattr(bluesky, "app_password", None)
    )


def _classify_client_error(error: object) -> str:
    text = f"{type(error).__name__}: {error}".lower()
    if "unauthorized" in text or "auth" in text or "credential" in text:
        return "auth"
    if "rate" in text or "limit" in text or "too many" in text:
        return "rate_limit"
    if "not found" in text or "missing" in text or "404" in text:
        return "not_found"
    if "timeout" in text or "network" in text or "connection" in text:
        return "network"
    if "invalid" in text or "unsupported" in text or "valueerror" in text:
        return "invalid_request"
    return "unknown"


def _row_content_id(row: dict) -> int | None:
    return row.get("content_id") or row.get("id")


def _row_post_ref(row: dict) -> str | None:
    return (
        row.get("bluesky_post_ref")
        or row.get("platform_post_id")
        or row.get("bluesky_uri")
    )


def _empty_result(row: dict, status: str, **extra) -> dict:
    result = {
        "content_id": _row_content_id(row),
        "platform_post_id": _row_post_ref(row),
        "status": status,
        "like_count": None,
        "repost_count": None,
        "reply_count": None,
        "quote_count": None,
        "engagement_score": None,
    }
    result.update(extra)
    return result


def _normalize_metrics(metrics: dict) -> dict:
    return {field: int(metrics.get(field, 0) or 0) for field in METRIC_FIELDS}


def _get_candidate_rows(db, max_age_days: int) -> list[dict]:
    if hasattr(db, "get_bluesky_publications_needing_engagement"):
        return db.get_bluesky_publications_needing_engagement(max_age_days=max_age_days)
    return db.get_content_needing_bluesky_engagement(max_age_days=max_age_days)


def fetch_bluesky_engagement(
    config,
    db,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    max_age_days: int = 7,
    client_factory: Callable[..., BlueskyClient] | None = None,
) -> list[dict]:
    """Fetch Bluesky engagement rows and optionally persist snapshots."""
    bluesky = getattr(config, "bluesky", None)
    if not bluesky or getattr(bluesky, "enabled", False) is not True:
        return []

    if should_skip_optional_api_call(
        config,
        db,
        "bluesky",
        operation="Bluesky engagement metrics fetch",
        logger=logger,
    ):
        return []

    rows = _get_candidate_rows(db, max_age_days=max_age_days)
    if limit is not None:
        rows = rows[:limit]
    if not rows:
        logger.info("No Bluesky posts need metrics fetching")
        return []

    if not _has_bluesky_credentials(config):
        results = [
            _empty_result(
                row,
                "error",
                error="Missing Bluesky credentials",
                error_category="auth",
            )
            for row in rows
        ]
        for result in results:
            logger.warning(
                "Skipping Bluesky engagement for content_id=%s; missing Bluesky credentials",
                result["content_id"],
            )
        return results

    client_factory = client_factory or BlueskyClient
    client = client_factory(
        handle=bluesky.handle,
        app_password=bluesky.app_password,
    )

    results = []
    for row in rows:
        content_id = _row_content_id(row)
        post_ref = _row_post_ref(row)
        if not post_ref:
            result = _empty_result(
                row,
                "error",
                error="Missing Bluesky platform_post_id",
                error_category="invalid_request",
            )
            logger.warning(
                "Skipping Bluesky engagement for content_id=%s; missing platform_post_id",
                content_id,
            )
            results.append(result)
            continue

        try:
            metrics = client.get_post_engagement(post_ref)
        except Exception as e:
            result = _empty_result(
                row,
                "error",
                error=f"{type(e).__name__}: {e}",
                error_category=_classify_client_error(e),
            )
            logger.warning(
                "Failed to fetch Bluesky metrics for content_id=%s: %s",
                content_id,
                e,
            )
            results.append(result)
            continue

        if metrics is None:
            result = _empty_result(
                row,
                "error",
                error="No metrics returned",
                error_category="not_found",
            )
            logger.warning("Failed to fetch Bluesky metrics for content_id=%s", content_id)
            results.append(result)
            continue

        normalized = _normalize_metrics(metrics)
        score = compute_engagement_score(
            normalized["like_count"],
            normalized["repost_count"],
            normalized["reply_count"],
            normalized["quote_count"],
        )

        if not dry_run:
            db.insert_bluesky_engagement(
                content_id=content_id,
                bluesky_uri=post_ref,
                like_count=normalized["like_count"],
                repost_count=normalized["repost_count"],
                reply_count=normalized["reply_count"],
                quote_count=normalized["quote_count"],
                engagement_score=score,
            )

        result = {
            "content_id": content_id,
            "platform_post_id": post_ref,
            "status": "dry_run" if dry_run else "inserted",
            **normalized,
            "engagement_score": score,
        }
        results.append(result)
        logger.info(
            "%s: %sL %sRP %sR %sQ = %.1f%s",
            post_ref,
            normalized["like_count"],
            normalized["repost_count"],
            normalized["reply_count"],
            normalized["quote_count"],
            score,
            " (dry run)" if dry_run else "",
        )

    return results


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch and print metrics without inserting rows",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Maximum number of eligible publications to process",
    )
    parser.add_argument("--json", action="store_true", help="Emit stable JSON result objects")
    parser.add_argument(
        "--max-age-days",
        type=int,
        default=7,
        help="Only process posts published within this age",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.WARNING if args.json else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    with script_context() as (config, db):
        results = fetch_bluesky_engagement(
            config,
            db,
            dry_run=args.dry_run,
            limit=args.limit,
            max_age_days=args.max_age_days,
        )

    if args.json:
        print(json.dumps(results, indent=2, sort_keys=True))
    else:
        inserted = sum(1 for item in results if item["status"] == "inserted")
        dry = sum(1 for item in results if item["status"] == "dry_run")
        errors = sum(1 for item in results if item["status"] == "error")
        logger.info("Done. inserted=%s dry_run=%s errors=%s", inserted, dry, errors)


if __name__ == "__main__":
    main()
