"""Import manually downloaded Bluesky engagement metrics from CSV."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from evaluation.engagement_scorer import compute_engagement_score


IDENTIFIER_FIELDS = ("content_id", "bluesky_uri", "published_url")
METRIC_ALIASES = {
    "like_count": ("like_count", "likes", "like_count_total"),
    "repost_count": ("repost_count", "reposts", "repost_count_total"),
    "reply_count": ("reply_count", "replies", "reply_count_total"),
    "quote_count": ("quote_count", "quotes", "quote_count_total"),
}


@dataclass(frozen=True)
class NormalizedBlueskyMetrics:
    """Normalized Bluesky engagement counts for one CSV row."""

    like_count: int
    repost_count: int
    reply_count: int
    quote_count: int
    engagement_score: float

    def as_dict(self) -> dict[str, int | float]:
        return {
            "like_count": self.like_count,
            "repost_count": self.repost_count,
            "reply_count": self.reply_count,
            "quote_count": self.quote_count,
            "engagement_score": self.engagement_score,
        }


def _clean_key(key: str | None) -> str:
    return (key or "").strip().lower().replace(" ", "_").replace("-", "_")


def _clean_value(value: Any) -> str:
    return str(value or "").strip()


def _normalize_row(row: dict[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in row.items():
        clean_key = _clean_key(key)
        if clean_key:
            normalized[clean_key] = _clean_value(value)
    return normalized


def _first_value(row: dict[str, str], aliases: Iterable[str]) -> str:
    for alias in aliases:
        value = row.get(alias)
        if value:
            return value
    return ""


def _parse_count(value: str, field_name: str) -> int:
    if value == "":
        return 0
    cleaned = value.replace(",", "")
    try:
        count = int(cleaned)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a non-negative integer") from exc
    if count < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")
    return count


def normalize_metrics(row: dict[str, Any]) -> NormalizedBlueskyMetrics:
    """Return normalized Bluesky counts from a CSV row."""
    normalized = _normalize_row(row)
    counts = {
        field: _parse_count(_first_value(normalized, aliases), field)
        for field, aliases in METRIC_ALIASES.items()
    }
    return NormalizedBlueskyMetrics(
        like_count=counts["like_count"],
        repost_count=counts["repost_count"],
        reply_count=counts["reply_count"],
        quote_count=counts["quote_count"],
        engagement_score=compute_engagement_score(
            counts["like_count"],
            counts["repost_count"],
            counts["reply_count"],
            counts["quote_count"],
        ),
    )


def _row_identifiers(row: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_row(row)
    identifiers: dict[str, Any] = {}
    content_id = normalized.get("content_id", "")
    if content_id:
        try:
            identifiers["content_id"] = int(content_id)
        except ValueError as exc:
            raise ValueError("content_id must be an integer") from exc
    for field in ("bluesky_uri", "published_url"):
        value = normalized.get(field, "")
        if value:
            identifiers[field] = value
    return identifiers


def import_bluesky_engagement_rows(
    db: Any,
    rows: Iterable[dict[str, Any]],
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Validate, match, and optionally import Bluesky engagement CSV rows."""
    result: dict[str, Any] = {
        "dry_run": dry_run,
        "counts": {
            "rows": 0,
            "matched": 0,
            "skipped": 0,
            "invalid": 0,
            "inserted": 0,
            "updated": 0,
        },
        "rows": [],
    }

    for row_number, row in enumerate(rows, start=1):
        result["counts"]["rows"] += 1
        row_result: dict[str, Any] = {"row": row_number}

        try:
            identifiers = _row_identifiers(row)
            if not any(key in identifiers for key in IDENTIFIER_FIELDS):
                result["counts"]["invalid"] += 1
                row_result.update(
                    {
                        "status": "invalid",
                        "error": "row must include content_id, bluesky_uri, or published_url",
                    }
                )
                result["rows"].append(row_result)
                continue
            metrics = normalize_metrics(row)
        except ValueError as exc:
            result["counts"]["invalid"] += 1
            row_result.update({"status": "invalid", "error": str(exc)})
            result["rows"].append(row_result)
            continue

        match = db.find_bluesky_engagement_content(**identifiers)
        if not match:
            result["counts"]["skipped"] += 1
            row_result.update(
                {
                    "status": "skipped",
                    "reason": "no matching content",
                    "identifiers": identifiers,
                }
            )
            result["rows"].append(row_result)
            continue

        bluesky_uri = identifiers.get("bluesky_uri") or match.get("bluesky_uri")
        if not bluesky_uri:
            result["counts"]["invalid"] += 1
            row_result.update(
                {
                    "status": "invalid",
                    "content_id": match["content_id"],
                    "error": "matched content has no Bluesky URI",
                }
            )
            result["rows"].append(row_result)
            continue

        result["counts"]["matched"] += 1
        row_result.update(
            {
                "status": "matched" if dry_run else "imported",
                "content_id": match["content_id"],
                "bluesky_uri": bluesky_uri,
                "metrics": metrics.as_dict(),
            }
        )

        if not dry_run:
            upsert = db.upsert_bluesky_engagement(
                content_id=match["content_id"],
                bluesky_uri=bluesky_uri,
                like_count=metrics.like_count,
                repost_count=metrics.repost_count,
                reply_count=metrics.reply_count,
                quote_count=metrics.quote_count,
                engagement_score=metrics.engagement_score,
            )
            action = upsert["action"]
            result["counts"][action] += 1
            row_result["action"] = action
            row_result["engagement_id"] = upsert["id"]

        result["rows"].append(row_result)

    return result


def import_bluesky_engagement_csv(
    db: Any,
    csv_path: str | Path,
    *,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Import Bluesky engagement metrics from a CSV file."""
    with Path(csv_path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return import_bluesky_engagement_rows(db, reader, dry_run=dry_run)
