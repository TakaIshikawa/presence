"""Import LinkedIn engagement snapshots from CSV exports."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

WEIGHT_LIKE = 1.0
WEIGHT_COMMENT = 4.0
WEIGHT_SHARE = 3.0
WEIGHT_IMPRESSION = 0.01

_INT_RE = re.compile(r"-?\d[\d,]*")
_LINKEDIN_ID_RE = re.compile(r"(?:activity|urn:li:activity:)[-_:]?(\d+)")

_HEADER_ALIASES = {
    "url": {
        "url",
        "post url",
        "post_url",
        "linkedin url",
        "linkedin_url",
        "permalink",
        "link",
    },
    "post_id": {
        "post id",
        "post_id",
        "activity id",
        "activity_id",
        "urn",
        "share id",
        "share_id",
    },
    "impressions": {
        "impressions",
        "impression count",
        "impression_count",
        "views",
        "view count",
        "view_count",
    },
    "likes": {"likes", "like count", "like_count", "reactions", "reaction count"},
    "comments": {"comments", "comment count", "comment_count", "replies"},
    "shares": {"shares", "share count", "share_count", "reposts", "repost count"},
}


@dataclass(frozen=True)
class LinkedInEngagementRow:
    """One normalized LinkedIn metrics row from a CSV."""

    source_row: int
    linkedin_url: str | None
    post_id: str | None
    impression_count: int
    like_count: int
    comment_count: int
    share_count: int
    engagement_score: float
    content_id: int | None = None


@dataclass(frozen=True)
class LinkedInEngagementImportResult:
    """Summary of one LinkedIn CSV import."""

    inserted: tuple[LinkedInEngagementRow, ...]
    unmatched: tuple[LinkedInEngagementRow, ...]
    dry_run: bool

    @property
    def insert_count(self) -> int:
        return len(self.inserted)

    @property
    def unmatched_count(self) -> int:
        return len(self.unmatched)


def compute_linkedin_engagement_score(
    like_count: int,
    comment_count: int,
    share_count: int,
    impression_count: int = 0,
) -> float:
    """Compute weighted LinkedIn engagement from imported counts."""
    return (
        int(like_count or 0) * WEIGHT_LIKE
        + int(comment_count or 0) * WEIGHT_COMMENT
        + int(share_count or 0) * WEIGHT_SHARE
        + int(impression_count or 0) * WEIGHT_IMPRESSION
    )


def import_linkedin_engagement_csv(
    db: Any,
    csv_path: str | Path,
    *,
    dry_run: bool = False,
    fetched_at: str | None = None,
) -> LinkedInEngagementImportResult:
    """Parse a LinkedIn CSV, match rows to content, and insert snapshots."""
    fetched_at = fetched_at or datetime.now(timezone.utc).isoformat()
    candidates = _load_linkedin_publication_candidates(db)
    inserted: list[LinkedInEngagementRow] = []
    unmatched: list[LinkedInEngagementRow] = []

    for row in parse_linkedin_engagement_csv(csv_path):
        content_id = match_linkedin_row(row, candidates)
        if content_id is None:
            unmatched.append(row)
            continue

        matched = LinkedInEngagementRow(
            source_row=row.source_row,
            linkedin_url=row.linkedin_url,
            post_id=row.post_id,
            impression_count=row.impression_count,
            like_count=row.like_count,
            comment_count=row.comment_count,
            share_count=row.share_count,
            engagement_score=row.engagement_score,
            content_id=content_id,
        )
        inserted.append(matched)
        if not dry_run:
            db.insert_linkedin_engagement(
                content_id=content_id,
                linkedin_url=matched.linkedin_url,
                post_id=matched.post_id,
                impression_count=matched.impression_count,
                like_count=matched.like_count,
                comment_count=matched.comment_count,
                share_count=matched.share_count,
                engagement_score=matched.engagement_score,
                fetched_at=fetched_at,
            )

    return LinkedInEngagementImportResult(
        inserted=tuple(inserted),
        unmatched=tuple(unmatched),
        dry_run=dry_run,
    )


def parse_linkedin_engagement_csv(csv_path: str | Path) -> list[LinkedInEngagementRow]:
    """Parse LinkedIn engagement rows with flexible, common header names."""
    with Path(csv_path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return []
        header_map = _resolve_headers(reader.fieldnames)
        rows = []
        for index, raw in enumerate(reader, start=2):
            url = _clean(raw.get(header_map.get("url", ""), ""))
            post_id = _clean(raw.get(header_map.get("post_id", ""), ""))
            if not post_id and url:
                post_id = extract_linkedin_post_id(url)
            impressions = _parse_int(raw.get(header_map.get("impressions", ""), ""))
            likes = _parse_int(raw.get(header_map.get("likes", ""), ""))
            comments = _parse_int(raw.get(header_map.get("comments", ""), ""))
            shares = _parse_int(raw.get(header_map.get("shares", ""), ""))
            rows.append(
                LinkedInEngagementRow(
                    source_row=index,
                    linkedin_url=url or None,
                    post_id=post_id or None,
                    impression_count=impressions,
                    like_count=likes,
                    comment_count=comments,
                    share_count=shares,
                    engagement_score=compute_linkedin_engagement_score(
                        likes,
                        comments,
                        shares,
                        impressions,
                    ),
                )
            )
    return rows


def match_linkedin_row(
    row: LinkedInEngagementRow,
    candidates: Iterable[dict[str, Any]],
) -> int | None:
    """Find a generated_content id for a parsed LinkedIn row."""
    row_url = normalize_url(row.linkedin_url)
    row_post_id = row.post_id
    for candidate in candidates:
        if row_url and row_url in candidate["urls"]:
            return int(candidate["content_id"])
        if row_post_id and row_post_id in candidate["post_ids"]:
            return int(candidate["content_id"])
    return None


def normalize_url(url: str | None) -> str | None:
    """Normalize URLs enough to match CSV exports with tracking params."""
    url = _clean(url)
    if not url:
        return None
    parts = urlsplit(url)
    if not parts.scheme or not parts.netloc:
        return url.rstrip("/")
    query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith("utm_")
    ]
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            parts.path.rstrip("/"),
            urlencode(query, doseq=True),
            "",
        )
    )


def extract_linkedin_post_id(value: str | None) -> str | None:
    """Extract a LinkedIn activity id from URLs or URNs when present."""
    value = _clean(value)
    if not value:
        return None
    match = _LINKEDIN_ID_RE.search(value)
    return match.group(1) if match else None


def _load_linkedin_publication_candidates(db: Any) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """SELECT gc.id AS content_id,
                  gc.published_url,
                  cp.platform_url,
                  cp.platform_post_id
           FROM generated_content gc
           LEFT JOIN content_publications cp
             ON cp.content_id = gc.id
            AND lower(cp.platform) = 'linkedin'
           WHERE gc.published_url IS NOT NULL
              OR cp.platform_url IS NOT NULL
              OR cp.platform_post_id IS NOT NULL"""
    ).fetchall()
    candidates: dict[int, dict[str, Any]] = {}
    for row in rows:
        content_id = int(row["content_id"])
        candidate = candidates.setdefault(
            content_id,
            {"content_id": content_id, "urls": set(), "post_ids": set()},
        )
        for url in (row["published_url"], row["platform_url"]):
            normalized = normalize_url(url)
            if normalized:
                candidate["urls"].add(normalized)
            post_id = extract_linkedin_post_id(url)
            if post_id:
                candidate["post_ids"].add(post_id)
        if row["platform_post_id"]:
            candidate["post_ids"].add(str(row["platform_post_id"]).strip())
            extracted = extract_linkedin_post_id(row["platform_post_id"])
            if extracted:
                candidate["post_ids"].add(extracted)
    return list(candidates.values())


def _resolve_headers(fieldnames: list[str]) -> dict[str, str]:
    normalized = {_normalize_header(name): name for name in fieldnames}
    resolved = {}
    for canonical, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            normalized_alias = _normalize_header(alias)
            if normalized_alias in normalized:
                resolved[canonical] = normalized[normalized_alias]
                break
    if "url" not in resolved and "post_id" not in resolved:
        raise ValueError("CSV must include a LinkedIn URL or post ID column")
    return resolved


def _normalize_header(value: str) -> str:
    return re.sub(r"[\s_-]+", " ", value.strip().lower())


def _parse_int(value: object) -> int:
    match = _INT_RE.search(str(value or ""))
    return int(match.group(0).replace(",", "")) if match else 0


def _clean(value: object) -> str:
    return str(value or "").strip()
