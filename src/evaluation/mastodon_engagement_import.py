"""Import Mastodon engagement snapshots from manual CSV exports."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from evaluation.engagement_scorer import compute_engagement_score


_INT_RE = re.compile(r"-?\d[\d,]*")
_STATUS_ID_RE = re.compile(r"(?:/statuses/|/status/|/@[^/]+/)(\d+)(?:$|[/?#])")

_HEADER_ALIASES = {
    "content_id": {"content id", "content_id", "generated_content_id"},
    "url": {
        "url",
        "post url",
        "post_url",
        "status url",
        "status_url",
        "mastodon url",
        "mastodon_url",
        "permalink",
        "link",
    },
    "post_id": {
        "post id",
        "post_id",
        "status id",
        "status_id",
        "toot id",
        "toot_id",
        "id",
    },
    "favourites": {
        "favourites",
        "favourites count",
        "favourite count",
        "favourite_count",
        "favorites",
        "favorite count",
        "likes",
    },
    "boosts": {
        "boosts",
        "boost count",
        "boost_count",
        "reblogs",
        "reblog count",
        "reblog_count",
        "reposts",
    },
    "replies": {
        "replies",
        "reply count",
        "reply_count",
        "comments",
        "comment count",
    },
}


@dataclass(frozen=True)
class MastodonEngagementRow:
    """One normalized Mastodon metrics row from a CSV."""

    source_row: int
    content_id: int | None
    mastodon_url: str | None
    post_id: str | None
    favourite_count: int
    boost_count: int
    reply_count: int
    engagement_score: float
    raw_metrics: dict[str, str]


def import_mastodon_engagement_csv(
    db: Any,
    csv_path: str | Path,
    *,
    fetched_at: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Parse, match, deduplicate, and optionally import a Mastodon CSV."""
    return import_mastodon_engagement_rows(
        db,
        parse_mastodon_engagement_csv(csv_path),
        fetched_at=fetched_at,
        dry_run=dry_run,
    )


def import_mastodon_engagement_rows(
    db: Any,
    rows: Iterable[MastodonEngagementRow | dict[str, Any]],
    *,
    fetched_at: str | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Import normalized or raw Mastodon CSV rows and return a deterministic report."""
    fetched_at = fetched_at or datetime.now(timezone.utc).isoformat()
    candidates = _load_mastodon_publication_candidates(db)
    seen_keys: set[tuple[str, str, str]] = set()
    report: dict[str, Any] = {
        "dry_run": dry_run,
        "fetched_at": fetched_at,
        "counts": {
            "rows": 0,
            "matched": 0,
            "unmatched": 0,
            "inserted": 0,
            "duplicates": 0,
            "invalid": 0,
        },
        "rows": [],
    }

    for index, raw_row in enumerate(rows, start=1):
        report["counts"]["rows"] += 1
        row_result: dict[str, Any] = {"row": _source_row(raw_row, index)}
        try:
            row = (
                raw_row
                if isinstance(raw_row, MastodonEngagementRow)
                else normalize_mastodon_engagement_row(raw_row, source_row=index)
            )
        except ValueError as exc:
            report["counts"]["invalid"] += 1
            row_result.update({"status": "invalid", "error": str(exc)})
            report["rows"].append(row_result)
            continue

        match = match_mastodon_row(row, candidates)
        if match is None:
            report["counts"]["unmatched"] += 1
            row_result.update(
                {
                    "status": "unmatched",
                    "mastodon_url": row.mastodon_url,
                    "post_id": row.post_id,
                }
            )
            report["rows"].append(row_result)
            continue

        mastodon_url = row.mastodon_url or _first_sorted(match["urls"])
        post_id = row.post_id or _first_sorted(match["post_ids"])
        if not mastodon_url and not post_id:
            report["counts"]["invalid"] += 1
            row_result.update(
                {
                    "status": "invalid",
                    "content_id": match["content_id"],
                    "error": "matched content has no Mastodon URL or post ID",
                }
            )
            report["rows"].append(row_result)
            continue

        report["counts"]["matched"] += 1
        duplicate_key = _dedupe_key(mastodon_url, post_id, fetched_at)
        is_duplicate = duplicate_key in seen_keys or _snapshot_exists(
            db,
            mastodon_url=mastodon_url,
            post_id=post_id,
            fetched_at=fetched_at,
        )
        seen_keys.add(duplicate_key)

        row_result.update(
            {
                "content_id": match["content_id"],
                "mastodon_url": mastodon_url,
                "post_id": post_id,
                "metrics": {
                    "favourite_count": row.favourite_count,
                    "boost_count": row.boost_count,
                    "reply_count": row.reply_count,
                    "engagement_score": row.engagement_score,
                },
            }
        )

        if is_duplicate:
            report["counts"]["duplicates"] += 1
            row_result["status"] = "duplicate"
            report["rows"].append(row_result)
            continue

        report["counts"]["inserted"] += 1
        row_result["status"] = "matched" if dry_run else "inserted"
        if not dry_run:
            row_result["engagement_id"] = _insert_mastodon_engagement(
                db,
                content_id=match["content_id"],
                mastodon_url=mastodon_url,
                post_id=post_id,
                favourite_count=row.favourite_count,
                boost_count=row.boost_count,
                reply_count=row.reply_count,
                engagement_score=row.engagement_score,
                fetched_at=fetched_at,
                raw_metrics=row.raw_metrics,
            )
        report["rows"].append(row_result)

    return report


def parse_mastodon_engagement_csv(csv_path: str | Path) -> list[MastodonEngagementRow]:
    """Parse Mastodon engagement rows with flexible common header names."""
    with Path(csv_path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return []
        header_map = _resolve_headers(reader.fieldnames)
        rows = []
        for index, raw in enumerate(reader, start=2):
            rows.append(_row_from_header_map(raw, header_map, source_row=index))
    return rows


def normalize_mastodon_engagement_row(
    row: dict[str, Any],
    *,
    source_row: int = 1,
) -> MastodonEngagementRow:
    """Normalize a raw dict row using the same aliases as CSV parsing."""
    header_map = _resolve_headers(list(row.keys()))
    return _row_from_header_map(row, header_map, source_row=source_row)


def match_mastodon_row(
    row: MastodonEngagementRow,
    candidates: Iterable[dict[str, Any]],
) -> dict[str, Any] | None:
    """Find the generated_content candidate for a parsed Mastodon row."""
    if row.content_id is not None:
        for candidate in candidates:
            if int(candidate["content_id"]) == row.content_id:
                return candidate
    for candidate in candidates:
        if row.mastodon_url and row.mastodon_url in candidate["urls"]:
            return candidate
        if row.post_id and row.post_id in candidate["post_ids"]:
            return candidate
    return None


def normalize_url(url: str | None) -> str | None:
    """Normalize Mastodon URLs enough to match exports with tracking params."""
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


def extract_mastodon_post_id(value: str | None) -> str | None:
    """Extract a Mastodon status id from common status URLs."""
    value = _clean(value)
    if not value:
        return None
    if value.isdigit():
        return value
    match = _STATUS_ID_RE.search(value if value.endswith("/") else f"{value}/")
    return match.group(1) if match else None


def _row_from_header_map(
    raw: dict[str, Any],
    header_map: dict[str, str],
    *,
    source_row: int,
) -> MastodonEngagementRow:
    content_id = _parse_optional_int(raw.get(header_map.get("content_id", ""), ""))
    mastodon_url = normalize_url(raw.get(header_map.get("url", ""), ""))
    post_id = _clean(raw.get(header_map.get("post_id", ""), "")) or None
    if not post_id and mastodon_url:
        post_id = extract_mastodon_post_id(mastodon_url)
    if not any((content_id is not None, mastodon_url, post_id)):
        raise ValueError("CSV row must include content_id, Mastodon URL, or post ID")

    favourites = _parse_count(raw.get(header_map.get("favourites", ""), ""))
    boosts = _parse_count(raw.get(header_map.get("boosts", ""), ""))
    replies = _parse_count(raw.get(header_map.get("replies", ""), ""))
    return MastodonEngagementRow(
        source_row=source_row,
        content_id=content_id,
        mastodon_url=mastodon_url,
        post_id=post_id,
        favourite_count=favourites,
        boost_count=boosts,
        reply_count=replies,
        engagement_score=compute_engagement_score(favourites, boosts, replies),
        raw_metrics={str(key): _clean(value) for key, value in raw.items() if key},
    )


def _load_mastodon_publication_candidates(db: Any) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """SELECT gc.id AS content_id,
                  gc.published_url,
                  cp.platform_url,
                  cp.platform_post_id
           FROM generated_content gc
           LEFT JOIN content_publications cp
             ON cp.content_id = gc.id
            AND lower(cp.platform) = 'mastodon'"""
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
            post_id = extract_mastodon_post_id(url)
            if post_id:
                candidate["post_ids"].add(post_id)
        platform_post_id = _clean(row["platform_post_id"])
        if platform_post_id:
            candidate["post_ids"].add(platform_post_id)
            extracted = extract_mastodon_post_id(platform_post_id)
            if extracted:
                candidate["post_ids"].add(extracted)
    return list(candidates.values())


def _snapshot_exists(
    db: Any,
    *,
    mastodon_url: str | None,
    post_id: str | None,
    fetched_at: str,
) -> bool:
    row = db.conn.execute(
        """SELECT id
           FROM mastodon_engagement
           WHERE fetched_at = ?
             AND ((? IS NOT NULL AND post_id = ?)
              OR  (? IS NOT NULL AND mastodon_url = ?))
           LIMIT 1""",
        (fetched_at, post_id, post_id, mastodon_url, mastodon_url),
    ).fetchone()
    return row is not None


def _insert_mastodon_engagement(
    db: Any,
    *,
    content_id: int,
    mastodon_url: str | None,
    post_id: str | None,
    favourite_count: int,
    boost_count: int,
    reply_count: int,
    engagement_score: float,
    fetched_at: str,
    raw_metrics: dict[str, str],
) -> int:
    cursor = db.conn.execute(
        """INSERT INTO mastodon_engagement
           (content_id, mastodon_url, post_id, favourite_count, boost_count,
            reply_count, engagement_score, fetched_at, raw_metrics)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            content_id,
            mastodon_url,
            post_id,
            favourite_count,
            boost_count,
            reply_count,
            engagement_score,
            fetched_at,
            json.dumps(raw_metrics, sort_keys=True),
        ),
    )
    db.conn.commit()
    return int(cursor.lastrowid)


def _resolve_headers(fieldnames: list[str]) -> dict[str, str]:
    normalized = {_normalize_header(name): name for name in fieldnames}
    resolved = {}
    for canonical, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            normalized_alias = _normalize_header(alias)
            if normalized_alias in normalized:
                resolved[canonical] = normalized[normalized_alias]
                break
    if not any(key in resolved for key in ("content_id", "url", "post_id")):
        raise ValueError("CSV must include content_id, Mastodon URL, or post ID column")
    return resolved


def _parse_count(value: object) -> int:
    count = _parse_optional_int(value) or 0
    if count < 0:
        raise ValueError("metric counts must be non-negative integers")
    return count


def _parse_optional_int(value: object) -> int | None:
    text = _clean(value)
    if not text:
        return None
    match = _INT_RE.search(text)
    if not match:
        raise ValueError(f"{text!r} must be an integer")
    return int(match.group(0).replace(",", ""))


def _normalize_header(value: str) -> str:
    return re.sub(r"[\s_-]+", " ", value.strip().lower())


def _clean(value: object) -> str:
    return str(value or "").strip()


def _first_sorted(values: set[str]) -> str | None:
    return sorted(values)[0] if values else None


def _dedupe_key(
    mastodon_url: str | None,
    post_id: str | None,
    fetched_at: str,
) -> tuple[str, str, str]:
    return (mastodon_url or "", post_id or "", fetched_at)


def _source_row(row: MastodonEngagementRow | dict[str, Any], fallback: int) -> int:
    if isinstance(row, MastodonEngagementRow):
        return row.source_row
    return fallback
