"""Import LinkedIn comment exports into the reply queue."""

from __future__ import annotations

import csv
import json
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


LINKEDIN_PLATFORM = "linkedin"
_INT_RE = re.compile(r"-?\d[\d,]*")
_LINKEDIN_ID_RE = re.compile(r"(?:activity|urn:li:activity:)[-_:]?(\d+)")

_HEADER_ALIASES = {
    "post_url": {
        "post url",
        "post_url",
        "url",
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
    "comment_id": {"comment id", "comment_id", "id", "urn", "comment urn", "comment_urn"},
    "author": {"author", "commenter", "name", "profile name", "profile_name"},
    "author_profile_url": {
        "author profile url",
        "author_profile_url",
        "profile url",
        "profile_url",
        "author url",
        "author_url",
    },
    "body": {"body", "comment", "comment text", "comment_text", "text", "message"},
    "created_at": {"created at", "created_at", "date", "timestamp", "commented at", "commented_at"},
    "like_count": {"like count", "like_count", "likes", "reaction count", "reaction_count"},
}


@dataclass(frozen=True)
class LinkedInComment:
    """One normalized LinkedIn comment export row."""

    source_row: int
    post_url: str | None
    post_id: str | None
    comment_id: str
    author: str
    author_profile_url: str | None
    body: str
    created_at: datetime | None
    like_count: int | None = None
    content_id: int | None = None


@dataclass(frozen=True)
class LinkedInCommentImportResult:
    inserted: tuple[LinkedInComment, ...]
    skipped: tuple[dict[str, Any], ...]
    dry_run: bool

    @property
    def insert_count(self) -> int:
        return len(self.inserted)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped)


def parse_linkedin_comments(path: str | Path, *, format: str) -> list[LinkedInComment]:
    """Parse a LinkedIn comments CSV or JSON export."""
    normalized_format = format.lower()
    if normalized_format == "csv":
        return parse_linkedin_comments_csv(path)
    if normalized_format == "json":
        return parse_linkedin_comments_json(path)
    raise ValueError("format must be csv or json")


def parse_linkedin_comments_csv(path: str | Path) -> list[LinkedInComment]:
    """Parse LinkedIn comments from a CSV with common export header variants."""
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if not reader.fieldnames:
            return []
        header_map = _resolve_headers(reader.fieldnames)
        return [
            _normalize_comment(raw, source_row=index, header_map=header_map)
            for index, raw in enumerate(reader, start=2)
        ]


def parse_linkedin_comments_json(path: str | Path) -> list[LinkedInComment]:
    """Parse LinkedIn comments from a JSON list or object containing comments."""
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        rows = payload.get("comments") or payload.get("data") or payload.get("items") or []
    else:
        rows = []
    if not isinstance(rows, list):
        raise ValueError("JSON comments export must be a list or contain a comments/data/items list")

    comments = []
    for index, raw in enumerate(rows, start=1):
        if not isinstance(raw, dict):
            continue
        comments.append(_normalize_comment(raw, source_row=index))
    return comments


def import_linkedin_comments(
    db: Any,
    path: str | Path,
    *,
    format: str,
    dry_run: bool = False,
    limit: int | None = None,
) -> LinkedInCommentImportResult:
    """Parse, match, deduplicate, and optionally queue LinkedIn comments."""
    comments = parse_linkedin_comments(path, format=format)
    if limit is not None:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        comments = comments[:limit]

    candidates = _load_linkedin_publication_candidates(db)
    processed_ids = processed_linkedin_comment_ids(db)
    seen_ids: set[str] = set()
    inserted: list[LinkedInComment] = []
    skipped: list[dict[str, Any]] = []

    for comment in comments:
        if not comment.comment_id:
            skipped.append({"source_row": comment.source_row, "reason": "missing_comment_id"})
            continue
        if not comment.body:
            skipped.append(
                {
                    "source_row": comment.source_row,
                    "comment_id": comment.comment_id,
                    "reason": "missing_body",
                }
            )
            continue
        if comment.comment_id in seen_ids or comment.comment_id in processed_ids:
            skipped.append(
                {
                    "source_row": comment.source_row,
                    "comment_id": comment.comment_id,
                    "reason": "already_processed",
                }
            )
            continue

        content = match_linkedin_comment(comment, candidates)
        matched = LinkedInComment(
            source_row=comment.source_row,
            post_url=comment.post_url,
            post_id=comment.post_id,
            comment_id=comment.comment_id,
            author=comment.author,
            author_profile_url=comment.author_profile_url,
            body=comment.body,
            created_at=comment.created_at,
            like_count=comment.like_count,
            content_id=int(content["content_id"]) if content else None,
        )
        inserted.append(matched)
        seen_ids.add(matched.comment_id)

        if not dry_run:
            db.insert_reply_draft(
                inbound_tweet_id=matched.comment_id,
                inbound_author_handle=matched.author or "unknown",
                inbound_author_id=matched.author_profile_url or "",
                inbound_text=matched.body,
                our_tweet_id=matched.post_id or matched.post_url or "unknown",
                our_content_id=matched.content_id,
                our_post_text=str(content["content"]) if content else "",
                draft_text="",
                platform=LINKEDIN_PLATFORM,
                inbound_url=matched.post_url,
                our_platform_id=matched.post_id,
                platform_metadata=json.dumps(
                    _comment_metadata(matched, matched_content=content),
                    sort_keys=True,
                ),
                intent="other",
                priority="normal",
                status="pending",
            )
            processed_ids.add(matched.comment_id)

    return LinkedInCommentImportResult(
        inserted=tuple(inserted),
        skipped=tuple(skipped),
        dry_run=dry_run,
    )


def match_linkedin_comment(
    comment: LinkedInComment,
    candidates: Iterable[dict[str, Any]],
) -> dict[str, Any] | None:
    """Find generated content for a LinkedIn comment's parent post."""
    row_url = normalize_url(comment.post_url)
    row_post_id = comment.post_id or extract_linkedin_post_id(comment.post_url)
    for candidate in candidates:
        if row_url and row_url in candidate["urls"]:
            return candidate
        if row_post_id and row_post_id in candidate["post_ids"]:
            return candidate
    return None


def processed_linkedin_comment_ids(db: Any) -> set[str]:
    """Return LinkedIn comment IDs already present in reply_queue."""
    ids: set[str] = set()
    if not getattr(db, "conn", None):
        return ids
    rows = db.conn.execute(
        """SELECT inbound_tweet_id, platform_metadata
           FROM reply_queue
           WHERE lower(COALESCE(platform, '')) = ?""",
        (LINKEDIN_PLATFORM,),
    ).fetchall()
    for row in rows:
        inbound_id = row["inbound_tweet_id"] if hasattr(row, "keys") else row[0]
        metadata_raw = row["platform_metadata"] if hasattr(row, "keys") else row[1]
        if inbound_id:
            ids.add(str(inbound_id))
        try:
            metadata = json.loads(metadata_raw or "{}")
        except (TypeError, json.JSONDecodeError):
            metadata = {}
        if metadata.get("comment_id"):
            ids.add(str(metadata["comment_id"]))
    return ids


def normalize_url(url: str | None) -> str | None:
    """Normalize LinkedIn URLs enough to match exports with tracking params."""
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
                  gc.content,
                  gc.published_url,
                  cp.platform_url,
                  cp.platform_post_id
           FROM generated_content gc
           LEFT JOIN content_publications cp
             ON cp.content_id = gc.id
            AND lower(cp.platform) = ?
           WHERE gc.published_url IS NOT NULL
              OR cp.platform_url IS NOT NULL
              OR cp.platform_post_id IS NOT NULL""",
        (LINKEDIN_PLATFORM,),
    ).fetchall()
    candidates: dict[int, dict[str, Any]] = {}
    for row in rows:
        content_id = int(row["content_id"])
        candidate = candidates.setdefault(
            content_id,
            {"content_id": content_id, "content": row["content"], "urls": set(), "post_ids": set()},
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


def _normalize_comment(
    raw: dict[str, Any],
    *,
    source_row: int,
    header_map: dict[str, str] | None = None,
) -> LinkedInComment:
    post_url = _field(raw, "post_url", header_map)
    post_id = _field(raw, "post_id", header_map) or extract_linkedin_post_id(post_url)
    return LinkedInComment(
        source_row=source_row,
        post_url=post_url or None,
        post_id=post_id or None,
        comment_id=_field(raw, "comment_id", header_map),
        author=_field(raw, "author", header_map),
        author_profile_url=_field(raw, "author_profile_url", header_map) or None,
        body=_field(raw, "body", header_map),
        created_at=_parse_datetime(_field(raw, "created_at", header_map)),
        like_count=_parse_optional_int(_field(raw, "like_count", header_map)),
    )


def _comment_metadata(
    comment: LinkedInComment,
    *,
    matched_content: dict[str, Any] | None,
) -> dict[str, Any]:
    metadata = {
        "source": "manual_linkedin_comment_import",
        "source_row": comment.source_row,
        "post_url": comment.post_url,
        "post_id": comment.post_id,
        "comment_id": comment.comment_id,
        "author": comment.author,
        "author_profile_url": comment.author_profile_url,
        "body": comment.body,
        "created_at": comment.created_at.isoformat() if comment.created_at else None,
        "like_count": comment.like_count,
        "matched_content_id": matched_content["content_id"] if matched_content else None,
    }
    return {key: value for key, value in metadata.items() if value not in (None, "")}


def _resolve_headers(fieldnames: list[str]) -> dict[str, str]:
    normalized = {_normalize_header(name): name for name in fieldnames}
    resolved = {}
    for canonical, aliases in _HEADER_ALIASES.items():
        for alias in aliases:
            if _normalize_header(alias) in normalized:
                resolved[canonical] = normalized[_normalize_header(alias)]
                break
    if "comment_id" not in resolved:
        raise ValueError("comments export must include a comment_id column")
    if "post_url" not in resolved and "post_id" not in resolved:
        raise ValueError("comments export must include post_url or post_id")
    return resolved


def _field(raw: dict[str, Any], canonical: str, header_map: dict[str, str] | None) -> str:
    if header_map is not None:
        return _clean(raw.get(header_map.get(canonical, ""), ""))
    for alias in _HEADER_ALIASES[canonical] | {canonical}:
        if alias in raw:
            return _clean(raw.get(alias))
        normalized_alias = _normalize_header(alias)
        for key, value in raw.items():
            if _normalize_header(str(key)) == normalized_alias:
                return _clean(value)
    return ""


def _normalize_header(value: str) -> str:
    return re.sub(r"[\s_-]+", " ", value.strip().lower())


def _parse_optional_int(value: object) -> int | None:
    raw = _clean(value)
    if not raw:
        return None
    match = _INT_RE.search(raw)
    return int(match.group(0).replace(",", "")) if match else None


def _parse_datetime(value: str) -> datetime | None:
    value = _clean(value)
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _clean(value: object) -> str:
    return str(value or "").strip()
