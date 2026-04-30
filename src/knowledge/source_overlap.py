"""Detect redundant curated knowledge sources without embeddings."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_DAYS = 90
DEFAULT_MIN_OVERLAP = 2
DEFAULT_SIMILARITY_THRESHOLD = 0.5
SOCIAL_DOMAINS = {
    "bsky.app",
    "linkedin.com",
    "twitter.com",
    "x.com",
}
TOKEN_RE = re.compile(r"[a-z0-9]+")


@dataclass(frozen=True)
class SourceRef:
    kind: str
    identifier: str

    @property
    def label(self) -> str:
        return f"{self.kind}:{self.identifier}"

    def to_dict(self) -> dict[str, str]:
        return {"kind": self.kind, "identifier": self.identifier, "label": self.label}


@dataclass(frozen=True)
class SourceOverlapMatch:
    left_id: int
    right_id: int
    similarity: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_id": self.left_id,
            "right_id": self.right_id,
            "similarity": self.similarity,
        }


@dataclass(frozen=True)
class SourceOverlapPair:
    left_source: SourceRef
    right_source: SourceRef
    overlap_count: int
    average_similarity: float
    representative_item_ids: list[int]
    suggested_action: str
    matches: list[SourceOverlapMatch]

    def to_dict(self) -> dict[str, Any]:
        return {
            "left_source": self.left_source.to_dict(),
            "right_source": self.right_source.to_dict(),
            "overlap_count": self.overlap_count,
            "average_similarity": self.average_similarity,
            "representative_item_ids": self.representative_item_ids,
            "suggested_action": self.suggested_action,
            "matches": [match.to_dict() for match in self.matches],
        }


@dataclass(frozen=True)
class SourceOverlapReport:
    days: int
    min_overlap: int
    similarity_threshold: float
    include_restricted: bool
    generated_at: str
    source_count: int
    row_count: int
    pair_count: int
    pairs: list[SourceOverlapPair]

    def to_dict(self) -> dict[str, Any]:
        return {
            "days": self.days,
            "min_overlap": self.min_overlap,
            "similarity_threshold": self.similarity_threshold,
            "include_restricted": self.include_restricted,
            "generated_at": self.generated_at,
            "source_count": self.source_count,
            "row_count": self.row_count,
            "pair_count": self.pair_count,
            "pairs": [pair.to_dict() for pair in self.pairs],
        }


def normalize_domain(url: str | None) -> str | None:
    """Return a stable lower-case registrable-enough domain for source grouping."""
    value = (url or "").strip()
    if not value:
        return None
    parsed = urlparse(value if "://" in value else f"https://{value}")
    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    return host or None


def normalize_author(author: str | None) -> str | None:
    value = (author or "").strip().lower()
    if not value:
        return None
    return value[1:] if value.startswith("@") else value


def source_ref_for_row(row: dict[str, Any]) -> SourceRef:
    domain = normalize_domain(row.get("source_url"))
    author = normalize_author(row.get("author"))
    if domain in SOCIAL_DOMAINS and author:
        return SourceRef("author", author)
    if domain:
        return SourceRef("domain", domain)
    if author:
        return SourceRef("author", author)
    source_id = str(row.get("source_id") or "unknown").strip().lower() or "unknown"
    return SourceRef("source_id", source_id)


def tokenize_text(text: str | None) -> set[str]:
    return set(TOKEN_RE.findall((text or "").lower()))


def jaccard_similarity(left: str | None, right: str | None) -> float:
    left_tokens = tokenize_text(left)
    right_tokens = tokenize_text(right)
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens | right_tokens)


def _knowledge_columns(conn: sqlite3.Connection) -> set[str]:
    return {row[1] for row in conn.execute("PRAGMA table_info(knowledge)")}


def _timestamp_expression(columns: set[str]) -> str:
    parts = [
        column
        for column in ("published_at", "ingested_at", "created_at")
        if column in columns
    ]
    if not parts:
        return "NULL"
    if len(parts) == 1:
        return parts[0]
    return f"COALESCE({', '.join(parts)})"


def _optional_column(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _parse_now(now: datetime | None) -> datetime:
    parsed = now or datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _fetch_curated_rows(
    conn: sqlite3.Connection,
    *,
    days: int,
    include_restricted: bool,
    now: datetime,
) -> list[dict[str, Any]]:
    columns = _knowledge_columns(conn)
    if not columns:
        return []

    timestamp_expr = _timestamp_expression(columns)
    where = ["source_type LIKE 'curated_%'"]
    params: list[Any] = []
    if "approved" in columns:
        where.append("approved = 1")
    if not include_restricted and "license" in columns:
        where.append("COALESCE(license, 'attribution_required') != 'restricted'")
    if days > 0 and timestamp_expr != "NULL":
        cutoff = (now - timedelta(days=days)).isoformat()
        where.append(f"{timestamp_expr} >= ?")
        params.append(cutoff)

    cursor = conn.execute(
        f"""SELECT id,
                   source_type,
                   {_optional_column(columns, "source_id")},
                   {_optional_column(columns, "source_url")},
                   {_optional_column(columns, "author")},
                   {_optional_column(columns, "content")},
                   {_optional_column(columns, "insight")},
                   {_optional_column(columns, "license")},
                   {_optional_column(columns, "approved")},
                   {timestamp_expr} AS item_timestamp
            FROM knowledge
            WHERE {' AND '.join(where)}
            ORDER BY id""",
        params,
    )
    names = [description[0] for description in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def _comparison_text(row: dict[str, Any]) -> str:
    return str(row.get("insight") or row.get("content") or "")


def _suggested_action(overlap_count: int, average_similarity: float) -> str:
    if overlap_count >= 5 or average_similarity >= 0.8:
        return "review for consolidation or source cap"
    return "monitor for repeated redundancy"


def build_source_overlap_report(
    conn: sqlite3.Connection,
    *,
    days: int = DEFAULT_DAYS,
    min_overlap: int = DEFAULT_MIN_OVERLAP,
    limit: int | None = None,
    include_restricted: bool = False,
    similarity_threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
    now: datetime | None = None,
) -> SourceOverlapReport:
    """Rank source pairs that repeatedly contribute near-identical insights."""
    if days < 1:
        raise ValueError("days must be at least 1")
    if min_overlap < 1:
        raise ValueError("min_overlap must be at least 1")
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    if not 0 < similarity_threshold <= 1:
        raise ValueError("similarity_threshold must be greater than 0 and at most 1")

    generated_at = _parse_now(now)
    rows = _fetch_curated_rows(
        conn,
        days=days,
        include_restricted=include_restricted,
        now=generated_at,
    )
    grouped: dict[SourceRef, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(source_ref_for_row(row), []).append(row)

    pair_matches: list[SourceOverlapPair] = []
    sources = sorted(grouped, key=lambda source: source.label)
    for index, left_source in enumerate(sources):
        for right_source in sources[index + 1 :]:
            matches: list[SourceOverlapMatch] = []
            for left in grouped[left_source]:
                for right in grouped[right_source]:
                    similarity = jaccard_similarity(
                        _comparison_text(left),
                        _comparison_text(right),
                    )
                    if similarity >= similarity_threshold:
                        matches.append(
                            SourceOverlapMatch(
                                left_id=int(left["id"]),
                                right_id=int(right["id"]),
                                similarity=round(similarity, 3),
                            )
                        )

            if len(matches) < min_overlap:
                continue

            matches.sort(
                key=lambda match: (
                    -match.similarity,
                    match.left_id,
                    match.right_id,
                )
            )
            average_similarity = round(
                sum(match.similarity for match in matches) / len(matches),
                3,
            )
            representative_ids: list[int] = []
            for match in matches:
                for item_id in (match.left_id, match.right_id):
                    if item_id not in representative_ids:
                        representative_ids.append(item_id)
                    if len(representative_ids) >= 4:
                        break
                if len(representative_ids) >= 4:
                    break
            pair_matches.append(
                SourceOverlapPair(
                    left_source=left_source,
                    right_source=right_source,
                    overlap_count=len(matches),
                    average_similarity=average_similarity,
                    representative_item_ids=representative_ids,
                    suggested_action=_suggested_action(len(matches), average_similarity),
                    matches=matches,
                )
            )

    pair_matches.sort(
        key=lambda pair: (
            -pair.overlap_count,
            -pair.average_similarity,
            pair.left_source.label,
            pair.right_source.label,
        )
    )
    if limit is not None:
        pair_matches = pair_matches[:limit]

    return SourceOverlapReport(
        days=days,
        min_overlap=min_overlap,
        similarity_threshold=similarity_threshold,
        include_restricted=include_restricted,
        generated_at=generated_at.isoformat(),
        source_count=len(grouped),
        row_count=len(rows),
        pair_count=len(pair_matches),
        pairs=pair_matches,
    )
