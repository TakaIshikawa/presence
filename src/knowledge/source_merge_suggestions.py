"""Suggest read-only merge candidates for duplicate curated sources."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse


DEFAULT_MIN_CONFIDENCE = 0.7

TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "igshid",
    "mc_cid",
    "mc_eid",
    "mkt_tok",
    "ref",
    "ref_src",
    "s",
    "spm",
}
HANDLE_TYPES = {"x_account", "twitter_account", "bluesky_account", "mastodon_account"}
URLISH_TYPES = {"blog", "newsletter", "site", "rss"}
CONFLICT_FIELDS = (
    "source_type",
    "identifier",
    "name",
    "license",
    "status",
    "active",
    "feed_url",
    "canonical_url",
    "link_title",
    "site_name",
)


@dataclass(frozen=True)
class SourceMergeCandidate:
    id: int
    source_type: str
    identifier: str
    name: str | None
    license: str | None
    status: str | None
    active: int | None
    feed_url: str | None
    canonical_url: str | None
    link_title: str | None
    site_name: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SourceMergeSuggestion:
    confidence: float
    evidence: tuple[str, ...]
    canonical_survivor_candidates: tuple[SourceMergeCandidate, ...]
    duplicate_ids: tuple[int, ...]
    conflicting_fields: dict[str, list[Any]]
    sources: tuple[SourceMergeCandidate, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "confidence": self.confidence,
            "evidence": list(self.evidence),
            "canonical_survivor_candidates": [
                candidate.to_dict() for candidate in self.canonical_survivor_candidates
            ],
            "duplicate_ids": list(self.duplicate_ids),
            "conflicting_fields": self.conflicting_fields,
            "sources": [source.to_dict() for source in self.sources],
        }


@dataclass(frozen=True)
class SourceMergeSuggestionReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    suggestions: tuple[SourceMergeSuggestion, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "filters": self.filters,
            "totals": self.totals,
            "suggestions": [suggestion.to_dict() for suggestion in self.suggestions],
            "missing_tables": list(self.missing_tables),
        }


def build_source_merge_suggestion_report(
    db_or_conn: Any,
    *,
    source_type: str | None = None,
    status: str | None = None,
    min_confidence: float = DEFAULT_MIN_CONFIDENCE,
    now: datetime | None = None,
) -> SourceMergeSuggestionReport:
    """Find likely duplicate curated_sources rows without mutating the database."""
    if min_confidence < 0 or min_confidence > 1:
        raise ValueError("min_confidence must be between 0 and 1")

    generated_at = _as_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    if not _table_exists(conn, "curated_sources"):
        return SourceMergeSuggestionReport(
            generated_at=generated_at.isoformat(),
            filters={
                "source_type": source_type,
                "status": status,
                "min_confidence": min_confidence,
            },
            totals={"source_count": 0, "suggestion_count": 0},
            suggestions=(),
            missing_tables=("curated_sources",),
        )

    rows = _load_sources(conn, source_type=source_type, status=status)
    pair_evidence = _pair_evidence(rows)
    components = _components(pair_evidence, min_confidence=min_confidence)
    rows_by_id = {int(row["id"]): row for row in rows}
    suggestions = [
        _build_suggestion(component, pair_evidence, rows_by_id)
        for component in components
    ]
    suggestions.sort(
        key=lambda suggestion: (
            -suggestion.confidence,
            suggestion.duplicate_ids,
            tuple(source.id for source in suggestion.sources),
        )
    )

    return SourceMergeSuggestionReport(
        generated_at=generated_at.isoformat(),
        filters={
            "source_type": source_type,
            "status": status,
            "min_confidence": min_confidence,
        },
        totals={"source_count": len(rows), "suggestion_count": len(suggestions)},
        suggestions=tuple(suggestions),
    )


def format_source_merge_suggestion_json(report: SourceMergeSuggestionReport) -> str:
    """Serialize a source merge suggestion report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_merge_suggestion_text(report: SourceMergeSuggestionReport) -> str:
    """Format source merge suggestions for terminal review."""
    lines = [
        "Curated Source Merge Suggestions",
        f"Generated: {report.generated_at}",
        f"Source type: {report.filters['source_type'] or 'all'}",
        f"Status: {report.filters['status'] or 'all'}",
        f"Minimum confidence: {report.filters['min_confidence']:.2f}",
        f"Sources scanned: {report.totals['source_count']}",
        f"Suggestions: {report.totals['suggestion_count']}",
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.suggestions:
        lines.append("No likely duplicate curated sources found.")
        return "\n".join(lines)

    for index, suggestion in enumerate(report.suggestions, start=1):
        survivor_ids = ", ".join(
            str(candidate.id) for candidate in suggestion.canonical_survivor_candidates
        )
        duplicate_ids = ", ".join(str(source_id) for source_id in suggestion.duplicate_ids)
        lines.append("")
        lines.append(
            f"{index}. confidence={suggestion.confidence:.2f} "
            f"survivor_candidates={survivor_ids} duplicate_ids={duplicate_ids}"
        )
        lines.append("   evidence=" + "; ".join(suggestion.evidence))
        if suggestion.conflicting_fields:
            conflicts = ", ".join(
                f"{field}={values}"
                for field, values in sorted(suggestion.conflicting_fields.items())
            )
            lines.append(f"   conflicts: {conflicts}")
        for source in suggestion.sources:
            lines.append(
                f"   - #{source.id} {source.source_type}:{source.identifier} "
                f"status={source.status or 'unknown'} "
                f"license={source.license or 'unknown'} "
                f"name={source.name or ''}"
            )
    return "\n".join(lines)


def normalize_handle(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    text = re.sub(r"^https?://(?:www\.)?(?:x|twitter)\.com/", "", text)
    text = text.split("?", 1)[0].strip("/")
    return text[1:] if text.startswith("@") else text


def normalize_domain(value: Any) -> str | None:
    parsed = _parse_urlish(value)
    if parsed is None:
        return None
    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if host.startswith("www."):
        host = host[4:]
    if "." not in host:
        return None
    return host or None


def normalize_url(value: Any) -> str | None:
    parsed = _parse_urlish(value)
    if parsed is None:
        return None
    scheme = "https"
    host = (parsed.hostname or "").strip().lower().rstrip(".")
    if not host:
        return None
    if host.startswith("www."):
        host = host[4:]
    path = re.sub(r"/+", "/", parsed.path or "/").rstrip("/")
    query_items = [
        (key, val)
        for key, val in parse_qsl(parsed.query, keep_blank_values=True)
        if not _is_tracking_query_key(key)
    ]
    query = urlencode(sorted(query_items))
    return urlunparse((scheme, host, path, "", query, ""))


def normalize_text(value: Any) -> str | None:
    text = re.sub(r"\s+", " ", str(value or "").strip().lower())
    return text or None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return db_or_conn.conn if hasattr(db_or_conn, "conn") else db_or_conn


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _load_sources(
    conn: sqlite3.Connection,
    *,
    source_type: str | None,
    status: str | None,
) -> list[dict[str, Any]]:
    where = []
    params: list[Any] = []
    if source_type:
        where.append("source_type = ?")
        params.append(source_type)
    if status:
        where.append("status = ?")
        params.append(status)
    sql = "SELECT * FROM curated_sources"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id"
    cursor = conn.execute(sql, params)
    return [dict(row) for row in cursor.fetchall()]


def _pair_evidence(rows: list[dict[str, Any]]) -> dict[tuple[int, int], dict[str, float]]:
    buckets: dict[tuple[str, str], list[int]] = defaultdict(list)
    for row in rows:
        source_id = int(row["id"])
        for key in _row_keys(row):
            buckets[key].append(source_id)

    pair_evidence: dict[tuple[int, int], dict[str, float]] = defaultdict(dict)
    for (kind, value), ids in buckets.items():
        unique_ids = sorted(set(ids))
        if len(unique_ids) < 2:
            continue
        label = f"{kind}:{value}"
        confidence = _base_confidence(kind)
        for index, left_id in enumerate(unique_ids[:-1]):
            for right_id in unique_ids[index + 1 :]:
                pair_evidence[(left_id, right_id)][label] = confidence
    return pair_evidence


def _row_keys(row: dict[str, Any]) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    source_type = str(row.get("source_type") or "").strip().lower()
    identifier = row.get("identifier")

    if source_type in HANDLE_TYPES:
        handle = normalize_handle(identifier)
        if handle:
            keys.add(("handle", handle))
    if source_type in URLISH_TYPES or (
        source_type not in HANDLE_TYPES and normalize_domain(identifier)
    ):
        domain = normalize_domain(identifier)
        if domain:
            keys.add(("domain", domain))

    for column, kind in (("feed_url", "feed_url"), ("canonical_url", "canonical_url")):
        normalized = normalize_url(row.get(column))
        if normalized:
            keys.add((kind, normalized))
            domain = normalize_domain(normalized)
            if domain:
                keys.add(("domain", domain))

    title = normalize_text(row.get("link_title"))
    site_name = normalize_text(row.get("site_name"))
    if title and site_name:
        keys.add(("link_metadata", f"{site_name}|{title}"))
    return keys


def _base_confidence(kind: str) -> float:
    return {
        "canonical_url": 1.0,
        "feed_url": 0.95,
        "handle": 0.9,
        "domain": 0.85,
        "link_metadata": 0.72,
    }[kind]


def _components(
    pair_evidence: dict[tuple[int, int], dict[str, float]],
    *,
    min_confidence: float,
) -> list[tuple[int, ...]]:
    parent: dict[int, int] = {}

    def find(item: int) -> int:
        parent.setdefault(item, item)
        while parent[item] != item:
            parent[item] = parent[parent[item]]
            item = parent[item]
        return item

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    for (left_id, right_id), evidence in pair_evidence.items():
        if _confidence(evidence) >= min_confidence:
            union(left_id, right_id)

    grouped: dict[int, list[int]] = defaultdict(list)
    for source_id in sorted(parent):
        grouped[find(source_id)].append(source_id)
    return [tuple(ids) for ids in grouped.values() if len(ids) > 1]


def _build_suggestion(
    component: tuple[int, ...],
    pair_evidence: dict[tuple[int, int], dict[str, float]],
    rows_by_id: dict[int, dict[str, Any]],
) -> SourceMergeSuggestion:
    component_set = set(component)
    evidence: dict[str, float] = {}
    for (left_id, right_id), pair_labels in pair_evidence.items():
        if left_id in component_set and right_id in component_set:
            evidence.update(pair_labels)
    rows = [rows_by_id[source_id] for source_id in sorted(component)]
    sources = tuple(_candidate(row) for row in rows)
    survivor_rows = _survivor_rows(rows)
    survivor_ids = {int(row["id"]) for row in survivor_rows}
    return SourceMergeSuggestion(
        confidence=_confidence(evidence),
        evidence=tuple(sorted(evidence)),
        canonical_survivor_candidates=tuple(_candidate(row) for row in survivor_rows),
        duplicate_ids=tuple(source.id for source in sources if source.id not in survivor_ids),
        conflicting_fields=_conflicts(rows),
        sources=sources,
    )


def _confidence(evidence: dict[str, float]) -> float:
    if not evidence:
        return 0.0
    score = max(evidence.values()) + (0.03 * (len(evidence) - 1))
    return round(min(score, 1.0), 2)


def _candidate(row: dict[str, Any]) -> SourceMergeCandidate:
    return SourceMergeCandidate(
        id=int(row["id"]),
        source_type=str(row.get("source_type") or ""),
        identifier=str(row.get("identifier") or ""),
        name=row.get("name"),
        license=row.get("license"),
        status=row.get("status"),
        active=row.get("active"),
        feed_url=row.get("feed_url"),
        canonical_url=row.get("canonical_url"),
        link_title=row.get("link_title"),
        site_name=row.get("site_name"),
    )


def _survivor_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    scores = [(_survivor_score(row), row) for row in rows]
    best = max(score for score, _row in scores)
    return [row for score, row in scores if score == best]


def _survivor_score(row: dict[str, Any]) -> tuple[int, int, int, int]:
    status_rank = {"active": 4, "candidate": 3, "paused": 2, "rejected": 1}
    metadata = sum(
        1
        for field in ("name", "feed_url", "canonical_url", "link_title", "site_name")
        if row.get(field)
    )
    active = 1 if row.get("active") in (1, True, None) else 0
    return (
        status_rank.get(str(row.get("status") or "").lower(), 0),
        active,
        metadata,
        -int(row["id"]),
    )


def _conflicts(rows: list[dict[str, Any]]) -> dict[str, list[Any]]:
    conflicts: dict[str, list[Any]] = {}
    for field in CONFLICT_FIELDS:
        values = sorted(
            {
                row.get(field)
                for row in rows
                if row.get(field) not in (None, "")
            },
            key=lambda value: str(value),
        )
        if len(values) > 1:
            conflicts[field] = values
    return conflicts


def _parse_urlish(value: Any):
    text = str(value or "").strip()
    if not text:
        return None
    parsed = urlparse(text if "://" in text else f"https://{text}")
    return parsed if parsed.netloc else None


def _is_tracking_query_key(key: str) -> bool:
    lowered = key.lower()
    return lowered in TRACKING_QUERY_KEYS or lowered.startswith(TRACKING_QUERY_PREFIXES)
