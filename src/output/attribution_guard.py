"""Publication guard for knowledge sources that require attribution."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse


ATTRIBUTION_REQUIRED_LICENSE = "attribution_required"


@dataclass(frozen=True)
class AttributionGuardSource:
    """Attribution-required knowledge source linked to generated content."""

    knowledge_id: int
    source_url: str | None
    author: str | None
    license: str

    def as_dict(self) -> dict:
        return {
            "knowledge_id": self.knowledge_id,
            "source_url": self.source_url,
            "author": self.author,
            "license": self.license,
        }


@dataclass(frozen=True)
class AttributionGuardResult:
    """Pass/block result for visible attribution checks."""

    status: str
    passed: bool
    blocked: bool
    required_sources: list[AttributionGuardSource]
    missing_sources: list[AttributionGuardSource]

    @property
    def action(self) -> str:
        return "block" if self.blocked else "pass"

    def as_dict(self) -> dict:
        return {
            "status": self.status,
            "action": self.action,
            "passed": self.passed,
            "blocked": self.blocked,
            "required_sources": [
                source.as_dict() for source in self.required_sources
            ],
            "missing_sources": [
                source.as_dict() for source in self.missing_sources
            ],
        }


def _as_text(publication_text: str | list[str] | tuple[str, ...]) -> str:
    if isinstance(publication_text, str):
        return publication_text
    return "\n".join(part for part in publication_text if part)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value.casefold()).strip()


def _url_candidates(source_url: str | None) -> set[str]:
    if not source_url:
        return set()

    raw = source_url.strip()
    if not raw:
        return set()

    candidates = {raw, raw.rstrip("/")}
    parsed = urlparse(raw)
    if parsed.netloc and parsed.path:
        host_path = f"{parsed.netloc}{parsed.path}".rstrip("/")
        candidates.add(host_path)
        if parsed.query:
            candidates.add(f"{host_path}?{parsed.query}")
        if parsed.netloc == "x.com":
            candidates.add(f"twitter.com{parsed.path}".rstrip("/"))
        elif parsed.netloc == "twitter.com":
            candidates.add(f"x.com{parsed.path}".rstrip("/"))
    return {_normalize_text(candidate) for candidate in candidates if candidate}


def _has_citation_url(source: AttributionGuardSource, publication_text: str) -> bool:
    normalized_text = _normalize_text(publication_text)
    return any(candidate in normalized_text for candidate in _url_candidates(source.source_url))


def _author_attribution_patterns(author: str) -> list[str]:
    escaped = re.escape(_normalize_text(author))
    return [
        rf"\bvia\s+{escaped}\b",
        rf"\bsource(?:d)?\s*(?:from|by|:|-)?\s*{escaped}\b",
        rf"\bcredit\s*(?:to|:|-)?\s*{escaped}\b",
        rf"\battribution\s*(?:to|:|-)?\s*{escaped}\b",
        rf"\bh/t\s+{escaped}\b",
        rf"\bby\s+{escaped}\b",
        rf"\bfrom\s+{escaped}\b",
        rf"\bthanks\s+to\s+{escaped}\b",
    ]


def _has_attribution_note(source: AttributionGuardSource, publication_text: str) -> bool:
    if not source.author or not source.author.strip():
        return False
    normalized_text = _normalize_text(publication_text)
    return any(
        re.search(pattern, normalized_text)
        for pattern in _author_attribution_patterns(source.author)
    )


def _has_visible_attribution(
    source: AttributionGuardSource,
    publication_text: str,
) -> bool:
    return _has_citation_url(source, publication_text) or _has_attribution_note(
        source,
        publication_text,
    )


def _attribution_sources_for_content(
    db: Any,
    content_id: int,
) -> list[AttributionGuardSource]:
    rows = db.conn.execute(
        """SELECT k.id AS knowledge_id,
                  k.source_url,
                  k.author,
                  k.license
           FROM content_knowledge_links ckl
           INNER JOIN knowledge k ON k.id = ckl.knowledge_id
           WHERE ckl.content_id = ?
             AND LOWER(COALESCE(k.license, '')) = ?
           ORDER BY ckl.relevance_score DESC, k.id ASC""",
        (content_id, ATTRIBUTION_REQUIRED_LICENSE),
    ).fetchall()

    sources = []
    for row in rows:
        if hasattr(row, "keys"):
            knowledge_id = row["knowledge_id"]
            source_url = row["source_url"]
            author = row["author"]
            license_value = row["license"]
        else:
            knowledge_id, source_url, author, license_value = row
        sources.append(
            AttributionGuardSource(
                knowledge_id=knowledge_id,
                source_url=source_url,
                author=author,
                license=license_value or ATTRIBUTION_REQUIRED_LICENSE,
            )
        )
    return sources


def check_publication_attribution_guard(
    db: Any,
    content_id: int,
    publication_text: str | list[str] | tuple[str, ...],
) -> AttributionGuardResult:
    """Return whether attribution-required linked knowledge is visibly cited."""
    required_sources = _attribution_sources_for_content(db, content_id)
    if not required_sources:
        return AttributionGuardResult(
            status="passed",
            passed=True,
            blocked=False,
            required_sources=[],
            missing_sources=[],
        )

    text = _as_text(publication_text)
    missing_sources = [
        source
        for source in required_sources
        if not _has_visible_attribution(source, text)
    ]
    blocked = bool(missing_sources)
    return AttributionGuardResult(
        status="blocked" if blocked else "passed",
        passed=not blocked,
        blocked=blocked,
        required_sources=required_sources,
        missing_sources=missing_sources,
    )
