"""Publication guard for content linked to restricted knowledge sources."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


STRICT_RESTRICTED_BEHAVIOR = "strict"
PERMISSIVE_RESTRICTED_BEHAVIOR = "permissive"
RESTRICTED_LICENSE = "restricted"


@dataclass(frozen=True)
class LicenseGuardSource:
    """Restricted knowledge source linked to generated content."""

    knowledge_id: int
    source_url: str | None
    license: str

    def as_dict(self) -> dict:
        return {
            "knowledge_id": self.knowledge_id,
            "source_url": self.source_url,
            "license": self.license,
        }


@dataclass(frozen=True)
class LicenseGuardResult:
    """Pass/warn/block result for publication license checks."""

    status: str
    passed: bool
    blocked: bool
    restricted_prompt_behavior: str
    override: bool
    restricted_sources: list[LicenseGuardSource]

    @property
    def action(self) -> str:
        if self.blocked:
            return "block"
        if self.restricted_sources:
            return "warn"
        return "pass"

    def as_dict(self) -> dict:
        return {
            "status": self.status,
            "action": self.action,
            "passed": self.passed,
            "blocked": self.blocked,
            "restricted_prompt_behavior": self.restricted_prompt_behavior,
            "override": self.override,
            "restricted_sources": [
                source.as_dict() for source in self.restricted_sources
            ],
        }


def restricted_prompt_behavior_from_config(config: object) -> str:
    """Read and normalize curated_sources.restricted_prompt_behavior."""
    curated_sources = getattr(config, "curated_sources", None)
    behavior = getattr(
        curated_sources,
        "restricted_prompt_behavior",
        STRICT_RESTRICTED_BEHAVIOR,
    )
    if behavior in {
        STRICT_RESTRICTED_BEHAVIOR,
        PERMISSIVE_RESTRICTED_BEHAVIOR,
    }:
        return behavior
    return STRICT_RESTRICTED_BEHAVIOR


def _restricted_sources_for_content(db: Any, content_id: int) -> list[LicenseGuardSource]:
    rows = db.conn.execute(
        """SELECT k.id AS knowledge_id,
                  k.source_url,
                  k.license
           FROM content_knowledge_links ckl
           INNER JOIN knowledge k ON k.id = ckl.knowledge_id
           WHERE ckl.content_id = ?
             AND LOWER(COALESCE(k.license, '')) = ?
           ORDER BY ckl.relevance_score DESC, k.id ASC""",
        (content_id, RESTRICTED_LICENSE),
    ).fetchall()
    sources = []
    for row in rows:
        if hasattr(row, "keys"):
            knowledge_id = row["knowledge_id"]
            source_url = row["source_url"]
            license_value = row["license"]
        else:
            knowledge_id, source_url, license_value = row
        sources.append(
            LicenseGuardSource(
                knowledge_id=knowledge_id,
                source_url=source_url,
                license=license_value or RESTRICTED_LICENSE,
            )
        )
    return sources


def check_publication_license_guard(
    db: Any,
    content_id: int,
    *,
    restricted_prompt_behavior: str = STRICT_RESTRICTED_BEHAVIOR,
    allow_restricted: bool = False,
) -> LicenseGuardResult:
    """Return whether linked restricted knowledge should block publication."""
    if restricted_prompt_behavior not in {
        STRICT_RESTRICTED_BEHAVIOR,
        PERMISSIVE_RESTRICTED_BEHAVIOR,
    }:
        restricted_prompt_behavior = STRICT_RESTRICTED_BEHAVIOR

    restricted_sources = _restricted_sources_for_content(db, content_id)
    if not restricted_sources:
        return LicenseGuardResult(
            status="passed",
            passed=True,
            blocked=False,
            restricted_prompt_behavior=restricted_prompt_behavior,
            override=allow_restricted,
            restricted_sources=[],
        )

    blocked = (
        restricted_prompt_behavior == STRICT_RESTRICTED_BEHAVIOR
        and not allow_restricted
    )
    return LicenseGuardResult(
        status="blocked" if blocked else "warning",
        passed=not blocked,
        blocked=blocked,
        restricted_prompt_behavior=restricted_prompt_behavior,
        override=allow_restricted,
        restricted_sources=restricted_sources,
    )
