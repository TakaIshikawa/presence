"""Publish-safety checks for generated content linked to knowledge rows."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


STATUS_PASS = "pass"
STATUS_WARN = "warn"
STATUS_BLOCK = "block"

LICENSE_OPEN = "open"
LICENSE_ATTRIBUTION_REQUIRED = "attribution_required"
LICENSE_RESTRICTED = "restricted"


@dataclass(frozen=True)
class KnowledgeLicenseFinding:
    """One publish-safety finding for a linked knowledge item."""

    kind: str
    severity: str
    message: str
    knowledge_id: int | None
    source_type: str | None
    source_id: str | None
    source_url: str | None
    license: str
    approved: bool | None


@dataclass(frozen=True)
class AttributionSnippet:
    """Attribution text required for a source URL."""

    knowledge_id: int
    author: str | None
    source_type: str | None
    source_id: str | None
    snippet: str


@dataclass(frozen=True)
class AttributionGroup:
    """Attribution snippets grouped by traceable source URL."""

    source_url: str
    snippets: list[AttributionSnippet]


@dataclass(frozen=True)
class KnowledgeLicenseReport:
    """JSON-serializable publish-safety report."""

    content_id: int
    platform: str
    strict: bool
    status: str
    passed: bool
    blocked: bool
    linked_knowledge_count: int
    findings: list[KnowledgeLicenseFinding]
    attribution_groups: list[AttributionGroup]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def check_knowledge_license(
    db: Any,
    content_id: int,
    *,
    platform: str = "unknown",
    strict: bool = False,
) -> KnowledgeLicenseReport:
    """Check whether linked knowledge is safe to publish.

    Restricted and unapproved knowledge always block publication. Attribution
    rows without source URLs warn by default and block in strict mode.
    """

    rows = _load_linked_knowledge(db, content_id)
    findings: list[KnowledgeLicenseFinding] = []
    attribution_by_url: dict[str, list[AttributionSnippet]] = {}

    for row in rows:
        license_value = _normalize_license(row.get("license"))
        approved = _bool_or_none(row.get("approved"))
        source_url = _clean_string(row.get("source_url"))
        knowledge_id = row.get("knowledge_id")

        if row.get("missing_knowledge"):
            findings.append(
                _finding(
                    row,
                    kind="missing_knowledge",
                    severity=STATUS_BLOCK,
                    message="Linked knowledge row no longer exists.",
                    license_value=license_value,
                )
            )
            continue

        if approved is False:
            findings.append(
                _finding(
                    row,
                    kind="unapproved_knowledge",
                    severity=STATUS_BLOCK,
                    message="Linked knowledge is not approved for publication.",
                    license_value=license_value,
                )
            )

        if license_value == LICENSE_RESTRICTED:
            findings.append(
                _finding(
                    row,
                    kind="restricted_license",
                    severity=STATUS_BLOCK,
                    message="Linked knowledge has a restricted license.",
                    license_value=license_value,
                )
            )

        if _requires_attribution(row, license_value):
            if source_url:
                attribution_by_url.setdefault(source_url, []).append(
                    AttributionSnippet(
                        knowledge_id=int(knowledge_id),
                        author=_clean_string(row.get("author")),
                        source_type=_clean_string(row.get("source_type")),
                        source_id=_clean_string(row.get("source_id")),
                        snippet=_snippet(row),
                    )
                )
            else:
                severity = STATUS_BLOCK if strict else STATUS_WARN
                findings.append(
                    _finding(
                        row,
                        kind="missing_attribution_source_url",
                        severity=severity,
                        message=(
                            "Attribution-required knowledge is missing source_url."
                        ),
                        license_value=license_value,
                    )
                )

    status = _status_from_findings(findings)
    attribution_groups = [
        AttributionGroup(source_url=source_url, snippets=snippets)
        for source_url, snippets in sorted(attribution_by_url.items())
    ]
    return KnowledgeLicenseReport(
        content_id=content_id,
        platform=platform,
        strict=strict,
        status=status,
        passed=status != STATUS_BLOCK,
        blocked=status == STATUS_BLOCK,
        linked_knowledge_count=len(rows),
        findings=findings,
        attribution_groups=attribution_groups,
    )


def _load_linked_knowledge(db: Any, content_id: int) -> list[dict[str, Any]]:
    rows = db.conn.execute(
        """SELECT ckl.knowledge_id,
                  ckl.relevance_score,
                  k.id AS matched_knowledge_id,
                  k.source_type,
                  k.source_id,
                  k.source_url,
                  k.author,
                  k.content,
                  k.insight,
                  k.attribution_required,
                  k.license,
                  k.approved
           FROM content_knowledge_links ckl
           LEFT JOIN knowledge k ON k.id = ckl.knowledge_id
           WHERE ckl.content_id = ?
           ORDER BY ckl.relevance_score DESC, ckl.knowledge_id ASC""",
        (content_id,),
    ).fetchall()
    loaded = []
    for row in rows:
        item = dict(row)
        item["missing_knowledge"] = item.get("matched_knowledge_id") is None
        loaded.append(item)
    return loaded


def _finding(
    row: dict[str, Any],
    *,
    kind: str,
    severity: str,
    message: str,
    license_value: str,
) -> KnowledgeLicenseFinding:
    return KnowledgeLicenseFinding(
        kind=kind,
        severity=severity,
        message=message,
        knowledge_id=row.get("knowledge_id"),
        source_type=_clean_string(row.get("source_type")),
        source_id=_clean_string(row.get("source_id")),
        source_url=_clean_string(row.get("source_url")),
        license=license_value,
        approved=_bool_or_none(row.get("approved")),
    )


def _status_from_findings(findings: list[KnowledgeLicenseFinding]) -> str:
    severities = {finding.severity for finding in findings}
    if STATUS_BLOCK in severities:
        return STATUS_BLOCK
    if STATUS_WARN in severities:
        return STATUS_WARN
    return STATUS_PASS


def _requires_attribution(row: dict[str, Any], license_value: str) -> bool:
    if license_value == LICENSE_ATTRIBUTION_REQUIRED:
        return True
    if license_value in {LICENSE_OPEN, LICENSE_RESTRICTED}:
        return False
    return bool(row.get("attribution_required"))


def _normalize_license(value: Any) -> str:
    text = str(value or LICENSE_ATTRIBUTION_REQUIRED).strip().lower()
    return text or LICENSE_ATTRIBUTION_REQUIRED


def _bool_or_none(value: Any) -> bool | None:
    if value is None:
        return None
    return bool(value)


def _clean_string(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _snippet(row: dict[str, Any], limit: int = 180) -> str:
    text = _clean_string(row.get("insight")) or _clean_string(row.get("content")) or ""
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."
