"""Summarize newsletter outbound link domain mix."""

from __future__ import annotations

from dataclasses import dataclass
import json
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

from output.newsletter_link_health import dedupe_links, extract_newsletter_links


DEFAULT_DOMINANT_SHARE = 0.5


@dataclass(frozen=True)
class NewsletterLinkDomain:
    """One unique valid HTTP(S) newsletter link."""

    url: str
    domain: str
    internal: bool
    occurrences: tuple[dict[str, Any], ...]

    @property
    def occurrence_count(self) -> int:
        return len(self.occurrences)

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "domain": self.domain,
            "internal": self.internal,
            "occurrence_count": self.occurrence_count,
            "occurrences": [dict(occurrence) for occurrence in self.occurrences],
        }


@dataclass(frozen=True)
class InvalidNewsletterUrl:
    """One unique newsletter URL that cannot be classified as an outbound link."""

    url: str
    reason: str
    occurrences: tuple[dict[str, Any], ...]

    @property
    def occurrence_count(self) -> int:
        return len(self.occurrences)

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "reason": self.reason,
            "occurrence_count": self.occurrence_count,
            "occurrences": [dict(occurrence) for occurrence in self.occurrences],
        }


@dataclass(frozen=True)
class DominantNewsletterDomain:
    """A domain whose occurrence share crosses the configured threshold."""

    domain: str
    count: int
    share: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "count": self.count,
            "share": round(self.share, 6),
        }


@dataclass(frozen=True)
class NewsletterLinkDomainReport:
    """Aggregated domain-mix result for one assembled newsletter."""

    source: str
    preferred_domains: tuple[str, ...]
    total_links: int
    unique_domains: int
    domain_counts: Mapping[str, int]
    dominant_domains: tuple[DominantNewsletterDomain, ...]
    internal_links: int
    external_links: int
    links: tuple[NewsletterLinkDomain, ...]
    unpreferred_links: tuple[NewsletterLinkDomain, ...]
    invalid_urls: tuple[InvalidNewsletterUrl, ...]
    dominant_share_threshold: float = DEFAULT_DOMINANT_SHARE

    @property
    def invalid_url_count(self) -> int:
        return sum(item.occurrence_count for item in self.invalid_urls)

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_link_domains",
            "source": self.source,
            "preferred_domains": list(self.preferred_domains),
            "total_links": self.total_links,
            "unique_domains": self.unique_domains,
            "domain_counts": dict(sorted(self.domain_counts.items())),
            "dominant_domains": [domain.to_dict() for domain in self.dominant_domains],
            "dominant_share_threshold": self.dominant_share_threshold,
            "internal_links": self.internal_links,
            "external_links": self.external_links,
            "unpreferred_links": [link.to_dict() for link in self.unpreferred_links],
            "invalid_url_count": self.invalid_url_count,
            "invalid_urls": [item.to_dict() for item in self.invalid_urls],
            "links": [link.to_dict() for link in self.links],
        }


def build_newsletter_link_domain_report(
    text: str,
    *,
    preferred_domains: Iterable[str] | None = None,
    source: str = "text",
    dominant_share_threshold: float = DEFAULT_DOMINANT_SHARE,
) -> NewsletterLinkDomainReport:
    """Extract newsletter links and summarize valid HTTP(S) domains."""
    threshold = float(dominant_share_threshold)
    if threshold <= 0 or threshold > 1:
        raise ValueError("dominant_share_threshold must be greater than 0 and at most 1")

    preferred = tuple(sorted(_normalize_domains(preferred_domains or ())))
    preferred_set = set(preferred)
    links: list[NewsletterLinkDomain] = []
    invalid_urls: list[InvalidNewsletterUrl] = []
    domain_counts: dict[str, int] = {}
    internal_count = 0
    external_count = 0

    occurrences = extract_newsletter_links(body=text)
    for url, grouped_occurrences in dedupe_links(occurrences):
        occurrence_dicts = tuple(
            occurrence.to_dict() if hasattr(occurrence, "to_dict") else dict(occurrence)
            for occurrence in grouped_occurrences
        )
        domain, reason = _classify_domain(url)
        occurrence_count = len(occurrence_dicts)
        if reason:
            invalid_urls.append(
                InvalidNewsletterUrl(
                    url=url,
                    reason=reason,
                    occurrences=occurrence_dicts,
                )
            )
            continue

        assert domain
        internal = bool(preferred_set) and _domain_matches(domain, preferred_set)
        if internal:
            internal_count += occurrence_count
        else:
            external_count += occurrence_count
        domain_counts[domain] = domain_counts.get(domain, 0) + occurrence_count
        links.append(
            NewsletterLinkDomain(
                url=url,
                domain=domain,
                internal=internal,
                occurrences=occurrence_dicts,
            )
        )

    total_links = sum(domain_counts.values())
    dominant_domains = tuple(
        DominantNewsletterDomain(
            domain=domain,
            count=count,
            share=(count / total_links if total_links else 0.0),
        )
        for domain, count in sorted(
            domain_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )
        if total_links and count / total_links >= threshold
    )
    sorted_links = tuple(sorted(links, key=lambda item: (item.domain, item.url)))
    return NewsletterLinkDomainReport(
        source=source,
        preferred_domains=preferred,
        total_links=total_links,
        unique_domains=len(domain_counts),
        domain_counts=domain_counts,
        dominant_domains=dominant_domains,
        internal_links=internal_count,
        external_links=external_count,
        links=sorted_links,
        unpreferred_links=tuple(link for link in sorted_links if preferred_set and not link.internal),
        invalid_urls=tuple(sorted(invalid_urls, key=lambda item: item.url)),
        dominant_share_threshold=threshold,
    )


def format_newsletter_link_domain_json(report: NewsletterLinkDomainReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_link_domain_text(report: NewsletterLinkDomainReport) -> str:
    """Render a compact human-readable domain-mix report."""
    lines = [
        "Newsletter Link Domains",
        f"Source: {report.source}",
        f"Links: {report.total_links}",
        f"Domains: {report.unique_domains}",
        f"Mix: {report.internal_links} internal, {report.external_links} external",
        f"Invalid URLs: {report.invalid_url_count}",
    ]
    if report.preferred_domains:
        lines.append("Preferred domains: " + ", ".join(report.preferred_domains))
    if not report.total_links and not report.invalid_urls:
        lines.append("No links found.")
        return "\n".join(lines)

    if report.domain_counts:
        lines.append("")
        lines.append("Domain counts:")
        for domain, count in sorted(report.domain_counts.items(), key=lambda item: (-item[1], item[0])):
            lines.append(f"  {domain}: {count}")
    if report.dominant_domains:
        lines.append("")
        lines.append("Dominant domains:")
        for domain in report.dominant_domains:
            lines.append(f"  {domain.domain}: {domain.count} ({domain.share:.0%})")
    if report.unpreferred_links:
        lines.append("")
        lines.append("Unpreferred links:")
        for link in report.unpreferred_links:
            lines.append(f"  {link.domain}: {link.url}")
    if report.invalid_urls:
        lines.append("")
        lines.append("Invalid URLs:")
        for item in report.invalid_urls:
            lines.append(f"  {item.reason}: {item.url}")
    return "\n".join(lines)


def _classify_domain(url: str) -> tuple[str, str]:
    if not url:
        return "", "empty"
    parsed = urlparse(url)
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"}:
        return "", "unsupported_scheme"
    domain = _normalize_domain(parsed.hostname or "")
    if not domain:
        return "", "missing_domain"
    return domain, ""


def _normalize_domains(domains: Iterable[str]) -> set[str]:
    normalized: set[str] = set()
    for domain in domains:
        parsed = urlparse(str(domain).strip())
        candidate = parsed.hostname if parsed.scheme else str(domain)
        value = _normalize_domain(candidate)
        if value:
            normalized.add(value)
    return normalized


def _normalize_domain(domain: str | None) -> str:
    value = str(domain or "").strip().casefold().rstrip(".")
    if value.startswith("www."):
        value = value[4:]
    return value


def _domain_matches(domain: str, preferred_domains: set[str]) -> bool:
    return any(domain == preferred or domain.endswith(f".{preferred}") for preferred in preferred_domains)
