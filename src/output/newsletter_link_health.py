"""Pre-send newsletter link extraction and health checks."""

from __future__ import annotations

import json
import re
import socket
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse


DEFAULT_TIMEOUT = 10.0
DEFAULT_UTM_PARAMETERS = ("utm_source", "utm_medium", "utm_campaign")
BROKEN_STATUS_MIN = 400
REDIRECT_STATUS_MIN = 300
REDIRECT_STATUS_MAX = 399

Fetcher = Callable[[str, float], "FetchResult"]

_BARE_URL_RE = re.compile(r"(?:[a-z][a-z0-9+.-]*://|mailto:)[^\s<>'\"]+", re.IGNORECASE)
_MARKDOWN_DESTINATION_RE = re.compile(r"(!?)\[[^\]]+\]\(([^)\s]+)\)")
_TRAILING_URL_PUNCTUATION = ".,;:!?)]}"


@dataclass(frozen=True)
class NewsletterLinkOccurrence:
    """One place where a newsletter URL appeared."""

    source: str
    raw_url: str
    url: str
    line: int
    column: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class FetchResult:
    """HTTP result returned by a newsletter link fetcher."""

    status_code: int | None
    final_url: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterLinkResult:
    """Health classification for a unique newsletter link."""

    url: str
    status: str
    ok: bool
    required: bool
    status_code: int | None = None
    final_url: str = ""
    error: str = ""
    skipped: bool = False
    skip_reason: str = ""
    missing_utm_parameters: tuple[str, ...] = ()
    occurrences: tuple[NewsletterLinkOccurrence, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "status": self.status,
            "ok": self.ok,
            "required": self.required,
            "status_code": self.status_code,
            "final_url": self.final_url,
            "error": self.error,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "missing_utm_parameters": list(self.missing_utm_parameters),
            "occurrences": [occurrence.to_dict() for occurrence in self.occurrences],
        }


@dataclass(frozen=True)
class NewsletterLinkHealthReport:
    """Aggregated newsletter link-health report."""

    results: tuple[NewsletterLinkResult, ...]
    require_utm: bool

    @property
    def ok(self) -> bool:
        return self.broken_required_count == 0

    @property
    def checked_count(self) -> int:
        return sum(1 for result in self.results if not result.skipped)

    @property
    def skipped_count(self) -> int:
        return sum(1 for result in self.results if result.skipped)

    @property
    def broken_required_count(self) -> int:
        return sum(1 for result in self.results if result.required and result.status == "broken")

    @property
    def missing_utm_count(self) -> int:
        return sum(1 for result in self.results if result.missing_utm_parameters)

    @property
    def status_counts(self) -> dict[str, int]:
        counts = {
            "healthy": 0,
            "redirected": 0,
            "broken": 0,
            "skipped": 0,
            "missing_utm": 0,
        }
        for result in self.results:
            counts[result.status] = counts.get(result.status, 0) + 1
        return counts

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "require_utm": self.require_utm,
            "total_links": len(self.results),
            "checked_count": self.checked_count,
            "skipped_count": self.skipped_count,
            "broken_required_count": self.broken_required_count,
            "missing_utm_count": self.missing_utm_count,
            "status_counts": self.status_counts,
            "results": [result.to_dict() for result in self.results],
        }


def extract_newsletter_links(
    *,
    subject: str = "",
    body: str = "",
    html: str = "",
) -> tuple[NewsletterLinkOccurrence, ...]:
    """Extract links from subject, plaintext body, and HTML body in first-seen order."""
    occurrences: list[NewsletterLinkOccurrence] = []
    occurrences.extend(_extract_text_links(subject, "subject"))
    occurrences.extend(_extract_text_links(body, "body"))
    occurrences.extend(_HtmlHrefParser.links_from(html))
    occurrences.extend(_extract_text_links(_HtmlTextParser.text_from(html), "html"))
    return tuple(occurrences)


def dedupe_links(
    occurrences: tuple[NewsletterLinkOccurrence, ...] | list[NewsletterLinkOccurrence],
) -> tuple[tuple[str, tuple[NewsletterLinkOccurrence, ...]], ...]:
    """Group URL occurrences by normalized URL while preserving first-seen URL order."""
    grouped: dict[str, list[NewsletterLinkOccurrence]] = {}
    for occurrence in occurrences:
        grouped.setdefault(occurrence.url, []).append(occurrence)
    return tuple((url, tuple(items)) for url, items in grouped.items())


def check_newsletter_links(
    *,
    subject: str = "",
    body: str = "",
    html: str = "",
    timeout: float = DEFAULT_TIMEOUT,
    require_utm: bool = False,
    fetcher: Fetcher | None = None,
) -> NewsletterLinkHealthReport:
    """Extract, deduplicate, and classify newsletter links before sending."""
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    fetch = fetcher or default_fetcher
    results: list[NewsletterLinkResult] = []

    for url, occurrences in dedupe_links(extract_newsletter_links(subject=subject, body=body, html=html)):
        skip, reason = classify_skipped_url(url)
        missing_utm = missing_utm_parameters(url) if require_utm and not skip else ()
        if skip:
            results.append(
                NewsletterLinkResult(
                    url=url,
                    status="skipped",
                    ok=True,
                    required=False,
                    skipped=True,
                    skip_reason=reason,
                    occurrences=occurrences,
                )
            )
            continue

        try:
            fetched = fetch(url, timeout)
        except Exception as exc:  # noqa: BLE001 - fetchers are user-injectable.
            fetched = FetchResult(status_code=None, error=str(exc))
        results.append(_result_from_fetch(url, occurrences, fetched, missing_utm))

    return NewsletterLinkHealthReport(results=tuple(results), require_utm=require_utm)


def classify_skipped_url(url: str) -> tuple[bool, str]:
    """Return whether a URL should be excluded from HTTP checks."""
    if not url:
        return True, "empty"
    if url.startswith("#"):
        return True, "internal_anchor"

    parsed = urlparse(url)
    scheme = parsed.scheme.casefold()
    if scheme == "mailto":
        return True, "mailto"
    if scheme not in {"http", "https"}:
        return True, "unsupported_scheme"
    return False, ""


def missing_utm_parameters(url: str) -> tuple[str, ...]:
    """Return required UTM parameters missing from an HTTP URL."""
    parsed = urlparse(url)
    if parsed.scheme.casefold() not in {"http", "https"}:
        return ()
    query = parse_qs(parsed.query, keep_blank_values=True)
    return tuple(
        parameter
        for parameter in DEFAULT_UTM_PARAMETERS
        if not any(value.strip() for value in query.get(parameter, []))
    )


def default_fetcher(url: str, timeout: float) -> FetchResult:
    """Fetch a URL with the standard library for CLI use."""
    return _urllib_fetch(url, timeout, method="HEAD")


def format_newsletter_link_health_json(report: NewsletterLinkHealthReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_link_health_text(report: NewsletterLinkHealthReport) -> str:
    """Render a compact human-readable link-health report."""
    counts = report.status_counts
    lines = [
        "Newsletter Link Health",
        f"Links: {len(report.results)} ({report.checked_count} checked, {report.skipped_count} skipped)",
        (
            "Status: "
            f"{counts['healthy']} healthy, {counts['redirected']} redirected, "
            f"{counts['broken']} broken, {counts['missing_utm']} missing_utm"
        ),
    ]
    if not report.results:
        lines.append("No links found.")
        return "\n".join(lines)

    lines.append("")
    for result in report.results:
        detail = result.status
        if result.status_code is not None:
            detail += f" HTTP {result.status_code}"
        if result.final_url and result.final_url != result.url:
            detail += f" -> {result.final_url}"
        if result.skip_reason:
            detail += f" ({result.skip_reason})"
        if result.missing_utm_parameters:
            detail += f" missing={','.join(result.missing_utm_parameters)}"
        if result.error:
            detail += f" error={result.error}"
        lines.append(f"{detail}: {result.url}")
    return "\n".join(lines)


def _result_from_fetch(
    url: str,
    occurrences: tuple[NewsletterLinkOccurrence, ...],
    fetched: FetchResult,
    missing_utm: tuple[str, ...],
) -> NewsletterLinkResult:
    status_code = fetched.status_code if isinstance(fetched.status_code, int) else None
    final_url = fetched.final_url or url
    redirected = (
        status_code is not None
        and (REDIRECT_STATUS_MIN <= status_code <= REDIRECT_STATUS_MAX or final_url != url)
    )
    broken = bool(fetched.error) or status_code is None or status_code >= BROKEN_STATUS_MIN

    if broken:
        status = "broken"
    elif missing_utm:
        status = "missing_utm"
    elif redirected:
        status = "redirected"
    else:
        status = "healthy"

    return NewsletterLinkResult(
        url=url,
        status=status,
        ok=not broken,
        required=True,
        status_code=status_code,
        final_url=final_url,
        error=fetched.error if fetched.error else (f"HTTP {status_code}" if broken else ""),
        missing_utm_parameters=missing_utm,
        occurrences=occurrences,
    )


def _urllib_fetch(url: str, timeout: float, *, method: str) -> FetchResult:
    request = urllib.request.Request(url, method=method, headers={"User-Agent": "presence-link-health/1.0"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            return FetchResult(
                status_code=getattr(response, "status", None),
                final_url=response.geturl(),
            )
    except urllib.error.HTTPError as exc:
        if method == "HEAD" and exc.code == 405:
            return _urllib_fetch(url, timeout, method="GET")
        return FetchResult(status_code=exc.code, final_url=exc.geturl(), error=f"HTTP {exc.code}")
    except (urllib.error.URLError, TimeoutError, socket.timeout) as exc:
        return FetchResult(status_code=None, error=str(exc))


def _extract_text_links(text: str, source: str) -> list[NewsletterLinkOccurrence]:
    markdown_spans: list[tuple[int, int]] = []
    occurrences: list[NewsletterLinkOccurrence] = []
    for match in _MARKDOWN_DESTINATION_RE.finditer(text):
        raw_url = match.group(2)
        markdown_spans.append(match.span(2))
        if not match.group(1):
            occurrence = _occurrence(text, source, raw_url, match.start(2))
        else:
            occurrence = None
        if occurrence is not None:
            occurrences.append(occurrence)

    for match in _BARE_URL_RE.finditer(text):
        if any(start <= match.start() < end for start, end in markdown_spans):
            continue
        occurrence = _occurrence(text, source, match.group(0), match.start())
        if occurrence:
            occurrences.append(occurrence)
    return occurrences


def _occurrence(
    text: str,
    source: str,
    raw_url: str,
    index: int,
) -> NewsletterLinkOccurrence | None:
    url = normalize_url(raw_url)
    if not url:
        return None
    line, column = _line_column(text, index)
    return NewsletterLinkOccurrence(source=source, raw_url=raw_url, url=url, line=line, column=column)


def normalize_url(url: str) -> str:
    """Normalize URL spellings for duplicate checks."""
    value = url.strip()
    if value.startswith("<") and value.endswith(">"):
        value = value[1:-1].strip()
    return value.rstrip(_TRAILING_URL_PUNCTUATION)


def _line_column(text: str, index: int) -> tuple[int, int]:
    line = text.count("\n", 0, index) + 1
    line_start = text.rfind("\n", 0, index)
    column = index + 1 if line_start == -1 else index - line_start
    return line, column


class _HtmlHrefParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[NewsletterLinkOccurrence] = []

    @classmethod
    def links_from(cls, html: str) -> list[NewsletterLinkOccurrence]:
        parser = cls()
        parser.feed(html)
        parser.close()
        return parser.links

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a":
            return
        href = dict(attrs).get("href")
        if not href:
            return
        url = normalize_url(href)
        if not url:
            return
        self.links.append(
            NewsletterLinkOccurrence(
                source="html",
                raw_url=href,
                url=url,
                line=getattr(self, "getpos")()[0],
                column=getattr(self, "getpos")()[1] + 1,
            )
        )


class _HtmlTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []

    @classmethod
    def text_from(cls, html: str) -> str:
        parser = cls()
        parser.feed(html)
        parser.close()
        return "\n".join(parser.parts)

    def handle_data(self, data: str) -> None:
        if data.strip():
            self.parts.append(data)
