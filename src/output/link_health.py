"""Markdown link extraction and HTTP health checks for newsletters."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

import requests

if not hasattr(requests, "RequestException"):
    requests.RequestException = Exception

if not hasattr(requests, "Session"):

    class _RequestsSession:
        def head(self, *args, **kwargs):
            head = getattr(requests, "head", None)
            if not callable(head):
                raise requests.RequestException("requests.head is unavailable")
            return head(*args, **kwargs)

        def get(self, *args, **kwargs):
            get = getattr(requests, "get", None)
            if not callable(get):
                raise requests.RequestException("requests.get is unavailable")
            return get(*args, **kwargs)

    requests.Session = _RequestsSession


@dataclass
class LinkOccurrence:
    """A single Markdown link occurrence."""

    url: str
    normalized_url: str
    label: str
    line: int
    column: int

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "normalized_url": self.normalized_url,
            "label": self.label,
            "line": self.line,
            "column": self.column,
        }


@dataclass
class LinkCheckResult:
    """HTTP check result for a unique newsletter link."""

    url: str
    ok: bool
    status_code: Optional[int] = None
    error: str = ""
    method: str = "HEAD"
    skipped: bool = False
    skip_reason: str = ""
    occurrences: list[LinkOccurrence] = field(default_factory=list)

    @property
    def required(self) -> bool:
        return not self.skipped

    def to_dict(self) -> dict:
        return {
            "url": self.url,
            "ok": self.ok,
            "status_code": self.status_code,
            "error": self.error,
            "method": self.method,
            "skipped": self.skipped,
            "skip_reason": self.skip_reason,
            "required": self.required,
            "occurrences": [occurrence.to_dict() for occurrence in self.occurrences],
        }


@dataclass
class LinkHealthReport:
    """Structured newsletter link-health report."""

    checked: list[LinkCheckResult] = field(default_factory=list)
    skipped: list[LinkCheckResult] = field(default_factory=list)

    @property
    def failures(self) -> list[LinkCheckResult]:
        return [result for result in self.checked if not result.ok]

    @property
    def ok(self) -> bool:
        return not self.failures

    @property
    def failure_count(self) -> int:
        return len(self.failures)

    def to_dict(self) -> dict:
        return {
            "ok": self.ok,
            "checked_count": len(self.checked),
            "skipped_count": len(self.skipped),
            "failure_count": self.failure_count,
            "checked": [result.to_dict() for result in self.checked],
            "skipped": [result.to_dict() for result in self.skipped],
            "failures": [result.to_dict() for result in self.failures],
        }


def _find_closing(text: str, start: int, closing: str) -> int:
    escaped = False
    for index in range(start, len(text)):
        char = text[index]
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == closing:
            return index
    return -1


def _find_destination_end(text: str, start: int) -> int:
    escaped = False
    depth = 0
    for index in range(start, len(text)):
        char = text[index]
        if escaped:
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == "(":
            depth += 1
            continue
        if char == ")":
            if depth == 0:
                return index
            depth -= 1
    return -1


def _line_column(markdown: str, index: int) -> tuple[int, int]:
    line = markdown.count("\n", 0, index) + 1
    line_start = markdown.rfind("\n", 0, index)
    column = index + 1 if line_start == -1 else index - line_start
    return line, column


def normalize_url(url: str) -> str:
    """Normalize equivalent Markdown URL spellings for duplicate checks."""
    value = url.strip()
    if value.startswith("<") and value.endswith(">"):
        value = value[1:-1].strip()
    return value


def should_skip_url(url: str) -> tuple[bool, str]:
    """Return whether a URL is intentionally excluded from HTTP checks."""
    normalized = normalize_url(url)
    if not normalized:
        return True, "empty"
    if normalized.startswith("#"):
        return True, "fragment"
    parsed = urlparse(normalized)
    if parsed.scheme.lower() == "mailto":
        return True, "mailto"
    return False, ""


def extract_markdown_links(markdown: str) -> list[LinkOccurrence]:
    """Extract inline Markdown links, excluding images and reference links."""
    occurrences: list[LinkOccurrence] = []
    index = 0
    while index < len(markdown):
        start = markdown.find("[", index)
        if start == -1:
            break
        if start > 0 and markdown[start - 1] == "!":
            index = start + 1
            continue

        label_end = _find_closing(markdown, start + 1, "]")
        if label_end == -1 or label_end + 1 >= len(markdown) or markdown[label_end + 1] != "(":
            index = start + 1
            continue

        destination_start = label_end + 2
        destination_end = _find_destination_end(markdown, destination_start)
        if destination_end == -1:
            index = label_end + 1
            continue

        raw_url = markdown[destination_start:destination_end].strip()
        normalized_url = normalize_url(raw_url)
        if normalized_url:
            line, column = _line_column(markdown, start)
            occurrences.append(
                LinkOccurrence(
                    url=raw_url,
                    normalized_url=normalized_url,
                    label=markdown[start + 1 : label_end],
                    line=line,
                    column=column,
                )
            )
        index = destination_end + 1
    return occurrences


class LinkHealthChecker:
    """Check unique newsletter links with HEAD and GET fallback."""

    def __init__(self, timeout: float = 30, session=None):
        self.timeout = timeout
        self.session = session or requests.Session()

    def check_markdown(self, markdown: str) -> LinkHealthReport:
        grouped: dict[str, list[LinkOccurrence]] = {}
        for occurrence in extract_markdown_links(markdown):
            grouped.setdefault(occurrence.normalized_url, []).append(occurrence)

        checked: list[LinkCheckResult] = []
        skipped: list[LinkCheckResult] = []
        for url, occurrences in grouped.items():
            skip, reason = should_skip_url(url)
            if skip:
                skipped.append(
                    LinkCheckResult(
                        url=url,
                        ok=True,
                        skipped=True,
                        skip_reason=reason,
                        occurrences=occurrences,
                    )
                )
                continue
            checked.append(self._check_url(url, occurrences))
        return LinkHealthReport(checked=checked, skipped=skipped)

    def _check_url(
        self, url: str, occurrences: list[LinkOccurrence]
    ) -> LinkCheckResult:
        try:
            response = self.session.head(
                url,
                allow_redirects=True,
                timeout=self.timeout,
            )
            status_code = getattr(response, "status_code", None)
            if status_code == 405:
                response = self.session.get(
                    url,
                    allow_redirects=True,
                    timeout=self.timeout,
                )
                status_code = getattr(response, "status_code", None)
                return self._result_from_status(url, occurrences, status_code, "GET")
            return self._result_from_status(url, occurrences, status_code, "HEAD")
        except requests.RequestException as exc:
            return LinkCheckResult(
                url=url,
                ok=False,
                error=str(exc),
                occurrences=occurrences,
            )

    def _result_from_status(
        self,
        url: str,
        occurrences: list[LinkOccurrence],
        status_code: Optional[int],
        method: str,
    ) -> LinkCheckResult:
        if not isinstance(status_code, int):
            status_code = None
        ok = status_code is not None and 200 <= status_code < 400
        return LinkCheckResult(
            url=url,
            ok=ok,
            status_code=status_code,
            error="" if ok else f"HTTP {status_code}",
            method=method,
            occurrences=occurrences,
        )
