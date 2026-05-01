"""Deterministic deliverability linting for newsletter drafts."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from html.parser import HTMLParser
from typing import Any


DEFAULT_MAX_LINKS = 20
DEFAULT_MAX_PREHEADER_CHARS = 140
ERROR_SEVERITY = "error"
WARNING_SEVERITY = "warning"

_SPAMMY_SUBJECT_PATTERNS = (
    (re.compile(r"\bfree\b", re.IGNORECASE), "free"),
    (re.compile(r"\bguaranteed?\b", re.IGNORECASE), "guarantee"),
    (re.compile(r"\bact now\b", re.IGNORECASE), "act now"),
    (re.compile(r"\burgent\b", re.IGNORECASE), "urgent"),
    (re.compile(r"\bwinner\b", re.IGNORECASE), "winner"),
    (re.compile(r"\b(?:limited time|expires today)\b", re.IGNORECASE), "scarcity"),
    (re.compile(r"\$\d+|\d+%\s*off", re.IGNORECASE), "promotional offer"),
)
_CTA_TEXTS = {
    "buy now",
    "click here",
    "get started",
    "learn more",
    "read more",
    "read the full post",
    "sign up",
    "subscribe",
}
_UNSUBSCRIBE_PLACEHOLDERS = (
    "{{ unsubscribe_url }}",
    "{{unsubscribe_url}}",
    "{{ unsubscribe_link }}",
    "{{unsubscribe_link}}",
    "%unsubscribe%",
    "%unsubscribe_url%",
    "*|unsub|*",
    "*|unsubscribe|*",
)


@dataclass(frozen=True)
class NewsletterDeliverabilityIssue:
    """One deliverability issue found in a newsletter draft."""

    severity: str
    code: str
    message: str
    remediation_hint: str
    target: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class NewsletterDeliverabilityReport:
    """Aggregated newsletter deliverability lint report."""

    ok: bool
    issue_count: int
    error_count: int
    warning_count: int
    link_count: int
    issues: tuple[NewsletterDeliverabilityIssue, ...]

    @property
    def blocking_issue_count(self) -> int:
        return self.error_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "issue_count": self.issue_count,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "blocking_issue_count": self.blocking_issue_count,
            "link_count": self.link_count,
            "issues": [issue.to_dict() for issue in self.issues],
        }


def lint_newsletter_deliverability(
    *,
    subject: str,
    preheader: str = "",
    html: str = "",
    plaintext: str = "",
    max_links: int = DEFAULT_MAX_LINKS,
    max_preheader_chars: int = DEFAULT_MAX_PREHEADER_CHARS,
) -> NewsletterDeliverabilityReport:
    """Lint assembled newsletter draft fields before delivery."""
    if max_links < 0:
        raise ValueError("max_links must be non-negative")
    if max_preheader_chars < 0:
        raise ValueError("max_preheader_chars must be non-negative")

    issues: list[NewsletterDeliverabilityIssue] = []
    _check_subject(subject, issues)
    _check_preheader(preheader, max_preheader_chars, issues)
    link_count = _check_html(html, max_links, issues)
    _check_plaintext(plaintext, issues)

    issues = sorted(
        issues,
        key=lambda item: (item.target, item.severity, item.code, item.message),
    )
    error_count = sum(1 for issue in issues if issue.severity == ERROR_SEVERITY)
    warning_count = sum(1 for issue in issues if issue.severity == WARNING_SEVERITY)
    return NewsletterDeliverabilityReport(
        ok=error_count == 0,
        issue_count=len(issues),
        error_count=error_count,
        warning_count=warning_count,
        link_count=link_count,
        issues=tuple(issues),
    )


def format_newsletter_deliverability_json(report: NewsletterDeliverabilityReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_deliverability_text(report: NewsletterDeliverabilityReport) -> str:
    """Render a compact human-readable deliverability lint report."""
    lines = [
        "Newsletter Deliverability Lint",
        f"Issues: {report.issue_count} ({report.error_count} error, {report.warning_count} warning)",
        f"Links: {report.link_count}",
    ]
    if not report.issues:
        lines.append("No newsletter deliverability issues found.")
        return "\n".join(lines)

    lines.append("")
    for issue in report.issues:
        lines.append(
            f"{issue.severity.upper()} {issue.target}: {issue.code}: "
            f"{issue.message} Hint: {issue.remediation_hint}"
        )
    return "\n".join(lines)


def _check_subject(subject: str, issues: list[NewsletterDeliverabilityIssue]) -> None:
    normalized = subject.strip()
    if not normalized:
        issues.append(
            _issue(
                ERROR_SEVERITY,
                "missing_subject",
                "Newsletter subject is missing.",
                "Pass --subject with the exact Buttondown subject before delivery.",
                "subject",
            )
        )
        return

    upper_letters = sum(1 for char in normalized if char.isalpha() and char.isupper())
    letters = sum(1 for char in normalized if char.isalpha())
    if letters >= 8 and upper_letters / letters >= 0.8:
        issues.append(
            _issue(
                WARNING_SEVERITY,
                "all_caps_subject",
                "Subject uses mostly uppercase letters.",
                "Rewrite the subject in sentence case or title case.",
                "subject",
            )
        )

    for pattern, label in _SPAMMY_SUBJECT_PATTERNS:
        if pattern.search(normalized):
            issues.append(
                _issue(
                    WARNING_SEVERITY,
                    "spammy_subject_pattern",
                    f"Subject contains a spam-prone pattern: {label}.",
                    "Use specific editorial language instead of promotional urgency.",
                    "subject",
                )
            )


def _check_preheader(
    preheader: str,
    max_preheader_chars: int,
    issues: list[NewsletterDeliverabilityIssue],
) -> None:
    normalized = " ".join(preheader.split())
    if len(normalized) > max_preheader_chars:
        issues.append(
            _issue(
                WARNING_SEVERITY,
                "oversized_preview_text",
                f"Preview text is {len(normalized)} characters.",
                f"Keep the preheader at or below {max_preheader_chars} characters.",
                "preheader",
            )
        )


def _check_html(
    html: str,
    max_links: int,
    issues: list[NewsletterDeliverabilityIssue],
) -> int:
    links = _extract_links(html)
    if len(links) > max_links:
        issues.append(
            _issue(
                WARNING_SEVERITY,
                "excessive_links",
                f"Draft contains {len(links)} links.",
                f"Reduce the draft to {max_links} or fewer links, or split sections into another issue.",
                "html",
            )
        )

    if _has_broken_unsubscribe_placeholder(html):
        issues.append(
            _issue(
                ERROR_SEVERITY,
                "broken_unsubscribe_placeholder",
                "Unsubscribe copy is present but no valid unsubscribe placeholder was found.",
                "Use a Buttondown-compatible unsubscribe placeholder such as {{ unsubscribe_url }}.",
                "html",
            )
        )

    repeated_ctas = _repeated_ctas(html)
    for cta, count in repeated_ctas:
        issues.append(
            _issue(
                WARNING_SEVERITY,
                "repeated_cta",
                f"CTA text '{cta}' appears {count} times.",
                "Vary repeated calls to action or keep only the primary CTA.",
                "html",
            )
        )
    return len(links)


def _check_plaintext(plaintext: str, issues: list[NewsletterDeliverabilityIssue]) -> None:
    if not plaintext.strip():
        issues.append(
            _issue(
                ERROR_SEVERITY,
                "missing_plaintext_body",
                "Plaintext newsletter body is missing.",
                "Provide a plaintext body so clients that block HTML still receive the issue.",
                "plaintext",
            )
        )


def _issue(
    severity: str,
    code: str,
    message: str,
    remediation_hint: str,
    target: str,
) -> NewsletterDeliverabilityIssue:
    return NewsletterDeliverabilityIssue(
        severity=severity,
        code=code,
        message=message,
        remediation_hint=remediation_hint,
        target=target,
    )


def _extract_links(value: str) -> tuple[str, ...]:
    html_links = _HtmlLinkParser.links_from(value)
    markdown_links = re.findall(r"\[[^\]]+\]\((https?://[^)\s]+)\)", value)
    bare_links = re.findall(r"(?<![\(\"'=])(https?://[^\s<>)]+)", value)
    return tuple(sorted(set(html_links + markdown_links + bare_links)))


def _has_broken_unsubscribe_placeholder(value: str) -> bool:
    lowered = value.lower()
    if "unsubscribe" not in lowered and "unsub" not in lowered:
        return False
    if any(placeholder in lowered for placeholder in _UNSUBSCRIBE_PLACEHOLDERS):
        return False
    if re.search(r"https?://[^\s\"'>]*unsubscribe[^\s\"'>]*", lowered):
        return False
    return True


def _repeated_ctas(value: str) -> tuple[tuple[str, int], ...]:
    ctas = _HtmlLinkParser.link_texts_from(value)
    ctas.extend(
        match.casefold()
        for match in re.findall(r"\[([^\]]+)\]\(https?://[^)]+\)", value)
    )
    counts: dict[str, int] = {}
    for raw in ctas:
        normalized = re.sub(r"\s+", " ", raw.strip()).casefold()
        if normalized in _CTA_TEXTS:
            counts[normalized] = counts.get(normalized, 0) + 1
    return tuple(sorted((cta, count) for cta, count in counts.items() if count >= 3))


class _HtmlLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.links: list[str] = []
        self.link_texts: list[str] = []
        self._active_link_text: list[str] | None = None

    @classmethod
    def links_from(cls, value: str) -> list[str]:
        parser = cls()
        parser.feed(value)
        parser.close()
        return parser.links

    @classmethod
    def link_texts_from(cls, value: str) -> list[str]:
        parser = cls()
        parser.feed(value)
        parser.close()
        return parser.link_texts

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)
        self._active_link_text = []

    def handle_data(self, data: str) -> None:
        if self._active_link_text is not None:
            self._active_link_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() == "a" and self._active_link_text is not None:
            self.link_texts.append("".join(self._active_link_text))
            self._active_link_text = None
