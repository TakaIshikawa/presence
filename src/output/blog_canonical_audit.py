"""Audit static blog markdown canonical URLs and publication identity."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from output.blog_frontmatter_validator import parse_markdown_frontmatter


DEFAULT_BLOG_PATH = "drafts"
CANONICAL_FIELDS = ("canonical_url", "canonical", "url")
CONTENT_ID_FIELDS = ("generated_content_id", "content_id")


@dataclass(frozen=True)
class BlogCanonicalIssue:
    """One canonical URL or publication identity audit issue."""

    severity: str
    code: str
    message: str
    file_path: str
    remediation_hint: str
    field: str | None = None
    related_paths: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["related_paths"] = list(self.related_paths)
        return payload


@dataclass(frozen=True)
class BlogCanonicalEntry:
    """Parsed metadata for one markdown file included in the audit."""

    file_path: str
    title: str | None
    slug: str | None
    canonical_url: str | None
    generated_content_ids: tuple[int, ...]
    frontmatter: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "file_path": self.file_path,
            "title": self.title,
            "slug": self.slug,
            "canonical_url": self.canonical_url,
            "generated_content_ids": list(self.generated_content_ids),
            "frontmatter": self.frontmatter,
        }


@dataclass(frozen=True)
class BlogCanonicalAuditReport:
    """Static audit result for blog markdown files."""

    ok: bool
    root_path: str
    file_count: int
    issue_count: int
    error_count: int
    warning_count: int
    entries: tuple[BlogCanonicalEntry, ...]
    issues: tuple[BlogCanonicalIssue, ...]

    @property
    def blocking_issue_count(self) -> int:
        return self.error_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "root_path": self.root_path,
            "file_count": self.file_count,
            "issue_count": self.issue_count,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "blocking_issue_count": self.blocking_issue_count,
            "entries": [entry.to_dict() for entry in self.entries],
            "issues": [issue.to_dict() for issue in self.issues],
        }


def build_blog_canonical_audit_report(
    root_path: str | Path = DEFAULT_BLOG_PATH,
) -> BlogCanonicalAuditReport:
    """Scan markdown blog files and validate canonical publication identity."""
    root = Path(root_path)
    if root.is_file():
        paths = [root]
    else:
        paths = sorted(path for path in root.rglob("*.md") if path.is_file())

    entries: list[BlogCanonicalEntry] = []
    issues: list[BlogCanonicalIssue] = []
    canonical_paths: dict[str, list[str]] = defaultdict(list)
    title_paths: dict[str, list[str]] = defaultdict(list)
    content_id_paths: dict[int, list[str]] = defaultdict(list)

    if not root.exists():
        issues.append(
            _issue(
                "error",
                "missing_blog_path",
                f"Blog path does not exist: {root}",
                str(root),
                "Pass an existing blog draft or output directory with --path.",
            )
        )

    for path in paths:
        file_path = str(path)
        markdown = path.read_text()
        frontmatter, _body, parse_issues = parse_markdown_frontmatter(
            markdown,
            path=file_path,
        )
        for parse_issue in parse_issues:
            issues.append(
                _issue(
                    "error",
                    parse_issue.code,
                    parse_issue.message,
                    file_path,
                    "Fix the markdown frontmatter so it uses the generated key: value format.",
                    field=parse_issue.field,
                )
            )
        if parse_issues:
            entries.append(
                BlogCanonicalEntry(
                    file_path=file_path,
                    title=_string_value(frontmatter.get("title")),
                    slug=_string_value(frontmatter.get("slug")),
                    canonical_url=_canonical_value(frontmatter),
                    generated_content_ids=tuple(_generated_content_ids(frontmatter)),
                    frontmatter=frontmatter,
                )
            )
            continue

        title = _string_value(frontmatter.get("title"))
        slug = _string_value(frontmatter.get("slug"))
        canonical_url = _canonical_value(frontmatter)
        generated_content_ids = tuple(_generated_content_ids(frontmatter))
        entries.append(
            BlogCanonicalEntry(
                file_path=file_path,
                title=title,
                slug=slug,
                canonical_url=canonical_url,
                generated_content_ids=generated_content_ids,
                frontmatter=frontmatter,
            )
        )

        if not title:
            issues.append(
                _issue(
                    "error",
                    "missing_title",
                    "Blog markdown is missing a non-empty title.",
                    file_path,
                    "Add a stable frontmatter title for the published article.",
                    field="title",
                )
            )
        else:
            title_paths[_title_key(title)].append(file_path)

        if canonical_url is None:
            issues.append(
                _issue(
                    "error",
                    "missing_canonical_url",
                    "Blog markdown is missing a canonical URL.",
                    file_path,
                    "Add canonical_url with the final published article URL.",
                    field="canonical_url",
                )
            )
        elif not _valid_canonical_url(canonical_url):
            issues.append(
                _issue(
                    "error",
                    "invalid_canonical_url",
                    "Canonical URL must be an absolute http(s) URL.",
                    file_path,
                    "Use the final absolute https:// URL for the published article.",
                    field="canonical_url",
                )
            )
        else:
            canonical_paths[_normalize_url(canonical_url)].append(file_path)

        expected_slug = _expected_slug(path, root)
        canonical_slug = _canonical_slug(canonical_url) if canonical_url else None
        if slug and slug != expected_slug:
            issues.append(
                _issue(
                    "error",
                    "slug_file_mismatch",
                    f"Frontmatter slug '{slug}' does not match file slug '{expected_slug}'.",
                    file_path,
                    "Rename the file or update slug so both identify the same article.",
                    field="slug",
                )
            )
        if canonical_slug and (slug or expected_slug) != canonical_slug:
            compared_slug = slug or expected_slug
            issues.append(
                _issue(
                    "error",
                    "slug_canonical_mismatch",
                    f"Slug '{compared_slug}' does not match canonical URL slug '{canonical_slug}'.",
                    file_path,
                    "Update canonical_url, slug, or filename so the publication identity is consistent.",
                    field="canonical_url",
                )
            )
        if title and (slug or expected_slug) and _slugify(title) != (slug or expected_slug):
            issues.append(
                _issue(
                    "warning",
                    "title_slug_mismatch",
                    "Title does not normalize to the configured blog slug.",
                    file_path,
                    "Confirm the title and slug intentionally refer to the same article.",
                    field="title",
                )
            )

        if not generated_content_ids:
            issues.append(
                _issue(
                    "error",
                    "missing_generated_content_reference",
                    "Blog markdown has no generated content ID reference.",
                    file_path,
                    "Add generated_content_id, content_id, or source_content_ids with positive integer IDs.",
                )
            )
        else:
            for content_id in generated_content_ids:
                content_id_paths[content_id].append(file_path)

        if _has_invalid_generated_content_ids(frontmatter):
            issues.append(
                _issue(
                    "error",
                    "invalid_generated_content_reference",
                    "Generated content references must be positive integers.",
                    file_path,
                    "Use positive integer IDs in generated_content_id, content_id, or source_content_ids.",
                )
            )

    _append_duplicate_issues(
        issues,
        canonical_paths,
        code="duplicate_canonical_url",
        message="Canonical URL is used by multiple markdown files.",
        field="canonical_url",
        remediation_hint="Keep one markdown file for this canonical URL or assign distinct canonical URLs.",
    )
    _append_duplicate_issues(
        issues,
        title_paths,
        code="duplicate_title",
        message="Title is used by multiple markdown files.",
        field="title",
        remediation_hint="Update duplicate titles so each published article has a distinct identity.",
    )
    _append_duplicate_issues(
        issues,
        {str(key): value for key, value in content_id_paths.items()},
        code="duplicate_generated_content_reference",
        message="Generated content ID is referenced by multiple markdown files.",
        field=None,
        remediation_hint="Point each article at its own generated content row, or remove duplicate drafts.",
    )

    issues = sorted(issues, key=lambda item: (item.file_path, item.severity, item.code))
    error_count = sum(1 for issue in issues if issue.severity == "error")
    warning_count = sum(1 for issue in issues if issue.severity == "warning")
    return BlogCanonicalAuditReport(
        ok=error_count == 0,
        root_path=str(root),
        file_count=len(paths),
        issue_count=len(issues),
        error_count=error_count,
        warning_count=warning_count,
        entries=tuple(entries),
        issues=tuple(issues),
    )


def format_blog_canonical_audit_json(report: BlogCanonicalAuditReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_canonical_audit_text(report: BlogCanonicalAuditReport) -> str:
    """Render a compact human-readable audit report."""
    lines = [
        "Blog Canonical Audit",
        f"Path: {report.root_path}",
        f"Files: {report.file_count}",
        f"Issues: {report.issue_count} ({report.error_count} error, {report.warning_count} warning)",
    ]
    if not report.issues:
        lines.append("No canonical URL or publication identity issues found.")
        return "\n".join(lines)

    lines.append("")
    for issue in report.issues:
        related = f" related={', '.join(issue.related_paths)}" if issue.related_paths else ""
        lines.append(
            f"{issue.severity.upper()} {issue.file_path}: "
            f"{issue.code}: {issue.message} Hint: {issue.remediation_hint}{related}"
        )
    return "\n".join(lines)


def _issue(
    severity: str,
    code: str,
    message: str,
    file_path: str,
    remediation_hint: str,
    *,
    field: str | None = None,
    related_paths: tuple[str, ...] = (),
) -> BlogCanonicalIssue:
    return BlogCanonicalIssue(
        severity=severity,
        code=code,
        message=message,
        file_path=file_path,
        remediation_hint=remediation_hint,
        field=field,
        related_paths=related_paths,
    )


def _string_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    return stripped or None


def _canonical_value(frontmatter: dict[str, Any]) -> str | None:
    for field in CANONICAL_FIELDS:
        value = _string_value(frontmatter.get(field))
        if value:
            return value
    return None


def _valid_canonical_url(value: str) -> bool:
    parts = urlsplit(value)
    return parts.scheme in {"http", "https"} and bool(parts.netloc) and bool(parts.path.strip("/"))


def _normalize_url(value: str) -> str:
    parts = urlsplit(value.strip())
    path = re.sub(r"/+", "/", parts.path).rstrip("/")
    return urlunsplit(
        (
            parts.scheme.lower(),
            parts.netloc.lower(),
            path,
            "",
            "",
        )
    )


def _canonical_slug(value: str | None) -> str | None:
    if not value or not _valid_canonical_url(value):
        return None
    path = urlsplit(value).path.rstrip("/")
    if not path:
        return None
    return Path(path).stem


def _expected_slug(path: Path, root: Path) -> str:
    if root.is_dir():
        try:
            relative = path.relative_to(root)
        except ValueError:
            relative = path.name
        return Path(relative).with_suffix("").as_posix()
    return path.stem


def _slugify(value: str) -> str:
    slug = value.strip().lower()
    slug = re.sub(r"[^a-z0-9]+", "-", slug)
    slug = slug.strip("-")
    return slug


def _title_key(value: str) -> str:
    return re.sub(r"\s+", " ", value.strip()).casefold()


def _generated_content_ids(frontmatter: dict[str, Any]) -> list[int]:
    ids: list[int] = []
    for field in CONTENT_ID_FIELDS:
        value = frontmatter.get(field)
        if isinstance(value, int) and not isinstance(value, bool) and value > 0:
            ids.append(value)
    source_ids = frontmatter.get("source_content_ids")
    if isinstance(source_ids, list):
        ids.extend(
            item
            for item in source_ids
            if isinstance(item, int) and not isinstance(item, bool) and item > 0
        )
    return sorted(set(ids))


def _has_invalid_generated_content_ids(frontmatter: dict[str, Any]) -> bool:
    for field in CONTENT_ID_FIELDS:
        value = frontmatter.get(field)
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool) or value <= 0
        ):
            return True
    source_ids = frontmatter.get("source_content_ids")
    if source_ids is not None and (
        not isinstance(source_ids, list)
        or not source_ids
        or any(not isinstance(item, int) or isinstance(item, bool) or item <= 0 for item in source_ids)
    ):
        return True
    return False


def _append_duplicate_issues(
    issues: list[BlogCanonicalIssue],
    path_map: dict[str, list[str]],
    *,
    code: str,
    message: str,
    field: str | None,
    remediation_hint: str,
) -> None:
    for paths in path_map.values():
        unique_paths = tuple(sorted(set(paths)))
        if len(unique_paths) < 2:
            continue
        for path in unique_paths:
            related = tuple(item for item in unique_paths if item != path)
            issues.append(
                _issue(
                    "error",
                    code,
                    message,
                    path,
                    remediation_hint,
                    field=field,
                    related_paths=related,
                )
            )
