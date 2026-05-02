"""Audit static blog markdown canonical URLs and publication identity."""

from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlsplit, urlunsplit

from output.blog_frontmatter_validator import parse_markdown_frontmatter


DEFAULT_BLOG_PATH = "drafts"
CANONICAL_FIELDS = ("canonical_url", "canonical", "url")
CONTENT_ID_FIELDS = ("generated_content_id", "content_id")
TRACKING_QUERY_PARAMS = {
    "fbclid",
    "gclid",
    "mc_cid",
    "mc_eid",
    "msclkid",
    "ref",
    "spm",
    "utm_campaign",
    "utm_content",
    "utm_medium",
    "utm_source",
    "utm_term",
}


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


@dataclass
class _AuditState:
    canonical_paths: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    title_paths: dict[str, list[str]] = field(default_factory=lambda: defaultdict(list))
    content_id_paths: dict[int, list[str]] = field(default_factory=lambda: defaultdict(list))


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
    audit_state = _AuditState()

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

        entry = _entry_from_frontmatter(file_path, frontmatter)
        entries.append(entry)
        _audit_entry(
            entry,
            expected_slug=_expected_slug(path, root),
            issues=issues,
            state=audit_state,
        )

    return _build_report(str(root), entries, issues, audit_state)


def build_blog_canonical_audit_report_from_records(
    records: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    *,
    root_path: str = "<records>",
) -> BlogCanonicalAuditReport:
    """Validate in-memory blog metadata dictionaries.

    Records may be raw frontmatter dictionaries or richer post dictionaries that
    contain a ``frontmatter``/``metadata`` mapping plus a path-like field.
    """
    entries: list[BlogCanonicalEntry] = []
    issues: list[BlogCanonicalIssue] = []
    audit_state = _AuditState()

    for index, record in enumerate(records):
        frontmatter = _record_frontmatter(record)
        file_path = _record_file_path(record, index)
        entry = _entry_from_frontmatter(file_path, frontmatter)
        entries.append(entry)
        _audit_entry(
            entry,
            expected_slug=_record_expected_slug(record, entry),
            issues=issues,
            state=audit_state,
        )

    return _build_report(root_path, entries, issues, audit_state)


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


def _entry_from_frontmatter(file_path: str, frontmatter: dict[str, Any]) -> BlogCanonicalEntry:
    return BlogCanonicalEntry(
        file_path=file_path,
        title=_string_value(frontmatter.get("title")),
        slug=_string_value(frontmatter.get("slug")),
        canonical_url=_canonical_value(frontmatter),
        generated_content_ids=tuple(_generated_content_ids(frontmatter)),
        frontmatter=frontmatter,
    )


def _audit_entry(
    entry: BlogCanonicalEntry,
    *,
    expected_slug: str | None,
    issues: list[BlogCanonicalIssue],
    state: _AuditState,
) -> None:
    if not entry.title:
        issues.append(
            _issue(
                "error",
                "missing_title",
                "Blog metadata is missing a non-empty title.",
                entry.file_path,
                "Add a stable title for the published article.",
                field="title",
            )
        )
    else:
        state.title_paths[_title_key(entry.title)].append(entry.file_path)

    canonical_issue = _canonical_url_issue(entry.canonical_url)
    if canonical_issue:
        code, message, hint = canonical_issue
        issues.append(
            _issue(
                "error",
                code,
                message,
                entry.file_path,
                hint,
                field="canonical_url",
            )
        )
    elif entry.canonical_url is not None:
        state.canonical_paths[_normalize_url(entry.canonical_url)].append(entry.file_path)

    canonical_slug = _canonical_slug(entry.canonical_url) if entry.canonical_url else None
    if entry.slug and expected_slug and entry.slug != expected_slug:
        issues.append(
            _issue(
                "error",
                "slug_file_mismatch",
                f"Frontmatter slug '{entry.slug}' does not match file slug '{expected_slug}'.",
                entry.file_path,
                "Rename the file or update slug so both identify the same article.",
                field="slug",
            )
        )
    compared_slug = entry.slug or expected_slug
    if canonical_slug and compared_slug and compared_slug != canonical_slug:
        issues.append(
            _issue(
                "error",
                "slug_canonical_mismatch",
                f"Slug '{compared_slug}' does not match canonical URL slug '{canonical_slug}'.",
                entry.file_path,
                "Update canonical_url, slug, or filename so the publication identity is consistent.",
                field="canonical_url",
            )
        )
    if entry.title and compared_slug and _slugify(entry.title) != compared_slug:
        issues.append(
            _issue(
                "warning",
                "title_slug_mismatch",
                "Title does not normalize to the configured blog slug.",
                entry.file_path,
                "Confirm the title and slug intentionally refer to the same article.",
                field="title",
            )
        )

    if not entry.generated_content_ids:
        issues.append(
            _issue(
                "error",
                "missing_generated_content_reference",
                "Blog metadata has no generated content ID reference.",
                entry.file_path,
                "Add generated_content_id, content_id, or source_content_ids with positive integer IDs.",
            )
        )
    else:
        for content_id in entry.generated_content_ids:
            state.content_id_paths[content_id].append(entry.file_path)

    if _has_invalid_generated_content_ids(entry.frontmatter):
        issues.append(
            _issue(
                "error",
                "invalid_generated_content_reference",
                "Generated content references must be positive integers.",
                entry.file_path,
                "Use positive integer IDs in generated_content_id, content_id, or source_content_ids.",
            )
        )


def _build_report(
    root_path: str,
    entries: list[BlogCanonicalEntry],
    issues: list[BlogCanonicalIssue],
    state: _AuditState,
) -> BlogCanonicalAuditReport:
    _append_duplicate_issues(
        issues,
        state.canonical_paths,
        code="duplicate_canonical_url",
        message="Canonical URL is used by multiple markdown files.",
        field="canonical_url",
        remediation_hint="Keep one markdown file for this canonical URL or assign distinct canonical URLs.",
    )
    _append_duplicate_issues(
        issues,
        state.title_paths,
        code="duplicate_title",
        message="Title is used by multiple markdown files.",
        field="title",
        remediation_hint="Update duplicate titles so each published article has a distinct identity.",
    )
    _append_duplicate_issues(
        issues,
        {str(key): value for key, value in state.content_id_paths.items()},
        code="duplicate_generated_content_reference",
        message="Generated content ID is referenced by multiple markdown files.",
        field=None,
        remediation_hint="Point each article at its own generated content row, or remove duplicate drafts.",
    )

    sorted_issues = sorted(issues, key=lambda item: (item.file_path, item.severity, item.code))
    error_count = sum(1 for issue in sorted_issues if issue.severity == "error")
    warning_count = sum(1 for issue in sorted_issues if issue.severity == "warning")
    return BlogCanonicalAuditReport(
        ok=error_count == 0,
        root_path=root_path,
        file_count=len(entries),
        issue_count=len(sorted_issues),
        error_count=error_count,
        warning_count=warning_count,
        entries=tuple(entries),
        issues=tuple(sorted_issues),
    )


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


def _canonical_url_issue(value: str | None) -> tuple[str, str, str] | None:
    if value is None:
        return (
            "missing_canonical_url",
            "Blog metadata is missing a canonical URL.",
            "Add canonical_url with the final published article URL.",
        )
    parts = urlsplit(value)
    if parts.scheme not in {"http", "https"} or not parts.netloc or not parts.path.strip("/"):
        return (
            "invalid_canonical_url",
            "Canonical URL must be an absolute http(s) URL.",
            "Use the final absolute https:// URL for the published article.",
        )
    if not _stable_host(parts.hostname):
        return (
            "unstable_canonical_host",
            "Canonical URL must use a stable public host.",
            "Use the production website host instead of a localhost, numeric, or malformed host.",
        )
    if _has_tracking_query_params(value):
        return (
            "tracking_parameter_canonical_url",
            "Canonical URL must not include tracking query parameters.",
            "Remove utm, click ID, ref, and other tracking parameters from canonical_url.",
        )
    return None


def _valid_canonical_url(value: str) -> bool:
    return _canonical_url_issue(value) is None


def _stable_host(value: str | None) -> bool:
    if value is None:
        return False
    host = value.strip().lower()
    if host in {"localhost", "127.0.0.1", "0.0.0.0"}:
        return False
    if "." not in host:
        return False
    return bool(re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", host))


def _has_tracking_query_params(value: str) -> bool:
    query_params = {key.casefold() for key, _value in parse_qsl(urlsplit(value).query)}
    return any(key in TRACKING_QUERY_PARAMS or key.startswith("utm_") for key in query_params)


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


def _record_frontmatter(record: dict[str, Any]) -> dict[str, Any]:
    for key in ("frontmatter", "metadata"):
        value = record.get(key)
        if isinstance(value, dict):
            merged = dict(record)
            merged.update(value)
            merged.pop("frontmatter", None)
            merged.pop("metadata", None)
            return merged
    return dict(record)


def _record_file_path(record: dict[str, Any], index: int) -> str:
    for field_name in ("file_path", "path", "filename", "slug", "id"):
        value = record.get(field_name)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
    return f"<record:{index}>"


def _record_expected_slug(record: dict[str, Any], entry: BlogCanonicalEntry) -> str | None:
    for field_name in ("expected_slug", "file_slug"):
        value = _string_value(record.get(field_name))
        if value:
            return value
    for field_name in ("file_path", "path", "filename"):
        value = _string_value(record.get(field_name))
        if value:
            return Path(value).with_suffix("").name
    return entry.slug


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
