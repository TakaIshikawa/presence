"""Report blog posts missing canonical URLs or slug-consistent canonicals."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import re
import sqlite3
from typing import Any
from urllib.parse import urlparse


BLOG_TYPES = {"blog_post", "long_form"}
PUBLISHED_STATUSES = {"published", "success", "posted", "complete", "completed"}


@dataclass(frozen=True)
class BlogCanonicalUrlIssue:
    content_id: int | None
    publication_id: int | None
    title: str | None
    slug: str | None
    canonical_url: str | None
    publication_url: str | None
    issue_type: str
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class BlogCanonicalUrlCoverageReport:
    generated_at: str
    filters: dict[str, Any]
    summary: dict[str, Any]
    issues: tuple[BlogCanonicalUrlIssue, ...]
    missing_tables: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "blog_canonical_url_coverage",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "issues": [issue.to_dict() for issue in self.issues],
            "missing_tables": list(self.missing_tables),
            "summary": dict(self.summary),
        }


def build_blog_canonical_url_coverage_report(
    db_or_conn: Any,
    *,
    expected_base_url: str | None = None,
    now: datetime | None = None,
) -> BlogCanonicalUrlCoverageReport:
    """Return generated or published blog rows missing canonical URL coverage."""
    generated_at = _utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    if "generated_content" not in schema:
        return _report(generated_at, expected_base_url, (), 0, ("generated_content",))

    contents = _load_content(conn, schema)
    publications = _load_publications(conn, schema)
    by_content: dict[int, list[dict[str, Any]]] = {}
    for publication in publications:
        if publication.get("content_id") is not None:
            by_content.setdefault(int(publication["content_id"]), []).append(publication)

    issues: list[BlogCanonicalUrlIssue] = []
    covered = 0
    seen_publication_ids: set[int] = set()
    for content in contents:
        pubs = by_content.get(int(content["content_id"]), [])
        if pubs:
            seen_publication_ids.update(int(pub["publication_id"]) for pub in pubs if pub.get("publication_id") is not None)
        canonical = content.get("canonical_url") or _first_clean(pub.get("canonical_url") or pub.get("publication_url") for pub in pubs)
        publication_url = _first_clean(pub.get("publication_url") for pub in pubs)
        if canonical:
            covered += 1
        issues.extend(_issues_for_row(content, pubs[0] if pubs else None, canonical, publication_url, expected_base_url))

    content_ids = {int(content["content_id"]) for content in contents}
    for publication in publications:
        publication_id = publication.get("publication_id")
        if publication_id is not None and int(publication_id) in seen_publication_ids:
            continue
        if publication.get("content_id") is not None and int(publication["content_id"]) in content_ids:
            continue
        canonical = publication.get("canonical_url") or publication.get("publication_url")
        if canonical:
            covered += 1
        issues.extend(_issues_for_row(publication, publication, canonical, publication.get("publication_url"), expected_base_url))

    total = len(contents) + sum(
        1
        for publication in publications
        if publication.get("content_id") is None or int(publication["content_id"]) not in content_ids
    )
    issues.sort(key=lambda issue: (issue.issue_type, issue.content_id or 0, issue.publication_id or 0))
    return _report(generated_at, expected_base_url, tuple(issues), total, ())


def format_blog_canonical_url_coverage_json(report: BlogCanonicalUrlCoverageReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_canonical_url_coverage_text(report: BlogCanonicalUrlCoverageReport) -> str:
    """Render a compact table-style report."""
    summary = report.summary
    lines = [
        "Blog Canonical URL Coverage",
        f"Generated: {report.generated_at}",
        (
            f"Summary: total={summary['total_posts']} covered={summary['covered_posts']} "
            f"missing={summary['missing_count']} mismatches={summary['mismatch_count']} "
            f"coverage_rate={summary['coverage_rate']:.1%}"
        ),
    ]
    if not report.issues:
        lines.extend(["", "No canonical URL coverage issues found."])
        return "\n".join(lines)
    lines.extend(["", "Issues:", "type                  content  publication  slug                 canonical"])
    for issue in report.issues:
        lines.append(
            f"{issue.issue_type:<21} {issue.content_id or '-':<8} "
            f"{issue.publication_id or '-':<12} {(issue.slug or '-')[:20]:<20} "
            f"{issue.canonical_url or '-'}"
        )
        lines.append(f"  detail: {issue.detail}")
    return "\n".join(lines)


def _load_content(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    cols = schema["generated_content"]
    if "id" not in cols:
        return []
    type_col = _first(cols, ("content_type", "type", "format"))
    text_col = _first(cols, ("content", "body", "text", "generated_text"))
    title_col = _first(cols, ("title", "headline"))
    slug_col = _first(cols, ("slug", "publication_slug"))
    canonical_col = _first(cols, ("canonical_url", "blog_url", "published_url", "url"))
    published_col = _first(cols, ("published", "is_published"))
    status_col = _first(cols, ("status", "publication_status"))
    select = [
        "id AS content_id",
        f"{type_col} AS content_type" if type_col else "'blog_post' AS content_type",
        f"{text_col} AS content_text" if text_col else "NULL AS content_text",
        f"{title_col} AS title" if title_col else "NULL AS title",
        f"{slug_col} AS slug" if slug_col else "NULL AS slug",
        f"{canonical_col} AS canonical_url" if canonical_col else "NULL AS canonical_url",
        f"{published_col} AS published" if published_col else "NULL AS published",
        f"{status_col} AS status" if status_col else "NULL AS status",
    ]
    where = []
    params: list[Any] = []
    if type_col:
        where.append(f"{type_col} IN ({','.join('?' for _ in BLOG_TYPES)})")
        params.extend(sorted(BLOG_TYPES))
    rows = conn.execute(
        f"SELECT {', '.join(select)} FROM generated_content"
        + (f" WHERE {' AND '.join(where)}" if where else "")
        + " ORDER BY id ASC",
        params,
    ).fetchall()
    return [_normalize_blog_row(dict(row)) for row in rows if _is_blog_record(dict(row))]


def _load_publications(conn: sqlite3.Connection, schema: dict[str, set[str]]) -> list[dict[str, Any]]:
    table = next((name for name in ("content_publications", "blog_publications", "content_exports") if name in schema), None)
    if not table or "id" not in schema[table]:
        return []
    cols = schema[table]
    content_id_col = _first(cols, ("content_id", "generated_content_id", "source_content_id"))
    channel_col = _first(cols, ("platform", "channel", "publication_type", "target"))
    status_col = _first(cols, ("status", "state", "publication_status"))
    slug_col = _first(cols, ("slug", "publication_slug"))
    canonical_col = _first(cols, ("canonical_url", "blog_url"))
    url_col = _first(cols, ("url", "published_url", "platform_url", "blog_url", "canonical_url"))
    title_col = _first(cols, ("title", "headline"))
    select = [
        "id AS publication_id",
        f"{content_id_col} AS content_id" if content_id_col else "NULL AS content_id",
        f"{channel_col} AS channel" if channel_col else "'blog' AS channel",
        f"{status_col} AS status" if status_col else "'published' AS status",
        f"{slug_col} AS slug" if slug_col else "NULL AS slug",
        f"{canonical_col} AS canonical_url" if canonical_col else "NULL AS canonical_url",
        f"{url_col} AS publication_url" if url_col else "NULL AS publication_url",
        f"{title_col} AS title" if title_col else "NULL AS title",
    ]
    rows = conn.execute(f"SELECT {', '.join(select)} FROM {table} ORDER BY id ASC").fetchall()
    return [_normalize_blog_row(dict(row)) for row in rows if _is_blog_record(dict(row))]


def _issues_for_row(
    row: dict[str, Any],
    publication: dict[str, Any] | None,
    canonical: str | None,
    publication_url: str | None,
    expected_base_url: str | None,
) -> list[BlogCanonicalUrlIssue]:
    slug = row.get("slug")
    issues: list[BlogCanonicalUrlIssue] = []
    if not canonical:
        return [
            _issue(
                row,
                publication,
                "missing_canonical_url",
                "No canonical URL was found on the generated blog row or publication row.",
                None,
                publication_url,
            )
        ]
    if publication_url and _normalize_url(canonical) != _normalize_url(publication_url):
        issues.append(_issue(row, publication, "canonical_publication_mismatch", "Canonical URL differs from the publication URL.", canonical, publication_url))
    expected = _expected_url(slug, expected_base_url)
    if expected and _normalize_url(canonical) != _normalize_url(expected):
        issues.append(_issue(row, publication, "canonical_slug_mismatch", f"Canonical URL does not match expected slug URL {expected}.", canonical, publication_url))
    elif slug and not _path_matches_slug(canonical, slug):
        issues.append(_issue(row, publication, "canonical_slug_mismatch", "Canonical URL path does not end with the blog slug.", canonical, publication_url))
    return issues


def _issue(row: dict[str, Any], publication: dict[str, Any] | None, kind: str, detail: str, canonical: str | None, publication_url: str | None) -> BlogCanonicalUrlIssue:
    return BlogCanonicalUrlIssue(
        content_id=_int_or_none(row.get("content_id")),
        publication_id=_int_or_none((publication or row).get("publication_id")),
        title=row.get("title"),
        slug=row.get("slug"),
        canonical_url=canonical,
        publication_url=publication_url,
        issue_type=kind,
        detail=detail,
    )


def _normalize_blog_row(row: dict[str, Any]) -> dict[str, Any]:
    text = _clean(row.get("content_text"))
    row["title"] = _clean(row.get("title")) or _frontmatter_value(text, "title") or _markdown_title(text)
    row["slug"] = _slugify(_clean(row.get("slug")) or _frontmatter_value(text, "slug"))
    row["canonical_url"] = _clean(row.get("canonical_url")) or _frontmatter_value(text, "canonical_url") or _frontmatter_value(text, "canonical")
    row["publication_url"] = _clean(row.get("publication_url"))
    return row


def _is_blog_record(row: dict[str, Any]) -> bool:
    content_type = _clean(row.get("content_type"))
    channel = _clean(row.get("channel"))
    status = (_clean(row.get("status")) or "").lower()
    published = row.get("published")
    return (
        content_type in BLOG_TYPES
        or (channel is not None and "blog" in channel.lower())
        or status in PUBLISHED_STATUSES
        or bool(published)
    )


def _report(generated_at: datetime, expected_base_url: str | None, issues: tuple[BlogCanonicalUrlIssue, ...], total: int, missing: tuple[str, ...]) -> BlogCanonicalUrlCoverageReport:
    missing_count = sum(1 for issue in issues if issue.issue_type == "missing_canonical_url")
    mismatch_count = len(issues) - missing_count
    covered = max(total - missing_count, 0)
    return BlogCanonicalUrlCoverageReport(
        generated_at=generated_at.isoformat(),
        filters={"expected_base_url": expected_base_url},
        summary={
            "total_posts": total,
            "covered_posts": covered,
            "missing_count": missing_count,
            "mismatch_count": mismatch_count,
            "coverage_rate": round(covered / total, 4) if total else 0.0,
        },
        issues=issues,
        missing_tables=missing,
    )


def _frontmatter_value(text: str | None, key: str) -> str | None:
    if not text:
        return None
    pattern = re.compile(rf"^\s*{re.escape(key)}\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE)
    match = pattern.search(text)
    return _clean(match.group(1).strip("\"'")) if match else None


def _markdown_title(text: str | None) -> str | None:
    if not text:
        return None
    for line in text.splitlines():
        match = re.match(r"^#\s+(.+)$", line.strip())
        if match:
            return match.group(1).strip()
    return None


def _expected_url(slug: str | None, base_url: str | None) -> str | None:
    if not slug or not base_url:
        return None
    return f"{base_url.rstrip('/')}/{slug}"


def _path_matches_slug(url: str, slug: str) -> bool:
    path = urlparse(url).path.rstrip("/")
    return path.endswith(f"/{slug}") or path == slug


def _normalize_url(url: str) -> str:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/")
    return f"{scheme}://{netloc}{path}"


def _slugify(value: str | None) -> str | None:
    if not value:
        return None
    text = value.strip().strip("/")
    return text or None


def _first_clean(values: Any) -> str | None:
    return next((cleaned for value in values if (cleaned := _clean(value))), None)


def _clean(value: Any) -> str | None:
    text = str(value).strip() if value is not None else ""
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _first(columns: set[str], names: tuple[str, ...]) -> str | None:
    return next((name for name in names if name in columns), None)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    return {str(row[0]): {str(col[1]) for col in conn.execute(f"PRAGMA table_info({row[0]})")} for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
