"""Audit generated blog posts for canonical URL health."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from typing import Any
from urllib.parse import urlparse


DEFAULT_SITE_BASE_URL = "https://example.com"


@dataclass(frozen=True)
class BlogCanonicalLinkHealthPost:
    content_id: int
    canonical_url: str | None
    canonical_status: str
    issue_codes: tuple[str, ...]
    duplicate_group: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["issue_codes"] = list(self.issue_codes)
        if self.duplicate_group is None:
            payload.pop("duplicate_group")
        return payload


@dataclass(frozen=True)
class BlogCanonicalLinkHealthReport:
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    posts: tuple[BlogCanonicalLinkHealthPost, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "blog_canonical_link_health",
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "posts": [post.to_dict() for post in self.posts],
            "totals": dict(self.totals),
        }


def build_blog_canonical_link_health_report(
    db_or_conn: Any,
    *,
    site_base_url: str = DEFAULT_SITE_BASE_URL,
    now: datetime | None = None,
) -> BlogCanonicalLinkHealthReport:
    """Return canonical URL health for generated blog posts."""
    site_base = _normalize_site_base(site_base_url)
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    conn = _connection(db_or_conn)
    rows = _load_blog_posts(conn)
    canonicals = {int(row["id"]): _extract_canonical_url(str(row["content"] or "")) for row in rows}
    duplicate_groups = _duplicate_groups(canonicals)

    posts: list[BlogCanonicalLinkHealthPost] = []
    for row in rows:
        content_id = int(row["id"])
        canonical_url = canonicals[content_id]
        issues: list[str] = []
        normalized = _normalize_url(canonical_url)
        if not canonical_url:
            issues.append("missing_canonical_url")
        elif not normalized:
            issues.append("malformed_canonical_url")
        else:
            if not _same_site(normalized, site_base):
                issues.append("wrong_site_canonical_url")
            if content_id in duplicate_groups:
                issues.append("duplicate_canonical_url")
        posts.append(
            BlogCanonicalLinkHealthPost(
                content_id=content_id,
                canonical_url=canonical_url,
                canonical_status="healthy" if not issues else "issue",
                issue_codes=tuple(issues),
                duplicate_group=duplicate_groups.get(content_id),
            )
        )

    posts.sort(key=lambda post: (post.canonical_status != "issue", post.content_id))
    return BlogCanonicalLinkHealthReport(
        generated_at=generated_at.isoformat(),
        filters={"site_base_url": site_base},
        totals={
            "post_count": len(posts),
            "issue_count": sum(1 for post in posts if post.issue_codes),
            "missing_count": sum("missing_canonical_url" in post.issue_codes for post in posts),
            "malformed_count": sum("malformed_canonical_url" in post.issue_codes for post in posts),
            "duplicate_count": sum("duplicate_canonical_url" in post.issue_codes for post in posts),
            "wrong_site_count": sum("wrong_site_canonical_url" in post.issue_codes for post in posts),
        },
        posts=tuple(posts),
    )


def format_blog_canonical_link_health_json(report: BlogCanonicalLinkHealthReport) -> str:
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_blog_canonical_link_health_table(report: BlogCanonicalLinkHealthReport) -> str:
    lines = [
        "Blog Canonical Link Health",
        f"Generated: {report.generated_at}",
        f"Site base: {report.filters['site_base_url']}",
        f"Totals: posts={report.totals['post_count']} issues={report.totals['issue_count']}",
        "",
        "content_id | canonical_status | canonical_url | issue_codes | duplicate_group",
    ]
    if not report.posts:
        lines.append("No generated blog posts found.")
        return "\n".join(lines)
    for post in report.posts:
        lines.append(
            " | ".join(
                [
                    str(post.content_id),
                    post.canonical_status,
                    post.canonical_url or "-",
                    ",".join(post.issue_codes) or "-",
                    post.duplicate_group or "-",
                ]
            )
        )
    return "\n".join(lines)


def _load_blog_posts(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _has_table(conn, "generated_content"):
        return []
    columns = _columns(conn, "generated_content")
    if {"id", "content", "content_type"} - columns:
        return []
    return conn.execute(
        """SELECT id, content
           FROM generated_content
           WHERE content_type = 'blog_post'
           ORDER BY id ASC"""
    ).fetchall()


def _extract_canonical_url(content: str) -> str | None:
    for line in content.splitlines():
        key, separator, value = line.partition(":")
        if separator and key.strip().lower() in {"canonical_url", "canonical"}:
            cleaned = value.strip().strip("'\"")
            return cleaned or None
    return None


def _duplicate_groups(canonicals: dict[int, str | None]) -> dict[int, str]:
    by_url: dict[str, list[int]] = {}
    for content_id, url in canonicals.items():
        normalized = _normalize_url(url)
        if normalized:
            by_url.setdefault(normalized, []).append(content_id)
    groups: dict[int, str] = {}
    for normalized, ids in sorted(by_url.items()):
        if len(ids) <= 1:
            continue
        group = ",".join(str(content_id) for content_id in sorted(ids))
        for content_id in ids:
            groups[content_id] = group
    return groups


def _normalize_site_base(value: str) -> str:
    normalized = _normalize_url(value)
    if not normalized:
        raise ValueError("site_base_url must be a fully qualified http(s) URL")
    return normalized


def _normalize_url(value: str | None) -> str | None:
    if not value:
        return None
    parsed = urlparse(value.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return parsed._replace(scheme=parsed.scheme.lower(), netloc=parsed.netloc.lower(), fragment="").geturl().rstrip("/")


def _same_site(url: str, site_base: str) -> bool:
    parsed = urlparse(url)
    base = urlparse(site_base)
    return parsed.scheme == base.scheme and parsed.netloc == base.netloc


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or object with .conn")
    conn.row_factory = sqlite3.Row
    return conn


def _has_table(conn: sqlite3.Connection, table: str) -> bool:
    return conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)).fetchone() is not None


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
