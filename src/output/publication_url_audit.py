"""Audit durable publication URLs for drift and duplicate attribution."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


DEFAULT_DAYS = 30
SUPPORTED_PLATFORMS = ("all", "x", "bluesky", "linkedin")
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_PARAMS = {
    "dclid",
    "fbclid",
    "gclid",
    "li_fat_id",
    "mc_cid",
    "mc_eid",
    "msclkid",
    "ref_src",
    "twclid",
}
EXPECTED_HOSTS = {
    "x": {"x.com", "twitter.com", "mobile.twitter.com"},
    "bluesky": {"bsky.app"},
    "linkedin": {"linkedin.com"},
}


@dataclass(frozen=True)
class PublicationUrlAuditIssue:
    """One publication URL warning."""

    issue_type: str
    publication_id: int | None
    content_id: int | None
    platform: str
    platform_url: str | None
    canonical_url: str | None
    detail: str
    related_content_ids: tuple[int, ...] = ()
    related_publication_ids: tuple[int, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def build_publication_url_audit(
    db: Any,
    *,
    platform: str | None = None,
    days: int = DEFAULT_DAYS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return publication URL warnings without mutating stored URLs."""
    selected_platform = platform or "all"
    if selected_platform not in SUPPORTED_PLATFORMS:
        raise ValueError(f"platform must be one of: {', '.join(SUPPORTED_PLATFORMS)}")
    if days <= 0:
        raise ValueError("days must be positive")

    conn = getattr(db, "conn", db)
    schema = _schema(conn)
    if "content_publications" not in schema:
        return _report(selected_platform, days, [], scanned_count=0)
    required = {"id", "content_id", "platform", "status", "platform_url", "published_at"}
    if not required.issubset(schema["content_publications"]):
        return _report(selected_platform, days, [], scanned_count=0)

    cutoff = _to_iso(_ensure_utc(now or datetime.now(timezone.utc)) - timedelta(days=days))
    where = ["LOWER(status) = 'published'", "published_at >= ?"]
    params: list[Any] = [cutoff]
    if selected_platform != "all":
        where.append("platform = ?")
        params.append(selected_platform)

    rows = [
        dict(row)
        for row in conn.execute(
            f"""SELECT id AS publication_id,
                       content_id,
                       platform,
                       platform_url,
                       published_at
                FROM content_publications
                WHERE {" AND ".join(where)}
                ORDER BY published_at DESC, id DESC""",
            tuple(params),
        ).fetchall()
    ]

    issues: list[PublicationUrlAuditIssue] = []
    canonical_groups: dict[str, list[dict[str, Any]]] = {}

    for row in rows:
        publication_id = int(row["publication_id"])
        content_id = int(row["content_id"])
        row_platform = str(row["platform"] or "")
        raw_url = str(row["platform_url"]).strip() if row.get("platform_url") else ""
        if not raw_url:
            issues.append(
                PublicationUrlAuditIssue(
                    issue_type="missing_url",
                    publication_id=publication_id,
                    content_id=content_id,
                    platform=row_platform,
                    platform_url=None,
                    canonical_url=None,
                    detail="published row has no platform_url",
                )
            )
            continue

        canonical_url = canonicalize_publication_url(raw_url)
        canonical_groups.setdefault(canonical_url, []).append(
            {
                **row,
                "publication_id": publication_id,
                "content_id": content_id,
                "platform": row_platform,
                "platform_url": raw_url,
                "canonical_url": canonical_url,
            }
        )

        if _host_mismatch(row_platform, raw_url):
            issues.append(
                PublicationUrlAuditIssue(
                    issue_type="host_mismatch",
                    publication_id=publication_id,
                    content_id=content_id,
                    platform=row_platform,
                    platform_url=raw_url,
                    canonical_url=canonical_url,
                    detail=f"{row_platform} publication URL host is not recognized",
                )
            )

    for canonical_url, group in sorted(canonical_groups.items()):
        content_ids = tuple(sorted({int(item["content_id"]) for item in group}))
        if len(content_ids) < 2:
            continue
        publication_ids = tuple(sorted(int(item["publication_id"]) for item in group))
        has_tracking_variant = any(
            _has_tracking_query_param(str(item["platform_url"])) for item in group
        )
        issue_type = "tracking_variant_duplicate" if has_tracking_variant else "duplicate_url"
        for item in sorted(group, key=lambda value: (int(value["content_id"]), int(value["publication_id"]))):
            issues.append(
                PublicationUrlAuditIssue(
                    issue_type=issue_type,
                    publication_id=int(item["publication_id"]),
                    content_id=int(item["content_id"]),
                    platform=str(item["platform"]),
                    platform_url=str(item["platform_url"]),
                    canonical_url=canonical_url,
                    detail="canonical URL is attached to multiple content IDs",
                    related_content_ids=content_ids,
                    related_publication_ids=publication_ids,
                )
            )

    issues.sort(
        key=lambda issue: (
            issue.issue_type,
            issue.canonical_url or "",
            issue.platform,
            issue.content_id or 0,
            issue.publication_id or 0,
        )
    )
    return _report(selected_platform, days, issues, scanned_count=len(rows))


def canonicalize_publication_url(url: str) -> str:
    """Return a stable URL key with known tracking query parameters removed."""
    parsed = urlsplit(url.strip())
    scheme = (parsed.scheme or "https").lower()
    host = _normalize_host(parsed.hostname or "")
    netloc = host
    if parsed.port and not (
        (scheme == "http" and parsed.port == 80)
        or (scheme == "https" and parsed.port == 443)
    ):
        netloc = f"{host}:{parsed.port}"

    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")

    query_pairs = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not _is_tracking_query_param(key)
    ]
    query = urlencode(sorted(query_pairs), doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def format_publication_url_audit_json(report: dict[str, Any]) -> str:
    """Render the audit report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_publication_url_audit_table(report: dict[str, Any]) -> str:
    """Render the audit report as a compact operator table."""
    lines = [
        f"Publication URL Audit (last {report['days']} days, platform={report['platform']})",
        f"scanned={report['scanned_count']} warnings={report['warning_count']}",
        "",
    ]
    issues = report["issues"]
    if not issues:
        lines.append("No publication URL warnings found.")
        return "\n".join(lines)

    columns = [
        ("issue_type", "ISSUE", 26),
        ("platform", "PLATFORM", 9),
        ("content_id", "CID", 6),
        ("publication_id", "PID", 6),
        ("canonical_url", "CANONICAL URL", 56),
        ("detail", "DETAIL", 42),
    ]
    lines.append("  ".join(label.ljust(width) for _, label, width in columns))
    lines.append("  ".join("-" * width for _, _, width in columns))
    for issue in issues:
        lines.append(
            "  ".join(
                _clip(issue.get(key) if issue.get(key) is not None else "-", width).ljust(width)
                for key, _, width in columns
            )
        )
    return "\n".join(lines)


def _report(
    platform: str,
    days: int,
    issues: list[PublicationUrlAuditIssue],
    *,
    scanned_count: int,
) -> dict[str, Any]:
    return {
        "days": days,
        "platform": platform,
        "scanned_count": scanned_count,
        "warning_count": len(issues),
        "issues": [issue.to_dict() for issue in issues],
    }


def _host_mismatch(platform: str, url: str) -> bool:
    expected = EXPECTED_HOSTS.get(platform)
    if not expected:
        return False
    host = _normalize_host(urlsplit(url.strip()).hostname or "")
    if not host:
        return True
    return not any(host == allowed or host.endswith(f".{allowed}") for allowed in expected)


def _is_tracking_query_param(name: str) -> bool:
    normalized = name.lower()
    return normalized in TRACKING_QUERY_PARAMS or any(
        normalized.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES
    )


def _has_tracking_query_param(url: str) -> bool:
    return any(_is_tracking_query_param(key) for key, _value in parse_qsl(urlsplit(url).query))


def _schema(conn: Any) -> dict[str, set[str]]:
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row["name"]: {column["name"] for column in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in tables
    }


def _normalize_host(host: str) -> str:
    normalized = host.lower().strip().rstrip(".")
    return normalized[4:] if normalized.startswith("www.") else normalized


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _to_iso(value: datetime) -> str:
    return value.isoformat()


def _clip(value: Any, width: int) -> str:
    text = str(value)
    if len(text) <= width:
        return text
    return text[: max(0, width - 3)] + "..."
