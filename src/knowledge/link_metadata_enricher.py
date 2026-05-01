"""Enrich stored curated links with fetched article metadata."""

from __future__ import annotations

from dataclasses import dataclass, field
from html.parser import HTMLParser
import json
import sqlite3
from typing import Any, Callable
from urllib.error import URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen

from .link_metadata import LinkMetadataError, parse_link_metadata


DEFAULT_LIMIT = 25
TRACKING_QUERY_PREFIXES = ("utm_",)
TRACKING_QUERY_PARAMS = {
    "fbclid",
    "gclid",
    "gbraid",
    "mc_cid",
    "mc_eid",
    "msclkid",
    "oly_anon_id",
    "oly_enc_id",
    "twclid",
    "vero_conv",
    "vero_id",
    "wbraid",
}
SOURCE_TYPES = (
    "all",
    "knowledge",
    "curated_sources",
    "curated_article",
    "curated_newsletter",
    "blog",
    "newsletter",
)

HttpClient = Callable[[str, float], str]


@dataclass(frozen=True)
class EnrichedLinkMetadata:
    canonical_url: str = ""
    title: str = ""
    site_name: str = ""
    published_at: str = ""
    description: str = ""
    image: str = ""

    def to_dict(self) -> dict[str, str]:
        return {
            key: value
            for key, value in {
                "canonical_url": self.canonical_url,
                "title": self.title,
                "site_name": self.site_name,
                "published_at": self.published_at,
                "description": self.description,
                "image": self.image,
            }.items()
            if value
        }


@dataclass(frozen=True)
class LinkMetadataEnrichmentResult:
    source_table: str
    row_id: int
    source_type: str
    source_id: str
    url: str
    status: str
    applied: bool = False
    metadata: dict[str, str] = field(default_factory=dict)
    updated_fields: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_table": self.source_table,
            "row_id": self.row_id,
            "source_type": self.source_type,
            "source_id": self.source_id,
            "url": self.url,
            "status": self.status,
            "applied": self.applied,
            "metadata": dict(self.metadata),
            "updated_fields": list(self.updated_fields),
            "error": self.error,
        }


@dataclass
class LinkMetadataEnrichmentReport:
    source_type: str
    limit: int
    apply: bool
    results: list[LinkMetadataEnrichmentResult]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "limit": self.limit,
            "apply": self.apply,
            "summary": {
                "scanned": len(self.results),
                "updated": sum(1 for result in self.results if result.status == "updated"),
                "unchanged": sum(1 for result in self.results if result.status == "unchanged"),
                "failed": sum(1 for result in self.results if result.status == "failed"),
                "applied": sum(1 for result in self.results if result.applied),
            },
            "results": [result.to_dict() for result in self.results],
        }


class _PublishedMetadataParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.published_at = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "meta" or self.published_at:
            return
        attr_map = {name.lower(): value or "" for name, value in attrs}
        key = (attr_map.get("property") or attr_map.get("name") or "").lower()
        if key in {
            "article:published_time",
            "date",
            "datepublished",
            "dc.date",
            "dc.date.issued",
            "pubdate",
            "publishdate",
        }:
            self.published_at = " ".join(attr_map.get("content", "").split())


def enrich_link_metadata(
    db_or_conn: Any,
    *,
    source_type: str = "all",
    limit: int = DEFAULT_LIMIT,
    apply: bool = False,
    timeout: float = 10.0,
    http_client: HttpClient | None = None,
) -> LinkMetadataEnrichmentReport:
    """Fetch missing metadata for curated knowledge and curated source URLs."""
    if source_type not in SOURCE_TYPES:
        raise ValueError(f"source_type must be one of: {', '.join(SOURCE_TYPES)}")
    if limit <= 0:
        raise ValueError("limit must be positive")
    if timeout <= 0:
        raise ValueError("timeout must be positive")

    conn = _connection(db_or_conn)
    fetch = http_client or _fetch_html
    rows = _candidate_rows(conn, source_type, limit)
    results: list[LinkMetadataEnrichmentResult] = []

    for row in rows:
        try:
            html_text = fetch(row["url"], timeout)
            fetched = extract_enriched_link_metadata(html_text, row["url"])
            result = _result_for_row(row, fetched, apply=apply)
            if apply and result.updated_fields:
                _apply_result(conn, row, result)
                result = LinkMetadataEnrichmentResult(
                    source_table=result.source_table,
                    row_id=result.row_id,
                    source_type=result.source_type,
                    source_id=result.source_id,
                    url=result.url,
                    status=result.status,
                    applied=True,
                    metadata=result.metadata,
                    updated_fields=result.updated_fields,
                    error=result.error,
                )
        except Exception as exc:
            result = LinkMetadataEnrichmentResult(
                source_table=row["source_table"],
                row_id=row["row_id"],
                source_type=row["source_type"],
                source_id=row["source_id"],
                url=row["url"],
                status="failed",
                error=f"{exc.__class__.__name__}: {exc}",
            )
        results.append(result)

    if apply:
        conn.commit()
    return LinkMetadataEnrichmentReport(
        source_type=source_type,
        limit=limit,
        apply=apply,
        results=results,
    )


def extract_enriched_link_metadata(html_text: str, page_url: str) -> EnrichedLinkMetadata:
    """Extract and normalize metadata from one fetched HTML document."""
    link_metadata = parse_link_metadata(html_text, page_url)
    published_parser = _PublishedMetadataParser()
    published_parser.feed(html_text or "")
    canonical_url = normalize_canonical_url(link_metadata.canonical_url or page_url)
    return EnrichedLinkMetadata(
        canonical_url=canonical_url,
        title=link_metadata.title,
        site_name=link_metadata.site_name,
        published_at=published_parser.published_at,
        description=link_metadata.description,
        image=normalize_canonical_url(link_metadata.image) if link_metadata.image else "",
    )


def normalize_canonical_url(url: str) -> str:
    """Normalize obvious tracking variants of a URL to the same canonical form."""
    if not url:
        return ""
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    netloc = parts.netloc.lower()
    path = parts.path or ""
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query_items = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        normalized_key = key.casefold()
        if normalized_key in TRACKING_QUERY_PARAMS:
            continue
        if any(normalized_key.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
            continue
        query_items.append((key, value))
    query = urlencode(sorted(query_items), doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def format_link_metadata_enrichment_json(report: LinkMetadataEnrichmentReport) -> str:
    """Render a link metadata enrichment report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_link_metadata_enrichment_text(report: LinkMetadataEnrichmentReport) -> str:
    """Render a concise operator-facing enrichment report."""
    payload = report.to_dict()
    summary = payload["summary"]
    mode = "apply" if report.apply else "dry-run"
    lines = [
        "Link metadata enrichment report",
        f"Mode: {mode}",
        f"Source type: {report.source_type}",
        (
            "Totals: "
            f"scanned={summary['scanned']} updated={summary['updated']} "
            f"unchanged={summary['unchanged']} failed={summary['failed']} "
            f"applied={summary['applied']}"
        ),
    ]
    if not report.results:
        lines.append("No eligible URLs found.")
        return "\n".join(lines)

    for result in report.results:
        source = f"{result.source_table}:{result.source_type}:{result.source_id}"
        if result.status == "failed":
            lines.append(f"- failed {source} {result.url}: {result.error}")
        elif result.updated_fields:
            fields = ",".join(result.updated_fields)
            lines.append(f"- {result.status} {source} fields={fields} url={result.url}")
        else:
            lines.append(f"- unchanged {source} url={result.url}")
    return "\n".join(lines)


def _fetch_html(url: str, timeout: float) -> str:
    headers = {"User-Agent": "PresenceBot/1.0 (+https://github.com/)"}
    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except URLError as exc:
        raise LinkMetadataError(f"Failed to fetch link metadata for {url}: {exc.reason}") from exc


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3 connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    return {
        row["name"]: {info[1] for info in conn.execute(f"PRAGMA table_info({row['name']})")}
        for row in rows
    }


def _candidate_rows(
    conn: sqlite3.Connection,
    source_type: str,
    limit: int,
) -> list[dict[str, Any]]:
    schema = _schema(conn)
    rows: list[dict[str, Any]] = []
    if source_type in {"all", "knowledge", "curated_article", "curated_newsletter"}:
        rows.extend(_knowledge_candidates(conn, schema, limit, source_type))
    remaining = limit - len(rows)
    if remaining > 0 and source_type in {"all", "curated_sources", "blog", "newsletter"}:
        rows.extend(_curated_source_candidates(conn, schema, remaining, source_type))
    return rows[:limit]


def _knowledge_candidates(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    limit: int,
    source_type: str,
) -> list[dict[str, Any]]:
    columns = schema.get("knowledge", set())
    if not {"id", "source_type", "source_id", "source_url", "metadata"}.issubset(columns):
        return []
    selected_source_types = (
        ("curated_article", "curated_newsletter")
        if source_type in {"all", "knowledge"}
        else (source_type,)
    )
    placeholders = ",".join("?" for _ in selected_source_types)
    rows = conn.execute(
        """SELECT id, source_type, source_id, source_url, published_at, metadata
           FROM knowledge
           WHERE source_type IN ({placeholders})
             AND COALESCE(TRIM(source_url), TRIM(source_id), '') != ''
           ORDER BY created_at ASC, id ASC
           LIMIT ?""".format(placeholders=placeholders),
        (*selected_source_types, limit * 3),
    ).fetchall()
    candidates = []
    for row in rows:
        metadata = _parse_metadata(row["metadata"])
        link_metadata = metadata.get("link_metadata")
        if not isinstance(link_metadata, dict):
            link_metadata = {}
        if (
            _has_text(link_metadata.get("title"))
            and _has_text(link_metadata.get("site_name"))
            and _has_text(row["published_at"] or link_metadata.get("published_at"))
        ):
            continue
        candidates.append(
            {
                "source_table": "knowledge",
                "row_id": int(row["id"]),
                "source_type": row["source_type"],
                "source_id": row["source_id"],
                "url": row["source_url"] or row["source_id"],
                "published_at": row["published_at"],
                "metadata": metadata,
            }
        )
        if len(candidates) >= limit:
            break
    return candidates


def _curated_source_candidates(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    limit: int,
    source_type: str,
) -> list[dict[str, Any]]:
    columns = schema.get("curated_sources", set())
    required = {"id", "source_type", "identifier", "feed_url"}
    metadata_columns = {"canonical_url", "link_title", "site_name", "published_at"}
    if not required.issubset(columns) or not metadata_columns.issubset(columns):
        return []
    selected_source_types = (
        ("blog", "newsletter")
        if source_type in {"all", "curated_sources"}
        else (source_type,)
    )
    placeholders = ",".join("?" for _ in selected_source_types)
    rows = conn.execute(
        """SELECT id, source_type, identifier, feed_url, canonical_url,
                  link_title, site_name, published_at
           FROM curated_sources
           WHERE source_type IN ({placeholders})
             AND status = 'active'
             AND COALESCE(TRIM(feed_url), '') != ''
           ORDER BY created_at ASC, id ASC
           LIMIT ?""".format(placeholders=placeholders),
        (*selected_source_types, limit * 3),
    ).fetchall()
    candidates = []
    for row in rows:
        if (
            _has_text(row["link_title"])
            and _has_text(row["site_name"])
            and _has_text(row["published_at"])
        ):
            continue
        candidates.append(
            {
                "source_table": "curated_sources",
                "row_id": int(row["id"]),
                "source_type": row["source_type"],
                "source_id": row["identifier"],
                "url": row["feed_url"],
                "canonical_url": row["canonical_url"],
                "link_title": row["link_title"],
                "site_name": row["site_name"],
                "published_at": row["published_at"],
            }
        )
        if len(candidates) >= limit:
            break
    return candidates


def _result_for_row(
    row: dict[str, Any],
    fetched: EnrichedLinkMetadata,
    *,
    apply: bool,
) -> LinkMetadataEnrichmentResult:
    if row["source_table"] == "knowledge":
        metadata, updated_fields = _merge_knowledge_metadata(row, fetched)
    else:
        metadata, updated_fields = _merge_curated_source_metadata(row, fetched)
    return LinkMetadataEnrichmentResult(
        source_table=row["source_table"],
        row_id=row["row_id"],
        source_type=row["source_type"],
        source_id=row["source_id"],
        url=row["url"],
        status="updated" if updated_fields else "unchanged",
        applied=apply and bool(updated_fields),
        metadata=metadata,
        updated_fields=updated_fields,
    )


def _merge_knowledge_metadata(
    row: dict[str, Any],
    fetched: EnrichedLinkMetadata,
) -> tuple[dict[str, str], list[str]]:
    metadata = dict(row.get("metadata") or {})
    link_metadata = metadata.get("link_metadata")
    if not isinstance(link_metadata, dict):
        link_metadata = {}
    else:
        link_metadata = dict(link_metadata)

    updates: dict[str, str] = {}
    updated_fields: list[str] = []
    for key, value in fetched.to_dict().items():
        target_key = "link_metadata." + key
        if key == "published_at" and _has_text(row.get("published_at")):
            continue
        if not _has_text(link_metadata.get(key)) and value:
            link_metadata[key] = value
            updates[key] = value
            updated_fields.append(target_key)
    if updates:
        metadata["link_metadata"] = link_metadata
    return updates, updated_fields


def _merge_curated_source_metadata(
    row: dict[str, Any],
    fetched: EnrichedLinkMetadata,
) -> tuple[dict[str, str], list[str]]:
    mapping = {
        "canonical_url": fetched.canonical_url,
        "link_title": fetched.title,
        "site_name": fetched.site_name,
        "published_at": fetched.published_at,
    }
    updates = {
        key: value
        for key, value in mapping.items()
        if value and not _has_text(row.get(key))
    }
    return updates, list(updates)


def _apply_result(
    conn: sqlite3.Connection,
    row: dict[str, Any],
    result: LinkMetadataEnrichmentResult,
) -> None:
    if result.source_table == "knowledge":
        original_metadata = dict(row.get("metadata") or {})
        link_metadata = original_metadata.get("link_metadata")
        if not isinstance(link_metadata, dict):
            link_metadata = {}
        else:
            link_metadata = dict(link_metadata)
        for key, value in result.metadata.items():
            link_metadata[key] = value
        original_metadata["link_metadata"] = link_metadata
        assignments = ["metadata = ?"]
        params: list[Any] = [json.dumps(original_metadata, sort_keys=True)]
        if result.metadata.get("published_at") and not _has_text(row.get("published_at")):
            assignments.append("published_at = ?")
            params.append(result.metadata["published_at"])
        params.append(result.row_id)
        conn.execute(
            f"UPDATE knowledge SET {', '.join(assignments)} WHERE id = ?",
            params,
        )
        return

    if not result.metadata:
        return
    assignments = [f"{key} = ?" for key in result.metadata]
    params = [*result.metadata.values(), result.row_id]
    conn.execute(
        f"UPDATE curated_sources SET {', '.join(assignments)} WHERE id = ?",
        params,
    )


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _has_text(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())
