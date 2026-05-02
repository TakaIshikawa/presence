"""Export newsletter outbound link inventory for review."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
import json
import re
import sqlite3
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse, urlunparse

from output.newsletter_link_health import normalize_url


DEFAULT_RECENT_COUNT = 10
CONTEXT_RADIUS = 80
METADATA_TEXT_KEYS = (
    "body",
    "html",
    "text",
    "content",
    "markdown",
    "preview",
)
WEAK_ANCHOR_TEXT = frozenset(
    {
        "",
        "click",
        "click here",
        "continue",
        "go",
        "here",
        "learn more",
        "link",
        "more",
        "read",
        "read more",
        "see more",
        "source",
        "this",
        "this link",
        "visit",
        "website",
    }
)

_BARE_URL_RE = re.compile(r"(?:[a-z][a-z0-9+.-]*://|mailto:)[^\s<>'\"]+", re.IGNORECASE)
_HTML_HREF_RE = re.compile(r"\bhref\s*=\s*(['\"])(.*?)\1", re.IGNORECASE | re.DOTALL)
_MARKDOWN_LINK_RE = re.compile(r"(!?)\[([^\]]*)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class NewsletterLinkOccurrence:
    """One place where a URL appears in newsletter content."""

    source: str
    raw_url: str
    url: str
    anchor_text: str
    context: str
    line: int
    column: int
    weak_anchor: bool

    def to_dict(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "raw_url": self.raw_url,
            "url": self.url,
            "anchor_text": self.anchor_text,
            "context": self.context,
            "line": self.line,
            "column": self.column,
            "weak_anchor": self.weak_anchor,
        }


@dataclass(frozen=True)
class NewsletterInventoryLink:
    """One unique outbound link in a newsletter inventory."""

    url: str
    domain: str
    count: int
    repeated: bool
    weak_anchor: bool
    flags: tuple[str, ...]
    anchors: tuple[str, ...]
    contexts: tuple[str, ...]
    occurrences: tuple[NewsletterLinkOccurrence, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "url": self.url,
            "domain": self.domain,
            "count": self.count,
            "repeated": self.repeated,
            "weak_anchor": self.weak_anchor,
            "flags": list(self.flags),
            "anchors": list(self.anchors),
            "contexts": list(self.contexts),
            "occurrences": [occurrence.to_dict() for occurrence in self.occurrences],
        }


@dataclass(frozen=True)
class NewsletterInventoryDomain:
    """Links grouped by normalized destination domain."""

    domain: str
    count: int
    unique_url_count: int
    repeated_url_count: int
    weak_anchor_count: int
    links: tuple[NewsletterInventoryLink, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "count": self.count,
            "unique_url_count": self.unique_url_count,
            "repeated_url_count": self.repeated_url_count,
            "weak_anchor_count": self.weak_anchor_count,
            "links": [link.to_dict() for link in self.links],
        }


@dataclass(frozen=True)
class NewsletterInventoryItem:
    """Link inventory for one newsletter draft or assembled issue."""

    newsletter_id: str
    item_type: str
    subject: str
    title: str
    status: str
    item_timestamp: str
    total_links: int
    unique_links: int
    unique_domains: int
    repeated_url_count: int
    weak_anchor_count: int
    domains: tuple[NewsletterInventoryDomain, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "newsletter_id": self.newsletter_id,
            "item_type": self.item_type,
            "subject": self.subject,
            "title": self.title,
            "status": self.status,
            "item_timestamp": self.item_timestamp,
            "total_links": self.total_links,
            "unique_links": self.unique_links,
            "unique_domains": self.unique_domains,
            "repeated_url_count": self.repeated_url_count,
            "weak_anchor_count": self.weak_anchor_count,
            "domains": [domain.to_dict() for domain in self.domains],
        }


@dataclass(frozen=True)
class NewsletterLinkInventoryReport:
    """Aggregated link inventory export."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    newsletters: tuple[NewsletterInventoryItem, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "newsletter_link_inventory",
            "generated_at": self.generated_at,
            "filters": dict(self.filters),
            "totals": dict(self.totals),
            "newsletters": [newsletter.to_dict() for newsletter in self.newsletters],
            "missing_tables": list(self.missing_tables),
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
        }


def build_newsletter_link_inventory_for_text(
    text: str,
    *,
    newsletter_id: str = "text",
    source: str = "text",
    subject: str = "",
    title: str = "",
    status: str = "",
    item_timestamp: str = "",
) -> NewsletterInventoryItem:
    """Build a link inventory for a rendered Markdown or HTML newsletter."""
    links = _links_from_occurrences(extract_newsletter_link_occurrences(text, source=source))
    domains = _domains_from_links(links)
    return NewsletterInventoryItem(
        newsletter_id=newsletter_id,
        item_type=source,
        subject=subject,
        title=title,
        status=status,
        item_timestamp=item_timestamp,
        total_links=sum(link.count for link in links),
        unique_links=len(links),
        unique_domains=len(domains),
        repeated_url_count=sum(1 for link in links if link.repeated),
        weak_anchor_count=sum(1 for link in links if link.weak_anchor),
        domains=domains,
    )


def build_newsletter_link_inventory_report(
    db_or_conn: Any,
    *,
    newsletter_ids: Sequence[str | int] = (),
    recent_count: int | None = None,
    now: datetime | None = None,
) -> NewsletterLinkInventoryReport:
    """Load newsletter sends/content and export outbound link inventories."""
    if recent_count is not None and recent_count <= 0:
        raise ValueError("recent_count must be positive")
    if not newsletter_ids and recent_count is None:
        recent_count = DEFAULT_RECENT_COUNT

    conn = _connection(db_or_conn)
    conn.row_factory = sqlite3.Row
    schema = _schema(conn)
    generated_at = _as_utc(now or datetime.now(timezone.utc)).isoformat()
    normalized_ids = tuple(str(value).strip() for value in newsletter_ids if str(value).strip())
    filters = {
        "newsletter_ids": list(normalized_ids),
        "recent_count": recent_count,
    }
    missing_tables, missing_columns = _schema_gaps(schema)
    if "newsletter_sends" in missing_tables and "generated_content" in missing_tables:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)
    if missing_columns:
        return _empty_report(generated_at, filters, missing_tables, missing_columns)

    rows = _load_newsletter_rows(conn, schema, newsletter_ids=normalized_ids, recent_count=recent_count)
    newsletters = tuple(_inventory_from_row(row) for row in rows)
    totals = {
        "newsletter_count": len(newsletters),
        "total_links": sum(item.total_links for item in newsletters),
        "unique_links": sum(item.unique_links for item in newsletters),
        "unique_domains": len(
            {
                domain.domain
                for item in newsletters
                for domain in item.domains
            }
        ),
        "repeated_url_count": sum(item.repeated_url_count for item in newsletters),
        "weak_anchor_count": sum(item.weak_anchor_count for item in newsletters),
    }
    return NewsletterLinkInventoryReport(
        generated_at=generated_at,
        filters=filters,
        totals=totals,
        newsletters=newsletters,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def extract_newsletter_link_occurrences(
    text: str,
    *,
    source: str = "text",
) -> tuple[NewsletterLinkOccurrence, ...]:
    """Extract Markdown, HTML anchor, and bare URL occurrences with context."""
    value = str(text or "")
    html_occurrences = _HtmlInventoryParser.links_from(value, source=source)
    text_occurrences = _extract_text_occurrences(value, source=source)
    all_occurrences = [*html_occurrences, *text_occurrences]
    all_occurrences.sort(key=lambda occurrence: (occurrence.line, occurrence.column, occurrence.url))
    return tuple(all_occurrences)


def format_newsletter_link_inventory_json(report: NewsletterLinkInventoryReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_link_inventory_text(report: NewsletterLinkInventoryReport) -> str:
    """Render a compact human-readable inventory."""
    lines = [
        "Newsletter Link Inventory",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"newsletter_ids={','.join(report.filters['newsletter_ids']) or 'all'} "
            f"recent_count={report.filters['recent_count']}"
        ),
        (
            "Totals: "
            f"newsletters={report.totals['newsletter_count']} "
            f"links={report.totals['total_links']} "
            f"unique_links={report.totals['unique_links']} "
            f"domains={report.totals['unique_domains']} "
            f"repeated={report.totals['repeated_url_count']} "
            f"weak_anchor={report.totals['weak_anchor_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        details = "; ".join(
            f"{table}({', '.join(columns)})"
            for table, columns in sorted(report.missing_columns.items())
        )
        lines.append("Missing columns: " + details)
    if not report.newsletters:
        lines.append("No newsletters found.")
        return "\n".join(lines)

    for newsletter in report.newsletters:
        label = newsletter.subject or newsletter.title or newsletter.newsletter_id
        lines.append("")
        lines.append(
            f"- {newsletter.item_type}:{newsletter.newsletter_id} "
            f"{_shorten(label, 72)} "
            f"links={newsletter.total_links} domains={newsletter.unique_domains}"
        )
        if not newsletter.total_links:
            lines.append("  No links found.")
            continue
        for domain in newsletter.domains:
            lines.append(f"  {domain.domain}: {domain.count}")
            for link in domain.links:
                flags = f" flags={','.join(link.flags)}" if link.flags else ""
                anchor = link.anchors[0] if link.anchors else ""
                lines.append(
                    f"    {link.count}x {link.url}{flags} anchor={_shorten(anchor, 48)}"
                )
    return "\n".join(lines)


def _links_from_occurrences(
    occurrences: Iterable[NewsletterLinkOccurrence],
) -> tuple[NewsletterInventoryLink, ...]:
    grouped: dict[str, list[NewsletterLinkOccurrence]] = {}
    for occurrence in occurrences:
        domain = _normalized_domain(occurrence.url)
        if not domain:
            continue
        grouped.setdefault(occurrence.url, []).append(occurrence)

    links: list[NewsletterInventoryLink] = []
    for url, items in grouped.items():
        count = len(items)
        repeated = count > 1
        weak_anchor = any(item.weak_anchor for item in items)
        flags = []
        if repeated:
            flags.append("repeated_url")
        if weak_anchor:
            flags.append("weak_anchor_text")
        links.append(
            NewsletterInventoryLink(
                url=url,
                domain=_normalized_domain(url),
                count=count,
                repeated=repeated,
                weak_anchor=weak_anchor,
                flags=tuple(flags),
                anchors=_unique(item.anchor_text for item in items if item.anchor_text),
                contexts=_unique(item.context for item in items if item.context),
                occurrences=tuple(items),
            )
        )
    return tuple(sorted(links, key=lambda link: (link.domain, link.url)))


def _domains_from_links(
    links: Iterable[NewsletterInventoryLink],
) -> tuple[NewsletterInventoryDomain, ...]:
    grouped: dict[str, list[NewsletterInventoryLink]] = {}
    for link in links:
        grouped.setdefault(link.domain, []).append(link)
    domains = [
        NewsletterInventoryDomain(
            domain=domain,
            count=sum(link.count for link in domain_links),
            unique_url_count=len(domain_links),
            repeated_url_count=sum(1 for link in domain_links if link.repeated),
            weak_anchor_count=sum(1 for link in domain_links if link.weak_anchor),
            links=tuple(sorted(domain_links, key=lambda link: (-link.count, link.url))),
        )
        for domain, domain_links in grouped.items()
    ]
    return tuple(sorted(domains, key=lambda domain: (-domain.count, domain.domain)))


def _extract_text_occurrences(text: str, *, source: str) -> list[NewsletterLinkOccurrence]:
    covered_spans: list[tuple[int, int]] = []
    occurrences: list[NewsletterLinkOccurrence] = []
    for match in _HTML_HREF_RE.finditer(text):
        covered_spans.append(match.span(2))
    for match in _MARKDOWN_LINK_RE.finditer(text):
        covered_spans.append(match.span(3))
        if match.group(1):
            continue
        occurrence = _occurrence(
            text,
            source=source,
            raw_url=match.group(3),
            anchor_text=match.group(2),
            index=match.start(3),
            context_start=match.start(),
            context_end=match.end(),
        )
        if occurrence is not None:
            occurrences.append(occurrence)

    for match in _BARE_URL_RE.finditer(text):
        if any(start <= match.start() < end for start, end in covered_spans):
            continue
        occurrence = _occurrence(
            text,
            source=source,
            raw_url=match.group(0),
            anchor_text="",
            index=match.start(),
            context_start=match.start(),
            context_end=match.end(),
        )
        if occurrence is not None:
            occurrences.append(occurrence)
    return occurrences


def _occurrence(
    text: str,
    *,
    source: str,
    raw_url: str,
    anchor_text: str,
    index: int,
    context_start: int,
    context_end: int,
) -> NewsletterLinkOccurrence | None:
    url = _normalized_url(normalize_url(raw_url))
    if not url:
        return None
    line, column = _line_column(text, index)
    anchor = _clean(anchor_text)
    return NewsletterLinkOccurrence(
        source=source,
        raw_url=raw_url,
        url=url,
        anchor_text=anchor,
        context=_context(text, context_start, context_end),
        line=line,
        column=column,
        weak_anchor=_is_weak_anchor(anchor, url),
    )


def _inventory_from_row(row: Mapping[str, Any]) -> NewsletterInventoryItem:
    item = build_newsletter_link_inventory_for_text(
        "\n".join(row.get("texts", ())),
        newsletter_id=str(row["newsletter_id"]),
        source=str(row["item_type"]),
        subject=_clean(row.get("subject")),
        title=_clean(row.get("title")),
        status=_clean(row.get("status")),
        item_timestamp=_clean(row.get("item_timestamp")),
    )
    return item


def _load_newsletter_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    newsletter_ids: Sequence[str],
    recent_count: int | None,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if newsletter_ids:
        rows.extend(_load_send_rows_by_ids(conn, schema, newsletter_ids))
        rows.extend(_load_generated_rows_by_ids(conn, schema, newsletter_ids))
    else:
        rows.extend(_load_recent_send_rows(conn, schema, recent_count or DEFAULT_RECENT_COUNT))
        rows.extend(_load_recent_generated_rows(conn, schema, recent_count or DEFAULT_RECENT_COUNT))
    rows.sort(key=lambda row: (row.get("item_timestamp") or "", row["item_type"], row["newsletter_id"]), reverse=True)
    if not newsletter_ids and recent_count is not None:
        rows = rows[:recent_count]
    return rows


def _load_send_rows_by_ids(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    newsletter_ids: Sequence[str],
) -> list[dict[str, Any]]:
    if "newsletter_sends" not in schema:
        return []
    selected = _selected_send_columns(schema["newsletter_sends"])
    clauses = ["CAST(id AS TEXT) IN (" + _placeholders(newsletter_ids) + ")"]
    params: list[Any] = list(newsletter_ids)
    if "issue_id" in schema["newsletter_sends"]:
        clauses.append("issue_id IN (" + _placeholders(newsletter_ids) + ")")
        params.extend(newsletter_ids)
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM newsletter_sends
            WHERE {' OR '.join(clauses)}""",
        params,
    )
    return [_send_row(_row_dict(cursor, row), schema["newsletter_sends"]) for row in cursor.fetchall()]


def _load_recent_send_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    limit: int,
) -> list[dict[str, Any]]:
    if "newsletter_sends" not in schema:
        return []
    columns = schema["newsletter_sends"]
    selected = _selected_send_columns(columns)
    timestamp_expr = _send_timestamp_expr(columns)
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM newsletter_sends
            ORDER BY datetime({timestamp_expr}) DESC, id DESC
            LIMIT ?""",
        (limit,),
    )
    return [_send_row(_row_dict(cursor, row), columns) for row in cursor.fetchall()]


def _load_generated_rows_by_ids(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    newsletter_ids: Sequence[str],
) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    columns = schema["generated_content"]
    selected = _selected_generated_columns(columns)
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM generated_content
            WHERE CAST(id AS TEXT) IN ({_placeholders(newsletter_ids)})
              AND {_newsletter_content_predicate(columns)}""",
        tuple(newsletter_ids),
    )
    return [_generated_row(_row_dict(cursor, row), columns) for row in cursor.fetchall()]


def _load_recent_generated_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    limit: int,
) -> list[dict[str, Any]]:
    if "generated_content" not in schema:
        return []
    columns = schema["generated_content"]
    selected = _selected_generated_columns(columns)
    timestamp_expr = "created_at" if "created_at" in columns else "id"
    cursor = conn.execute(
        f"""SELECT {', '.join(selected)}
            FROM generated_content
            WHERE {_newsletter_content_predicate(columns)}
            ORDER BY datetime({timestamp_expr}) DESC, id DESC
            LIMIT ?""",
        (limit,),
    )
    return [_generated_row(_row_dict(cursor, row), columns) for row in cursor.fetchall()]


def _send_row(row: Mapping[str, Any], columns: set[str]) -> dict[str, Any]:
    metadata = _parse_json(row.get("metadata")) if "metadata" in columns else None
    texts = _row_texts(row, columns)
    texts.extend(_metadata_texts(metadata, prefix="newsletter_sends.metadata"))
    return {
        "newsletter_id": row.get("issue_id") or row.get("id"),
        "item_type": "newsletter_send",
        "subject": row.get("subject") or "",
        "title": "",
        "status": row.get("status") or "",
        "item_timestamp": row.get("sent_at") or row.get("created_at") or "",
        "texts": texts,
    }


def _generated_row(row: Mapping[str, Any], columns: set[str]) -> dict[str, Any]:
    metadata = _parse_json(row.get("metadata")) if "metadata" in columns else None
    texts = _row_texts(row, columns)
    texts.extend(_metadata_texts(metadata, prefix="generated_content.metadata"))
    return {
        "newsletter_id": row.get("id"),
        "item_type": "generated_content",
        "subject": "",
        "title": row.get("title") or "",
        "status": row.get("curation_quality") or "",
        "item_timestamp": row.get("created_at") or "",
        "texts": texts,
    }


def _selected_send_columns(columns: set[str]) -> list[str]:
    return [
        column
        for column in (
            "id",
            "issue_id",
            "subject",
            "status",
            "body",
            "content",
            "html",
            "text",
            "markdown",
            "metadata",
            "created_at",
            "sent_at",
        )
        if column in columns
    ]


def _selected_generated_columns(columns: set[str]) -> list[str]:
    return [
        column
        for column in (
            "id",
            "content_type",
            "title",
            "content",
            "body",
            "html",
            "text",
            "markdown",
            "metadata",
            "curation_quality",
            "created_at",
        )
        if column in columns
    ]


def _row_texts(row: Mapping[str, Any], columns: set[str]) -> list[str]:
    texts = []
    for key in ("subject", "title", "body", "content", "html", "text", "markdown"):
        if key in columns and row.get(key):
            texts.append(str(row[key]))
    return texts


def _metadata_texts(value: Any, *, prefix: str) -> list[str]:
    del prefix
    texts: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_lower = str(key).casefold()
            if isinstance(item, str) and any(marker in key_lower for marker in METADATA_TEXT_KEYS):
                texts.append(item)
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_texts(item, prefix="metadata"))
    elif isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, str):
                texts.append(item)
            elif isinstance(item, (Mapping, list, tuple)):
                texts.extend(_metadata_texts(item, prefix="metadata"))
    return texts


def _newsletter_content_predicate(columns: set[str]) -> str:
    if "content_type" not in columns:
        return "1 = 1"
    return "LOWER(COALESCE(content_type, '')) LIKE '%newsletter%'"


def _send_timestamp_expr(columns: set[str]) -> str:
    candidates = [column for column in ("sent_at", "created_at") if column in columns]
    if not candidates:
        return "id"
    return "COALESCE(" + ", ".join(candidates) + ")"


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    missing_tables = tuple(
        table for table in ("newsletter_sends", "generated_content") if table not in schema
    )
    missing_columns: dict[str, tuple[str, ...]] = {}
    if "newsletter_sends" in schema and "id" not in schema["newsletter_sends"]:
        missing_columns["newsletter_sends"] = ("id",)
    if "generated_content" in schema and "id" not in schema["generated_content"]:
        missing_columns["generated_content"] = ("id",)
    return missing_tables, missing_columns


def _empty_report(
    generated_at: str,
    filters: dict[str, Any],
    missing_tables: tuple[str, ...],
    missing_columns: dict[str, tuple[str, ...]],
) -> NewsletterLinkInventoryReport:
    return NewsletterLinkInventoryReport(
        generated_at=generated_at,
        filters=filters,
        totals={
            "newsletter_count": 0,
            "total_links": 0,
            "unique_links": 0,
            "unique_domains": 0,
            "repeated_url_count": 0,
            "weak_anchor_count": 0,
        },
        newsletters=(),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def _normalized_domain(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme.casefold() not in {"http", "https"}:
        return ""
    host = (parsed.hostname or "").casefold()
    return host[4:] if host.startswith("www.") else host


def _normalized_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme.casefold() not in {"http", "https"}:
        return url
    scheme = parsed.scheme.casefold()
    host = (parsed.hostname or "").casefold()
    if not host:
        return url
    if host.startswith("www."):
        host = host[4:]
    port = f":{parsed.port}" if parsed.port else ""
    return urlunparse((scheme, f"{host}{port}", parsed.path, parsed.params, parsed.query, parsed.fragment))


def _is_weak_anchor(anchor_text: str, url: str) -> bool:
    normalized = _clean(anchor_text).casefold()
    if not normalized:
        return True
    if normalized in WEAK_ANCHOR_TEXT:
        return True
    parsed = urlparse(url)
    url_bits = {url.casefold(), (parsed.hostname or "").casefold(), parsed.netloc.casefold()}
    return normalized in {bit for bit in url_bits if bit}


def _context(text: str, start: int, end: int) -> str:
    left = max(0, start - CONTEXT_RADIUS)
    right = min(len(text), end + CONTEXT_RADIUS)
    return _clean(text[left:right])


def _line_column(text: str, index: int) -> tuple[int, int]:
    line = text.count("\n", 0, index) + 1
    line_start = text.rfind("\n", 0, index)
    column = index + 1 if line_start == -1 else index - line_start
    return line, column


def _clean(value: Any) -> str:
    return _WHITESPACE_RE.sub(" ", str(value or "").strip())


def _shorten(value: Any, limit: int) -> str:
    text = _clean(value)
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _unique(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        clean = _clean(value)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        unique.append(clean)
    return tuple(unique)


def _parse_json(raw_value: Any) -> Any:
    if raw_value is None or raw_value == "":
        return None
    if isinstance(raw_value, (dict, list)):
        return raw_value
    try:
        return json.loads(str(raw_value))
    except (TypeError, json.JSONDecodeError):
        return None


def _placeholders(values: Sequence[Any]) -> str:
    return ", ".join("?" for _value in values)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("expected sqlite3.Connection or Database-like object")
    return conn


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        columns = conn.execute(f"PRAGMA table_info({table})").fetchall()
        schema[str(table)] = {
            column["name"] if isinstance(column, sqlite3.Row) else column[1]
            for column in columns
        }
    return schema


def _row_dict(cursor: sqlite3.Cursor, row: Any) -> dict[str, Any]:
    if isinstance(row, sqlite3.Row):
        return dict(row)
    names = [description[0] for description in cursor.description or ()]
    return dict(zip(names, row))


class _HtmlInventoryParser(HTMLParser):
    def __init__(self, *, source: str) -> None:
        super().__init__(convert_charrefs=True)
        self.source = source
        self.links: list[NewsletterLinkOccurrence] = []
        self.visible_parts: list[str] = []
        self.active_link: dict[str, Any] | None = None

    @classmethod
    def links_from(cls, html: str, *, source: str) -> list[NewsletterLinkOccurrence]:
        parser = cls(source=source)
        parser.feed(html)
        parser.close()
        return parser.links

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.casefold() in {"br", "p", "div", "li"}:
            self.visible_parts.append(" ")
        if tag.casefold() != "a":
            return
        href = dict(attrs).get("href")
        if not href:
            return
        line, column = self.getpos()
        self.active_link = {
            "href": href,
            "line": line,
            "column": column + 1,
            "before": _clean(" ".join(self.visible_parts))[-CONTEXT_RADIUS:],
            "anchor_parts": [],
        }

    def handle_endtag(self, tag: str) -> None:
        if tag.casefold() not in {"a", "p", "div", "li"}:
            return
        if tag.casefold() != "a" or self.active_link is None:
            self.visible_parts.append(" ")
            return
        link = self.active_link
        self.active_link = None
        anchor = _clean(" ".join(link["anchor_parts"]))
        url = _normalized_url(normalize_url(str(link["href"])))
        if not url:
            return
        after = _clean(" ".join(self.visible_parts))[-CONTEXT_RADIUS:]
        context = _clean(f"{link['before']} {anchor} {after}")
        self.links.append(
            NewsletterLinkOccurrence(
                source=self.source,
                raw_url=str(link["href"]),
                url=url,
                anchor_text=anchor,
                context=context,
                line=int(link["line"]),
                column=int(link["column"]),
                weak_anchor=_is_weak_anchor(anchor, url),
            )
        )

    def handle_data(self, data: str) -> None:
        clean = _clean(data)
        if not clean:
            return
        self.visible_parts.append(clean)
        if self.active_link is not None:
            self.active_link["anchor_parts"].append(clean)
