"""Find newsletter click events that cannot be attributed to known targets."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Iterable
from urllib.parse import urlparse

from storage.db import _normalize_newsletter_attribution_url


DEFAULT_DAYS = 90
DEFAULT_MIN_CLICK_COUNT = 2
DEFAULT_SAMPLE_LIMIT = 5


@dataclass(frozen=True)
class NewsletterOrphanClickGroup:
    """A normalized orphan URL click group."""

    normalized_url: str
    domain: str
    click_count: int
    event_count: int
    first_click_at: str | None
    last_click_at: str | None
    sample_event_ids: tuple[int, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["sample_event_ids"] = list(self.sample_event_ids)
        return payload


@dataclass(frozen=True)
class NewsletterOrphanClicksReport:
    """Read-only orphan newsletter click attribution report."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    groups: tuple[NewsletterOrphanClickGroup, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "groups": [group.to_dict() for group in self.groups],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "totals": dict(sorted(self.totals.items())),
        }


def build_newsletter_orphan_clicks_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_click_count: int = DEFAULT_MIN_CLICK_COUNT,
    sample_limit: int = DEFAULT_SAMPLE_LIMIT,
    known_url_aliases: dict[str, str] | None = None,
    known_newsletter_urls: Iterable[str] = (),
    now: datetime | None = None,
) -> NewsletterOrphanClicksReport:
    """Group unattributed newsletter clicks by normalized URL/domain."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_click_count <= 0:
        raise ValueError("min_click_count must be positive")
    if sample_limit <= 0:
        raise ValueError("sample_limit must be positive")
    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = (generated_at - timedelta(days=days)).isoformat()
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    missing_tables, missing_columns = _schema_gaps(schema)
    rows = _load_rows(conn, cutoff) if not missing_tables and not missing_columns else []
    known = _known_urls(conn, schema, known_newsletter_urls)
    alias_map = {
        _normalize(url): _normalize(target)
        for url, target in (known_url_aliases or {}).items()
    }
    groups = _groups(
        rows,
        known_urls=known,
        alias_map=alias_map,
        min_click_count=min_click_count,
        sample_limit=sample_limit,
    )
    return NewsletterOrphanClicksReport(
        artifact_type="newsletter_orphan_clicks",
        generated_at=generated_at.isoformat(),
        filters={
            "days": days,
            "min_click_count": min_click_count,
            "sample_limit": sample_limit,
        },
        totals={
            "group_count": len(groups),
            "orphan_click_count": sum(group.click_count for group in groups),
            "row_count": len(rows),
        },
        groups=tuple(groups),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_newsletter_orphan_clicks_json(report: NewsletterOrphanClicksReport) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_orphan_clicks_text(report: NewsletterOrphanClicksReport) -> str:
    """Render a concise text report."""
    lines = [
        "Newsletter Orphan Clicks",
        f"Generated: {report.generated_at}",
        (
            f"Groups: {report.totals['group_count']} "
            f"orphan_clicks={report.totals['orphan_click_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report.missing_columns.items())
            )
        )
    if not report.groups:
        lines.append("No orphan newsletter clicks found.")
        return "\n".join(lines)
    for group in report.groups:
        lines.append(
            f"- {group.normalized_url} domain={group.domain} clicks={group.click_count} "
            f"events={group.event_count} first={group.first_click_at or '-'} "
            f"last={group.last_click_at or '-'} samples={','.join(map(str, group.sample_event_ids))}"
        )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, cutoff: str) -> list[dict[str, Any]]:
    return [
        dict(row)
        for row in conn.execute(
            """SELECT id, link_url, raw_url, clicks, content_id, source_kind, fetched_at, created_at
               FROM newsletter_link_clicks
               WHERE datetime(fetched_at) >= datetime(?)
               ORDER BY fetched_at ASC, id ASC""",
            (cutoff,),
        ).fetchall()
    ]


def _groups(
    rows: list[dict[str, Any]],
    *,
    known_urls: set[str],
    alias_map: dict[str, str],
    min_click_count: int,
    sample_limit: int,
) -> list[NewsletterOrphanClickGroup]:
    buckets: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"clicks": 0, "ids": [], "first": None, "last": None}
    )
    for row in rows:
        normalized = _normalize(row.get("link_url") or row.get("raw_url"))
        if not normalized:
            continue
        alias_target = alias_map.get(normalized)
        if row.get("content_id") is not None or _clean(row.get("source_kind")):
            continue
        if normalized in known_urls or alias_target in known_urls:
            continue
        bucket = buckets[normalized]
        clicks = int(row.get("clicks") or 0)
        timestamp = str(row.get("fetched_at") or row.get("created_at") or "")
        bucket["clicks"] += clicks
        bucket["ids"].append(int(row["id"]))
        bucket["first"] = min(filter(None, [bucket["first"], timestamp]), default=timestamp)
        bucket["last"] = max(filter(None, [bucket["last"], timestamp]), default=timestamp)

    groups = [
        NewsletterOrphanClickGroup(
            normalized_url=url,
            domain=urlparse(url).netloc.lower(),
            click_count=item["clicks"],
            event_count=len(item["ids"]),
            first_click_at=item["first"],
            last_click_at=item["last"],
            sample_event_ids=tuple(item["ids"][:sample_limit]),
        )
        for url, item in buckets.items()
        if item["clicks"] >= min_click_count
    ]
    groups.sort(key=lambda group: (-group.click_count, group.domain, group.normalized_url))
    return groups


def _known_urls(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    explicit_urls: Iterable[str],
) -> set[str]:
    urls = {_normalize(url) for url in explicit_urls if _normalize(url)}
    if "generated_content" in schema and "published_url" in schema["generated_content"]:
        for row in conn.execute(
            "SELECT published_url FROM generated_content WHERE published_url IS NOT NULL"
        ).fetchall():
            normalized = _normalize(row[0])
            if normalized:
                urls.add(normalized)
    if "newsletter_sends" in schema and "metadata" in schema["newsletter_sends"]:
        for row in conn.execute("SELECT metadata FROM newsletter_sends").fetchall():
            urls.update(_metadata_urls(row[0]))
    return urls


def _metadata_urls(value: Any) -> set[str]:
    if not value:
        return set()
    try:
        decoded = json.loads(str(value)) if not isinstance(value, dict) else value
    except json.JSONDecodeError:
        return set()
    urls: set[str] = set()
    stack = [decoded]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, str) and item.startswith(("http://", "https://")):
            urls.add(_normalize(item))
    return urls


def _schema_gaps(schema: dict[str, set[str]]) -> tuple[tuple[str, ...], dict[str, tuple[str, ...]]]:
    required = {
        "newsletter_link_clicks": {
            "clicks",
            "content_id",
            "created_at",
            "fetched_at",
            "id",
            "link_url",
            "raw_url",
            "source_kind",
        }
    }
    missing_tables = tuple(sorted(table for table in required if table not in schema))
    missing_columns = {
        table: tuple(sorted(columns - schema.get(table, set())))
        for table, columns in required.items()
        if table in schema and columns - schema.get(table, set())
    }
    return missing_tables, missing_columns


def _normalize(url: Any) -> str:
    return _normalize_newsletter_attribution_url(str(url or ""))


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
