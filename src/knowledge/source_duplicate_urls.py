"""Report likely duplicate knowledge source URLs after light canonicalization."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from .link_metadata_enricher import TRACKING_QUERY_PARAMS, TRACKING_QUERY_PREFIXES


DEFAULT_LIMIT = 50
TRACKING_QUERY_KEYS = TRACKING_QUERY_PARAMS | {
    "campaign_id",
    "cmpid",
    "fb_action_ids",
    "fb_action_types",
    "fb_ref",
    "ga_source",
    "igshid",
    "mkt_tok",
    "ref",
    "ref_src",
    "s",
    "spm",
}
STRIPPED_HOST_PREFIXES = ("www.", "m.", "mobile.", "amp.")


@dataclass(frozen=True)
class DuplicateUrlSource:
    """One knowledge record that shares a canonicalized source URL."""

    knowledge_id: int | None
    source_id: str | None
    source_type: str | None
    title: str | None
    source_url: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DuplicateUrlCluster:
    """A group of records with the same normalized source URL."""

    normalized_url: str
    source_count: int
    sources: tuple[DuplicateUrlSource, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "normalized_url": self.normalized_url,
            "source_count": self.source_count,
            "sources": [source.to_dict() for source in self.sources],
        }


@dataclass(frozen=True)
class KnowledgeDuplicateUrlReport:
    """Read-only report of duplicate knowledge source URL clusters."""

    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, int]
    clusters: tuple[DuplicateUrlCluster, ...]
    missing_tables: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "knowledge_source_duplicate_urls",
            "clusters": [cluster.to_dict() for cluster in self.clusters],
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "missing_tables": list(self.missing_tables),
            "totals": dict(self.totals),
        }


def normalize_source_url(url: Any) -> str | None:
    """Normalize URL variants that should identify the same stored source."""
    text = str(url or "").strip()
    if not text:
        return None
    parts = urlsplit(text if "://" in text else f"https://{text}")
    scheme = parts.scheme.lower() or "https"
    if scheme not in {"http", "https"}:
        return None
    host = (parts.hostname or "").strip().lower().rstrip(".")
    if not host:
        return None
    for prefix in STRIPPED_HOST_PREFIXES:
        if host.startswith(prefix):
            host = host[len(prefix) :]
            break
    port = parts.port
    netloc = host
    if port and not ((scheme == "http" and port == 80) or (scheme == "https" and port == 443)):
        netloc = f"{host}:{port}"
    path = parts.path or ""
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")
    query_items = []
    for key, value in parse_qsl(parts.query, keep_blank_values=True):
        normalized_key = key.casefold()
        if normalized_key in TRACKING_QUERY_KEYS:
            continue
        if any(normalized_key.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
            continue
        query_items.append((key, value))
    query = urlencode(sorted(query_items), doseq=True)
    return urlunsplit((scheme, netloc, path, query, ""))


def build_knowledge_duplicate_url_report(
    db_or_rows: Any,
    *,
    source_type: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> KnowledgeDuplicateUrlReport:
    """Group knowledge records that canonicalize to the same source URL."""
    if limit is not None and limit < 0:
        raise ValueError("limit must be nonnegative")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc)).isoformat()
    missing_tables: tuple[str, ...] = ()
    if _looks_like_rows(db_or_rows):
        raw_rows = [_mapping(row) for row in db_or_rows]
    else:
        conn = _connection(db_or_rows)
        if not _table_exists(conn, "knowledge"):
            missing_tables = ("knowledge",)
            raw_rows = []
        else:
            raw_rows = _load_knowledge_rows(conn, source_type=source_type)

    sources = [
        source
        for source in (_source_from_row(row) for row in raw_rows)
        if source is not None and (source_type is None or source.source_type == source_type)
    ]
    clusters = _duplicate_clusters(sources)
    limited = clusters[:limit] if limit is not None else clusters
    return KnowledgeDuplicateUrlReport(
        generated_at=generated_at,
        filters={"source_type": source_type, "limit": limit},
        totals={
            "cluster_count": len(clusters),
            "duplicate_source_count": sum(cluster.source_count for cluster in clusters),
            "returned_cluster_count": len(limited),
            "rows_scanned": len(raw_rows),
            "url_source_count": len(sources),
        },
        clusters=tuple(limited),
        missing_tables=missing_tables,
    )


def build_knowledge_duplicate_url_report_from_fixture(
    path: Path,
    *,
    source_type: str | None = None,
    limit: int | None = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> KnowledgeDuplicateUrlReport:
    """Build the duplicate URL report from JSON array or JSONL fixture records."""
    return build_knowledge_duplicate_url_report(
        _load_fixture_payload(path.read_text()),
        source_type=source_type,
        limit=limit,
        now=now,
    )


def format_knowledge_duplicate_url_json(report: KnowledgeDuplicateUrlReport) -> str:
    """Serialize a duplicate URL report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_knowledge_duplicate_url_text(report: KnowledgeDuplicateUrlReport) -> str:
    """Format duplicate URL clusters for terminal review."""
    filters = report.filters
    totals = report.totals
    lines = [
        "Knowledge source duplicate URLs",
        f"Generated: {report.generated_at}",
        (
            "Filters: "
            f"source_type={filters['source_type'] or 'all'} "
            f"limit={filters['limit'] if filters['limit'] is not None else '-'}"
        ),
        (
            "Totals: "
            f"rows_scanned={totals['rows_scanned']} "
            f"url_sources={totals['url_source_count']} "
            f"clusters={totals['cluster_count']} "
            f"returned={totals['returned_cluster_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if not report.clusters:
        lines.append("No duplicate source URL clusters found.")
        return "\n".join(lines)

    for index, cluster in enumerate(report.clusters, start=1):
        lines.append("")
        lines.append(f"{index}. {cluster.normalized_url} sources={cluster.source_count}")
        for source in cluster.sources:
            label = source.title or source.source_id or "-"
            lines.append(
                f"   - knowledge_id={source.knowledge_id or '-'} "
                f"source_type={source.source_type or '-'} "
                f"source_id={source.source_id or '-'} "
                f"title={label}"
            )
    return "\n".join(lines)


def _duplicate_clusters(
    sources: Iterable[DuplicateUrlSource],
) -> list[DuplicateUrlCluster]:
    grouped: dict[str, list[DuplicateUrlSource]] = {}
    for source in sources:
        normalized_url = normalize_source_url(source.source_url)
        if normalized_url:
            grouped.setdefault(normalized_url, []).append(source)
    clusters = [
        DuplicateUrlCluster(
            normalized_url=normalized_url,
            source_count=len(group),
            sources=tuple(sorted(group, key=_source_sort_key)),
        )
        for normalized_url, group in grouped.items()
        if len(group) > 1
    ]
    return sorted(
        clusters,
        key=lambda cluster: (
            -cluster.source_count,
            cluster.normalized_url,
            tuple(_source_sort_key(source) for source in cluster.sources),
        ),
    )


def _source_from_row(row: Mapping[str, Any]) -> DuplicateUrlSource | None:
    source_url = _first_text(
        row.get("source_url"),
        row.get("url"),
        row.get("canonical_url"),
        _metadata_value(row, "canonical_url"),
    )
    if not source_url or not normalize_source_url(source_url):
        return None
    return DuplicateUrlSource(
        knowledge_id=_int_or_none(row.get("knowledge_id") or row.get("id")),
        source_id=_clean(row.get("source_id")),
        source_type=_clean(row.get("source_type")),
        title=_first_text(
            row.get("title"),
            row.get("source_title"),
            row.get("link_title"),
            _metadata_value(row, "title"),
            _metadata_value(row, "link_title"),
        ),
        source_url=source_url,
    )


def _load_knowledge_rows(
    conn: sqlite3.Connection,
    *,
    source_type: str | None,
) -> list[dict[str, Any]]:
    columns = {row[1] for row in conn.execute("PRAGMA table_info(knowledge)")}
    if not {"id", "source_type"}.issubset(columns):
        return []
    select = {
        "source_id": _column_expr(columns, "source_id"),
        "source_url": _column_expr(columns, "source_url"),
        "metadata": _column_expr(columns, "metadata"),
    }
    where = []
    params: list[Any] = []
    if source_type:
        where.append("source_type = ?")
        params.append(source_type)
    query = f"""SELECT id,
                       source_type,
                       {select['source_id']} AS source_id,
                       {select['source_url']} AS source_url,
                       {select['metadata']} AS metadata
                FROM knowledge"""
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " ORDER BY id ASC"
    return [dict(row) for row in conn.execute(query, params).fetchall()]


def _load_fixture_payload(text: str) -> list[dict[str, Any]]:
    stripped = text.strip()
    if not stripped:
        return []
    if stripped.startswith("["):
        payload = json.loads(stripped)
        if not isinstance(payload, list):
            raise ValueError("fixture JSON must be an array of records")
        return [_require_mapping(item) for item in payload]
    return [_require_mapping(json.loads(line)) for line in stripped.splitlines() if line.strip()]


def _metadata_value(row: Mapping[str, Any], key: str) -> Any:
    metadata = _parse_metadata(row.get("metadata"))
    link_metadata = metadata.get("link_metadata")
    if isinstance(link_metadata, Mapping) and _clean(link_metadata.get(key)):
        return link_metadata.get(key)
    return metadata.get(key)


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


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    conn = getattr(db_or_conn, "conn", db_or_conn)
    if not isinstance(conn, sqlite3.Connection):
        raise TypeError("db_or_conn must be a sqlite3 connection or Database-like object")
    conn.row_factory = sqlite3.Row
    return conn


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        is not None
    )


def _column_expr(columns: set[str], column: str) -> str:
    return column if column in columns else "NULL"


def _looks_like_rows(value: Any) -> bool:
    return isinstance(value, Iterable) and not isinstance(
        value,
        (str, bytes, bytearray, sqlite3.Connection),
    ) and not hasattr(value, "conn")


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, sqlite3.Row):
        return dict(value)
    if isinstance(value, Mapping):
        return dict(value)
    raise TypeError("rows must be mappings")


def _require_mapping(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("fixture records must be JSON objects")
    return value


def _first_text(*values: Any) -> str | None:
    for value in values:
        cleaned = _clean(value)
        if cleaned:
            return cleaned
    return None


def _clean(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _source_sort_key(source: DuplicateUrlSource) -> tuple[Any, ...]:
    return (
        source.knowledge_id if source.knowledge_id is not None else 10**12,
        source.source_type or "",
        source.source_id or "",
        source.source_url,
    )


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
